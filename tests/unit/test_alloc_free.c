#include "test.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "mem.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#define PAGE_BYTES 4096u
#define VIEW_PAGES 16u

/* Contexto de un solo nodo con la vista mapeada y el directorio MSI listo,
 * publicado como contexto global para que dsm_alloc() y dsm_free() lo usen. */
static dsm_ctx_t *ctx_create(void)
{
    dsm_ctx_t *ctx = dsm_ctx_create_test();
    if (!ctx) return NULL;

    ctx->cfg.page_size         = PAGE_BYTES;
    ctx->cfg.view_size         = (uint64_t)VIEW_PAGES * PAGE_BYTES;
    ctx->cfg.node_mem          = (uint64_t)8 * PAGE_BYTES;
    ctx->cfg.home_slack_permille = 1250;
    ctx->cfg.base_addr         = 0x200000000000ULL;
    ctx->cfg.base_addr_tsan    = 0x4000000000ULL;
    ctx->cfg.page_lock_stripes = 8;
    ctx->cfg.cluster_id        = 0x534F3031u;

    if (dsm_mem_init(&ctx->mem, &ctx->cfg) != DSM_OK) {
        free(ctx);
        return NULL;
    }
    if (msi_init(ctx) != DSM_OK) {
        dsm_mem_destroy(&ctx->mem);
        free(ctx);
        return NULL;
    }

    pthread_rwlock_wrlock(&ctx->memb.rw);
    ctx->memb.self_slot = 0;
    pthread_rwlock_unlock(&ctx->memb.rw);
    membership_add_self(ctx);

    dsm_ctx_install_test(ctx);
    return ctx;
}

static void ctx_destroy(void)
{
    dsm_ctx_t *ctx = dsm_ctx_get();

    dsm_ctx_reset();
    if (!ctx) return;

    msi_destroy(ctx);
    dsm_mem_destroy(&ctx->mem);
    ring_free(&ctx->ring);
    sync_destroy(ctx);
    membership_destroy(ctx);
    net_destroy(ctx);
    pthread_cond_destroy(&ctx->alloc.sync_cv);
    pthread_mutex_destroy(&ctx->alloc.mu);
    free(ctx);
}

/* Permisos de la region de /proc/self/maps que contiene addr. */
static int prot_string_of(const void *addr, char *out, size_t out_len)
{
    FILE *f = fopen("/proc/self/maps", "r");
    if (!f) return 0;

    char line[512];
    uintptr_t target = (uintptr_t)addr;
    int found = 0;

    while (fgets(line, sizeof(line), f)) {
        unsigned long lo = 0;
        unsigned long hi = 0;
        char perms[8] = "";
        if (sscanf(line, "%lx-%lx %7s", &lo, &hi, perms) != 3) continue;
        if (target < lo || target >= hi) continue;

        snprintf(out, out_len, "%s", perms);
        found = 1;
        break;
    }

    fclose(f);
    return found;
}

/* Primera pagina de la vista ocupada por ptr. */
static uint32_t page_of(dsm_ctx_t *ctx, void *ptr)
{
    return (uint32_t)(((uintptr_t)ptr - (uintptr_t)ctx->mem.view) / ctx->cfg.page_size);
}

/* Un bloque liberado vuelve a entregarse en la misma direccion y el espacio
 * disponible no retrocede. */
static int test_reutiliza_hueco_misma_direccion(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *first = NULL;
    REQUIRE(dsm_alloc(3 * PAGE_BYTES, &first) == DSM_OK);

    void *marker = NULL;
    REQUIRE(dsm_alloc(PAGE_BYTES, &marker) == DSM_OK);
    uint32_t next_before = ctx->alloc.next_page;

    REQUIRE(dsm_free(first, 3 * PAGE_BYTES) == DSM_OK);

    void *again = NULL;
    REQUIRE(dsm_alloc(3 * PAGE_BYTES, &again) == DSM_OK);
    CHECK(again == first);
    CHECK(ctx->alloc.next_page == next_before);

    /* Liberar el sobrante deja la vista en su estado inicial. */
    CHECK(dsm_free(again, 3 * PAGE_BYTES) == DSM_OK);
    CHECK(dsm_free(marker, PAGE_BYTES) == DSM_OK);
    CHECK(ctx->alloc.nholes == 1);
    CHECK(ctx->alloc.holes[0].first_page == 0);
    CHECK(ctx->alloc.holes[0].npages == ctx->alloc.next_page);

    ctx_destroy();
    return 0;
}

/* Tras la liberacion la pagina queda en estado I e inaccesible. */
static int test_pagina_liberada_en_invalido(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *block = NULL;
    REQUIRE(dsm_alloc(2 * PAGE_BYTES, &block) == DSM_OK);

    uint32_t page = page_of(ctx, block);
    for (uint32_t p = page; p < page + 2; p++) {
        REQUIRE(dsm_mem_set_prot(&ctx->mem, p, PROT_READ | PROT_WRITE) == DSM_OK);
        ctx->mem.local[p].state = DSM_PAGE_M;
    }

    REQUIRE(dsm_free(block, 2 * PAGE_BYTES) == DSM_OK);

    for (uint32_t p = page; p < page + 2; p++) {
        char perms[8] = "";
        page_local_t *loc = dsm_mem_local(&ctx->mem, p);
        REQUIRE(loc != NULL);
        CHECK(loc->state == DSM_PAGE_I);

        void *addr = ctx->mem.view + ((uint64_t)p * PAGE_BYTES);
        REQUIRE(prot_string_of(addr, perms, sizeof(perms)));
        /* La region conserva el mapeo compartido, pero sin lectura, escritura
         * ni ejecucion. */
        CHECK(perms[0] == '-');
        CHECK(perms[1] == '-');
        CHECK(perms[2] == '-');
    }

    ctx_destroy();
    return 0;
}

/* Cien ciclos de reserva y liberacion del mismo tamaño no consumen paginas
 * nuevas de la vista. */
static int test_cien_ciclos_no_agotan(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *first = NULL;
    REQUIRE(dsm_alloc(PAGE_BYTES, &first) == DSM_OK);
    uint32_t next_after_first = ctx->alloc.next_page;
    CHECK(next_after_first == 1);

    REQUIRE(dsm_free(first, PAGE_BYTES) == DSM_OK);

    for (int i = 0; i < 100; i++) {
        void *p = NULL;
        REQUIRE(dsm_alloc(PAGE_BYTES, &p) == DSM_OK);
        CHECK(p == first);
        REQUIRE(dsm_free(p, PAGE_BYTES) == DSM_OK);
    }

    CHECK(ctx->alloc.next_page == next_after_first);
    CHECK(ctx->alloc.nholes == 1);
    CHECK(ctx->alloc.holes[0].first_page == 0);
    CHECK(ctx->alloc.holes[0].npages == 1);

    uint64_t global = 0;
    CHECK(dsm_global_size(&global) == DSM_OK);
    CHECK(global > 0);

    ctx_destroy();
    return 0;
}

/* Punteros fuera de la vista, desalineados o que no abren un bloque registrado
 * se rechazan. */
static int test_free_puntero_invalido(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *block = NULL;
    REQUIRE(dsm_alloc(2 * PAGE_BYTES, &block) == DSM_OK);

    CHECK(dsm_free(NULL, PAGE_BYTES) == DSM_ERR_INVALID);
    CHECK(dsm_free((uint8_t *)block + 1, PAGE_BYTES) == DSM_ERR_INVALID);

    /* Pagina contigua al bloque pero no registrada como reserva. */
    CHECK(dsm_free((uint8_t *)block + 2 * PAGE_BYTES, PAGE_BYTES) == DSM_ERR_INVALID);

    /* Fuera de la vista por ambos extremos. */
    CHECK(dsm_free(ctx->mem.view - PAGE_BYTES, PAGE_BYTES) == DSM_ERR_INVALID);
    CHECK(dsm_free(ctx->mem.view + ctx->mem.view_size, PAGE_BYTES) == DSM_ERR_INVALID);

    /* Segunda liberacion del mismo bloque. */
    REQUIRE(dsm_free(block, 2 * PAGE_BYTES) == DSM_OK);
    CHECK(dsm_free(block, 2 * PAGE_BYTES) == DSM_ERR_INVALID);

    ctx_destroy();
    return 0;
}

/* Un tamaño que redondea a otro numero de paginas no corresponde al bloque. */
static int test_free_tamano_erroneo(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *block = NULL;
    REQUIRE(dsm_alloc(2 * PAGE_BYTES, &block) == DSM_OK);

    CHECK(dsm_free(block, 0) == DSM_ERR_INVALID);
    CHECK(dsm_free(block, PAGE_BYTES) == DSM_ERR_INVALID);
    CHECK(dsm_free(block, 3 * PAGE_BYTES) == DSM_ERR_INVALID);
    CHECK(dsm_free(block, 2 * PAGE_BYTES + 1) == DSM_ERR_INVALID);

    /* Ningun rechazo modifico la lista de huecos. */
    CHECK(ctx->alloc.nholes == 0);
    CHECK(ctx->alloc.depth == 1);
    CHECK(ctx->alloc.stack[0].npages == 2);

    REQUIRE(dsm_free(block, 2 * PAGE_BYTES) == DSM_OK);
    ctx_destroy();
    return 0;
}

/* El primer hueco que alcanza el tamaño pedido se usa, aunque exista uno menor
 * antes; el sobrante queda disponible. */
static int test_huecos_first_fit(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    void *a = NULL, *b = NULL, *c = NULL, *d = NULL;
    REQUIRE(dsm_alloc(PAGE_BYTES, &a) == DSM_OK);           /* paginas 0 */
    REQUIRE(dsm_alloc(PAGE_BYTES, &b) == DSM_OK);           /* pagina 1  */
    REQUIRE(dsm_alloc(2 * PAGE_BYTES, &c) == DSM_OK);       /* paginas 2-3 */
    REQUIRE(dsm_alloc(PAGE_BYTES, &d) == DSM_OK);           /* pagina 4 */

    CHECK(dsm_free(c, 2 * PAGE_BYTES) == DSM_OK);           /* hueco 2-3 */
    CHECK(dsm_free(a, PAGE_BYTES) == DSM_OK);               /* hueco 0 */

    REQUIRE(ctx->alloc.nholes == 2);
    CHECK(ctx->alloc.holes[0].first_page == 0);
    CHECK(ctx->alloc.holes[0].npages == 1);
    CHECK(ctx->alloc.holes[1].first_page == 2);
    CHECK(ctx->alloc.holes[1].npages == 2);

    /* El pedido de dos paginas salta el hueco de una y toma el de dos. */
    void *two = NULL;
    REQUIRE(dsm_alloc(2 * PAGE_BYTES, &two) == DSM_OK);
    CHECK(page_of(ctx, two) == 2);
    CHECK(ctx->alloc.nholes == 1);

    /* El pedido de una pagina usa el hueco que quedo al inicio. */
    void *one = NULL;
    REQUIRE(dsm_alloc(PAGE_BYTES, &one) == DSM_OK);
    CHECK(page_of(ctx, one) == 0);
    CHECK(ctx->alloc.nholes == 0);

    CHECK(dsm_free(one, PAGE_BYTES) == DSM_OK);
    CHECK(dsm_free(b, PAGE_BYTES) == DSM_OK);
    CHECK(dsm_free(two, 2 * PAGE_BYTES) == DSM_OK);
    CHECK(dsm_free(d, PAGE_BYTES) == DSM_OK);

    /* Las liberaciones contiguas se fusionan en un unico rango. */
    CHECK(ctx->alloc.nholes == 1);
    CHECK(ctx->alloc.holes[0].first_page == 0);
    CHECK(ctx->alloc.holes[0].npages == 5);
    CHECK(ctx->alloc.depth == 4);

    ctx_destroy();
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Reserva y liberacion de la vista (alloc/free) ===\n");
    RUN_TEST(test_reutiliza_hueco_misma_direccion);
    RUN_TEST(test_pagina_liberada_en_invalido);
    RUN_TEST(test_cien_ciclos_no_agotan);
    RUN_TEST(test_free_puntero_invalido);
    RUN_TEST(test_free_tamano_erroneo);
    RUN_TEST(test_huecos_first_fit);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
