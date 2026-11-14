#include "test.h"
#include "dsm_internal.h"
#include "ring.h"

#include <stdint.h>
#include <string.h>

#define RING_TEST_PAGE_SIZE 4096u
#define RING_TEST_VNODES_PER_MB 16u

/* Contexto mínimo con membresía inicializada y configuración de página */
static void ring_test_ctx_init(dsm_ctx_t *ctx, uint32_t self_slot)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->cfg.page_size    = RING_TEST_PAGE_SIZE;
    ctx->cfg.vnodes_per_mb = RING_TEST_VNODES_PER_MB;
    ctx->cfg.max_runtime_nodes = MAX_NODES;
    membership_init(ctx);
    ctx->memb.self_slot = self_slot;
    ctx->memb.epoch     = 1;
}

/* Registra un nodo con UUID derivado de su ranura y la memoria indicada */
static void ring_test_add_node(dsm_ctx_t *ctx, uint32_t slot, uint64_t mem_bytes)
{
    node_info_t *ni = &ctx->memb.nodes[slot];
    memset(ni, 0, sizeof(*ni));
    for (uint32_t i = 0; i < 16; i++) {
        ni->uuid[i] = (uint8_t)(slot * 37u + i * 11u + 1u);
    }
    ni->slot      = slot;
    ni->state     = DSM_NODE_ALIVE;
    ni->mem_pages = (uint32_t)(mem_bytes / RING_TEST_PAGE_SIZE);
    ctx->memb.present[slot / 8] |= (uint8_t)(1u << (slot % 8));
}

static void ring_test_ctx_destroy(dsm_ctx_t *ctx)
{
    ring_free(&ctx->ring);
    membership_destroy(ctx);
}

/* (a) Determinismo: dos contextos con la misma membresía asignan el mismo
 * home a cada página, sin importar el orden en que se registraron los nodos. */
static int test_ring_deterministic(void)
{
    dsm_ctx_t a, b;
    ring_test_ctx_init(&a, 0);
    ring_test_ctx_init(&b, 1);

    ring_test_add_node(&a, 0, 256ULL << 20);
    ring_test_add_node(&a, 1, 256ULL << 20);
    ring_test_add_node(&a, 2, 512ULL << 20);

    /* Mismo conjunto de nodos insertado en orden inverso */
    ring_test_add_node(&b, 2, 512ULL << 20);
    ring_test_add_node(&b, 1, 256ULL << 20);
    ring_test_add_node(&b, 0, 256ULL << 20);

    int mismatches = 0;
    for (uint32_t p = 0; p < 20000; p++) {
        if (ring_home_of(&a, p) != ring_home_of(&b, p)) mismatches++;
    }
    CHECK(mismatches == 0);

    /* La misma consulta repetida sobre el mismo contexto es estable */
    for (uint32_t p = 0; p < 1000; p++) {
        uint32_t h1 = ring_home_of(&a, p);
        uint32_t h2 = ring_home_of(&a, p);
        CHECK(h1 == h2);
    }

    /* Todos los homes pertenecen a la membresía */
    for (uint32_t p = 0; p < 20000; p++) {
        uint32_t h = ring_home_of(&a, p);
        CHECK(h == 0 || h == 1 || h == 2);
    }

    ring_test_ctx_destroy(&a);
    ring_test_ctx_destroy(&b);
    return 0;
}

/* (b) Reparto proporcional: un nodo con el doble de memoria recibe
 * aproximadamente el doble de páginas. */
static int test_ring_proportional_to_memory(void)
{
    dsm_ctx_t ctx;
    ring_test_ctx_init(&ctx, 0);

    ring_test_add_node(&ctx, 0, 256ULL << 20);
    ring_test_add_node(&ctx, 1, 512ULL << 20);

    const uint32_t pages = 200000;
    uint32_t count[2] = { 0, 0 };
    for (uint32_t p = 0; p < pages; p++) {
        count[ring_home_of(&ctx, p)]++;
    }

    /* El nodo con 512 MiB debe quedar cerca del 66.7 % de las páginas */
    double fraction = (double)count[1] / (double)pages;
    printf("   fraccion slot1=%.4f slot0=%.4f\n", fraction,
           (double)count[0] / (double)pages);
    CHECK(fraction > 0.62 && fraction < 0.71);
    CHECK(count[1] > count[0]);

    /* Con tres nodos de 256, 512 y 1024 MiB la proporción sigue la memoria */
    ring_test_add_node(&ctx, 2, 1024ULL << 20);
    ctx.memb.epoch++;
    uint32_t count3[3] = { 0, 0, 0 };
    for (uint32_t p = 0; p < pages; p++) {
        count3[ring_home_of(&ctx, p)]++;
    }
    double f0 = (double)count3[0] / (double)pages;
    double f1 = (double)count3[1] / (double)pages;
    double f2 = (double)count3[2] / (double)pages;
    printf("   fraccion 256=%.4f 512=%.4f 1024=%.4f\n", f0, f1, f2);
    CHECK(f0 > 0.10 && f0 < 0.21);
    CHECK(f1 > 0.24 && f1 < 0.35);
    CHECK(f2 > 0.52 && f2 < 0.63);

    ring_test_ctx_destroy(&ctx);
    return 0;
}

/* (c) Consistencia: al entrar o salir un nodo solo cambian de home las
 * páginas que caen en los tramos del nodo que se une o se va. */
static int test_ring_minimal_remap(void)
{
    dsm_ctx_t ctx;
    ring_test_ctx_init(&ctx, 0);

    const uint32_t pages = 200000;
    static uint32_t before[200000];
    static uint32_t after[200000];

    ring_test_add_node(&ctx, 0, 256ULL << 20);
    ring_test_add_node(&ctx, 1, 256ULL << 20);
    ring_test_add_node(&ctx, 2, 256ULL << 20);
    for (uint32_t p = 0; p < pages; p++) before[p] = ring_home_of(&ctx, p);

    /* Sale el nodo 2: las páginas que no eran suyas conservan el home */
    ctx.memb.nodes[2].state = DSM_NODE_DEAD;
    ctx.memb.present[0] &= (uint8_t)~(1u << 2);
    ctx.memb.epoch++;
    uint32_t moved = 0;
    uint32_t kept = 0;
    for (uint32_t p = 0; p < pages; p++) {
        uint32_t h = ring_home_of(&ctx, p);
        if (before[p] == 2) {
            CHECK(h != 2);
            moved++;
        } else if (h == before[p]) {
            kept++;
        }
    }
    printf("   salida: reasignadas=%u conservadas=%u\n", moved, kept);
    /* Las páginas ajenas al nodo saliente no cambian de home */
    CHECK(kept == pages - moved);
    /* La fracción reasignada ronda la memoria del nodo saliente (1/3) */
    double frac_out = (double)moved / (double)pages;
    CHECK(frac_out > 0.28 && frac_out < 0.39);

    /* Entra un cuarto nodo: solo sus páginas cambian de dueño */
    for (uint32_t p = 0; p < pages; p++) after[p] = ring_home_of(&ctx, p);
    ring_test_add_node(&ctx, 3, 256ULL << 20);
    ctx.memb.epoch++;
    uint32_t to_new = 0;
    uint32_t stable = 0;
    for (uint32_t p = 0; p < pages; p++) {
        uint32_t h = ring_home_of(&ctx, p);
        if (h == 3) {
            to_new++;
        } else if (h == after[p]) {
            stable++;
        }
    }
    printf("   entrada: al_nuevo=%u estables=%u\n", to_new, stable);
    CHECK(stable == pages - to_new);
    /* El nodo entrante toma su tramo del anillo, cercano a 1/4 del total con
     * cuatro nodos iguales; los demás conservan el home. */
    double frac_in = (double)to_new / (double)pages;
    CHECK(frac_in > 0.28 && frac_in < 0.38);

    ring_test_ctx_destroy(&ctx);
    return 0;
}

/* El anillo se reconstruye cuando cambia la época de membresía */
static int test_ring_rebuild_on_epoch(void)
{
    dsm_ctx_t ctx;
    ring_test_ctx_init(&ctx, 0);
    ring_test_add_node(&ctx, 0, 256ULL << 20);

    CHECK(ring_home_of(&ctx, 0) == 0);
    CHECK(ctx.ring.built == true);
    CHECK(ctx.ring.n == 256u * RING_TEST_VNODES_PER_MB);
    CHECK(ctx.ring.epoch == ctx.memb.epoch);

    ring_test_add_node(&ctx, 1, 256ULL << 20);
    ctx.memb.epoch++;
    uint32_t seen_one = 0;
    for (uint32_t p = 0; p < 5000; p++) {
        if (ring_home_of(&ctx, p) == 1) seen_one++;
    }
    CHECK(seen_one > 0);
    CHECK(ctx.ring.n == 2u * 256u * RING_TEST_VNODES_PER_MB);
    CHECK(ctx.ring.epoch == ctx.memb.epoch);

    ring_test_ctx_destroy(&ctx);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Ring ===\n");
    RUN_TEST(test_ring_deterministic);
    RUN_TEST(test_ring_proportional_to_memory);
    RUN_TEST(test_ring_minimal_remap);
    RUN_TEST(test_ring_rebuild_on_epoch);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
