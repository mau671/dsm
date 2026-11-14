#include "test.h"
#include "dsm_internal.h"
#include "mem.h"
#include "msi.h"
#include "ring.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_BYTES 4096u
#define VIEW_PAGES 16u
#define PATTERN_WORDS (PAGE_BYTES / sizeof(uint32_t))

/* Contexto de un solo nodo en el que todas las páginas tienen como home al
 * propio nodo, de modo que el writeback se resuelve sin red. */
static dsm_ctx_t *ctx_single_node_create(void)
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

    return ctx;
}

static void ctx_single_node_destroy(dsm_ctx_t *ctx)
{
    msi_destroy(ctx);
    dsm_mem_destroy(&ctx->mem);
    ring_free(&ctx->ring);
    sync_destroy(ctx);
    membership_destroy(ctx);
    net_destroy(ctx);
    free(ctx);
}

static void fill_pattern(uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < PATTERN_WORDS; i++) {
        page[i] = base + i;
    }
}

static int check_pattern(const uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < PATTERN_WORDS; i++) {
        if (page[i] != base + i) return 0;
    }
    return 1;
}

/* El writeback local copia la página M al marco de respaldo del home y deja
 * el marco reservado para esa página. */
static int test_writeback_persiste_en_respaldo_local(void)
{
    dsm_ctx_t *ctx = ctx_single_node_create();
    REQUIRE(ctx != NULL);

    uint32_t page = 3;
    fill_pattern((uint32_t *)dsm_mem_page_ptr(&ctx->mem, page), 0xA5A50000u);
    ctx->mem.local[page].state = DSM_PAGE_M;

    CHECK(dsm_mem_page_frame(&ctx->mem, page) == DSM_PAGE_ID_NONE);
    CHECK(msi_writeback_page(ctx, page) == DSM_OK);

    uint32_t frame = dsm_mem_page_frame(&ctx->mem, page);
    CHECK(frame != DSM_PAGE_ID_NONE);
    REQUIRE(frame < ctx->mem.backing_pages);

    uint8_t *stored = malloc(PAGE_BYTES);
    REQUIRE(stored != NULL);
    CHECK(dsm_mem_backing_load(&ctx->mem, page, stored) == DSM_OK);
    CHECK(check_pattern((const uint32_t *)stored, 0xA5A50000u));
    CHECK(memcmp(stored, dsm_mem_frame_ptr(&ctx->mem, frame), PAGE_BYTES) == 0);
    free(stored);

    /* El marco asignado se conserva entre escrituras sucesivas. */
    CHECK(msi_writeback_page(ctx, page) == DSM_OK);
    CHECK(dsm_mem_page_frame(&ctx->mem, page) == frame);

    ctx_single_node_destroy(ctx);
    return 0;
}

/* Tras una segunda escritura el respaldo refleja el contenido más reciente. */
static int test_writeback_actualiza_marco_existente(void)
{
    dsm_ctx_t *ctx = ctx_single_node_create();
    REQUIRE(ctx != NULL);

    uint32_t page = 1;
    fill_pattern((uint32_t *)dsm_mem_page_ptr(&ctx->mem, page), 0x11110000u);
    ctx->mem.local[page].state = DSM_PAGE_M;
    REQUIRE(msi_writeback_page(ctx, page) == DSM_OK);

    fill_pattern((uint32_t *)dsm_mem_page_ptr(&ctx->mem, page), 0x22220000u);
    REQUIRE(msi_writeback_page(ctx, page) == DSM_OK);

    uint8_t *stored = malloc(PAGE_BYTES);
    REQUIRE(stored != NULL);
    CHECK(dsm_mem_backing_load(&ctx->mem, page, stored) == DSM_OK);
    CHECK(check_pattern((const uint32_t *)stored, 0x22220000u));
    free(stored);

    ctx_single_node_destroy(ctx);
    return 0;
}

/* El handler del mensaje WRITEBACK escribe en el respaldo del home antes de
 * responder el acuse. */
static int test_on_writeback_persiste_payload(void)
{
    dsm_ctx_t *ctx = ctx_single_node_create();
    REQUIRE(ctx != NULL);

    uint32_t page = 5;
    uint8_t *payload = malloc(PAGE_BYTES);
    REQUIRE(payload != NULL);
    fill_pattern((uint32_t *)payload, 0x33330000u);

    dsm_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.magic         = DSM_MAGIC;
    msg.hdr.cluster_id    = ctx->cfg.cluster_id;
    msg.hdr.proto_version = DSM_PROTO_VERSION;
    msg.hdr.type          = MSG_WRITEBACK;
    msg.hdr.src_slot      = 0;
    msg.hdr.page_id       = page;
    msg.hdr.payload_len   = PAGE_BYTES;
    msg.hdr.flags         = DSM_FLAG_HAS_DATA;
    msg.payload           = payload;

    msi_on_writeback(ctx, &msg);

    uint8_t *stored = malloc(PAGE_BYTES);
    REQUIRE(stored != NULL);
    CHECK(dsm_mem_backing_load(&ctx->mem, page, stored) == DSM_OK);
    CHECK(check_pattern((const uint32_t *)stored, 0x33330000u));

    free(stored);
    free(payload);
    ctx_single_node_destroy(ctx);
    return 0;
}

/* Los marcos del respaldo se reparten sin repetirse y se recuperan al liberarlos. */
static int test_marcos_respaldo_se_reutilizan(void)
{
    dsm_ctx_t *ctx = ctx_single_node_create();
    REQUIRE(ctx != NULL);

    uint32_t first = dsm_mem_frame_alloc(&ctx->mem);
    uint32_t second = dsm_mem_frame_alloc(&ctx->mem);
    CHECK(first != DSM_PAGE_ID_NONE);
    CHECK(second != DSM_PAGE_ID_NONE);
    CHECK(first != second);

    dsm_mem_frame_free(&ctx->mem, first);
    CHECK(dsm_mem_frame_alloc(&ctx->mem) == first);

    dsm_mem_frame_free(&ctx->mem, ctx->mem.backing_pages);
    CHECK(dsm_mem_frame_ptr(&ctx->mem, ctx->mem.backing_pages) == NULL);

    ctx_single_node_destroy(ctx);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Escritura de respaldo (writeback) ===\n");
    RUN_TEST(test_writeback_persiste_en_respaldo_local);
    RUN_TEST(test_writeback_actualiza_marco_existente);
    RUN_TEST(test_on_writeback_persiste_payload);
    RUN_TEST(test_marcos_respaldo_se_reutilizan);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
