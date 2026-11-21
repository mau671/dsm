#include "test.h"
#include "dsm_internal.h"
#include "dsm.h"
#include "mem.h"
#include "msi.h"
#include "ring.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_BYTES 4096u
#define VIEW_PAGES 16u

typedef struct {
    dsm_ctx_t  *ctx;
    dsm_msg_t   msg;
} busy_thread_t;

static dsm_ctx_t *ctx_create(void)
{
    dsm_ctx_t *ctx = dsm_ctx_create_test();
    if (!ctx) return NULL;

    ctx->cfg.page_size = PAGE_BYTES;
    ctx->cfg.view_size = (uint64_t)VIEW_PAGES * PAGE_BYTES;
    ctx->cfg.node_mem = (uint64_t)8 * PAGE_BYTES;
    ctx->cfg.home_slack_permille = 1250;
    ctx->cfg.base_addr = 0x200000000000ULL;
    ctx->cfg.base_addr_tsan = 0x4000000000ULL;
    ctx->cfg.page_lock_stripes = 8;
    ctx->cfg.cluster_id = 0x534F3031u;
    ctx->cfg.request_timeout_ms = 100;
    ctx->cfg.request_retries = 1;

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
    if (ring_build(&ctx->ring, &ctx->memb) != DSM_OK) {
        msi_destroy(ctx);
        dsm_mem_destroy(&ctx->mem);
        membership_destroy(ctx);
        net_destroy(ctx);
        free(ctx);
        return NULL;
    }

    dsm_ctx_install_test(ctx);
    return ctx;
}

static void ctx_destroy(dsm_ctx_t *ctx)
{
    dsm_ctx_reset();
    msi_destroy(ctx);
    dsm_mem_destroy(&ctx->mem);
    ring_free(&ctx->ring);
    membership_destroy(ctx);
    net_destroy(ctx);
    free(ctx);
}

static dsm_msg_t page_request(uint32_t page)
{
    dsm_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.magic = DSM_MAGIC;
    msg.hdr.cluster_id = 0x534F3031u;
    msg.hdr.proto_version = DSM_PROTO_VERSION;
    msg.hdr.type = MSG_PAGE_REQUEST;
    msg.hdr.src_slot = 0;
    msg.hdr.dst_slot = 0;
    msg.hdr.page_id = page;
    msg.hdr.access = DSM_ACCESS_READ;
    msg.hdr.payload_len = 0;
    return msg;
}

static void *first_request(void *arg)
{
    busy_thread_t *t = (busy_thread_t *)arg;
    msi_home_handle_request(t->ctx, &t->msg);
    return NULL;
}

static int run_busy_case(bool fail)
{
    dsm_ctx_t *ctx = ctx_create();
    if (!ctx) return -1;

    uint32_t page = 0;
    ctx->msi.dir[page].busy = 0;
    msi_test_busy_hook_config(page, fail);

    busy_thread_t first;
    first.ctx = ctx;
    first.msg = page_request(page);

    pthread_t thread;
    if (pthread_create(&thread, NULL, first_request, &first) != 0) {
        ctx_destroy(ctx);
        return -1;
    }

    msi_test_busy_hook_wait_entered();
    CHECK(ctx->msi.dir[page].busy == 1);

    dsm_msg_t second = page_request(page);
    msi_home_handle_request(ctx, &second);
    CHECK(msi_test_busy_hook_second_seen());

    msi_test_busy_hook_release();
    if (pthread_join(thread, NULL) != 0) {
        CHECK(ctx->msi.dir[page].busy == 0);
        ctx_destroy(ctx);
        return -1;
    }
    CHECK(ctx->msi.dir[page].busy == 0);

    ctx_destroy(ctx);
    return 0;
}

static int test_busy_success(void)
{
    return run_busy_case(false);
}

static int test_busy_failure(void)
{
    return run_busy_case(true);
}

static void add_owner_node(dsm_ctx_t *ctx, uint32_t slot)
{
    pthread_rwlock_wrlock(&ctx->memb.rw);
    ctx->memb.present[slot / 8] |= (uint8_t)(1u << (slot % 8));
    ctx->memb.nodes[slot].slot = slot;
    ctx->memb.nodes[slot].state = DSM_NODE_ALIVE;
    ctx->memb.nodes[slot].mem_pages = 0u;
    ctx->memb.nodes[slot].is_client = 1u;
    ctx->memb.nodes[slot].incarnation = 1;
    memset(ctx->memb.nodes[slot].uuid, (int)(slot + 1), 16);
    pthread_rwlock_unlock(&ctx->memb.rw);
}

static dsm_msg_t ghost_request(uint32_t requester, uint32_t page, bool write)
{
    dsm_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.magic = DSM_MAGIC;
    msg.hdr.cluster_id = 0x534F3031u;
    msg.hdr.proto_version = DSM_PROTO_VERSION;
    msg.hdr.type = MSG_PAGE_REQUEST;
    msg.hdr.src_slot = requester;
    msg.hdr.dst_slot = 0;
    msg.hdr.page_id = page;
    msg.hdr.access = write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ;
    return msg;
}

static int test_owner_fantasma_reintenta(void)
{
    dsm_ctx_t *ctx = ctx_create();
    if (!ctx) return -1;
    add_owner_node(ctx, 1);

    uint32_t page = 0;
    uint8_t pattern[PAGE_BYTES];
    memset(pattern, 0x5Au, sizeof(pattern));
    if (dsm_mem_backing_store(&ctx->mem, page, pattern) != DSM_OK) {
        ctx_destroy(ctx);
        return -1;
    }

    ctx->msi.dir[page].busy = 0;
    ctx->msi.dir[page].state = DSM_DIR_MODIFIED;
    ctx->msi.dir[page].owner = 1;
    memset(ctx->msi.dir[page].copyset, 0, sizeof(ctx->msi.dir[page].copyset));
    ctx->msi.dir[page].copyset[1 / 8] |= (uint8_t)(1u << (1 % 8));
    msi_test_downgrade_reset();

    dsm_msg_t msg = ghost_request(1, page, false);
    msi_home_handle_request(ctx, &msg);

    CHECK(msi_test_downgrade_calls() == 0);
    CHECK(ctx->msi.dir[page].busy == 0);
    CHECK(ctx->msi.dir[page].state == DSM_DIR_SHARED);
    CHECK(ctx->msi.dir[page].owner == DSM_SLOT_NONE);
    CHECK((ctx->msi.dir[page].copyset[1 / 8] &
           (uint8_t)(1u << (1 % 8))) != 0);
    ctx_destroy(ctx);
    return 0;
}

static int test_owner_i_recupera_desde_respaldo(void)
{
    dsm_ctx_t *ctx = ctx_create();
    if (!ctx) return -1;
    add_owner_node(ctx, 1);

    uint32_t page = 1;
    uint8_t pattern[PAGE_BYTES];
    memset(pattern, 0xA5u, sizeof(pattern));
    if (dsm_mem_backing_store(&ctx->mem, page, pattern) != DSM_OK) {
        ctx_destroy(ctx);
        return -1;
    }

    ctx->msi.dir[page].busy = 0;
    ctx->msi.dir[page].state = DSM_DIR_MODIFIED;
    ctx->msi.dir[page].owner = 1;
    memset(ctx->msi.dir[page].copyset, 0, sizeof(ctx->msi.dir[page].copyset));
    ctx->msi.dir[page].copyset[1 / 8] |= (uint8_t)(1u << (1 % 8));
    msi_test_downgrade_config(DSM_ST_BAD_STATE);

    dsm_msg_t msg = ghost_request(2, page, false);
    msi_home_handle_request(ctx, &msg);

    CHECK(msi_test_downgrade_calls() == 1);
    CHECK(ctx->msi.dir[page].busy == 0);
    CHECK(ctx->msi.dir[page].state == DSM_DIR_SHARED);
    CHECK(ctx->msi.dir[page].owner == DSM_SLOT_NONE);
    CHECK((ctx->msi.dir[page].copyset[2 / 8] &
           (uint8_t)(1u << (2 % 8))) != 0);
    CHECK((ctx->msi.dir[page].copyset[1 / 8] &
           (uint8_t)(1u << (1 % 8))) == 0);
    msi_test_downgrade_reset();
    ctx_destroy(ctx);
    return 0;
}

static int test_owner_pend_recibe_busy(void)
{
    dsm_ctx_t *ctx = ctx_create();
    if (!ctx) return -1;
    add_owner_node(ctx, 1);

    uint32_t page = 2;
    ctx->msi.dir[page].busy = 0;
    ctx->msi.dir[page].state = DSM_DIR_MODIFIED;
    ctx->msi.dir[page].owner = 1;
    memset(ctx->msi.dir[page].copyset, 0, sizeof(ctx->msi.dir[page].copyset));
    ctx->msi.dir[page].copyset[1 / 8] |= (uint8_t)(1u << (1 % 8));
    msi_test_downgrade_config(DSM_ST_BUSY);

    dsm_msg_t msg = ghost_request(2, page, false);
    msi_home_handle_request(ctx, &msg);

    CHECK(msi_test_downgrade_calls() == 1);
    CHECK(ctx->msi.dir[page].busy == 0);
    CHECK(ctx->msi.dir[page].state == DSM_DIR_MODIFIED);
    CHECK(ctx->msi.dir[page].owner == 1);
    CHECK((ctx->msi.dir[page].copyset[1 / 8] &
           (uint8_t)(1u << (1 % 8))) != 0);
    msi_test_downgrade_reset();
    ctx_destroy(ctx);
    return 0;
}

static int test_shared_solicitante_no_es_fantasma(void)
{
    dsm_ctx_t *ctx = ctx_create();
    if (!ctx) return -1;

    uint32_t page = 3;
    uint8_t pattern[PAGE_BYTES];
    memset(pattern, 0x3Cu, sizeof(pattern));
    if (dsm_mem_backing_store(&ctx->mem, page, pattern) != DSM_OK) {
        ctx_destroy(ctx);
        return -1;
    }

    ctx->msi.dir[page].busy = 0;
    ctx->msi.dir[page].state = DSM_DIR_SHARED;
    ctx->msi.dir[page].owner = 0;
    memset(ctx->msi.dir[page].copyset, 0, sizeof(ctx->msi.dir[page].copyset));
    ctx->msi.dir[page].copyset[1 / 8] |= (uint8_t)(1u << (1 % 8));
    msi_test_downgrade_reset();

    dsm_msg_t msg = ghost_request(1, page, true);
    msi_home_handle_request(ctx, &msg);

    CHECK(ctx->msi.dir[page].busy == 0);
    CHECK(ctx->msi.dir[page].state == DSM_DIR_MODIFIED);
    CHECK(ctx->msi.dir[page].owner == 1);
    ctx_destroy(ctx);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Busy serial del directorio MSI ===\n");
    RUN_TEST(test_busy_success);
    RUN_TEST(test_busy_failure);
    RUN_TEST(test_owner_fantasma_reintenta);
    RUN_TEST(test_owner_i_recupera_desde_respaldo);
    RUN_TEST(test_owner_pend_recibe_busy);
    RUN_TEST(test_shared_solicitante_no_es_fantasma);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
