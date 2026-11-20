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
    REQUIRE(ctx != NULL);

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

    REQUIRE(dsm_mem_init(&ctx->mem, &ctx->cfg) == DSM_OK);
    REQUIRE(msi_init(ctx) == DSM_OK);

    pthread_rwlock_wrlock(&ctx->memb.rw);
    ctx->memb.self_slot = 0;
    pthread_rwlock_unlock(&ctx->memb.rw);
    membership_add_self(ctx);
    REQUIRE(ring_build(&ctx->ring, &ctx->memb) == DSM_OK);

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

static void run_busy_case(bool fail)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    uint32_t page = 0;
    ctx->msi.dir[page].busy = 0;
    msi_test_busy_hook_config(page, fail);

    busy_thread_t first;
    first.ctx = ctx;
    first.msg = page_request(page);

    pthread_t thread;
    REQUIRE(pthread_create(&thread, NULL, first_request, &first) == 0);

    msi_test_busy_hook_wait_entered();
    CHECK(ctx->msi.dir[page].busy == 1);

    dsm_msg_t second = page_request(page);
    msi_home_handle_request(ctx, &second);
    CHECK(msi_test_busy_hook_second_seen());

    msi_test_busy_hook_release();
    REQUIRE(pthread_join(thread, NULL) == 0);
    CHECK(ctx->msi.dir[page].busy == 0);

    ctx_destroy(ctx);
}

static void test_busy_success(void)
{
    run_busy_case(false);
}

static void test_busy_failure(void)
{
    run_busy_case(true);
}

int main(void)
{
    TEST_BEGIN("Busy serial del directorio MSI");
    RUN_TEST(test_busy_success);
    RUN_TEST(test_busy_failure);
    TEST_END();
}
