#include "sync.h"
#include "dsm_internal.h"
#include <string.h>

int sync_init(dsm_ctx_t *ctx)
{
    memset(&ctx->sync, 0, sizeof(ctx->sync));
    pthread_mutex_init(&ctx->sync.mu, NULL);
    for (int i = 0; i < 256; i++) {
        ctx->sync.locks[i].owner_slot = DSM_SLOT_NONE;
    }
    return DSM_OK;
}

void sync_destroy(dsm_ctx_t *ctx)
{
    pthread_mutex_destroy(&ctx->sync.mu);
}

void sync_on_lock_req(dsm_ctx_t *ctx, const dsm_msg_t *m)       { (void)ctx; (void)m; }
void sync_on_lock_rel(dsm_ctx_t *ctx, const dsm_msg_t *m)       { (void)ctx; (void)m; }
void sync_on_barrier_arrive(dsm_ctx_t *ctx, const dsm_msg_t *m) { (void)ctx; (void)m; }
void sync_on_barrier_go(dsm_ctx_t *ctx, const dsm_msg_t *m)     { (void)ctx; (void)m; }
