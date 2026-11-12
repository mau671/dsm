#include "msi.h"
#include "dsm_internal.h"
#include <stdlib.h>
#include <string.h>

int msi_init(dsm_ctx_t *ctx)
{
    dsm_msi_t *msi = &ctx->msi;
    memset(msi, 0, sizeof(*msi));
    pthread_mutex_init(&msi->dir_mu, NULL);
    return DSM_OK;
}

void msi_destroy(dsm_ctx_t *ctx)
{
    pthread_mutex_destroy(&ctx->msi.dir_mu);
}

int msi_local_fault(dsm_ctx_t *ctx, fault_slot_t *s)
{
    (void)ctx;
    (void)s;
    return DSM_OK;
}

void msi_home_handle_request(dsm_ctx_t *ctx, const dsm_msg_t *m) { (void)ctx; (void)m; }
void msi_on_page_data(dsm_ctx_t *ctx, const dsm_msg_t *m)        { (void)ctx; (void)m; }
void msi_on_grant(dsm_ctx_t *ctx, const dsm_msg_t *m)            { (void)ctx; (void)m; }
void msi_on_invalidate(dsm_ctx_t *ctx, const dsm_msg_t *m)       { (void)ctx; (void)m; }
void msi_on_invalidate_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)   { (void)ctx; (void)m; }
void msi_on_downgrade(dsm_ctx_t *ctx, const dsm_msg_t *m)        { (void)ctx; (void)m; }
void msi_on_downgrade_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)    { (void)ctx; (void)m; }
void msi_on_writeback(dsm_ctx_t *ctx, const dsm_msg_t *m)        { (void)ctx; (void)m; }

int msi_writeback_page(dsm_ctx_t *ctx, uint32_t page)
{
    (void)ctx;
    (void)page;
    return DSM_OK;
}

void msi_counters(dsm_ctx_t *ctx, uint64_t *out_m, uint64_t *out_s, uint64_t *out_i)
{
    uint64_t m = 0, s = 0, i = 0;
    if (ctx && ctx->mem.local) {
        for (uint32_t p = 0; p < ctx->mem.npages; p++) {
            switch (ctx->mem.local[p].state) {
            case DSM_PAGE_M: m++; break;
            case DSM_PAGE_S: s++; break;
            default:         i++; break;
            }
        }
    }
    if (out_m) *out_m = m;
    if (out_s) *out_s = s;
    if (out_i) *out_i = i;
}
