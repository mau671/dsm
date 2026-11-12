#include "msi.h"
#include "dsm_internal.h"

#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>

static bool copyset_has(const dir_entry_t *entry, uint32_t slot)
{
    return slot < MAX_NODES &&
           (entry->copyset[slot / 8] & (uint8_t)(1u << (slot % 8))) != 0;
}

static void copyset_set(dir_entry_t *entry, uint32_t slot)
{
    if (slot < MAX_NODES) {
        entry->copyset[slot / 8] |= (uint8_t)(1u << (slot % 8));
    }
}

static int page_apply(dsm_ctx_t *ctx, uint32_t page, dsm_page_state_t state,
                      const void *payload, uint32_t payload_len)
{
    if (page >= ctx->mem.npages) return DSM_ERR_INVALID;
    if (payload_len != 0 && payload_len != ctx->mem.page_size) {
        return DSM_ERR_PROTO;
    }

    pthread_mutex_t *mu = dsm_mem_stripe(&ctx->mem, page);
    pthread_mutex_lock(mu);
    if (payload_len != 0) {
        dsm_mem_install_page(&ctx->mem, page, payload);
    }

    /* PAGE_DATA/GRANT completan I*_PEND/SM_PEND y abren la vista DSM. */
    int prot = (state == DSM_PAGE_M) ? (PROT_READ | PROT_WRITE) : PROT_READ;
    int rc = dsm_mem_set_prot(&ctx->mem, page, prot);
    if (rc == DSM_OK) {
        ctx->mem.local[page].state = (uint8_t)state;
    }
    pthread_mutex_unlock(mu);
    return rc;
}

static void page_invalidate_local(dsm_ctx_t *ctx, uint32_t page)
{
    pthread_mutex_t *mu = dsm_mem_stripe(&ctx->mem, page);
    pthread_mutex_lock(mu);
    /* INVALIDATE transforma cualquier copia local en I y la vuelve inaccesible. */
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    pthread_mutex_unlock(mu);
}

static int invalidate_remote_copies(dsm_ctx_t *ctx, uint32_t page,
                                    const dir_entry_t *entry,
                                    uint32_t keep_slot)
{
    for (uint32_t slot = 0; slot < MAX_NODES; slot++) {
        if (!copyset_has(entry, slot) || slot == keep_slot) {
            continue;
        }
        if (slot == ctx->memb.self_slot) {
            page_invalidate_local(ctx, page);
            continue;
        }

        uint16_t status = DSM_ST_OK;
        uint32_t rx_len = 0;
        int rc = dsm_net_rpc(ctx, slot, MSG_INVALIDATE, page, entry->tseq,
                             DSM_ACCESS_WRITE, 0, NULL, 0, &status, NULL,
                             &rx_len);
        if (rc != DSM_OK || status != DSM_ST_OK) {
            return (rc == DSM_OK) ? DSM_ERR_NET : rc;
        }
    }
    return DSM_OK;
}

static int home_local_fault(dsm_ctx_t *ctx, uint32_t page, bool write)
{
    dir_entry_t entry;
    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    uint32_t previous_owner = entry.owner;
    uint8_t *data = NULL;
    uint32_t data_len = 0;

    if (entry.state == DSM_DIR_MODIFIED &&
        entry.owner != DSM_SLOT_NONE &&
        entry.owner != ctx->memb.self_slot) {
        data = malloc(ctx->mem.page_size);
        if (!data) return DSM_ERR_MEM;
        uint16_t status = DSM_ST_OK;
        data_len = ctx->mem.page_size;
        int rc = dsm_net_rpc(ctx, entry.owner, MSG_DOWNGRADE, page,
                             entry.tseq, write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                             DSM_FLAG_HAS_DATA, NULL, 0, &status, data,
                             &data_len);
        if (rc != DSM_OK || status != DSM_ST_OK) {
            free(data);
            return (rc == DSM_OK) ? DSM_ERR_NET : rc;
        }
    }

    if (write) {
        int rc = invalidate_remote_copies(ctx, page, &entry,
                                          ctx->memb.self_slot);
        if (rc != DSM_OK) {
            free(data);
            return rc;
        }
    }

    int rc = page_apply(ctx, page, write ? DSM_PAGE_M : DSM_PAGE_S,
                        data, data_len);
    free(data);
    if (rc != DSM_OK) return rc;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    if (write) {
        /* SM_PEND/IM_PEND -> M: las copias remotas ya fueron invalidadas. */
        entry.state = DSM_DIR_MODIFIED;
        entry.owner = ctx->memb.self_slot;
        memset(entry.copyset, 0, sizeof(entry.copyset));
        copyset_set(&entry, ctx->memb.self_slot);
    } else {
        /* IS_PEND -> S: el home conserva una copia válida para lectura. */
        /* Downgrade M->S: el dueño entrega la versión y conserva lectura. */
        entry.state = DSM_DIR_SHARED;
        entry.owner = DSM_SLOT_NONE;
        copyset_set(&entry, ctx->memb.self_slot);
        if (previous_owner != DSM_SLOT_NONE) {
            copyset_set(&entry, previous_owner);
        }
    }
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return DSM_OK;
}

int msi_init(dsm_ctx_t *ctx)
{
    dsm_msi_t *msi = &ctx->msi;
    memset(msi, 0, sizeof(*msi));
    pthread_mutex_init(&msi->dir_mu, NULL);

    if (ctx->cfg.page_size == 0 || ctx->cfg.view_size < ctx->cfg.page_size) {
        return DSM_OK;
    }

    msi->cap = (size_t)(ctx->cfg.view_size / ctx->cfg.page_size);
    msi->dir = calloc(msi->cap, sizeof(*msi->dir));
    if (!msi->dir) return DSM_ERR_MEM;

    for (size_t i = 0; i < msi->cap; i++) {
        msi->dir[i].owner = DSM_SLOT_NONE;
        msi->dir[i].state = DSM_DIR_UNCACHED;
        msi->dir[i].frame = DSM_PAGE_ID_NONE;
        msi->dir[i].wait_head = DSM_WAIT_NONE;
        msi->dir[i].wait_tail = DSM_WAIT_NONE;
        msi->dir[i].txn = DSM_TXN_NONE;
    }
    return DSM_OK;
}

void msi_destroy(dsm_ctx_t *ctx)
{
    free(ctx->msi.dir);
    ctx->msi.dir = NULL;
    ctx->msi.cap = 0;
    pthread_mutex_destroy(&ctx->msi.dir_mu);
}

int msi_local_fault(dsm_ctx_t *ctx, fault_slot_t *s)
{
    if (!ctx || !s || s->page >= ctx->mem.npages) return DSM_ERR_INVALID;

    uint32_t page = s->page;
    bool write = s->access == DSM_ACCESS_WRITE;
    pthread_mutex_t *page_mu = dsm_mem_stripe(&ctx->mem, page);

    pthread_mutex_lock(page_mu);
    uint8_t current = ctx->mem.local[page].state;
    if ((!write && (current == DSM_PAGE_S || current == DSM_PAGE_M)) ||
        (write && current == DSM_PAGE_M)) {
        pthread_mutex_unlock(page_mu);
        return DSM_OK;
    }
    if (current == DSM_PAGE_IS_PEND || current == DSM_PAGE_IM_PEND ||
        current == DSM_PAGE_SM_PEND) {
        pthread_mutex_unlock(page_mu);
        return DSM_ERR_BUSY;
    }

    /* I/S -> estado pendiente: se bloquea el acceso mientras llega la copia. */
    if (write) {
        ctx->mem.local[page].state =
            (current == DSM_PAGE_S) ? DSM_PAGE_SM_PEND : DSM_PAGE_IM_PEND;
    } else {
        ctx->mem.local[page].state = DSM_PAGE_IS_PEND;
    }
    pthread_mutex_unlock(page_mu);

    uint32_t home = ring_home_of(ctx, page);
    if (home == ctx->memb.self_slot) {
        int rc = home_local_fault(ctx, page, write);
        if (rc != DSM_OK) page_invalidate_local(ctx, page);
        return rc;
    }

    uint8_t *rx = malloc(ctx->mem.page_size);
    if (!rx) {
        page_invalidate_local(ctx, page);
        return DSM_ERR_MEM;
    }
    uint32_t rx_len = ctx->mem.page_size;
    uint16_t status = DSM_ST_OK;
    uint16_t request_flags = write ? DSM_FLAG_WANT_DATA : DSM_FLAG_WANT_DATA;
    int rc = dsm_net_rpc(ctx, home, MSG_PAGE_REQUEST, page,
                         ctx->mem.local[page].tseq_seen,
                         write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                         request_flags, NULL, 0, &status, rx, &rx_len);
    if (rc == DSM_OK && status == DSM_ST_OK) {
        rc = page_apply(ctx, page, write ? DSM_PAGE_M : DSM_PAGE_S,
                        rx, rx_len);
    } else if (rc == DSM_OK) {
        rc = DSM_ERR_NET;
    }
    free(rx);
    if (rc != DSM_OK) {
        page_invalidate_local(ctx, page);
    }
    return rc;
}

void msi_home_handle_request(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;

    uint32_t page = m->hdr.page_id;
    uint32_t requester = m->hdr.src_slot;
    bool write = m->hdr.access == DSM_ACCESS_WRITE;
    dir_entry_t entry;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    uint8_t *data = NULL;
    uint32_t data_len = 0;
    int rc = DSM_OK;

    if (!write) {
        if (entry.state == DSM_DIR_MODIFIED &&
            entry.owner != DSM_SLOT_NONE &&
            entry.owner != ctx->memb.self_slot) {
            data = malloc(ctx->mem.page_size);
            if (!data) return;
            uint16_t status = DSM_ST_OK;
            data_len = ctx->mem.page_size;
            rc = dsm_net_rpc(ctx, entry.owner, MSG_DOWNGRADE, page,
                             entry.tseq, DSM_ACCESS_READ, DSM_FLAG_HAS_DATA,
                             NULL, 0, &status, data, &data_len);
            if (rc != DSM_OK || status != DSM_ST_OK) {
                free(data);
                return;
            }
        } else {
            data = malloc(ctx->mem.page_size);
            if (!data) return;
            memcpy(data, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
            data_len = ctx->mem.page_size;
            if (entry.state == DSM_DIR_MODIFIED &&
                entry.owner == ctx->memb.self_slot) {
                (void)page_apply(ctx, page, DSM_PAGE_S, NULL, 0);
            }
        }

        pthread_mutex_lock(&ctx->msi.dir_mu);
        entry = ctx->msi.dir[page];
        entry.state = DSM_DIR_SHARED;
        entry.owner = DSM_SLOT_NONE;
        copyset_set(&entry, requester);
        copyset_set(&entry, ctx->memb.self_slot);
        ctx->msi.dir[page] = entry;
        pthread_mutex_unlock(&ctx->msi.dir_mu);

        msg_header_t response;
        memset(&response, 0, sizeof(response));
        response.magic = DSM_MAGIC;
        response.cluster_id = ctx->cfg.cluster_id;
        response.proto_version = DSM_PROTO_VERSION;
        response.type = MSG_PAGE_DATA;
        response.src_slot = ctx->memb.self_slot;
        response.dst_slot = requester;
        response.req_id = m->hdr.req_id;
        response.page_id = page;
        response.data_version = entry.version;
        response.payload_len = data_len;
        (void)dsm_net_send(ctx, requester, &response, data);
        free(data);
        return;
    }

    if (entry.state == DSM_DIR_MODIFIED &&
        entry.owner != DSM_SLOT_NONE && entry.owner != requester &&
        entry.owner != ctx->memb.self_slot) {
        data = malloc(ctx->mem.page_size);
        if (!data) return;
        uint16_t status = DSM_ST_OK;
        data_len = ctx->mem.page_size;
        rc = dsm_net_rpc(ctx, entry.owner, MSG_DOWNGRADE, page, entry.tseq,
                         DSM_ACCESS_WRITE, DSM_FLAG_HAS_DATA, NULL, 0,
                         &status, data, &data_len);
        if (rc != DSM_OK || status != DSM_ST_OK) {
            free(data);
            return;
        }
    } else if (entry.state != DSM_DIR_UNCACHED) {
        data = malloc(ctx->mem.page_size);
        if (!data) return;
        memcpy(data, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
        data_len = ctx->mem.page_size;
    }

    rc = invalidate_remote_copies(ctx, page, &entry, requester);
    if (rc != DSM_OK) {
        free(data);
        return;
    }

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    entry.state = DSM_DIR_MODIFIED;
    entry.owner = requester;
    memset(entry.copyset, 0, sizeof(entry.copyset));
    copyset_set(&entry, requester);
    entry.tseq++;
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    msg_header_t response;
    memset(&response, 0, sizeof(response));
    response.magic = DSM_MAGIC;
    response.cluster_id = ctx->cfg.cluster_id;
    response.proto_version = DSM_PROTO_VERSION;
    response.type = MSG_GRANT;
    response.src_slot = ctx->memb.self_slot;
    response.dst_slot = requester;
    response.req_id = m->hdr.req_id;
    response.page_id = page;
    response.data_version = entry.version;
    response.flags = data_len ? DSM_FLAG_HAS_DATA : 0;
    response.payload_len = data_len;
    (void)dsm_net_send(ctx, requester, &response, data);
    free(data);
}

void msi_on_page_data(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    /* PAGE_DATA completa IS_PEND -> S y protege la página como solo lectura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_S,
                     m->payload, m->hdr.payload_len);
}

void msi_on_grant(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    /* GRANT completa IM_PEND/SM_PEND -> M y permite escritura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_M,
                     m->payload, m->hdr.payload_len);
}

void msi_on_invalidate(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;
    /* INVALIDATE fuerza S/M -> I antes de confirmar al home. */
    page_invalidate_local(ctx, m->hdr.page_id);

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic = DSM_MAGIC;
    ack.cluster_id = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type = MSG_INVALIDATE_ACK;
    ack.src_slot = ctx->memb.self_slot;
    ack.dst_slot = m->hdr.src_slot;
    ack.req_id = m->hdr.req_id;
    ack.page_id = m->hdr.page_id;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, NULL);
}

void msi_on_invalidate_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

void msi_on_downgrade(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;

    uint32_t page = m->hdr.page_id;
    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) return;
    pthread_mutex_t *mu = dsm_mem_stripe(&ctx->mem, page);
    pthread_mutex_lock(mu);
    memcpy(data, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
    /* DOWNGRADE transforma M -> S después de copiar la versión más reciente. */
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_READ);
    ctx->mem.local[page].state = DSM_PAGE_S;
    pthread_mutex_unlock(mu);

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic = DSM_MAGIC;
    ack.cluster_id = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type = MSG_DOWNGRADE_ACK;
    ack.src_slot = ctx->memb.self_slot;
    ack.dst_slot = m->hdr.src_slot;
    ack.req_id = m->hdr.req_id;
    ack.page_id = page;
    ack.flags = DSM_FLAG_HAS_DATA;
    ack.payload_len = ctx->mem.page_size;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, data);
    free(data);
}

void msi_on_downgrade_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

void msi_on_writeback(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

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
