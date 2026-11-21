#include "raid.h"
#include "dsm_internal.h"
#include "sigsegv.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>

#define DSM_RAID_MAX_GATHER   (DSM_RAID_MAX_K + 1u)
#define DSM_RAID_PARITY_ATTEMPTS 3u
#define DSM_RAID_REBUILD_QUEUE_CAP MAX_NODES

/* --- Tabla de solicitudes pendientes ------------------------------------- */

static void raid_pending_clear(raid_pending_t *p)
{
    p->req_id = 0;
    p->t0_us = 0;
    p->rx_len = 0;
    p->data_version = 0;
    p->rx_buf = NULL;
    p->rx_cap = 0;
    p->status = DSM_ST_OK;
    p->used = 0;
    p->completed = 0;
    p->consumed = 0;
    p->kind = 0;
}

/* Registra una solicitud y devuelve su indice, o -1 si la tabla esta llena.
 * Una entrada completada no se recicla hasta que su dueño la consume: el
 * que despierta despues de un cambio de ranura no debe leer el resultado de
 * otra solicitud. */
static int raid_pending_add(dsm_ctx_t *ctx, uint8_t kind,
                            uint8_t *rx_buf, uint32_t rx_cap,
                            uint32_t *out_req)
{
    dsm_raid_t *r = &ctx->raid;
    uint64_t now = mono_us();
    uint64_t max_age = (uint64_t)ctx->cfg.request_timeout_ms * 2000ULL;
    int chosen = -1;

    pthread_mutex_lock(&r->mu);
    for (uint32_t i = 0; i < DSM_RAID_PENDING_MAX; i++) {
        raid_pending_t *p = &r->pending[i];
        if (p->used && p->completed && p->consumed) {
            raid_pending_clear(p);
        } else if (p->used && !p->completed &&
                   p->kind == DSM_RAID_PENDING_ACK &&
                   now - p->t0_us > max_age) {
            /* Un acuse sin dueno en espera desde hace dos plazos: su
             * llamador ya agoto el suyo y libero por otro camino. */
            raid_pending_clear(p);
        }
    }
    for (uint32_t i = 0; i < DSM_RAID_PENDING_MAX; i++) {
        if (!r->pending[i].used) {
            chosen = (int)i;
            break;
        }
    }
    if (chosen < 0) {
        pthread_mutex_unlock(&r->mu);
        return -1;
    }

    uint32_t req = dsm_net_next_req(ctx);
    raid_pending_t *p = &r->pending[chosen];
    raid_pending_clear(p);
    p->req_id = req;
    p->t0_us = now;
    p->rx_buf = rx_buf;
    p->rx_cap = rx_cap;
    p->kind = kind;
    p->used = 1;
    pthread_mutex_unlock(&r->mu);

    if (out_req) *out_req = req;
    return chosen;
}

/* Solo cancela si la entrada sigue siendo la del emisor: una espera ya
 * resuelta puede haber liberado la ranura y otro llamador puede usarla. */
static void raid_pending_cancel(dsm_ctx_t *ctx, int idx, uint32_t req)
{
    if (idx < 0) return;
    dsm_raid_t *r = &ctx->raid;
    pthread_mutex_lock(&r->mu);
    if (r->pending[idx].used && r->pending[idx].req_id == req) {
        raid_pending_clear(&r->pending[idx]);
    }
    pthread_cond_broadcast(&r->cond);
    pthread_mutex_unlock(&r->mu);
}

/* Copia la carga de respuesta al buffer del llamador y despierta su espera. */
static void raid_pending_complete(dsm_ctx_t *ctx, uint32_t req_id, uint8_t kind,
                                  uint16_t status, uint32_t version,
                                  const uint8_t *data, uint32_t len)
{
    dsm_raid_t *r = &ctx->raid;
    pthread_mutex_lock(&r->mu);
    for (uint32_t i = 0; i < DSM_RAID_PENDING_MAX; i++) {
        raid_pending_t *p = &r->pending[i];
        if (!p->used || p->completed || p->kind != kind || p->req_id != req_id) {
            continue;
        }
        uint32_t n = (len < p->rx_cap) ? len : p->rx_cap;
        if (n > 0 && data && p->rx_buf) {
            memcpy(p->rx_buf, data, n);
        }
        p->rx_len = n;
        p->status = status;
        p->data_version = version;
        p->completed = 1;
        pthread_cond_broadcast(&r->cond);
        break;
    }
    pthread_mutex_unlock(&r->mu);
}

static int raid_pending_wait(dsm_ctx_t *ctx, int idx, uint32_t req,
                             uint32_t timeout_ms,
                             uint16_t *out_status, uint32_t *out_version)
{
    if (idx < 0) return DSM_ERR_INVALID;
    dsm_raid_t *r = &ctx->raid;

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    uint32_t secs = timeout_ms / 1000u;
    long nsecs = (long)(timeout_ms % 1000u) * 1000000L;
    deadline.tv_sec += (time_t)secs;
    deadline.tv_nsec += nsecs;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }

    int rc = 0;
    pthread_mutex_lock(&r->mu);
    while (!r->pending[idx].completed && rc == 0) {
        rc = pthread_cond_timedwait(&r->cond, &r->mu, &deadline);
    }
    /* La ranura pudo reciclarse mientras se esperaba: un resultado que no
     * corresponde a esta solicitud no se consume ni se libera. */
    if (!r->pending[idx].used || r->pending[idx].req_id != req) {
        pthread_cond_broadcast(&r->cond);
        pthread_mutex_unlock(&r->mu);
        return DSM_ERR_TIMEOUT;
    }
    uint16_t status = r->pending[idx].status;
    uint32_t version = r->pending[idx].data_version;
    r->pending[idx].consumed = 1;
    raid_pending_clear(&r->pending[idx]);
    pthread_cond_broadcast(&r->cond);
    pthread_mutex_unlock(&r->mu);

    if (out_status) *out_status = status;
    if (out_version) *out_version = version;
    return (rc == 0) ? DSM_OK : DSM_ERR_TIMEOUT;
}

/* --- Utilidades de franja ------------------------------------------------ */

static void raid_stripe_refresh(dsm_ctx_t *ctx, stripe_rt_t *s)
{
    uint32_t k = ctx->cfg.raid_k;
    if (k > DSM_RAID_MAX_K) k = DSM_RAID_MAX_K;

    for (uint32_t j = 0; j < DSM_RAID_MAX_K; j++) {
        if (j < k && s->data_page[j] != DSM_PAGE_ID_NONE) {
            s->data_slot[j] = ring_home_of(ctx, s->data_page[j]);
        } else {
            s->data_page[j] = DSM_PAGE_ID_NONE;
            s->data_slot[j] = DSM_SLOT_NONE;
        }
    }

    uint32_t limit = ctx->cfg.max_runtime_nodes;
    if (limit > MAX_NODES) limit = MAX_NODES;
    uint32_t chosen = DSM_SLOT_NONE;
    for (uint32_t slot = 0; slot < limit; slot++) {
        if (!membership_is_alive(ctx, slot)) continue;
        bool used = false;
        for (uint32_t j = 0; j < k; j++) {
            if (s->data_slot[j] == slot) {
                used = true;
                break;
            }
        }
        if (!used) {
            chosen = slot;
            break;
        }
    }

    s->k = k;
    s->parity_slot = chosen;
}

/* La membresia se consulta siempre antes de tomar el mutex de la franja. */
static void raid_stripe_sync(dsm_ctx_t *ctx, uint32_t sid, stripe_rt_t *out)
{
    dsm_raid_t *r = &ctx->raid;
    stripe_rt_t tmp;

    pthread_mutex_lock(&r->mu);
    tmp = r->stripes[sid];
    pthread_mutex_unlock(&r->mu);

    uint32_t previous_parity = tmp.parity_slot;
    raid_stripe_refresh(ctx, &tmp);

    pthread_mutex_lock(&r->mu);
    r->stripes[sid].k = tmp.k;
    memcpy(r->stripes[sid].data_slot, tmp.data_slot,
           sizeof(r->stripes[sid].data_slot));
    r->stripes[sid].parity_slot = tmp.parity_slot;
    /* Un custodio nuevo no recibio el marco del anterior: hasta que la
     * reconstruccion lo instale, la franja se trata como sin paridad. */
    if (previous_parity != tmp.parity_slot) {
        r->stripes[sid].parity_frame_valid = 0u;
    }
    if (out) *out = r->stripes[sid];
    pthread_mutex_unlock(&r->mu);
}

static bool raid_local_page(dsm_ctx_t *ctx, uint32_t page, uint8_t *dst)
{
    if (page >= ctx->mem.npages) return false;
    if (dsm_mem_backing_load(&ctx->mem, page, dst) == DSM_OK) return true;

    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t state = ctx->mem.local[page].state;
    bool copied = (state == DSM_PAGE_M || state == DSM_PAGE_S);
    if (copied) {
        memcpy(dst, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);
    return copied;
}

static void raid_send_word(dsm_ctx_t *ctx, uint32_t dst, uint16_t type,
                           uint32_t value, uint16_t status, uint32_t version,
                           uint32_t req_id, uint32_t page_id)
{
    uint8_t raw[DSM_WIRE_WORD_LEN];
    wire_word_t w = { .value = value };
    (void)dsm_pack_word(&w, raw);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = type;
    hdr.epoch         = ctx->memb.epoch;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.page_id       = page_id;
    hdr.data_version  = version;
    hdr.status        = status;
    hdr.payload_len   = DSM_WIRE_WORD_LEN;
    (void)dsm_net_send(ctx, dst, &hdr, raw);
}

static void raid_reply_word(dsm_ctx_t *ctx, const dsm_msg_t *m, uint16_t type,
                            uint32_t value, uint16_t status, uint32_t version)
{
    if (!m || m->hdr.src_slot >= MAX_NODES) return;
    raid_send_word(ctx, m->hdr.src_slot, type, value, status, version,
                   m->hdr.req_id, m->hdr.page_id);
}

static void raid_invalidate_page(dsm_ctx_t *ctx, uint32_t page)
{
    if (page >= ctx->mem.npages) return;
    dsm_mem_stripe_lock(&ctx->mem, page);
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    dsm_mem_stripe_unlock(&ctx->mem, page);
    (void)dsm_mem_backing_drop(&ctx->mem, page);

    if (page < ctx->msi.cap) {
        pthread_mutex_lock(&ctx->msi.dir_mu);
        dir_entry_t *e = &ctx->msi.dir[page];
        e->state = DSM_DIR_UNCACHED;
        e->owner = DSM_SLOT_NONE;
        memset(e->copyset, 0, sizeof(e->copyset));
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    }
}

static uint32_t raid_stripe_leader(dsm_ctx_t *ctx, const stripe_rt_t *s)
{
    uint32_t leader = DSM_SLOT_NONE;
    uint32_t k = (s->k <= DSM_RAID_MAX_K) ? s->k : DSM_RAID_MAX_K;

    for (uint32_t j = 0; j < k; j++) {
        uint32_t slot = s->data_slot[j];
        if (slot != DSM_SLOT_NONE && membership_is_alive(ctx, slot) &&
            (leader == DSM_SLOT_NONE || slot < leader)) {
            leader = slot;
        }
    }
    if (s->parity_slot != DSM_SLOT_NONE &&
        membership_is_alive(ctx, s->parity_slot) &&
        (leader == DSM_SLOT_NONE || s->parity_slot < leader)) {
        leader = s->parity_slot;
    }
    return leader;
}

/* Cola acotada de ranuras pendientes de reconstruccion. Se accede siempre
 * bajo rebuild_mu; rebuild_active/rebuild_thread siguen en el contexto. */
static uint32_t rebuild_queue[DSM_RAID_REBUILD_QUEUE_CAP];
static uint32_t rebuild_qhead;
static uint32_t rebuild_qtail;
static uint32_t rebuild_qlen;

/* --- Ciclo de vida ------------------------------------------------------- */

void raid_init(dsm_ctx_t *ctx)
{
    if (!ctx) return;
    dsm_raid_t *r = &ctx->raid;
    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->mu, NULL);
    pthread_cond_init(&r->cond, NULL);
    pthread_mutex_init(&r->rebuild_mu, NULL);
    rebuild_qhead = 0;
    rebuild_qtail = 0;
    rebuild_qlen = 0;

    if (!ctx->cfg.redundancy_raid5) return;

    uint32_t k = ctx->cfg.raid_k;
    uint32_t ps = (uint32_t)ctx->mem.page_size;
    uint32_t npages = ctx->mem.npages;
    if (k < 2 || k > DSM_RAID_MAX_K || ps == 0 || npages == 0) {
        DSM_LOG_WARN("RAID5: configuracion insuficiente; paridad desactivada");
        return;
    }

    uint32_t nstripes = npages / k + ((npages % k) != 0 ? 1u : 0u);
    if ((size_t)nstripes > SIZE_MAX / ps) {
        DSM_LOG_WARN("RAID5: tamano de paridad fuera de rango");
        return;
    }

    r->stripes = calloc(nstripes, sizeof(*r->stripes));
    if (!r->stripes) {
        DSM_LOG_WARN("RAID5: sin memoria para la tabla de franjas");
        return;
    }
    r->parity = calloc((size_t)nstripes * ps, 1u);
    if (!r->parity) {
        free(r->stripes);
        r->stripes = NULL;
        DSM_LOG_WARN("RAID5: sin memoria para los marcos de paridad");
        return;
    }

    r->nstripes = nstripes;
    r->page_size = ps;
    r->k = k;
    for (uint32_t sid = 0; sid < nstripes; sid++) {
        stripe_rt_t *s = &r->stripes[sid];
        s->stripe_id = sid;
        s->k = k;
        s->state = DSM_STRIPE_ACTIVE;
        s->parity_slot = DSM_SLOT_NONE;
        s->version = 0;
        for (uint32_t j = 0; j < DSM_RAID_MAX_K; j++) {
            uint32_t page = (j < k) ? (sid * k + j) : DSM_PAGE_ID_NONE;
            if (page >= npages) page = DSM_PAGE_ID_NONE;
            s->data_page[j] = page;
            s->data_slot[j] = (page != DSM_PAGE_ID_NONE)
                                  ? ring_home_of(ctx, page) : DSM_SLOT_NONE;
        }
    }
    DSM_LOG_INFO("RAID5: %u franjas de K=%u, %u bytes por marco de paridad",
                 nstripes, k, ps);
}

void raid_shutdown(dsm_ctx_t *ctx)
{
    if (!ctx) return;
    dsm_raid_t *r = &ctx->raid;

    pthread_mutex_lock(&r->rebuild_mu);
    rebuild_qhead = 0;
    rebuild_qtail = 0;
    rebuild_qlen = 0;
    r->rebuild_pending = false;
    bool active = r->rebuild_active;
    pthread_mutex_unlock(&r->rebuild_mu);
    if (active) {
        pthread_join(r->rebuild_thread, NULL);
    }
    pthread_mutex_destroy(&r->rebuild_mu);

    free(r->parity);
    free(r->stripes);
    r->parity = NULL;
    r->stripes = NULL;
    r->nstripes = 0;
    pthread_cond_destroy(&r->cond);
    pthread_mutex_destroy(&r->mu);
}

/* --- Escritura ----------------------------------------------------------- */

/* Restaura la version publicada solo si nadie la ha avanzado mientras tanto. */
static void raid_version_restore(dsm_ctx_t *ctx, uint32_t sid,
                                 uint32_t published, uint32_t previous)
{
    pthread_mutex_lock(&ctx->raid.mu);
    if (ctx->raid.stripes[sid].version == published) {
        ctx->raid.stripes[sid].version = previous;
    }
    pthread_mutex_unlock(&ctx->raid.mu);
}

int raid_on_write(dsm_ctx_t *ctx, uint32_t page,
                  const uint8_t *old_data, const uint8_t *new_data)
{
    if (!ctx || !new_data) return DSM_ERR_INVALID;
    if (!ctx->cfg.redundancy_raid5) return DSM_OK;

    uint32_t ps = (uint32_t)ctx->mem.page_size;
    uint32_t k = ctx->raid.k;
    if (ps == 0 || k < 2 || k > DSM_RAID_MAX_K || page >= ctx->mem.npages) {
        return DSM_ERR_INVALID;
    }
    uint32_t sid = page / k;
    if (sid >= ctx->raid.nstripes) return DSM_ERR_INVALID;

    stripe_rt_t s;
    raid_stripe_sync(ctx, sid, &s);
    if (s.parity_slot == DSM_SLOT_NONE || s.parity_slot >= MAX_NODES ||
        !membership_is_alive(ctx, s.parity_slot)) {
        DSM_LOG_WARN("RAID5: franja %u sin custodio de paridad; delta omitido", sid);
        return DSM_ERR_NODE_DOWN;
    }

    uint8_t *delta = malloc(ps);
    uint8_t *payload = malloc((size_t)DSM_PARITY_DELTA_HDR_LEN + ps);
    if (!delta || !payload) {
        free(delta);
        free(payload);
        return DSM_ERR_MEM;
    }
    for (uint32_t i = 0; i < ps; i++) {
        uint8_t before = old_data ? old_data[i] : 0u;
        delta[i] = (uint8_t)(before ^ new_data[i]);
    }

    parity_delta_wire_t d = { .stripe_id = s.stripe_id };
    size_t plen = dsm_pack_parity_delta(&d, delta, ps, payload);

    int rc = DSM_ERR_NET;
    for (uint32_t attempt = 0; attempt < DSM_RAID_PARITY_ATTEMPTS; attempt++) {
        uint32_t version;
        uint32_t prev;
        pthread_mutex_lock(&ctx->raid.mu);
        prev = ctx->raid.stripes[sid].version;
        version = prev + 1u;
        ctx->raid.stripes[sid].version = version;
        pthread_mutex_unlock(&ctx->raid.mu);

        uint32_t req = 0;
        int pidx = raid_pending_add(ctx, DSM_RAID_PENDING_ACK, NULL, 0, &req);
        if (pidx < 0) {
            raid_version_restore(ctx, sid, version, prev);
            rc = DSM_ERR_NO_SPACE;
            break;
        }

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_PARITY_DELTA;
        hdr.epoch         = ctx->memb.epoch;
        hdr.src_slot      = ctx->memb.self_slot;
        hdr.dst_slot      = s.parity_slot;
        hdr.req_id        = req;
        hdr.page_id       = page;
        hdr.data_version  = version;
        hdr.payload_len   = (uint32_t)plen;

        int sent = dsm_net_send(ctx, s.parity_slot, &hdr, payload);
        if (sent != DSM_OK) {
            raid_pending_cancel(ctx, pidx, req);
            raid_version_restore(ctx, sid, version, prev);
            rc = sent;
            break;
        }
        atomic_fetch_add_explicit(&ctx->ctr.parity_deltas, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->ctr.parity_bytes,
                                  (uint64_t)DSM_HDR_LEN + (uint64_t)plen,
                                  memory_order_relaxed);

        uint16_t status = DSM_ST_OK;
        uint32_t applied = 0;
        int wr = raid_pending_wait(ctx, pidx, req, ctx->cfg.request_timeout_ms,
                                   &status, &applied);
        if (wr != DSM_OK) {
            DSM_LOG_WARN("RAID5: franja %u sin acuse; se conserva la version %u",
                         sid, version);
            rc = DSM_ERR_TIMEOUT;
            break;
        }
        if (status == DSM_ST_OK) {
            rc = DSM_OK;
            break;
        }
        if (status != DSM_ST_BAD_STATE) {
            DSM_LOG_WARN("RAID5: acuse de paridad de la franja %u rechazado (estado %u)",
                         sid, status);
            rc = DSM_ERR_PROTO;
            break;
        }

        /* Un custodio con version menor a la que este nodo publico tiene un
         * marco atrasado: reenviar con su version haria aplicar el delta
         * sobre un contenido equivocado, asi que el delta se abandona y la
         * franja queda sin paridad hasta su reconstruccion. */
        if (applied < prev) {
            raid_version_restore(ctx, sid, version, prev);
            DSM_LOG_WARN("RAID5: custodio de la franja %u con version %u "
                         "ante una publicada %u; delta omitido",
                         sid, applied, prev);
            rc = DSM_ERR_PROTO;
            break;
        }

        pthread_mutex_lock(&ctx->raid.mu);
        ctx->raid.stripes[sid].version = applied;
        pthread_mutex_unlock(&ctx->raid.mu);
        if (attempt + 1u < DSM_RAID_PARITY_ATTEMPTS) {
            DSM_LOG_WARN("RAID5: franja %u con version obsoleta; reenvio con version %u",
                         sid, applied + 1u);
        }
    }

    if (rc != DSM_OK) {
        DSM_LOG_WARN("RAID5: no se pudo actualizar la paridad de la franja %u "
                     "en el slot %u", sid, s.parity_slot);
    }

    free(delta);
    free(payload);
    return rc;
}

/* --- Manejadores --------------------------------------------------------- */

void raid_on_parity_delta(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload) return;
    uint32_t ps = ctx->raid.page_size;
    if (ps == 0 || m->hdr.payload_len != DSM_PARITY_DELTA_WIRE_LEN(ps)) return;

    uint8_t *delta = malloc(ps);
    if (!delta) return;
    parity_delta_wire_t d;
    dsm_unpack_parity_delta(m->payload, ps, &d, delta);

    uint32_t sid = d.stripe_id;
    uint16_t status = DSM_ST_OK;
    uint32_t version = 0;

    if (sid >= ctx->raid.nstripes) {
        status = DSM_ST_NOT_HOME;
    } else {
        stripe_rt_t s;
        raid_stripe_sync(ctx, sid, &s);
        if (s.parity_slot != ctx->memb.self_slot) {
            status = DSM_ST_NOT_HOME;
        } else {
            pthread_mutex_lock(&ctx->raid.mu);
            stripe_rt_t *live = &ctx->raid.stripes[sid];
            uint32_t expected = live->version + 1u;
            version = live->version;
            bool frame_usable = live->parity_frame_valid ||
                                live->state == DSM_STRIPE_INSTALL ||
                                live->version == 0u;
            if (!frame_usable) {
                /* El marco local no representa el contenido de la franja:
                 * aplicar el delta sobre el daria una paridad falsa. Se
                 * rechaza con NOT_HOME para que el emisor no reintente
                 * contra este marco. */
                status = DSM_ST_NOT_HOME;
                DSM_LOG_WARN("RAID5: delta de la franja %u sobre un marco "
                             "sin instalar", sid);
            } else if (m->hdr.data_version != expected) {
                status = DSM_ST_BAD_STATE;
                DSM_LOG_WARN("RAID5: delta de la franja %u con version %u, esperada %u",
                             sid, m->hdr.data_version, expected);
            } else {
                uint8_t *frame = ctx->raid.parity + (size_t)sid * ps;
                for (uint32_t i = 0; i < ps; i++) {
                    frame[i] = (uint8_t)(frame[i] ^ delta[i]);
                }
                live->parity_frame_valid = 1u;
                live->version = expected;
                if (live->state == DSM_STRIPE_INSTALL) {
                    live->state = DSM_STRIPE_ACTIVE;
                }
                version = expected;
            }
            pthread_mutex_unlock(&ctx->raid.mu);
        }
    }

    raid_reply_word(ctx, m, MSG_PARITY_ACK, sid, status, version);
    free(delta);
}

void raid_on_parity_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_WIRE_WORD_LEN) return;

    wire_word_t w;
    dsm_unpack_word(m->payload, &w);
    uint64_t now = mono_us();
    uint64_t elapsed = 0;
    bool matched = false;

    pthread_mutex_lock(&ctx->raid.mu);
    for (uint32_t i = 0; i < DSM_RAID_PENDING_MAX; i++) {
        raid_pending_t *p = &ctx->raid.pending[i];
        if (!p->used || p->completed || p->kind != DSM_RAID_PENDING_ACK ||
            p->req_id != m->hdr.req_id) {
            continue;
        }
        elapsed = now - p->t0_us;
        p->status = m->hdr.status;
        p->data_version = m->hdr.data_version;
        p->completed = 1;
        matched = true;
        pthread_cond_broadcast(&ctx->raid.cond);
        break;
    }
    pthread_mutex_unlock(&ctx->raid.mu);

    if (matched) {
        atomic_fetch_add_explicit(&ctx->ctr.parity_latency_us, elapsed,
                                  memory_order_relaxed);
    }
    if (m->hdr.status != DSM_ST_OK) {
        DSM_LOG_WARN("RAID5: acuse de paridad de la franja %u rechazado (estado %u)",
                     w.value, m->hdr.status);
    }
}

void raid_on_stripe_read(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_STRIPE_READ_WIRE_LEN) return;

    uint32_t ps = ctx->raid.page_size;
    if (ps == 0) return;

    stripe_read_wire_t req;
    dsm_unpack_stripe_read(m->payload, &req);

    uint8_t *buf = malloc((size_t)ps * 2u);
    if (!buf) return;
    uint8_t *wire = buf + ps;
    memset(buf, 0, (size_t)ps * 2u);

    uint16_t status = DSM_ST_OK;

    if (req.stripe_id >= ctx->raid.nstripes) {
        status = DSM_ST_NOT_HOME;
    } else if (req.kind == DSM_STRIPE_READ_PARITY) {
        pthread_mutex_lock(&ctx->raid.mu);
        stripe_rt_t *s = &ctx->raid.stripes[req.stripe_id];
        if (s->parity_frame_valid) {
            memcpy(buf, ctx->raid.parity + (size_t)req.stripe_id * ps, ps);
        } else {
            status = DSM_ST_NO_SPACE;
        }
        pthread_mutex_unlock(&ctx->raid.mu);
    } else if (!raid_local_page(ctx, m->hdr.page_id, buf)) {
        status = DSM_ST_NO_SPACE;
    }

    (void)dsm_pack_stripe_data(buf, wire, ps);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_STRIPE_DATA;
    hdr.epoch         = ctx->memb.epoch;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = m->hdr.src_slot;
    hdr.req_id        = m->hdr.req_id;
    hdr.page_id       = m->hdr.page_id;
    hdr.data_version  = m->hdr.data_version;
    hdr.flags         = DSM_FLAG_HAS_DATA;
    hdr.status        = status;
    hdr.payload_len   = ps;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &hdr, wire);

    free(buf);
}

void raid_on_stripe_data(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload) return;
    uint32_t ps = ctx->raid.page_size;
    if (ps == 0 || m->hdr.payload_len != ps) return;

    raid_pending_complete(ctx, m->hdr.req_id, DSM_RAID_PENDING_DATA,
                          m->hdr.status, m->hdr.data_version,
                          m->payload, m->hdr.payload_len);
}

void raid_on_stripe_put(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_STRIPE_WIRE_LEN) return;

    stripe_wire_t sw;
    dsm_unpack_stripe(m->payload, &sw);

    uint16_t status = DSM_ST_OK;
    if (sw.stripe_id >= ctx->raid.nstripes ||
        sw.k < 2u || sw.k > DSM_RAID_MAX_K) {
        status = DSM_ST_BAD_STATE;
    } else {
        pthread_mutex_lock(&ctx->raid.mu);
        stripe_rt_t *s = &ctx->raid.stripes[sw.stripe_id];
        s->k = sw.k;
        memcpy(s->data_page, sw.data_page, sizeof(s->data_page));
        memcpy(s->data_slot, sw.data_slot, sizeof(s->data_slot));
        s->parity_slot = sw.parity_slot;
        s->version = m->hdr.data_version;
        /* El marco de paridad llega despues, como delta completo: hasta
         * confirmarlo la franja no esta operativa y su respaldo local parte
         * de ceros para que el delta la reconstruya tal cual. */
        memset(ctx->raid.parity + (size_t)sw.stripe_id * ctx->raid.page_size,
               0, ctx->raid.page_size);
        s->parity_frame_valid = 0u;
        s->state = DSM_STRIPE_INSTALL;
        pthread_mutex_unlock(&ctx->raid.mu);
    }

    raid_reply_word(ctx, m, MSG_STRIPE_PUT_ACK, sw.stripe_id, status,
                    m->hdr.data_version);
}

void raid_on_stripe_put_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_WIRE_WORD_LEN) return;
    wire_word_t w;
    dsm_unpack_word(m->payload, &w);
    raid_pending_complete(ctx, m->hdr.req_id, DSM_RAID_PENDING_PUT,
                          m->hdr.status, m->hdr.data_version, NULL, 0);
}

void raid_on_state_report(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_WIRE_WORD_LEN) return;
    wire_word_t w;
    dsm_unpack_word(m->payload, &w);
    if (w.value >= ctx->raid.nstripes) return;

    pthread_mutex_lock(&ctx->raid.mu);
    if (ctx->raid.stripes[w.value].state == DSM_STRIPE_ACTIVE) {
        ctx->raid.stripes[w.value].state = DSM_STRIPE_DEGRADED;
    }
    pthread_mutex_unlock(&ctx->raid.mu);
    DSM_LOG_WARN("RAID5: franja %u degradada por caida notificada", w.value);
}

void raid_on_dir_done(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_WIRE_WORD_LEN) return;
    wire_word_t w;
    dsm_unpack_word(m->payload, &w);
    if (w.value >= ctx->raid.nstripes) return;

    pthread_mutex_lock(&ctx->raid.mu);
    ctx->raid.stripes[w.value].state = DSM_STRIPE_ACTIVE;
    pthread_mutex_unlock(&ctx->raid.mu);
}

/* --- Reconstruccion ------------------------------------------------------ */

static bool raid_send_stripe_read(dsm_ctx_t *ctx, uint32_t dst, uint32_t sid,
                                  uint32_t page, uint8_t kind, uint32_t version,
                                  uint8_t *buf, int *pidx, uint32_t *req)
{
    uint8_t raw[DSM_STRIPE_READ_WIRE_LEN];
    stripe_read_wire_t sr = { .stripe_id = sid, .kind = (uint8_t)kind };
    memset(sr.reserved, 0, sizeof(sr.reserved));
    (void)dsm_pack_stripe_read(&sr, raw);

    *pidx = raid_pending_add(ctx, DSM_RAID_PENDING_DATA, buf,
                             ctx->raid.page_size, req);
    if (*pidx < 0) return false;

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_STRIPE_READ;
    hdr.epoch         = ctx->memb.epoch;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = *req;
    hdr.page_id       = page;
    hdr.data_version  = version;
    hdr.payload_len   = DSM_STRIPE_READ_WIRE_LEN;
    if (dsm_net_send(ctx, dst, &hdr, raw) != DSM_OK) {
        raid_pending_cancel(ctx, *pidx, *req);
        *pidx = -1;
        return false;
    }
    return true;
}

/* Instala la descripcion de la franja en el nuevo custodio de paridad y le
 * entrega el marco completo como un delta con la siguiente version. */
static bool raid_install_parity(dsm_ctx_t *ctx, uint32_t sid, uint32_t dst,
                                const stripe_rt_t *s, const uint8_t *frame,
                                uint32_t version)
{
    stripe_wire_t sw;
    memset(&sw, 0, sizeof(sw));
    sw.stripe_id = sid;
    sw.k = (uint8_t)s->k;
    sw.parity_slot = dst;
    memcpy(sw.data_page, s->data_page, sizeof(sw.data_page));
    memcpy(sw.data_slot, s->data_slot, sizeof(sw.data_slot));

    uint8_t raw[DSM_STRIPE_WIRE_LEN];
    (void)dsm_pack_stripe(&sw, raw);

    uint32_t req = 0;
    int pidx = raid_pending_add(ctx, DSM_RAID_PENDING_PUT, NULL, 0, &req);
    if (pidx < 0) return false;

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_STRIPE_PUT;
    hdr.epoch         = ctx->memb.epoch;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req;
    hdr.page_id       = s->data_page[0];
    hdr.data_version  = version;
    hdr.payload_len   = DSM_STRIPE_WIRE_LEN;
    if (dsm_net_send(ctx, dst, &hdr, raw) != DSM_OK) {
        raid_pending_cancel(ctx, pidx, req);
        return false;
    }

    uint16_t status = DSM_ST_OK;
    int wr = raid_pending_wait(ctx, pidx, req, ctx->cfg.request_timeout_ms,
                               &status, NULL);
    if (wr != DSM_OK || status != DSM_ST_OK) return false;

    uint32_t ps = ctx->raid.page_size;
    uint8_t *payload = malloc((size_t)DSM_PARITY_DELTA_HDR_LEN + ps);
    if (!payload) return false;
    parity_delta_wire_t d = { .stripe_id = sid };
    size_t plen = dsm_pack_parity_delta(&d, frame, ps, payload);

    uint32_t dreq = 0;
    int didx = raid_pending_add(ctx, DSM_RAID_PENDING_ACK, NULL, 0, &dreq);
    if (didx < 0) {
        free(payload);
        return false;
    }

    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_PARITY_DELTA;
    hdr.epoch         = ctx->memb.epoch;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = dreq;
    hdr.page_id       = s->data_page[0];
    hdr.data_version  = version + 1u;
    hdr.payload_len   = (uint32_t)plen;
    int rc = dsm_net_send(ctx, dst, &hdr, payload);
    free(payload);
    if (rc != DSM_OK) {
        raid_pending_cancel(ctx, didx, dreq);
        return false;
    }

    /* La paridad solo cuenta como instalada cuando el custodio acuso el
     * delta completo: sin ese acuse no se publica la franja como valida. */
    uint16_t delta_status = DSM_ST_OK;
    int dwr = raid_pending_wait(ctx, didx, dreq, ctx->cfg.request_timeout_ms,
                                &delta_status, NULL);
    return dwr == DSM_OK && delta_status == DSM_ST_OK;
}

static void raid_deliver_page(dsm_ctx_t *ctx, uint32_t page, uint32_t home,
                              const uint8_t *data, uint32_t sid)
{
    uint32_t ps = ctx->raid.page_size;
    if (home == ctx->memb.self_slot) {
        if (dsm_mem_backing_store(&ctx->mem, page, data) == DSM_OK) {
            dsm_mem_stripe_lock(&ctx->mem, page);
            dsm_mem_install_page(&ctx->mem, page, data);
            (void)dsm_mem_set_prot(&ctx->mem, page, PROT_READ);
            ctx->mem.local[page].state = DSM_PAGE_S;
            dsm_mem_stripe_unlock(&ctx->mem, page);
        } else {
            DSM_LOG_WARN("RAID5: sin marco para instalar la pagina %u", page);
        }
    } else {
        uint8_t *wire = malloc(ps);
        if (wire) {
            size_t n = dsm_pack_page_move(data, wire, ps);
            uint16_t status = DSM_ST_OK;
            uint32_t rx_len = 0;
            int rc = dsm_net_rpc(ctx, home, MSG_PAGE_MOVE, page, 0,
                                 DSM_ACCESS_READ, DSM_FLAG_HAS_DATA,
                                 wire, (uint32_t)n, &status, NULL, &rx_len);
            if (rc != DSM_OK || status != DSM_ST_OK) {
                DSM_LOG_WARN("RAID5: traslado de la pagina %u al slot %u fallo (%d/%u)",
                             page, home, rc, status);
            }
            free(wire);
        }
    }

    if (page < ctx->msi.cap) {
        pthread_mutex_lock(&ctx->msi.dir_mu);
        dir_entry_t *e = &ctx->msi.dir[page];
        ctx->msi.home_map[page] = home;
        e->version++;
        e->state = DSM_DIR_SHARED;
        e->owner = DSM_SLOT_NONE;
        memset(e->copyset, 0, sizeof(e->copyset));
        if (home < MAX_NODES) {
            e->copyset[home / 8u] |= (uint8_t)(1u << (home % 8u));
        }
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    }
    if (home != ctx->memb.self_slot) {
        raid_send_word(ctx, home, MSG_DIR_DONE, sid, DSM_ST_OK, 0, 0, page);
    }
}

/* XOR de todas las piezas reunidas. Con perdida de un dato, las piezas son
 * la paridad y los k-1 datos supervivientes, de modo que el resultado es el
 * dato perdido. Con perdida de la paridad, son los k datos. */
void raid_xor_rebuild(const uint8_t **parts, uint32_t n, uint32_t ps,
                      uint8_t *out)
{
    memset(out, 0, ps);
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t b = 0; b < ps; b++) {
            out[b] = (uint8_t)(out[b] ^ parts[i][b]);
        }
    }
}

/* Reconstruccion completa de las franjas afectadas. Se ejecuta en un hilo
 * propio: el hilo listener que recibe la baja debe seguir aceptando las
 * respuestas STRIPE_DATA que esta funcion espera. */
static void raid_rebuild_run(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES || !ctx->raid.stripes) return;

    uint32_t ps = ctx->raid.page_size;
    uint32_t k = ctx->raid.k;
    if (ps == 0 || k < 2u || k > DSM_RAID_MAX_K) return;

    for (uint32_t sid = 0; sid < ctx->raid.nstripes; sid++) {
        stripe_rt_t old_s;
        pthread_mutex_lock(&ctx->raid.mu);
        old_s = ctx->raid.stripes[sid];
        pthread_mutex_unlock(&ctx->raid.mu);

        bool affected = (old_s.parity_slot == slot);
        uint32_t lost = DSM_RAID_MAX_K;
        for (uint32_t j = 0; j < k; j++) {
            if (old_s.data_slot[j] == slot) {
                affected = true;
                lost = j;
                break;
            }
        }
        if (!affected) continue;

        pthread_mutex_lock(&ctx->raid.mu);
        ctx->raid.stripes[sid].state = DSM_STRIPE_DEGRADED;
        pthread_mutex_unlock(&ctx->raid.mu);

        /* Se recalculan los custodios despues de la baja. Toda consulta a
         * membresia ocurre antes de tomar el mutex de la franja. */
        stripe_rt_t s;
        raid_stripe_sync(ctx, sid, &s);
        if (raid_stripe_leader(ctx, &s) != ctx->memb.self_slot) continue;

        pthread_mutex_lock(&ctx->raid.mu);
        ctx->raid.stripes[sid].state = DSM_STRIPE_REBUILD;
        pthread_mutex_unlock(&ctx->raid.mu);

        for (uint32_t j = 0; j < k; j++) {
            uint32_t peer = s.data_slot[j];
            if (peer != DSM_SLOT_NONE && peer != ctx->memb.self_slot &&
                membership_is_alive(ctx, peer)) {
                raid_send_word(ctx, peer, MSG_STATE_REPORT, sid,
                               DSM_ST_OK, s.version, 0, s.data_page[j]);
            }
        }
        if (s.parity_slot != DSM_SLOT_NONE &&
            s.parity_slot != ctx->memb.self_slot &&
            membership_is_alive(ctx, s.parity_slot)) {
            raid_send_word(ctx, s.parity_slot, MSG_STATE_REPORT, sid,
                           DSM_ST_OK, s.version, 0, s.data_page[0]);
        }

        bool parity_loss = (lost == DSM_RAID_MAX_K);
        bool parity_gone = (old_s.parity_slot == slot);
        if (!parity_loss && (parity_gone || s.parity_slot == DSM_SLOT_NONE ||
                             !membership_is_alive(ctx, s.parity_slot))) {
            /* La franja perdio su copia y no queda paridad utilizable: solo
             * la pagina del nodo caido se perdio de verdad, el resto sigue
             * vivo en sus homes. */
            DSM_LOG_WARN("RAID5: franja %u sin paridad ni copia del nodo caido", sid);
            if (s.data_page[lost] != DSM_PAGE_ID_NONE) {
                raid_invalidate_page(ctx, s.data_page[lost]);
            }
            pthread_mutex_lock(&ctx->raid.mu);
            ctx->raid.stripes[sid].state = DSM_STRIPE_DEGRADED;
            pthread_mutex_unlock(&ctx->raid.mu);
            continue;
        }
        if (parity_loss && s.parity_slot == DSM_SLOT_NONE) {
            pthread_mutex_lock(&ctx->raid.mu);
            ctx->raid.stripes[sid].state = DSM_STRIPE_DEGRADED;
            pthread_mutex_unlock(&ctx->raid.mu);
            continue;
        }

        /* Se reune la paridad y los datos supervivientes. Las paginas que
         * no existen en la franja (final truncado) no aportan pieza: el
         * XOR las excluye de entrada en lugar de pedir un marco inexistente. */
        uint8_t *gather = calloc(DSM_RAID_MAX_GATHER, ps);
        if (!gather) continue;
        int pidx[DSM_RAID_MAX_GATHER];
        uint32_t reqs[DSM_RAID_MAX_GATHER];
        uint32_t pages[DSM_RAID_MAX_GATHER];
        uint32_t srcs[DSM_RAID_MAX_GATHER];
        uint8_t kinds[DSM_RAID_MAX_GATHER];
        uint32_t ngather = 0;
        bool sent = true;

        for (uint32_t j = 0; j < k && sent; j++) {
            if (!parity_loss && j == lost) continue;
            if (s.data_page[j] == DSM_PAGE_ID_NONE) continue;

            pidx[ngather] = -1;
            reqs[ngather] = 0;
            srcs[ngather] = s.data_slot[j];
            pages[ngather] = s.data_page[j];
            kinds[ngather] = (uint8_t)DSM_STRIPE_READ_DATA;
            if (srcs[ngather] == DSM_SLOT_NONE ||
                !membership_is_alive(ctx, srcs[ngather]) ||
                !raid_send_stripe_read(ctx, srcs[ngather], sid,
                                       pages[ngather], kinds[ngather],
                                       s.version,
                                       gather + (size_t)ngather * ps,
                                       &pidx[ngather], &reqs[ngather])) {
                sent = false;
                break;
            }
            ngather++;
        }

        if (sent && !parity_loss) {
            pidx[ngather] = -1;
            reqs[ngather] = 0;
            srcs[ngather] = s.parity_slot;
            pages[ngather] = s.data_page[0];
            kinds[ngather] = (uint8_t)DSM_STRIPE_READ_PARITY;
            if (srcs[ngather] == DSM_SLOT_NONE ||
                !membership_is_alive(ctx, srcs[ngather]) ||
                !raid_send_stripe_read(ctx, srcs[ngather], sid,
                                       pages[ngather], kinds[ngather],
                                       s.version,
                                       gather + (size_t)ngather * ps,
                                       &pidx[ngather], &reqs[ngather])) {
                sent = false;
            } else {
                ngather++;
            }
        }

        bool have = (sent && ngather > 0);
        if (have) {
            for (uint32_t i = 0; i < ngather; i++) {
                uint16_t status = DSM_ST_OK;
                uint32_t rx_version = 0;
                int wr = raid_pending_wait(ctx, pidx[i], reqs[i],
                                           ctx->cfg.request_timeout_ms,
                                           &status, &rx_version);
                if (wr != DSM_OK || status != DSM_ST_OK) {
                    have = false;
                    break;
                }
                if (rx_version != s.version) {
                    DSM_LOG_WARN("RAID5: franja %u, copia del slot %u con version %u "
                                 "en lugar de %u; descartada",
                                 sid, srcs[i], rx_version, s.version);
                    have = false;
                    break;
                }
            }
        }

        if (have) {
            uint8_t *xor = malloc(ps);
            if (xor) {
                const uint8_t *parts[DSM_RAID_MAX_GATHER];
                for (uint32_t i = 0; i < ngather; i++) {
                    parts[i] = gather + (size_t)i * ps;
                }
                raid_xor_rebuild(parts, ngather, ps, xor);
                if (parity_loss) {
                    if (s.parity_slot == ctx->memb.self_slot) {
                        /* El lider es el nuevo custodio: el marco
                         * reconstruido se instala directo en el respaldo
                         * local, sin ida y vuelta por la red. */
                        pthread_mutex_lock(&ctx->raid.mu);
                        memcpy(ctx->raid.parity + (size_t)sid * ps, xor, ps);
                        ctx->raid.stripes[sid].parity_frame_valid = 1u;
                        pthread_mutex_unlock(&ctx->raid.mu);
                        have = true;
                    } else {
                        have = raid_install_parity(ctx, sid, s.parity_slot,
                                                   &s, xor, s.version);
                    }
                } else {
                    uint32_t page = s.data_page[lost];
                    if (page != DSM_PAGE_ID_NONE) {
                        raid_deliver_page(ctx, page, s.data_slot[lost],
                                          xor, sid);
                    }
                }
                free(xor);
            } else {
                have = false;
            }
        }

        for (uint32_t i = 0; i < ngather; i++) {
            if (pidx[i] >= 0) raid_pending_cancel(ctx, pidx[i], reqs[i]);
        }
        free(gather);

        if (!have) {
            /* Solo se invalida lo que de verdad se perdio: con perdida de
             * paridad las paginas siguen vivas en sus homes; con perdida
             * de dato, la unica perdida es la del nodo caido. */
            if (!parity_loss && s.data_page[lost] != DSM_PAGE_ID_NONE) {
                raid_invalidate_page(ctx, s.data_page[lost]);
            }
            DSM_LOG_WARN("RAID5: franja %u irrecuperable tras la baja del slot %u",
                         sid, slot);
            pthread_mutex_lock(&ctx->raid.mu);
            ctx->raid.stripes[sid].state = DSM_STRIPE_DEGRADED;
            if (parity_loss) {
                ctx->raid.stripes[sid].parity_frame_valid = 0u;
            }
            pthread_mutex_unlock(&ctx->raid.mu);
            continue;
        }

        atomic_fetch_add_explicit(&ctx->ctr.stripe_rebuilds, 1,
                                  memory_order_relaxed);
        pthread_mutex_lock(&ctx->raid.mu);
        ctx->raid.stripes[sid].state = DSM_STRIPE_ACTIVE;
        if (parity_loss) {
            ctx->raid.stripes[sid].parity_frame_valid = 1u;
            if (ctx->raid.stripes[sid].version < s.version + 1u) {
                ctx->raid.stripes[sid].version = s.version + 1u;
            }
        }
        pthread_mutex_unlock(&ctx->raid.mu);
    }
}

static bool raid_rebuild_enqueue(uint32_t slot)
{
    for (uint32_t i = 0; i < rebuild_qlen; i++) {
        uint32_t idx = (rebuild_qhead + i) % DSM_RAID_REBUILD_QUEUE_CAP;
        if (rebuild_queue[idx] == slot) return false;
    }
    if (rebuild_qlen >= DSM_RAID_REBUILD_QUEUE_CAP) {
        DSM_LOG_WARN("RAID5: cola de reconstruccion llena; baja del slot %u omitida",
                     slot);
        return false;
    }
    rebuild_queue[rebuild_qtail] = slot;
    rebuild_qtail = (rebuild_qtail + 1u) % DSM_RAID_REBUILD_QUEUE_CAP;
    rebuild_qlen++;
    return true;
}

static void *raid_rebuild_thread(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    dsm_raid_t *r = &ctx->raid;

    for (;;) {
        pthread_mutex_lock(&r->rebuild_mu);
        if (rebuild_qlen == 0) {
            r->rebuild_active = false;
            pthread_mutex_unlock(&r->rebuild_mu);
            break;
        }
        uint32_t slot = rebuild_queue[rebuild_qhead];
        rebuild_qhead = (rebuild_qhead + 1u) % DSM_RAID_REBUILD_QUEUE_CAP;
        rebuild_qlen--;
        r->rebuild_pending = (rebuild_qlen > 0);
        pthread_mutex_unlock(&r->rebuild_mu);

        raid_rebuild_run(ctx, slot);
    }
    return NULL;
}

void raid_on_node_down(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES || !ctx->cfg.redundancy_raid5 ||
        !ctx->raid.stripes) return;

    dsm_raid_t *r = &ctx->raid;
    pthread_mutex_lock(&r->rebuild_mu);
    bool added = raid_rebuild_enqueue(slot);
    if (!added || r->rebuild_active) {
        pthread_mutex_unlock(&r->rebuild_mu);
        return;
    }
    r->rebuild_active = true;
    r->rebuild_pending = true;
    int rc = pthread_create(&r->rebuild_thread, NULL, raid_rebuild_thread, ctx);
    if (rc != 0) {
        /* La baja mas reciente no se proceso: se retira de la cola por el
         * final, sin descartar las anteriores que siguen pendientes. */
        rebuild_qtail = (rebuild_qtail + DSM_RAID_REBUILD_QUEUE_CAP - 1u) %
                        DSM_RAID_REBUILD_QUEUE_CAP;
        rebuild_qlen--;
        r->rebuild_active = false;
        r->rebuild_pending = (rebuild_qlen > 0);
        pthread_mutex_unlock(&r->rebuild_mu);
        DSM_LOG_WARN("RAID5: no se pudo iniciar el hilo de reconstruccion (%d)", rc);
        return;
    }
    pthread_mutex_unlock(&r->rebuild_mu);
}
