#include "wire.h"
#include <string.h>
#include <arpa/inet.h>

/* Utilidades para enteros en orden de red */
static inline void put_u16(uint8_t *p, uint16_t v) { v = htons(v); memcpy(p, &v, 2); }
static inline void put_u32(uint8_t *p, uint32_t v) { v = htonl(v); memcpy(p, &v, 4); }
static inline void put_u64(uint8_t *p, uint64_t v) { v = dsm_htonll(v); memcpy(p, &v, 8); }

static inline uint16_t get_u16(const uint8_t *p) { uint16_t v; memcpy(&v, p, 2); return ntohs(v); }
static inline uint32_t get_u32(const uint8_t *p) { uint32_t v; memcpy(&v, p, 4); return ntohl(v); }
static inline uint64_t get_u64(const uint8_t *p) { uint64_t v; memcpy(&v, p, 8); return dsm_ntohll(v); }

uint64_t dsm_htonll(uint64_t v)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl((uint32_t)(v & 0xFFFFFFFFu)) << 32) |
           (uint64_t)htonl((uint32_t)(v >> 32));
#else
    return v;
#endif
}

uint64_t dsm_ntohll(uint64_t v)
{
    return dsm_htonll(v);
}

/* Tabla CRC32 precalculada para IEEE 802.3 (0xEDB88320) */
static uint32_t crc_table[256];
static int crc_initialized = 0;

static void crc32_init_table(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        crc_table[i] = c;
    }
    crc_initialized = 1;
}

uint32_t dsm_crc32(uint32_t seed, const void *buf, size_t len)
{
    if (!crc_initialized) {
        crc32_init_table();
    }
    const uint8_t *p = (const uint8_t *)buf;
    uint32_t c = seed ^ 0xFFFFFFFFu;
    while (len--) {
        c = crc_table[(c ^ *p++) & 0xFFu] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

void dsm_wire_pack(const msg_header_t *h, uint8_t out[DSM_HDR_LEN])
{
    memset(out, 0, DSM_HDR_LEN);
    put_u32(out + OFF_MAGIC, h->magic);
    put_u32(out + OFF_CLUSTER_ID, h->cluster_id);
    put_u16(out + OFF_PROTO_VER, h->proto_version);
    put_u16(out + OFF_TYPE, h->type);
    put_u64(out + OFF_EPOCH, h->epoch);
    put_u32(out + OFF_SRC_SLOT, h->src_slot);
    put_u32(out + OFF_DST_SLOT, h->dst_slot);
    put_u32(out + OFF_REQ_ID, h->req_id);
    put_u32(out + OFF_PAGE_ID, h->page_id);
    put_u32(out + OFF_TSEQ, h->tseq);
    put_u32(out + OFF_DATA_VERSION, h->data_version);
    out[OFF_ACCESS] = h->access;
    out[OFF_FLAGS]  = h->flags;
    put_u16(out + OFF_STATUS, h->status);
    put_u32(out + OFF_PAYLOAD_LEN, h->payload_len);
    put_u32(out + OFF_CRC32, 0); /* CRC se rellena en dsm_wire_finish */
}

int dsm_wire_unpack(const uint8_t in[DSM_HDR_LEN], msg_header_t *out)
{
    out->magic         = get_u32(in + OFF_MAGIC);
    out->cluster_id    = get_u32(in + OFF_CLUSTER_ID);
    out->proto_version = get_u16(in + OFF_PROTO_VER);
    out->type          = get_u16(in + OFF_TYPE);
    out->epoch         = get_u64(in + OFF_EPOCH);
    out->src_slot      = get_u32(in + OFF_SRC_SLOT);
    out->dst_slot      = get_u32(in + OFF_DST_SLOT);
    out->req_id        = get_u32(in + OFF_REQ_ID);
    out->page_id       = get_u32(in + OFF_PAGE_ID);
    out->tseq          = get_u32(in + OFF_TSEQ);
    out->data_version  = get_u32(in + OFF_DATA_VERSION);
    out->access        = in[OFF_ACCESS];
    out->flags         = in[OFF_FLAGS];
    out->status        = get_u16(in + OFF_STATUS);
    out->payload_len   = get_u32(in + OFF_PAYLOAD_LEN);
    out->crc32         = get_u32(in + OFF_CRC32);

    if (out->magic != DSM_MAGIC || out->proto_version != DSM_PROTO_VERSION) {
        return DSM_ERR_PROTO;
    }
    return DSM_OK;
}

uint32_t dsm_wire_finish(msg_header_t *hdr, uint8_t out[DSM_HDR_LEN],
                         const void *payload, uint32_t payload_len)
{
    hdr->payload_len = payload_len;
    dsm_wire_pack(hdr, out);

    uint32_t c = dsm_crc32(0, out, DSM_HDR_LEN);
    if (payload && payload_len > 0) {
        c = dsm_crc32(c, payload, payload_len);
    }
    hdr->crc32 = c;
    put_u32(out + OFF_CRC32, c);
    return c;
}

int dsm_wire_validate(const msg_header_t *hdr, const uint8_t raw[DSM_HDR_LEN],
                      const void *payload, uint32_t cluster_id)
{
    if (hdr->magic != DSM_MAGIC) {
        return DSM_ERR_PROTO;
    }
    if (hdr->proto_version != DSM_PROTO_VERSION) {
        return DSM_ERR_PROTO;
    }
    if (hdr->cluster_id != cluster_id) {
        return DSM_ERR_PROTO;
    }
    if (hdr->payload_len > DSM_MAX_PAYLOAD) {
        return DSM_ERR_PROTO;
    }
    /* Una cabecera que declara carga pero llega sin buffer no se puede
     * verificar: si se saltara el CRC de la carga, una trama construida con
     * dsm_wire_finish(hdr, raw, NULL, len) pasaria como integra sin haber
     * comprobado un solo byte del cuerpo. */
    if (hdr->payload_len > 0 && payload == NULL) {
        return DSM_ERR_PROTO;
    }

    uint8_t zero_crc_hdr[DSM_HDR_LEN];
    memcpy(zero_crc_hdr, raw, DSM_HDR_LEN);
    put_u32(zero_crc_hdr + OFF_CRC32, 0);

    uint32_t calc_crc = dsm_crc32(0, zero_crc_hdr, DSM_HDR_LEN);
    if (payload && hdr->payload_len > 0) {
        calc_crc = dsm_crc32(calc_crc, payload, hdr->payload_len);
    }

    if (calc_crc != hdr->crc32) {
        return DSM_ERR_PROTO;
    }
    return DSM_OK;
}

bool wire_payload_ok(uint16_t type, uint8_t flags, uint32_t len, uint32_t page_size)
{
    switch (type) {
    case MSG_HELLO:
        return len == 20u; /* uuid[16] + raw_hash[4] */
    case MSG_WELCOME:
        return len >= 32u && len <= DSM_MAX_PAYLOAD;
    case MSG_JOIN:
        return len == DSM_MEMBERSHIP_WIRE_LEN;
    case MSG_JOIN_ACK:
        return len == 0;
    case MSG_HEARTBEAT:
        return len == 12u; /* incarnation[8] + free_pages[4] */
    case MSG_LEAVE:
    case MSG_NODE_DOWN:
        return len == 0 || len == 28u;
    case MSG_PAGE_REQUEST:
    case MSG_DOWNGRADE:
    case MSG_WRITEBACK_ACK:
    case MSG_LOCK_REQ:
    case MSG_LOCK_GRANT:
    case MSG_LOCK_REL:
    case MSG_NACK:
        return len == 0;
    case MSG_GRANT:
        if (flags & DSM_FLAG_HAS_DATA) {
            return page_size != 0 && len == page_size;
        }
        return len == 0;
    case MSG_PAGE_DATA:
    case MSG_WRITEBACK:
        return page_size != 0 && len == page_size;
    case MSG_INVALIDATE:
        return len == 0;
    case MSG_INVALIDATE_ACK:
    case MSG_DOWNGRADE_ACK:
        if (flags & DSM_FLAG_HAS_DATA) {
            return page_size != 0 && len == page_size;
        }
        return len == 0;
    case MSG_BARRIER_ARRIVE:
        return len == 8u; /* n[4] + generation[4] */
    case MSG_BARRIER_GO:
        return len == 4u; /* generation[4] */
    case MSG_LOCK_MIGRATE:
        return len == DSM_LOCK_MIGRATE_WIRE_LEN;
    case MSG_LOCK_STATE:
        return len >= DSM_LOCK_STATE_HDR_LEN &&
               (len - DSM_LOCK_STATE_HDR_LEN) % DSM_LOCK_STATE_REC_LEN == 0 &&
               (len - DSM_LOCK_STATE_HDR_LEN) / DSM_LOCK_STATE_REC_LEN <=
                   DSM_LOCK_STATE_MAX_RECS;
    case MSG_DIST_RUN:
        return len == DSM_DIST_RUN_WIRE_LEN;
    case MSG_DIST_DONE:
        return len == DSM_DIST_DONE_WIRE_LEN;
    case MSG_PAGE_MOVE:
    case MSG_STRIPE_DATA:
        return page_size != 0 && len == page_size;
    case MSG_PAGE_MOVE_ACK:
        return len == 0;
    case MSG_PARITY_DELTA:
        return page_size != 0 &&
               page_size <= DSM_MAX_PAYLOAD - DSM_PARITY_DELTA_HDR_LEN &&
               len == DSM_PARITY_DELTA_WIRE_LEN(page_size);
    case MSG_PARITY_ACK:
    case MSG_STRIPE_PUT_ACK:
    case MSG_STATE_REPORT:
    case MSG_DIR_DONE:
        return len == DSM_WIRE_WORD_LEN;
    case MSG_STRIPE_PUT:
        return len == DSM_STRIPE_WIRE_LEN;
    case MSG_STRIPE_READ:
        return len == DSM_STRIPE_READ_WIRE_LEN;
    case MSG_ALLOC_REQ:
    case MSG_ALLOC_RESP:
    case MSG_FREE_REQ:
    case MSG_FREE_RESP:
        return len == DSM_ALLOC_MSG_LEN;
    default:
        return len <= DSM_MAX_PAYLOAD;
    }
}

size_t dsm_pack_membership(const membership_wire_t *m, uint8_t out[DSM_MEMBERSHIP_WIRE_LEN])
{
    memcpy(out, m->uuid, 16);
    put_u32(out + 16, m->ip);
    put_u16(out + 20, m->port);
    out[22] = m->slot;
    out[23] = m->state;
    put_u64(out + 24, m->incarnation);
    put_u32(out + 32, m->mem_pages);
    return DSM_MEMBERSHIP_WIRE_LEN;
}

void dsm_unpack_membership(const uint8_t in[DSM_MEMBERSHIP_WIRE_LEN], membership_wire_t *m)
{
    memcpy(m->uuid, in, 16);
    m->ip          = get_u32(in + 16);
    m->port        = get_u16(in + 20);
    m->slot        = in[22];
    m->state       = in[23];
    m->incarnation = get_u64(in + 24);
    m->mem_pages   = get_u32(in + 32);
}

size_t dsm_pack_alloc_rec(const alloc_rec_t *a, uint8_t out[DSM_ALLOC_REC_LEN])
{
    put_u32(out + 0, a->alloc_id);
    put_u32(out + 4, a->first_page);
    put_u32(out + 8, a->npages);
    put_u32(out + 12, a->slot);
    put_u32(out + 16, a->pid);
    put_u32(out + 20, a->reserved);
    return DSM_ALLOC_REC_LEN;
}

void dsm_unpack_alloc_rec(const uint8_t in[DSM_ALLOC_REC_LEN], alloc_rec_t *a)
{
    a->alloc_id   = get_u32(in + 0);
    a->first_page = get_u32(in + 4);
    a->npages     = get_u32(in + 8);
    a->slot       = get_u32(in + 12);
    a->pid        = get_u32(in + 16);
    a->reserved   = get_u32(in + 20);
}

size_t dsm_pack_dir_entry(const dir_entry_t *d, uint8_t out[DSM_DIR_ENTRY_LEN])
{
    put_u32(out + 0, d->page_id);
    put_u32(out + 4, d->owner);
    put_u32(out + 8, d->version);
    put_u32(out + 12, d->tseq);
    put_u32(out + 16, d->frame);
    put_u32(out + 20, d->stripe_id);
    put_u32(out + 24, d->wait_head);
    put_u32(out + 28, d->wait_tail);
    put_u32(out + 32, d->txn);
    out[36] = d->state;
    out[37] = d->busy;
    put_u16(out + 38, d->reserved);
    memcpy(out + 40, d->copyset, 32);
    return DSM_DIR_ENTRY_LEN;
}

void dsm_unpack_dir_entry(const uint8_t in[DSM_DIR_ENTRY_LEN], dir_entry_t *d)
{
    d->page_id   = get_u32(in + 0);
    d->owner     = get_u32(in + 4);
    d->version   = get_u32(in + 8);
    d->tseq      = get_u32(in + 12);
    d->frame     = get_u32(in + 16);
    d->stripe_id = get_u32(in + 20);
    d->wait_head = get_u32(in + 24);
    d->wait_tail = get_u32(in + 28);
    d->txn       = get_u32(in + 32);
    d->state     = in[36];
    d->busy      = in[37];
    d->reserved  = get_u16(in + 38);
    memcpy(d->copyset, in + 40, 32);
}

/* --- Ejecución distribuida --- */

size_t dsm_pack_dist_run(const dist_run_wire_t *d, uint8_t out[DSM_DIST_RUN_WIRE_LEN])
{
    put_u32(out + 0, d->job);
    put_u32(out + 4, d->kernel);
    put_u32(out + 8, d->part_id);
    put_u32(out + 12, d->nparts);
    put_u32(out + 16, d->deadline_ms);
    put_u64(out + 20, d->src_va);
    put_u64(out + 28, d->dst_va);
    put_u64(out + 36, d->nbytes);
    put_u32(out + 44, d->launch_seq);
    return DSM_DIST_RUN_WIRE_LEN;
}

void dsm_unpack_dist_run(const uint8_t in[DSM_DIST_RUN_WIRE_LEN], dist_run_wire_t *d)
{
    d->job         = get_u32(in + 0);
    d->kernel      = get_u32(in + 4);
    d->part_id     = get_u32(in + 8);
    d->nparts      = get_u32(in + 12);
    d->deadline_ms = get_u32(in + 16);
    d->src_va      = get_u64(in + 20);
    d->dst_va      = get_u64(in + 28);
    d->nbytes      = get_u64(in + 36);
    d->launch_seq  = get_u32(in + 44);
}

size_t dsm_pack_dist_done(const dist_done_wire_t *d, uint8_t out[DSM_DIST_DONE_WIRE_LEN])
{
    put_u32(out + 0, d->job);
    put_u32(out + 4, d->part_id);
    put_u32(out + 8, d->status);
    put_u32(out + 12, d->items);
    put_u64(out + 16, d->crc);
    put_u64(out + 24, d->us);
    put_u32(out + 32, d->launch_seq);
    put_u32(out + 36, d->faults_r);
    put_u32(out + 40, d->faults_w);
    return DSM_DIST_DONE_WIRE_LEN;
}

void dsm_unpack_dist_done(const uint8_t in[DSM_DIST_DONE_WIRE_LEN], dist_done_wire_t *d)
{
    d->job        = get_u32(in + 0);
    d->part_id    = get_u32(in + 4);
    d->status     = get_u32(in + 8);
    d->items      = get_u32(in + 12);
    d->crc        = get_u64(in + 16);
    d->us         = get_u64(in + 24);
    d->launch_seq = get_u32(in + 32);
    d->faults_r   = get_u32(in + 36);
    d->faults_w   = get_u32(in + 40);
}

/* --- Paridad y migración de páginas --- */

size_t dsm_pack_stripe(const stripe_wire_t *s, uint8_t out[DSM_STRIPE_WIRE_LEN])
{
    size_t off = 12;

    put_u32(out + 0, s->stripe_id);
    out[4] = s->k;
    memcpy(out + 5, s->reserved, 3);
    put_u32(out + 8, s->parity_slot);
    for (uint32_t i = 0; i < DSM_STRIPE_DATA_SLOTS; i++) {
        put_u32(out + off, s->data_page[i]);
        off += 4;
    }
    for (uint32_t i = 0; i < DSM_STRIPE_DATA_SLOTS; i++) {
        put_u32(out + off, s->data_slot[i]);
        off += 4;
    }
    return DSM_STRIPE_WIRE_LEN;
}

void dsm_unpack_stripe(const uint8_t in[DSM_STRIPE_WIRE_LEN], stripe_wire_t *s)
{
    size_t off = 12;

    s->stripe_id = get_u32(in + 0);
    s->k         = in[4];
    memcpy(s->reserved, in + 5, 3);
    s->parity_slot = get_u32(in + 8);
    for (uint32_t i = 0; i < DSM_STRIPE_DATA_SLOTS; i++) {
        s->data_page[i] = get_u32(in + off);
        off += 4;
    }
    for (uint32_t i = 0; i < DSM_STRIPE_DATA_SLOTS; i++) {
        s->data_slot[i] = get_u32(in + off);
        off += 4;
    }
}

size_t dsm_pack_parity_delta(const parity_delta_wire_t *d, const uint8_t *delta,
                             uint32_t page_size, uint8_t *out)
{
    if (!d || !delta || !out || page_size == 0 ||
        page_size > DSM_MAX_PAYLOAD - DSM_PARITY_DELTA_HDR_LEN) {
        return 0;
    }
    put_u32(out + 0, d->stripe_id);
    memcpy(out + DSM_PARITY_DELTA_HDR_LEN, delta, page_size);
    return DSM_PARITY_DELTA_WIRE_LEN(page_size);
}

void dsm_unpack_parity_delta(const uint8_t *in, uint32_t page_size,
                             parity_delta_wire_t *d, uint8_t *delta)
{
    if (!in || !d || !delta || page_size == 0 ||
        page_size > DSM_MAX_PAYLOAD - DSM_PARITY_DELTA_HDR_LEN) {
        return;
    }
    d->stripe_id = get_u32(in + 0);
    memcpy(delta, in + DSM_PARITY_DELTA_HDR_LEN, page_size);
}

size_t dsm_pack_word(const wire_word_t *w, uint8_t out[DSM_WIRE_WORD_LEN])
{
    put_u32(out + 0, w->value);
    return DSM_WIRE_WORD_LEN;
}

void dsm_unpack_word(const uint8_t in[DSM_WIRE_WORD_LEN], wire_word_t *w)
{
    w->value = get_u32(in + 0);
}

size_t dsm_pack_stripe_read(const stripe_read_wire_t *r, uint8_t out[DSM_STRIPE_READ_WIRE_LEN])
{
    put_u32(out + 0, r->stripe_id);
    out[4] = r->kind;
    memcpy(out + 5, r->reserved, 3);
    return DSM_STRIPE_READ_WIRE_LEN;
}

void dsm_unpack_stripe_read(const uint8_t in[DSM_STRIPE_READ_WIRE_LEN], stripe_read_wire_t *r)
{
    r->stripe_id = get_u32(in + 0);
    r->kind      = in[4];
    memcpy(r->reserved, in + 5, 3);
}

/* --- Coordinación del espacio global de la vista --- */

size_t dsm_pack_alloc_msg(const alloc_msg_t *a, uint8_t out[DSM_ALLOC_MSG_LEN])
{
    put_u32(out + 0, a->npages);
    put_u32(out + 4, a->first_page);
    put_u32(out + 8, a->status);
    put_u32(out + 12, a->pid);
    return DSM_ALLOC_MSG_LEN;
}

void dsm_unpack_alloc_msg(const uint8_t in[DSM_ALLOC_MSG_LEN], alloc_msg_t *a)
{
    a->npages     = get_u32(in + 0);
    a->first_page = get_u32(in + 4);
    a->status     = get_u32(in + 8);
    a->pid        = get_u32(in + 12);
}

size_t dsm_pack_page_payload(const uint8_t *data, uint8_t *out, uint32_t len)
{
    if (!data || !out || len == 0) {
        return 0;
    }
    memcpy(out, data, len);
    return len;
}

void dsm_unpack_page_payload(const uint8_t *in, uint8_t *data, uint32_t len)
{
    if (!in || !data || len == 0) {
        return;
    }
    memcpy(data, in, len);
}

size_t dsm_pack_stripe_data(const uint8_t *data, uint8_t *out, uint32_t len)
{
    return dsm_pack_page_payload(data, out, len);
}

void dsm_unpack_stripe_data(const uint8_t *in, uint8_t *data, uint32_t len)
{
    dsm_unpack_page_payload(in, data, len);
}

size_t dsm_pack_page_move(const uint8_t *data, uint8_t *out, uint32_t len)
{
    return dsm_pack_page_payload(data, out, len);
}

/* --- Consultas de diagnóstico --- */

size_t dsm_pack_query_req(const query_req_t *q, uint8_t out[DSM_QUERY_REQ_LEN])
{
    out[0] = q->kind;
    out[1] = q->scope;
    put_u16(out + 2, q->reserved);
    put_u32(out + 4, q->first);
    put_u32(out + 8, q->max);
    return DSM_QUERY_REQ_LEN;
}

void dsm_unpack_query_req(const uint8_t in[DSM_QUERY_REQ_LEN], query_req_t *q)
{
    q->kind     = in[0];
    q->scope    = in[1];
    q->reserved = get_u16(in + 2);
    q->first    = get_u32(in + 4);
    q->max      = get_u32(in + 8);
}

size_t dsm_pack_query_resp(const query_resp_t *r, uint8_t out[DSM_QUERY_RESP_HDR_LEN])
{
    out[0] = r->kind;
    out[1] = r->more;
    put_u16(out + 2, r->reserved);
    put_u32(out + 4, r->count);
    return DSM_QUERY_RESP_HDR_LEN;
}

void dsm_unpack_query_resp(const uint8_t in[DSM_QUERY_RESP_HDR_LEN], query_resp_t *r)
{
    r->kind     = in[0];
    r->more     = in[1];
    r->reserved = get_u16(in + 2);
    r->count    = get_u32(in + 4);
}

/* --- Estadísticas para dsmctl --- */

size_t dsm_pack_stats(const stats_wire_t *s, uint8_t out[DSM_STATS_WIRE_LEN])
{
    put_u64(out +  0, s->read_faults);
    put_u64(out +  8, s->write_faults);
    put_u64(out + 16, s->msgs_sent);
    put_u64(out + 24, s->msgs_recv);
    put_u64(out + 32, s->bytes_sent);
    put_u64(out + 40, s->bytes_recv);
    put_u64(out + 48, s->dist_tasks_run);
    put_u64(out + 56, s->dist_tasks_failed);
    put_u64(out + 64, s->parity_deltas);
    put_u64(out + 72, s->stripe_rebuilds);
    put_u64(out + 80, s->tseq_discarded);
    put_u64(out + 88, s->hb_missed);
    return DSM_STATS_WIRE_LEN;
}

void dsm_unpack_stats(const uint8_t in[DSM_STATS_WIRE_LEN], stats_wire_t *s)
{
    s->read_faults       = get_u64(in +  0);
    s->write_faults      = get_u64(in +  8);
    s->msgs_sent         = get_u64(in + 16);
    s->msgs_recv         = get_u64(in + 24);
    s->bytes_sent        = get_u64(in + 32);
    s->bytes_recv        = get_u64(in + 40);
    s->dist_tasks_run    = get_u64(in + 48);
    s->dist_tasks_failed = get_u64(in + 56);
    s->parity_deltas     = get_u64(in + 64);
    s->stripe_rebuilds   = get_u64(in + 72);
    s->tseq_discarded    = get_u64(in + 80);
    s->hb_missed         = get_u64(in + 88);
}

/* --- Migración de cerrojos --- */

size_t dsm_pack_lock_migrate(const lock_migrate_wire_t *m,
                             uint8_t out[DSM_LOCK_MIGRATE_WIRE_LEN])
{
    put_u64(out + 0, m->epoch);
    put_u32(out + 8, m->dead_slot);
    return DSM_LOCK_MIGRATE_WIRE_LEN;
}

void dsm_unpack_lock_migrate(const uint8_t in[DSM_LOCK_MIGRATE_WIRE_LEN],
                             lock_migrate_wire_t *m)
{
    m->epoch     = get_u64(in + 0);
    m->dead_slot = get_u32(in + 8);
}

size_t dsm_pack_lock_state(const lock_state_wire_t *s,
                           const lock_state_rec_t *recs, uint8_t *out)
{
    put_u64(out + 0, s->epoch);
    put_u32(out + 8, s->dead_slot);
    put_u32(out + 12, s->slot);
    put_u32(out + 16, s->count);

    size_t off = DSM_LOCK_STATE_HDR_LEN;
    for (uint32_t i = 0; i < s->count; i++) {
        put_u32(out + off + 0, recs[i].lock_id);
        put_u32(out + off + 4, recs[i].req_id);
        out[off + 8] = recs[i].owner ? 1u : 0u;
        out[off + 9] = 0;
        out[off + 10] = 0;
        out[off + 11] = 0;
        off += DSM_LOCK_STATE_REC_LEN;
    }
    return off;
}

int dsm_unpack_lock_state(const uint8_t *in, size_t len,
                          lock_state_wire_t *s, lock_state_rec_t *recs)
{
    if (!in || !s || len < DSM_LOCK_STATE_HDR_LEN) return DSM_ERR_PROTO;

    s->epoch     = get_u64(in + 0);
    s->dead_slot = get_u32(in + 8);
    s->slot      = get_u32(in + 12);
    s->count     = get_u32(in + 16);

    if (s->count > DSM_LOCK_STATE_MAX_RECS ||
        DSM_LOCK_STATE_HDR_LEN + (size_t)s->count * DSM_LOCK_STATE_REC_LEN != len) {
        return DSM_ERR_PROTO;
    }
    if (!recs && s->count > 0) return DSM_ERR_PROTO;

    size_t off = DSM_LOCK_STATE_HDR_LEN;
    for (uint32_t i = 0; i < s->count; i++) {
        recs[i].lock_id = get_u32(in + off + 0);
        recs[i].req_id  = get_u32(in + off + 4);
        recs[i].owner   = in[off + 8] ? 1u : 0u;
        memset(recs[i].reserved, 0, sizeof(recs[i].reserved));
        off += DSM_LOCK_STATE_REC_LEN;
    }
    return DSM_OK;
}

size_t dsm_pack_node_rec(const node_rec_t *n, uint8_t out[DSM_NODE_REC_LEN])
{
    put_u32(out + 0, n->slot);
    memcpy(out + 4, n->uuid, 16);
    put_u32(out + 20, n->ip);
    put_u16(out + 24, n->port);
    out[26] = n->state;
    out[27] = n->missed_hb;
    put_u32(out + 28, n->mem_pages);
    put_u32(out + 32, n->free_pages);
    put_u64(out + 36, n->epoch);
    put_u64(out + 44, n->last_hb_ms);
    return DSM_NODE_REC_LEN;
}

void dsm_unpack_node_rec(const uint8_t in[DSM_NODE_REC_LEN], node_rec_t *n)
{
    n->slot       = get_u32(in + 0);
    memcpy(n->uuid, in + 4, 16);
    n->ip         = get_u32(in + 20);
    n->port       = get_u16(in + 24);
    n->state      = in[26];
    n->missed_hb  = in[27];
    n->mem_pages  = get_u32(in + 28);
    n->free_pages = get_u32(in + 32);
    n->epoch      = get_u64(in + 36);
    n->last_hb_ms = get_u64(in + 44);
}

size_t dsm_pack_page_rec(const page_rec_t *p, uint8_t out[DSM_PAGE_REC_LEN])
{
    put_u32(out + 0, p->page_id);
    put_u32(out + 4, p->home);
    put_u32(out + 8, p->version);
    out[12] = p->state;
    out[13] = p->pend_inval;
    out[14] = p->pend_down;
    out[15] = p->reserved;
    put_u32(out + 16, p->pid);
    put_u32(out + 20, p->order);
    return DSM_PAGE_REC_LEN;
}

void dsm_unpack_page_rec(const uint8_t in[DSM_PAGE_REC_LEN], page_rec_t *p)
{
    p->page_id    = get_u32(in + 0);
    p->home       = get_u32(in + 4);
    p->version    = get_u32(in + 8);
    p->state      = in[12];
    p->pend_inval = in[13];
    p->pend_down  = in[14];
    p->reserved   = in[15];
    p->pid        = get_u32(in + 16);
    p->order      = get_u32(in + 20);
}

size_t dsm_pack_welcome(const welcome_t *w, uint8_t *out, size_t max_out)
{
    /* El empaquetador es la ultima linea de defensa: un count fuera de rango
     * haria leer fuera de w->members. Se rechaza igual que en unpack. */
    if (!w || !out || w->count > MAX_NODES || w->alloc_count > 64) {
        return 0;
    }

    size_t needed = 28 + (size_t)w->count * DSM_MEMBERSHIP_WIRE_LEN + 4 + (size_t)w->alloc_count * DSM_ALLOC_REC_LEN;
    if (needed > max_out) {
        return 0;
    }

    put_u64(out + 0, w->epoch);
    memcpy(out + 8, w->origin_uuid, 16);
    put_u32(out + 24, w->count);

    size_t off = 28;
    for (uint32_t i = 0; i < w->count; i++) {
        dsm_pack_membership(&w->members[i], out + off);
        off += DSM_MEMBERSHIP_WIRE_LEN;
    }

    put_u32(out + off, w->alloc_count);
    off += 4;

    for (uint32_t i = 0; i < w->alloc_count; i++) {
        dsm_pack_alloc_rec(&w->allocs[i], out + off);
        off += DSM_ALLOC_REC_LEN;
    }

    return off;
}

int dsm_unpack_welcome(const uint8_t *in, size_t len, welcome_t *w)
{
    if (len < 32) {
        return DSM_ERR_PROTO;
    }

    w->epoch = get_u64(in + 0);
    memcpy(w->origin_uuid, in + 8, 16);
    w->count = get_u32(in + 24);

    if (w->count > MAX_NODES) {
        return DSM_ERR_PROTO;
    }

    size_t expected_min = 28 + (size_t)w->count * DSM_MEMBERSHIP_WIRE_LEN + 4;
    if (len < expected_min) {
        return DSM_ERR_PROTO;
    }

    size_t off = 28;
    for (uint32_t i = 0; i < w->count; i++) {
        dsm_unpack_membership(in + off, &w->members[i]);
        off += DSM_MEMBERSHIP_WIRE_LEN;
    }

    w->alloc_count = get_u32(in + off);
    off += 4;

    if (w->alloc_count > 64) {
        return DSM_ERR_PROTO;
    }

    size_t expected_total = off + (size_t)w->alloc_count * DSM_ALLOC_REC_LEN;
    if (len < expected_total) {
        return DSM_ERR_PROTO;
    }

    for (uint32_t i = 0; i < w->alloc_count; i++) {
        dsm_unpack_alloc_rec(in + off, &w->allocs[i]);
        off += DSM_ALLOC_REC_LEN;
    }

    return DSM_OK;
}

const char *dsm_msg_type_name(uint16_t type)
{
    switch (type) {
    case MSG_HELLO:           return "HELLO";
    case MSG_WELCOME:         return "WELCOME";
    case MSG_JOIN:            return "JOIN";
    case MSG_JOIN_ACK:        return "JOIN_ACK";
    case MSG_HEARTBEAT:       return "HEARTBEAT";
    case MSG_LEAVE:           return "LEAVE";
    case MSG_NODE_DOWN:       return "NODE_DOWN";
    case MSG_PAGE_MOVE:       return "PAGE_MOVE";
    case MSG_PAGE_MOVE_ACK:   return "PAGE_MOVE_ACK";
    case MSG_PAGE_REQUEST:    return "PAGE_REQUEST";
    case MSG_PAGE_DATA:       return "PAGE_DATA";
    case MSG_GRANT:           return "GRANT";
    case MSG_INVALIDATE:      return "INVALIDATE";
    case MSG_INVALIDATE_ACK:  return "INVALIDATE_ACK";
    case MSG_DOWNGRADE:       return "DOWNGRADE";
    case MSG_DOWNGRADE_ACK:   return "DOWNGRADE_ACK";
    case MSG_WRITEBACK:       return "WRITEBACK";
    case MSG_WRITEBACK_ACK:   return "WRITEBACK_ACK";
    case MSG_PARITY_DELTA:    return "PARITY_DELTA";
    case MSG_PARITY_ACK:      return "PARITY_ACK";
    case MSG_STRIPE_PUT:      return "STRIPE_PUT";
    case MSG_STRIPE_PUT_ACK:  return "STRIPE_PUT_ACK";
    case MSG_STRIPE_READ:     return "STRIPE_READ";
    case MSG_STRIPE_DATA:     return "STRIPE_DATA";
    case MSG_STATE_REPORT:    return "STATE_REPORT";
    case MSG_DIR_DONE:        return "DIR_DONE";
    case MSG_LOCK_REQ:        return "LOCK_REQ";
    case MSG_LOCK_GRANT:      return "LOCK_GRANT";
    case MSG_LOCK_REL:        return "LOCK_REL";
    case MSG_BARRIER_ARRIVE:  return "BARRIER_ARRIVE";
    case MSG_BARRIER_GO:      return "BARRIER_GO";
    case MSG_LOCK_MIGRATE:    return "LOCK_MIGRATE";
    case MSG_LOCK_STATE:      return "LOCK_STATE";
    case MSG_QUERY_REQ:       return "QUERY_REQ";
    case MSG_QUERY_RESP:      return "QUERY_RESP";
    case MSG_DIST_RUN:        return "DIST_RUN";
    case MSG_DIST_DONE:       return "DIST_DONE";
    case MSG_ALLOC_REQ:       return "ALLOC_REQ";
    case MSG_ALLOC_RESP:      return "ALLOC_RESP";
    case MSG_FREE_REQ:        return "FREE_REQ";
    case MSG_FREE_RESP:       return "FREE_RESP";
    case MSG_NACK:            return "NACK";
    default:                  return "UNKNOWN";
    }
}

bool dsm_msg_is_response(uint16_t type)
{
    return (type == MSG_WELCOME || type == MSG_JOIN_ACK ||
            type == MSG_PAGE_DATA || type == MSG_GRANT ||
            type == MSG_INVALIDATE_ACK || type == MSG_DOWNGRADE_ACK ||
            type == MSG_WRITEBACK_ACK || type == MSG_LOCK_GRANT ||
            type == MSG_QUERY_RESP || type == MSG_DIST_DONE ||
            type == MSG_ALLOC_RESP || type == MSG_FREE_RESP ||
            type == MSG_NACK);
}

bool dsm_msg_is_membership(uint16_t type)
{
    return (type == MSG_HELLO || type == MSG_WELCOME ||
            type == MSG_JOIN || type == MSG_JOIN_ACK ||
            type == MSG_HEARTBEAT || type == MSG_LEAVE ||
            type == MSG_NODE_DOWN);
}
