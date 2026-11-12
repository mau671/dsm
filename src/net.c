#include "net.h"
#include "dsm_internal.h"
#include "wire.h"
#include "log.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/socket.h>

int net_init(dsm_ctx_t *ctx)
{
    dsm_net_t *n = &ctx->net;
    n->listen_fd = -1;
    pthread_mutex_init(&n->pending_mu, NULL);

    for (uint32_t i = 0; i < MAX_NODES; i++) {
        n->peers[i].slot   = i;
        n->peers[i].out_fd = -1;
        pthread_mutex_init(&n->peers[i].send_mu, NULL);
        atomic_store(&n->peers[i].connected, false);

        n->inbound[i].fd   = -1;
        atomic_store(&n->inbound[i].alive, false);
    }

    return DSM_OK;
}

static pending_req_t *pending_find_locked(dsm_ctx_t *ctx, uint32_t req_id,
                                          pending_req_t **prev_out)
{
    uint32_t bucket = req_id % 1024u;
    pending_req_t *prev = NULL;
    pending_req_t *cur = ctx->net.pending[bucket];
    while (cur) {
        if (cur->req_id == req_id) {
            if (prev_out) *prev_out = prev;
            return cur;
        }
        prev = cur;
        cur = cur->next;
    }
    return NULL;
}

static void pending_remove(dsm_ctx_t *ctx, pending_req_t *pending)
{
    uint32_t bucket = pending->req_id % 1024u;
    pthread_mutex_lock(&ctx->net.pending_mu);
    pending_req_t *prev = NULL;
    pending_req_t *found = pending_find_locked(ctx, pending->req_id, &prev);
    if (found == pending) {
        if (prev) {
            prev->next = found->next;
        } else {
            ctx->net.pending[bucket] = found->next;
        }
    }
    pthread_mutex_unlock(&ctx->net.pending_mu);
}

static int pending_complete(dsm_ctx_t *ctx, const msg_header_t *hdr,
                            const uint8_t *payload)
{
    pthread_mutex_lock(&ctx->net.pending_mu);
    pending_req_t *pending = pending_find_locked(ctx, hdr->req_id, NULL);
    if (!pending) {
        pthread_mutex_unlock(&ctx->net.pending_mu);
        return DSM_ERR_NOT_FOUND;
    }

    pending->status     = hdr->status;
    pending->rx_version = hdr->data_version;
    pending->rx_len     = hdr->payload_len;
    pending->result     = DSM_OK;
    if (hdr->payload_len > pending->rx_capacity) {
        pending->result = DSM_ERR_NO_SPACE;
        pending->rx_len = 0;
    } else if (hdr->payload_len > 0) {
        memcpy(pending->rx_payload, payload, hdr->payload_len);
    }
    pending->completed = 1;
    sem_post(&pending->sem);
    pthread_mutex_unlock(&ctx->net.pending_mu);
    return DSM_OK;
}

void net_destroy(dsm_ctx_t *ctx)
{
    dsm_net_t *n = &ctx->net;
    for (uint32_t i = 0; i < MAX_NODES; i++) {
        pthread_mutex_lock(&n->peers[i].send_mu);
        if (n->peers[i].out_fd >= 0) {
            shutdown(n->peers[i].out_fd, SHUT_RDWR);
            close(n->peers[i].out_fd);
            n->peers[i].out_fd = -1;
        }
        atomic_store(&n->peers[i].connected, false);
        pthread_mutex_unlock(&n->peers[i].send_mu);
        pthread_mutex_destroy(&n->peers[i].send_mu);

        if (n->inbound[i].fd >= 0) {
            shutdown(n->inbound[i].fd, SHUT_RDWR);
            close(n->inbound[i].fd);
            n->inbound[i].fd = -1;
        }
    }
    pthread_mutex_destroy(&n->pending_mu);
}

static void *listener_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    int sfd = ctx->net.listen_fd;

    while (atomic_load(&ctx->running)) {
        struct pollfd pfd = { .fd = sfd, .events = POLLIN, .revents = 0 };
        int pr = poll(&pfd, 1, 500);
        if (pr <= 0) continue;

        struct sockaddr_in cli_addr;
        socklen_t clen = sizeof(cli_addr);
        int cfd = accept(sfd, (struct sockaddr *)&cli_addr, &clen);
        if (cfd < 0) continue;

        set_cloexec(cfd);

        /* Leer cabecera de mensaje entrante */
        uint8_t hdr_raw[DSM_HDR_LEN];
        if (dsm_read_full_deadline(cfd, hdr_raw, DSM_HDR_LEN, ctx->cfg.connect_timeout_ms) != DSM_OK) {
            close(cfd);
            continue;
        }

        msg_header_t hdr;
        if (dsm_wire_unpack(hdr_raw, &hdr) != DSM_OK) {
            close(cfd);
            continue;
        }

        /* Si es HELLO de descubrimiento, responder inmediatamente con WELCOME */
        if (hdr.type == MSG_HELLO && hdr.payload_len == 20) {
            uint8_t hello_payload[20];
            if (dsm_read_full_deadline(cfd, hello_payload, 20, ctx->cfg.discovery_timeout_ms) == DSM_OK) {
                atomic_fetch_add_explicit(&ctx->ctr.msgs_recv, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&ctx->ctr.bytes_recv, DSM_HDR_LEN + 20, memory_order_relaxed);
                uint32_t peer_hash = 0;
                memcpy(&peer_hash, hello_payload + 16, 4);
                peer_hash = ntohl(peer_hash);

                welcome_t w;
                memset(&w, 0, sizeof(w));
                w.epoch = ctx->memb.epoch;
                memcpy(w.origin_uuid, ctx->memb.self_uuid, 16);

                uint32_t count = 0;
                pthread_rwlock_rdlock(&ctx->memb.rw);
                for (uint32_t s = 0; s < MAX_NODES; s++) {
                    if (membership_slot_present(ctx, s)) {
                        node_info_t *ni = &ctx->memb.nodes[s];
                        membership_wire_t *wire = &w.members[count++];
                        memcpy(wire->uuid, ni->uuid, 16);
                        wire->ip          = ni->ip;
                        wire->port        = ni->port;
                        wire->slot        = (uint8_t)s;
                        wire->state       = ni->state;
                        wire->incarnation = ni->incarnation;
                        wire->mem_pages   = ni->mem_pages;
                    }
                }
                pthread_rwlock_unlock(&ctx->memb.rw);
                w.count = count;

                uint8_t welcome_buf[4096];
                size_t wlen = dsm_pack_welcome(&w, welcome_buf, sizeof(welcome_buf));

                msg_header_t resp_hdr;
                memset(&resp_hdr, 0, sizeof(resp_hdr));
                resp_hdr.magic         = DSM_MAGIC;
                resp_hdr.cluster_id    = ctx->cfg.cluster_id;
                resp_hdr.proto_version = DSM_PROTO_VERSION;
                resp_hdr.type          = MSG_WELCOME;
                resp_hdr.status        = (peer_hash == ctx->cfg.raw_hash) ? DSM_ST_OK : DSM_ST_CONFIG_MISMATCH;
                resp_hdr.payload_len   = (uint32_t)wlen;

                uint8_t raw_resp[DSM_HDR_LEN];
                dsm_wire_finish(&resp_hdr, raw_resp, welcome_buf, (uint32_t)wlen);

                dsm_send_full(cfd, raw_resp, DSM_HDR_LEN);
                if (resp_hdr.status == DSM_ST_OK) {
                    dsm_send_full(cfd, welcome_buf, wlen);
                }
                atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN + (resp_hdr.status == DSM_ST_OK ? wlen : 0), memory_order_relaxed);
            }
            close(cfd);
            continue;
        }

        /* Si es mensaje general, procesarlo a través de workers */
        uint8_t *payload = NULL;
        if (hdr.payload_len > 0 && hdr.payload_len <= DSM_MAX_PAYLOAD) {
            payload = malloc(hdr.payload_len);
            if (payload) {
                if (dsm_read_full_deadline(cfd, payload, hdr.payload_len, ctx->cfg.send_timeout_ms) != DSM_OK) {
                    free(payload);
                    close(cfd);
                    continue;
                }
            }
        }

        if (dsm_wire_validate(&hdr, hdr_raw, payload, ctx->cfg.cluster_id) == DSM_OK &&
            wire_payload_ok(hdr.type, hdr.flags, hdr.payload_len, ctx->cfg.page_size)) {
            atomic_fetch_add_explicit(&ctx->ctr.msgs_recv, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&ctx->ctr.bytes_recv, DSM_HDR_LEN + hdr.payload_len, memory_order_relaxed);
            if (dsm_msg_is_response(hdr.type) &&
                pending_complete(ctx, &hdr, payload) == DSM_OK) {
                free(payload);
                payload = NULL;
                close(cfd);
                continue;
            }
            dsm_msg_t msg = { .hdr = hdr, .payload = payload };
            if (hdr.type == MSG_JOIN) {
                membership_on_join_fd(ctx, &msg, cfd);
            } else if (hdr.type == MSG_HEARTBEAT) {
                membership_on_heartbeat(ctx, &msg);
            } else if (hdr.type == MSG_LEAVE) {
                membership_on_leave(ctx, &msg);
            } else if (hdr.type == MSG_NODE_DOWN) {
                membership_on_node_down(ctx, &msg);
            } else {
                int enqueue_rc = worker_enqueue(ctx, &msg, payload);
                if (enqueue_rc == DSM_OK) {
                    payload = NULL; /* Transferencia de propiedad a worker */
                }
            }
        }

        if (payload) {
            free(payload);
        }
        close(cfd);
    }
    return NULL;
}

int net_start_listener(dsm_ctx_t *ctx)
{
    if (listen(ctx->net.listen_fd, 128) != 0) {
        DSM_LOG_ERR("listen fallo: %s", strerror(errno));
        return DSM_ERR_NET;
    }
    if (pthread_create(&ctx->net.listener, NULL, listener_thread_main, ctx) != 0) {
        DSM_LOG_ERR("No se pudo crear hilo listener de red: %s", strerror(errno));
        return DSM_ERR_MEM;
    }
    return DSM_OK;
}

int net_stop_listener(dsm_ctx_t *ctx)
{
    if (ctx->net.listen_fd >= 0) {
        shutdown(ctx->net.listen_fd, SHUT_RDWR);
        close(ctx->net.listen_fd);
        ctx->net.listen_fd = -1;
    }
    if (ctx->net.listener) {
        pthread_join(ctx->net.listener, NULL);
        ctx->net.listener = 0;
    }
    return DSM_OK;
}

int peer_ensure_connected(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot >= MAX_NODES) return DSM_ERR_INVALID;
    dsm_peer_t *p = &ctx->net.peers[slot];

    pthread_mutex_lock(&p->send_mu);
    if (atomic_load(&p->connected) && p->out_fd >= 0) {
        pthread_mutex_unlock(&p->send_mu);
        return DSM_OK;
    }

    /* Obtener IP y puerto desde la tabla de membresía */
    uint32_t ip;
    uint16_t port;
    pthread_rwlock_rdlock(&ctx->memb.rw);
    if (!membership_slot_present(ctx, slot)) {
        pthread_rwlock_unlock(&ctx->memb.rw);
        pthread_mutex_unlock(&p->send_mu);
        return DSM_ERR_NODE_DOWN;
    }
    ip   = ctx->memb.nodes[slot].ip;
    port = ctx->memb.nodes[slot].port;
    pthread_rwlock_unlock(&ctx->memb.rw);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ip;
    addr.sin_port        = htons(port);

    int fd = dsm_connect_timeout(&addr, ctx->cfg.connect_timeout_ms);
    if (fd < 0) {
        pthread_mutex_unlock(&p->send_mu);
        return DSM_ERR_NET;
    }

    p->out_fd = fd;
    atomic_store(&p->connected, true);
    pthread_mutex_unlock(&p->send_mu);
    return DSM_OK;
}

void peer_mark_down(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot >= MAX_NODES) return;
    dsm_peer_t *p = &ctx->net.peers[slot];
    pthread_mutex_lock(&p->send_mu);
    if (p->out_fd >= 0) {
        shutdown(p->out_fd, SHUT_RDWR);
        close(p->out_fd);
        p->out_fd = -1;
    }
    atomic_store(&p->connected, false);
    pthread_mutex_unlock(&p->send_mu);
}

int dsm_net_send(dsm_ctx_t *ctx, uint32_t dst, const msg_header_t *hdr, const void *payload)
{
    if (dst >= MAX_NODES) return DSM_ERR_INVALID;
    if (peer_ensure_connected(ctx, dst) != DSM_OK) {
        return DSM_ERR_NET;
    }

    dsm_peer_t *p = &ctx->net.peers[dst];
    pthread_mutex_lock(&p->send_mu);
    if (p->out_fd < 0) {
        pthread_mutex_unlock(&p->send_mu);
        return DSM_ERR_NET;
    }

    msg_header_t h = *hdr;
    uint8_t raw_hdr[DSM_HDR_LEN];
    dsm_wire_finish(&h, raw_hdr, payload, hdr->payload_len);

    if (dsm_send_full(p->out_fd, raw_hdr, DSM_HDR_LEN) != 0 ||
        (payload && hdr->payload_len > 0 && dsm_send_full(p->out_fd, payload, hdr->payload_len) != 0)) {
        close(p->out_fd);
        p->out_fd = -1;
        atomic_store(&p->connected, false);
        pthread_mutex_unlock(&p->send_mu);
        return DSM_ERR_NET;
    }

    atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN + hdr->payload_len, memory_order_relaxed);

    /*
     * El listener procesa una trama y cierra su conexión. Mantener el
     * descriptor como persistente haría que la siguiente trama viajara por
     * un socket que el receptor ya cerró.
     */
    shutdown(p->out_fd, SHUT_RDWR);
    close(p->out_fd);
    p->out_fd = -1;
    atomic_store(&p->connected, false);
    pthread_mutex_unlock(&p->send_mu);
    return DSM_OK;
}

int net_send_heartbeat(dsm_ctx_t *ctx, uint32_t dst, uint32_t free_pages)
{
    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_HEARTBEAT;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.epoch         = ctx->memb.epoch;
    hdr.payload_len   = 12;

    uint8_t payload[12];
    uint64_t inc_net = dsm_htonll(1);
    uint32_t fp_net  = htonl(free_pages);
    memcpy(payload, &inc_net, 8);
    memcpy(payload + 8, &fp_net, 4);

    return dsm_net_send(ctx, dst, &hdr, payload);
}

int net_send_nack(dsm_ctx_t *ctx, uint32_t dst, uint32_t req_id, uint16_t status)
{
    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_NACK;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.status        = status;

    return dsm_net_send(ctx, dst, &hdr, NULL);
}

int dsm_net_rpc(dsm_ctx_t *ctx, uint32_t dst, uint16_t type, uint32_t page_id,
                 uint32_t tseq, uint8_t access, uint8_t flags,
                 const void *tx_payload, uint32_t tx_len,
                 uint16_t *out_status, void *rx_payload, uint32_t *rx_len)
{
    if (!ctx || dst >= MAX_NODES || tx_len > DSM_MAX_PAYLOAD ||
        (tx_len > 0 && !tx_payload) || !rx_len) {
        return DSM_ERR_INVALID;
    }
    if (rx_payload == NULL && *rx_len != 0) {
        return DSM_ERR_INVALID;
    }

    pending_req_t pending;
    memset(&pending, 0, sizeof(pending));
    pending.req_id      = (uint32_t)atomic_fetch_add(&ctx->net.req_counter, 1) + 1u;
    pending.page_id     = page_id;
    pending.rx_payload  = rx_payload;
    pending.rx_capacity = *rx_len;
    pending.result      = DSM_ERR_TIMEOUT;
    if (sem_init(&pending.sem, 0, 0) != 0) {
        return DSM_ERR_MEM;
    }

    uint32_t bucket = pending.req_id % 1024u;
    pthread_mutex_lock(&ctx->net.pending_mu);
    pending.next = ctx->net.pending[bucket];
    ctx->net.pending[bucket] = &pending;
    pthread_mutex_unlock(&ctx->net.pending_mu);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = type;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = pending.req_id;
    hdr.page_id       = page_id;
    hdr.tseq          = tseq;
    hdr.access        = access;
    hdr.flags         = flags;
    hdr.epoch         = ctx->memb.epoch;

    int send_rc = dsm_net_send(ctx, dst, &hdr, tx_payload);
    if (send_rc != DSM_OK) {
        pending_remove(ctx, &pending);
        sem_destroy(&pending.sem);
        return send_rc;
    }

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)(ctx->cfg.request_timeout_ms / 1000u);
    deadline.tv_nsec += (long)(ctx->cfg.request_timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }

    int wait_rc;
    do {
        wait_rc = sem_timedwait(&pending.sem, &deadline);
    } while (wait_rc != 0 && errno == EINTR);

    if (wait_rc != 0) {
        pending_remove(ctx, &pending);
        sem_destroy(&pending.sem);
        return (errno == ETIMEDOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
    }

    pending_remove(ctx, &pending);
    if (out_status) *out_status = pending.status;
    *rx_len = pending.rx_len;
    int result = pending.result;
    sem_destroy(&pending.sem);
    return result;
}

int dsm_net_read_one(int fd, uint32_t cluster_id, dsm_msg_t *out, uint8_t **owned)
{
    if (fd < 0 || !out || !owned) return DSM_ERR_INVALID;

    uint8_t raw_hdr[DSM_HDR_LEN];
    int rc = dsm_read_full_deadline(fd, raw_hdr, DSM_HDR_LEN, 3000);
    if (rc != DSM_OK) return rc;

    msg_header_t hdr;
    rc = dsm_wire_unpack(raw_hdr, &hdr);
    if (rc != DSM_OK) return rc;

    uint8_t *payload = NULL;
    if (hdr.payload_len > 0) {
        if (hdr.payload_len > DSM_MAX_PAYLOAD) return DSM_ERR_PROTO;
        payload = malloc(hdr.payload_len);
        if (!payload) return DSM_ERR_MEM;
        rc = dsm_read_full_deadline(fd, payload, hdr.payload_len, 3000);
        if (rc != DSM_OK) {
            free(payload);
            return rc;
        }
    }

    if (dsm_wire_validate(&hdr, raw_hdr, payload, cluster_id) != DSM_OK) {
        free(payload);
        return DSM_ERR_PROTO;
    }

    out->hdr = hdr;
    out->payload = payload;
    *owned = payload;
    return DSM_OK;
}

int net_probe(dsm_ctx_t *ctx, const struct sockaddr_in *addr, welcome_t *out)
{
    (void)ctx;
    (void)addr;
    (void)out;
    return DSM_OK;
}
