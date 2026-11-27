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

/* Cierra un descriptor de socket tolerando que ya no sea valido. */
static void fd_close_quiet(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

/* IP IPv4 de un extremo del socket, en orden de red; cero si no se puede
 * observar. */
static uint32_t socket_endpoint_ip(int fd, bool peer)
{
    struct sockaddr_in sa;
    socklen_t len = sizeof(sa);

    int rc = peer ? getpeername(fd, (struct sockaddr *)&sa, &len)
                  : getsockname(fd, (struct sockaddr *)&sa, &len);
    if (rc != 0 || sa.sin_family != AF_INET) {
        return 0;
    }
    return sa.sin_addr.s_addr;
}

int net_init(dsm_ctx_t *ctx)
{
    dsm_net_t *n = &ctx->net;
    n->listen_fd = -1;
    if (pthread_mutex_init(&n->pending_mu, NULL) != 0) {
        return DSM_ERR_MEM;
    }

    for (uint32_t i = 0; i < MAX_NODES; i++) {
        n->peers[i].slot   = i;
        n->peers[i].out_fd = -1;
        if (pthread_mutex_init(&n->peers[i].send_mu, NULL) != 0) {
            /* Solo los mutex ya creados pueden destruirse. */
            pthread_mutex_destroy(&n->pending_mu);
            for (uint32_t j = 0; j < i; j++) {
                pthread_mutex_destroy(&n->peers[j].send_mu);
            }
            return DSM_ERR_MEM;
        }
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
    atomic_fetch_sub_explicit(&ctx->ctr.pending_count, 1, memory_order_relaxed);
    pthread_mutex_unlock(&ctx->net.pending_mu);
}

static int pending_complete(dsm_ctx_t *ctx, const msg_header_t *hdr,
                            const uint8_t *payload)
{
    /* Una respuesta de pagina repetida o antigua no despierta al emisor: el
     * listener la descarta sin tocar la tabla de paginas. */
    if ((hdr->type == MSG_PAGE_DATA || hdr->type == MSG_GRANT) &&
        msi_tseq_stale(ctx, hdr->page_id, hdr->tseq)) {
        return DSM_ERR_NOT_FOUND;
    }
    if (hdr->type == MSG_LOCK_GRANT && sync_epoch_stale(ctx, hdr->epoch)) {
        return DSM_ERR_NOT_FOUND;
    }

    pthread_mutex_lock(&ctx->net.pending_mu);
    pending_req_t *pending = pending_find_locked(ctx, hdr->req_id, NULL);
    if (!pending) {
        pthread_mutex_unlock(&ctx->net.pending_mu);
        return DSM_ERR_NOT_FOUND;
    }

    /* El acuse de degradacion llega como respuesta sincronica: ademas de
     * despertar la RPC se entrega al estado de acuses para que cierre su
     * transaccion. */
    if (hdr->type == MSG_DOWNGRADE_ACK) {
        dsm_msg_t ack = { .hdr = *hdr, .payload = payload };
        msi_on_downgrade_ack(ctx, &ack);
    }

    pending->status     = hdr->status;
    pending->rx_version = hdr->data_version;
    pending->rx_tseq    = hdr->tseq;
    pending->rx_len     = hdr->payload_len;
    pending->result     = DSM_OK;
    if (hdr->payload_len > pending->rx_capacity) {
        pending->result = DSM_ERR_NO_SPACE;
        pending->rx_len = 0;
    } else if (hdr->payload_len > 0) {
        memcpy(pending->rx_payload, payload, hdr->payload_len);
    }
    pending->completed = 1;
    /* El emisor puede haber agotado el plazo y liberado el semaforo: el
     * post solo debe ocurrir mientras el mutex garantiza su existencia. */
    if (sem_post(&pending->sem) != 0) {
        pending->result = DSM_ERR_NET;
    }
    bool lock_grant = (hdr->type == MSG_LOCK_GRANT);
    pthread_mutex_unlock(&ctx->net.pending_mu);

    if (lock_grant) {
        sync_send_lock_ack(ctx, hdr->src_slot, hdr->page_id, hdr->req_id);
    }
    return DSM_OK;
}

void net_destroy(dsm_ctx_t *ctx)
{
    dsm_net_t *n = &ctx->net;
    for (uint32_t i = 0; i < MAX_NODES; i++) {
        pthread_mutex_lock(&n->peers[i].send_mu);
        if (n->peers[i].out_fd >= 0) {
            shutdown(n->peers[i].out_fd, SHUT_RDWR);
            fd_close_quiet(&n->peers[i].out_fd);
        }
        atomic_store(&n->peers[i].connected, false);
        pthread_mutex_unlock(&n->peers[i].send_mu);
        pthread_mutex_destroy(&n->peers[i].send_mu);

        if (n->inbound[i].fd >= 0) {
            shutdown(n->inbound[i].fd, SHUT_RDWR);
            fd_close_quiet(&n->inbound[i].fd);
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
        if (pr < 0) {
            if (errno == EINTR) continue;
            DSM_LOG_ERR("poll sobre el listener fallo: %s", strerror(errno));
            break;
        }
        if (pr == 0) continue;
        if (pfd.revents & POLLNVAL) {
            /* El cierre del descriptor de escucha es la senal de apagado. */
            if (atomic_load(&ctx->running)) {
                DSM_LOG_ERR("Descriptor de escucha invalido en el listener");
            }
            break;
        }

        struct sockaddr_in cli_addr;
        socklen_t clen = sizeof(cli_addr);
        int cfd = accept(sfd, (struct sockaddr *)&cli_addr, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK ||
                errno == ECONNABORTED) {
                continue;
            }
            /* El socket de escucha dejo de ser utilizable: solo el cierre
             * externo (net_stop_listener) produce este caso al apagar. */
            if (atomic_load(&ctx->running)) {
                DSM_LOG_ERR("accept sobre el listener fallo: %s", strerror(errno));
                sleep_ms(10);
            }
            continue;
        }

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
                /* El HELLO se atiende fuera de la ruta de los workers, pero
                 * pasa por la misma validación que cualquier otra trama:
                 * magic, versión, cluster_id, longitud y CRC. Un HELLO ajeno
                 * o corrupto no puede registrar un par ni recibir WELCOME. */
                if (dsm_wire_validate(&hdr, hdr_raw, hello_payload,
                                      ctx->cfg.cluster_id) != DSM_OK ||
                    !wire_payload_ok(hdr.type, hdr.flags, hdr.payload_len,
                                     ctx->cfg.page_size)) {
                    fd_close_quiet(&cfd);
                    continue;
                }
                atomic_fetch_add_explicit(&ctx->ctr.msgs_recv, 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&ctx->ctr.bytes_recv, DSM_HDR_LEN + 20, memory_order_relaxed);
                uint32_t peer_hash = 0;
                memcpy(&peer_hash, hello_payload + 16, 4);
                peer_hash = ntohl(peer_hash);

                /* El HELLO identifica al par y trae el hash de su
                 * configuración compartida. La IP que se publica en el
                 * WELCOME es la observada en este socket, no una dirección
                 * inventada. */
                membership_observe_self_ip(ctx, socket_endpoint_ip(cfd, false));
                membership_observe_peer_ip(ctx, hello_payload,
                                           socket_endpoint_ip(cfd, true));

                welcome_t w;
                memset(&w, 0, sizeof(w));
                w.epoch = atomic_load(&ctx->memb.epoch);
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

                /* La instantánea del mapa global viaja con la membresía: un
                 * nodo que entrega WELCOME replica la marca de agua y los
                 * huecos que conoce, de modo que el relevo del coordinador no
                 * empiece con un mapa vacío. */
                w.alloc_count = dsm_alloc_export(ctx, w.allocs, 64u);

                uint8_t welcome_buf[DSM_WELCOME_WIRE_MAX];
                size_t wlen = dsm_pack_welcome(&w, welcome_buf, sizeof(welcome_buf));

                msg_header_t resp_hdr;
                memset(&resp_hdr, 0, sizeof(resp_hdr));
                resp_hdr.magic         = DSM_MAGIC;
                resp_hdr.cluster_id    = ctx->cfg.cluster_id;
                resp_hdr.proto_version = DSM_PROTO_VERSION;
                resp_hdr.type          = MSG_WELCOME;
                resp_hdr.status        = (peer_hash == ctx->cfg.shared_hash) ? DSM_ST_OK
                                                                          : DSM_ST_CONFIG_MISMATCH;
                if (wlen == 0) {
                    /* El empaquetado no cabe ni en el peor caso: un aviso
                     * con estado explicito en lugar de un OK sin carga. */
                    resp_hdr.status = DSM_ST_NO_SPACE;
                }
                resp_hdr.payload_len   = (uint32_t)wlen;

                uint8_t raw_resp[DSM_HDR_LEN];
                dsm_wire_finish(&resp_hdr, raw_resp, welcome_buf, (uint32_t)wlen);

                /* Un WELCOME truncado deja al solicitante esperando un
                 * mensaje incompleto: se cuenta solo si salio entero. */
                int welcome_ok = (dsm_send_full_deadline(cfd, raw_resp, DSM_HDR_LEN,
                                                         ctx->cfg.send_timeout_ms) == 0);
                if (welcome_ok && resp_hdr.status == DSM_ST_OK) {
                    welcome_ok = (dsm_send_full_deadline(cfd, welcome_buf, wlen,
                                                         ctx->cfg.send_timeout_ms) == 0);
                }
                if (welcome_ok) {
                    atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
                    atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN + (resp_hdr.status == DSM_ST_OK ? wlen : 0), memory_order_relaxed);
                } else {
                    DSM_LOG_WARN("No se pudo enviar WELCOME al solicitante: %s", strerror(errno));
                }
            }
            fd_close_quiet(&cfd);
            continue;
        }

        /* Si es mensaje general, procesarlo a través de workers */
        uint8_t *payload = NULL;
        if (hdr.payload_len > 0 && hdr.payload_len <= DSM_MAX_PAYLOAD) {
            payload = malloc(hdr.payload_len);
            if (!payload) {
                /* Sin buffer no se puede consumir la trama: cerrar la conexion
                 * evita que el emisor quede esperando una respuesta. */
                DSM_LOG_WARN("Sin memoria para el payload de %u bytes", hdr.payload_len);
                fd_close_quiet(&cfd);
                continue;
            }
            if (dsm_read_full_deadline(cfd, payload, hdr.payload_len,
                                       ctx->cfg.send_timeout_ms) != DSM_OK) {
                free(payload);
                fd_close_quiet(&cfd);
                continue;
            }
        }

        if (dsm_wire_validate(&hdr, hdr_raw, payload, ctx->cfg.cluster_id) == DSM_OK &&
            wire_payload_ok(hdr.type, hdr.flags, hdr.payload_len, ctx->cfg.page_size)) {
            atomic_fetch_add_explicit(&ctx->ctr.msgs_recv, 1, memory_order_relaxed);
            atomic_fetch_add_explicit(&ctx->ctr.bytes_recv, DSM_HDR_LEN + hdr.payload_len, memory_order_relaxed);
            /* Los acuses de concesion y de barrera se resuelven aqui: si se
             * encolaran, un unico worker podria quedar bloqueado esperando su
             * propio acuse. */
            if ((hdr.type == MSG_LOCK_REL || hdr.type == MSG_BARRIER_GO) &&
                (hdr.flags & DSM_FLAG_PROBE)) {
                sync_ack_received(ctx, hdr.req_id);
                free(payload);
                payload = NULL;
                fd_close_quiet(&cfd);
                continue;
            }
            if (dsm_msg_is_response(hdr.type) || hdr.type == MSG_PAGE_MOVE_ACK) {
                int complete_rc = pending_complete(ctx, &hdr, payload);
                /* Una respuesta de pagina sin pending pertenece a una RPC ya
                 * agotada: descartarla evita sobrescribir una copia vigente. */
                if (complete_rc == DSM_OK ||
                    hdr.type == MSG_PAGE_DATA || hdr.type == MSG_GRANT) {
                    free(payload);
                    payload = NULL;
                    fd_close_quiet(&cfd);
                    continue;
                }
                /* Los acuses de invalidacion cierran esperas de transaccion
                 * que solo este hilo puede despertar: se resuelven aqui y
                 * no ocupan un worker, que podria estar esperando su
                 * propio acuse. Un WRITEBACK_ACK sin pending no tiene
                 * efecto local y tambien se descarta aqui. */
                if (hdr.type == MSG_INVALIDATE_ACK) {
                    dsm_msg_t ack = { .hdr = hdr, .payload = payload };
                    msi_on_invalidate_ack(ctx, &ack);
                    free(payload);
                    payload = NULL;
                    fd_close_quiet(&cfd);
                    continue;
                }
                if (hdr.type == MSG_WRITEBACK_ACK) {
                    free(payload);
                    payload = NULL;
                    fd_close_quiet(&cfd);
                    continue;
                }
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
            } else if (hdr.type == MSG_ALLOC_STATE) {
                /* Respuesta de sincronización del mapa: se atiende en el
                 * listener para que el worker que espera la marca de agua no
                 * dependa de otro worker libre. */
                dsm_alloc_on_sync_state(ctx, &msg);
            } else if (hdr.type == MSG_QUERY_REQ) {
                membership_handle_query_fd(ctx, &msg, cfd);
            } else {
                int enqueue_rc = worker_enqueue(ctx, &msg, payload);
                if (enqueue_rc == DSM_OK) {
                    payload = NULL; /* Transferencia de propiedad a worker */
                } else {
                    DSM_LOG_WARN("No se pudo encolar mensaje tipo %u: %d", hdr.type, enqueue_rc);
                }
            }
        }

        if (payload) {
            free(payload);
        }
        fd_close_quiet(&cfd);
    }
    return NULL;
}

int net_start_listener(dsm_ctx_t *ctx)
{
    if (listen(ctx->net.listen_fd, 128) != 0) {
        DSM_LOG_ERR("listen fallo: %s", strerror(errno));
        return DSM_ERR_NET;
    }
    int rc = pthread_create(&ctx->net.listener, NULL, listener_thread_main, ctx);
    if (rc != 0) {
        /* pthread_create devuelve el codigo de error, no lo publica en errno. */
        DSM_LOG_ERR("No se pudo crear hilo listener de red: %s", strerror(rc));
        ctx->net.listener = 0;
        return DSM_ERR_MEM;
    }
    return DSM_OK;
}

int net_stop_listener(dsm_ctx_t *ctx)
{
    int rc = DSM_OK;
    if (ctx->net.listen_fd >= 0) {
        shutdown(ctx->net.listen_fd, SHUT_RDWR);
        /* Un cierre fallido no impide unir el hilo: el apagado continua. */
        if (close(ctx->net.listen_fd) != 0) {
            DSM_LOG_WARN("Cierre del socket de escucha fallo: %s", strerror(errno));
            rc = DSM_ERR_NET;
        }
        ctx->net.listen_fd = -1;
    }
    if (ctx->net.listener) {
        int join_rc = pthread_join(ctx->net.listener, NULL);
        if (join_rc != 0) {
            DSM_LOG_ERR("No se pudo unir el hilo listener: %s", strerror(join_rc));
            rc = DSM_ERR_NET;
        }
        ctx->net.listener = 0;
    }
    return rc;
}

/* Un descriptor conectado que solo espera escritura esta sano; si el par ya
 * cerro su extremo, poll lo reporta como colgado y la conexion se descarta. */
static bool peer_socket_alive(int fd)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
    int pr;

    do {
        pr = poll(&pfd, 1, 0);
    } while (pr < 0 && errno == EINTR);

    if (pr == 0) {
        return true;
    }
    if (pr < 0) {
        return false;
    }
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return false;
    }
#ifdef POLLRDHUP
    if (pfd.revents & POLLRDHUP) {
        return false;
    }
#endif
    if (pfd.revents & POLLIN) {
        /* Un cierre ordenado del par se anuncia como legible con EOF. */
        uint8_t byte;
        ssize_t n = recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) {
            return false;
        }
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
            errno != EINTR) {
            return false;
        }
    }
    return true;
}

int peer_ensure_connected(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot >= MAX_NODES) return DSM_ERR_INVALID;
    dsm_peer_t *p = &ctx->net.peers[slot];

    pthread_mutex_lock(&p->send_mu);
    if (atomic_load(&p->connected) && p->out_fd >= 0) {
        if (peer_socket_alive(p->out_fd)) {
            pthread_mutex_unlock(&p->send_mu);
            return DSM_OK;
        }
        /* El par cerro el enlace mientras estaba inactivo: se descarta para
         * que la escritura abra uno nuevo. */
        shutdown(p->out_fd, SHUT_RDWR);
        fd_close_quiet(&p->out_fd);
        atomic_store(&p->connected, false);
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

    /*
     * La conexion ocurre dentro de send_mu para que dos emisores no abran
     * descriptores distintos para el mismo par. El bloqueo esta acotado por
     * dsm_connect_timeout: el socket es no bloqueante y poll() vence en
     * connect_timeout_ms, asi que el mutex nunca queda retenido de forma
     * indefinida. Un plazo de cero se sustituye por el valor por defecto.
     */
    uint32_t connect_ms = ctx->cfg.connect_timeout_ms ? ctx->cfg.connect_timeout_ms : 200u;
    int fd = dsm_connect_timeout(&addr, connect_ms);
    if (fd < 0) {
        pthread_mutex_unlock(&p->send_mu);
        return (fd == DSM_ERR_TIMEOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
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
        fd_close_quiet(&p->out_fd);
    }
    atomic_store(&p->connected, false);
    pthread_mutex_unlock(&p->send_mu);
}

int dsm_net_send(dsm_ctx_t *ctx, uint32_t dst, const msg_header_t *hdr, const void *payload)
{
    if (dst >= MAX_NODES) return DSM_ERR_INVALID;
    if (hdr->payload_len > 0 && !payload) return DSM_ERR_INVALID;

    /* Un enlace puede morir entre la conexion y la escritura. Se reintenta una
     * sola vez con un descriptor nuevo: escribir a un socket ya cerrado no se
     * reporta como exito silencioso. */
    for (uint32_t attempt = 0; attempt < 2u; attempt++) {
        int conn_rc = peer_ensure_connected(ctx, dst);
        if (conn_rc != DSM_OK) {
            return conn_rc;
        }

        dsm_peer_t *p = &ctx->net.peers[dst];
        pthread_mutex_lock(&p->send_mu);
        if (p->out_fd < 0) {
            /* Otro emisor cerro el descriptor entre ambas secciones. */
            pthread_mutex_unlock(&p->send_mu);
            continue;
        }

        msg_header_t h = *hdr;
        uint8_t raw_hdr[DSM_HDR_LEN];
        dsm_wire_finish(&h, raw_hdr, payload, hdr->payload_len);

        /* La escritura hereda el plazo de envio: un par que deja de leer no
         * puede retener send_mu ni el hilo emisor de forma indefinida. */
        uint32_t send_ms = ctx->cfg.send_timeout_ms ? ctx->cfg.send_timeout_ms : 2000u;
        if (dsm_send_full_deadline(p->out_fd, raw_hdr, DSM_HDR_LEN, send_ms) != 0 ||
            (payload && hdr->payload_len > 0 &&
             dsm_send_full_deadline(p->out_fd, payload, hdr->payload_len, send_ms) != 0)) {
            shutdown(p->out_fd, SHUT_RDWR);
            fd_close_quiet(&p->out_fd);
            atomic_store(&p->connected, false);
            pthread_mutex_unlock(&p->send_mu);
            continue;
        }

        atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN + hdr->payload_len, memory_order_relaxed);

        /*
         * El listener procesa una trama y cierra su conexión. Mantener el
         * descriptor como persistente haría que la siguiente trama viajara por
         * un socket que el receptor ya cerró.
         */
        shutdown(p->out_fd, SHUT_RDWR);
        fd_close_quiet(&p->out_fd);
        atomic_store(&p->connected, false);
        pthread_mutex_unlock(&p->send_mu);
        return DSM_OK;
    }

    return DSM_ERR_NET;
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
    hdr.epoch         = atomic_load(&ctx->memb.epoch);
    hdr.payload_len   = DSM_HEARTBEAT_ID_WIRE_LEN;

    uint8_t payload[DSM_HEARTBEAT_ID_WIRE_LEN];
    uint64_t inc_net = dsm_htonll(1);
    uint32_t fp_net  = htonl(free_pages);
    memcpy(payload, &inc_net, 8);
    memcpy(payload + 8, &fp_net, 4);

    /* La membresía del emisor viaja tras la cabecera del latido. Un receptor
     * que no recibió el JOIN original puede incorporar la ranura desde aquí y
     * pedir una vista completa en su siguiente pasada de descubrimiento. */
    membership_wire_t self;
    memset(&self, 0, sizeof(self));
    memcpy(self.uuid, ctx->memb.self_uuid, 16);
    pthread_rwlock_rdlock(&ctx->memb.rw);
    self.slot = (uint8_t)ctx->memb.self_slot;
    self.ip   = ctx->memb.self_ip ? ctx->memb.self_ip
                                  : htonl(INADDR_LOOPBACK);
    pthread_rwlock_unlock(&ctx->memb.rw);
    self.port        = ctx->net.listen_port;
    self.state       = DSM_NODE_ALIVE;
    self.incarnation = 1;
    uint32_t ps = ctx->cfg.page_size ? ctx->cfg.page_size : 4096u;
    self.mem_pages = ctx->cfg.client_mode ? 0u
                                          : (uint32_t)(ctx->cfg.node_mem / ps);
    dsm_pack_membership(&self, payload + DSM_HEARTBEAT_WIRE_LEN);

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

uint32_t dsm_net_next_req(dsm_ctx_t *ctx)
{
    return (uint32_t)atomic_fetch_add(&ctx->net.req_counter, 1) + 1u;
}

/* Ejecuta un intento completo de RPC: registra la solicitud pendiente con un
 * req_id nuevo, envia la trama y espera la respuesta hasta
 * REQUEST_TIMEOUT_MS. Devuelve DSM_ERR_TIMEOUT o DSM_ERR_NET ante fallos de
 * transporte, que son los unicos sobre los que tiene sentido reintentar. */
static int net_rpc_attempt(dsm_ctx_t *ctx, uint32_t dst, uint16_t type,
                           uint32_t page_id, uint32_t tseq, uint8_t access,
                           uint8_t flags, const void *tx_payload, uint32_t tx_len,
                           uint16_t *out_status, void *rx_payload,
                           uint32_t rx_capacity, uint32_t *rx_len,
                           uint32_t req_id, uint32_t *out_tseq)
{
    pending_req_t pending;
    memset(&pending, 0, sizeof(pending));
    pending.req_id      = req_id ? req_id : dsm_net_next_req(ctx);
    pending.page_id     = page_id;
    pending.rx_payload  = rx_payload;
    pending.rx_capacity = rx_capacity;
    pending.result      = DSM_ERR_TIMEOUT;
    if (sem_init(&pending.sem, 0, 0) != 0) {
        return DSM_ERR_MEM;
    }

    uint32_t bucket = pending.req_id % 1024u;
    pthread_mutex_lock(&ctx->net.pending_mu);
    pending.next = ctx->net.pending[bucket];
    ctx->net.pending[bucket] = &pending;
    atomic_fetch_add_explicit(&ctx->ctr.pending_count, 1, memory_order_relaxed);
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
    hdr.epoch         = atomic_load(&ctx->memb.epoch);
    hdr.payload_len   = tx_len;

    int send_rc = dsm_net_send(ctx, dst, &hdr, tx_payload);
    if (send_rc != DSM_OK) {
        pending_remove(ctx, &pending);
        if (sem_destroy(&pending.sem) != 0) {
            DSM_LOG_WARN("sem_destroy de la solicitud %u fallo", pending.req_id);
        }
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
        int wait_errno = errno;
        pending_remove(ctx, &pending);
        if (sem_destroy(&pending.sem) != 0) {
            DSM_LOG_WARN("sem_destroy de la solicitud %u fallo", pending.req_id);
        }
        return (wait_errno == ETIMEDOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
    }

    pending_remove(ctx, &pending);
    if (out_status) *out_status = pending.status;
    int result = pending.result;
    /* El emisor declara la capacidad del buffer: una respuesta mayor no debe
     * anunciarse como valida. */
    if (pending.rx_len > rx_capacity) {
        result = DSM_ERR_NO_SPACE;
        pending.rx_len = 0;
    }
    *rx_len = pending.rx_len;
    if (out_tseq) *out_tseq = pending.rx_tseq;
    if (sem_destroy(&pending.sem) != 0) {
        DSM_LOG_WARN("sem_destroy de la solicitud %u fallo", pending.req_id);
        if (result == DSM_OK) {
            result = DSM_ERR_NET;
        }
    }
    return result;
}

int dsm_net_rpc_ex(dsm_ctx_t *ctx, uint32_t dst, uint16_t type, uint32_t page_id,
                   uint32_t tseq, uint8_t access, uint8_t flags,
                   const void *tx_payload, uint32_t tx_len,
                   uint16_t *out_status, void *rx_payload, uint32_t *rx_len,
                   uint32_t req_id, uint32_t *out_tseq)
{
    if (!ctx || dst >= MAX_NODES || tx_len > DSM_MAX_PAYLOAD ||
        (tx_len > 0 && !tx_payload) || !rx_len) {
        return DSM_ERR_INVALID;
    }
    if (rx_payload == NULL && *rx_len != 0) {
        return DSM_ERR_INVALID;
    }
    if (out_tseq) *out_tseq = 0;

    /* Solo los fallos de transporte justifican reenviar: una respuesta con
     * estado distinto de OK es una respuesta del protocolo y llegaria igual
     * en el siguiente intento. */
    uint32_t attempts = ctx->cfg.request_retries ? ctx->cfg.request_retries : 1u;
    uint32_t capacity = *rx_len;
    int rc = DSM_ERR_NET;

    for (uint32_t attempt = 0; attempt < attempts; attempt++) {
        if (attempt > 0) {
            /* Espera exponencial acotada por RECONNECT_MAX_MS; cada reintento
             * abre un enlace nuevo porque dsm_net_send cierra el descriptor
             * al terminar cada trama. */
            uint32_t backoff = ctx->cfg.reconnect_min_ms;
            for (uint32_t i = 1; i < attempt && backoff < ctx->cfg.reconnect_max_ms; i++) {
                backoff = (backoff > UINT32_MAX / 2u) ? UINT32_MAX : backoff * 2u;
            }
            if (backoff > ctx->cfg.reconnect_max_ms) {
                backoff = ctx->cfg.reconnect_max_ms;
            }
            if (backoff > 0) {
                sleep_ms(backoff);
            }
        }

        *rx_len = capacity;
        rc = net_rpc_attempt(ctx, dst, type, page_id, tseq, access, flags,
                             tx_payload, tx_len, out_status, rx_payload,
                             capacity, rx_len, req_id, out_tseq);
        if (rc != DSM_ERR_TIMEOUT && rc != DSM_ERR_NET) {
            break;
        }
    }

    return rc;
}

int dsm_net_rpc(dsm_ctx_t *ctx, uint32_t dst, uint16_t type, uint32_t page_id,
                uint32_t tseq, uint8_t access, uint8_t flags,
                const void *tx_payload, uint32_t tx_len,
                uint16_t *out_status, void *rx_payload, uint32_t *rx_len)
{
    return dsm_net_rpc_ex(ctx, dst, type, page_id, tseq, access, flags,
                          tx_payload, tx_len, out_status, rx_payload, rx_len,
                          0, NULL);
}

