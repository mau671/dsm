#include "discovery.h"
#include "dsm_internal.h"
#include "wire.h"
#include "log.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/socket.h>

int uuid_load_or_create(dsm_ctx_t *ctx, uint8_t out[16])
{
    const char *tag = getenv("DSM_NODE_TAG");
    return uuid_load_or_create_file(ctx->cfg.cluster_id, tag, out);
}

int discovery_bind_first_free(dsm_ctx_t *ctx)
{
    dsm_config_t *cfg = &ctx->cfg;
    int fd = -1;

    for (uint16_t port = cfg->port_min; port <= cfg->port_max; port++) {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            continue;
        }

        set_cloexec(fd);
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port        = htons(port);

        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            ctx->net.listen_fd   = fd;
            ctx->net.listen_port = port;
            DSM_LOG_INFO("Puerto de escucha asignado localmente: %u", port);
            return DSM_OK;
        }

        close(fd);
    }

    DSM_LOG_ERR("No hay puertos disponibles en el rango asignado [%u, %u]",
                cfg->port_min, cfg->port_max);
    return DSM_ERR_NET;
}

static int probe_peer(dsm_ctx_t *ctx, const struct sockaddr_in *addr, welcome_t *w)
{
    int fd = dsm_connect_timeout(addr, ctx->cfg.connect_timeout_ms);
    if (fd < 0) return DSM_ERR_NET;

    /* Enviar mensaje HELLO: 20 bytes (uuid[16] + raw_hash[4]) */
    uint8_t hello_payload[20];
    memcpy(hello_payload, ctx->memb.self_uuid, 16);
    uint32_t hash_net = htonl(ctx->cfg.raw_hash);
    memcpy(hello_payload + 16, &hash_net, 4);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_HELLO;
    hdr.flags         = DSM_FLAG_PROBE;
    hdr.payload_len   = 20;

    uint8_t raw_hdr[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw_hdr, hello_payload, 20);

    if (dsm_send_full(fd, raw_hdr, DSM_HDR_LEN) != 0 ||
        dsm_send_full(fd, hello_payload, 20) != 0) {
        close(fd);
        return DSM_ERR_NET;
    }

    /* Recibir respuesta WELCOME */
    uint8_t rx_hdr_buf[DSM_HDR_LEN];
    if (dsm_read_full_deadline(fd, rx_hdr_buf, DSM_HDR_LEN, ctx->cfg.discovery_timeout_ms) != DSM_OK) {
        close(fd);
        return DSM_ERR_NET;
    }

    msg_header_t rx_hdr;
    if (dsm_wire_unpack(rx_hdr_buf, &rx_hdr) != DSM_OK) {
        close(fd);
        return DSM_ERR_PROTO;
    }

    if (rx_hdr.type != MSG_WELCOME) {
        close(fd);
        return DSM_ERR_PROTO;
    }

    if (rx_hdr.status == DSM_ST_CONFIG_MISMATCH) {
        DSM_LOG_ERR("Rechazado por par: discrepancia en dsm.conf (raw_hash distinto)");
        close(fd);
        return DSM_ERR_CONFIG;
    }

    if (rx_hdr.payload_len == 0 || rx_hdr.payload_len > DSM_MAX_PAYLOAD) {
        close(fd);
        return DSM_ERR_PROTO;
    }

    uint8_t *rx_payload = malloc(rx_hdr.payload_len);
    if (!rx_payload) {
        close(fd);
        return DSM_ERR_MEM;
    }

    if (dsm_read_full_deadline(fd, rx_payload, rx_hdr.payload_len, ctx->cfg.discovery_timeout_ms) != DSM_OK) {
        free(rx_payload);
        close(fd);
        return DSM_ERR_NET;
    }

    if (dsm_wire_validate(&rx_hdr, rx_hdr_buf, rx_payload, ctx->cfg.cluster_id) != DSM_OK) {
        free(rx_payload);
        close(fd);
        return DSM_ERR_PROTO;
    }

    int unpack_rc = dsm_unpack_welcome(rx_payload, rx_hdr.payload_len, w);
    free(rx_payload);
    close(fd);

    return (unpack_rc == DSM_OK) ? DSM_OK : DSM_ERR_PROTO;
}

int discovery_scan(dsm_ctx_t *ctx, welcome_t *out)
{
    dsm_config_t *cfg = &ctx->cfg;

    for (uint32_t h = 0; h < cfg->peer_host_count; h++) {
        struct hostent *he = gethostbyname(cfg->peer_hosts[h]);
        if (!he || !he->h_addr_list[0]) {
            continue;
        }

        struct in_addr in;
        memcpy(&in, he->h_addr_list[0], sizeof(in));

        for (uint16_t port = cfg->port_min; port <= cfg->port_max; port++) {
            if (port == ctx->net.listen_port &&
                (in.s_addr == htonl(INADDR_LOOPBACK) || in.s_addr == htonl(INADDR_ANY) ||
                 (in.s_addr & htonl(0xFF000000)) == htonl(0x7F000000))) {
                continue; /* No escanearse a sí mismo en loopback */
            }

            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr   = in;
            addr.sin_port   = htons(port);

            welcome_t w;
            int pr = probe_peer(ctx, &addr, &w);
            if (pr == DSM_ERR_CONFIG) {
                DSM_LOG_ERR("probe_peer devolvio DSM_ERR_CONFIG en %s:%u", cfg->peer_hosts[h], port);
                return DSM_ERR_CONFIG;
            }
            if (pr == DSM_OK) {
                /* Descartar si el par tiene nuestro mismo UUID */
                if (memcmp(w.origin_uuid, ctx->memb.self_uuid, 16) == 0) {
                    continue;
                }
                *out = w;
                DSM_LOG_INFO("Par descubierto en %s:%u (epoch=%lu, nodos=%u)",
                             cfg->peer_hosts[h], port, (unsigned long)w.epoch, w.count);
                return DSM_OK;
            }
        }
    }

    return DSM_ERR_NOT_FOUND;
}

int discovery_send_join_all(dsm_ctx_t *ctx, const welcome_t *w, uint16_t *status)
{
    *status = DSM_ST_OK;
    membership_wire_t my_wire;
    memset(&my_wire, 0, sizeof(my_wire));
    memcpy(my_wire.uuid, ctx->memb.self_uuid, 16);
    my_wire.ip          = htonl(INADDR_LOOPBACK);
    my_wire.port        = ctx->net.listen_port;
    my_wire.slot        = (uint8_t)ctx->memb.self_slot;
    my_wire.state       = DSM_NODE_ALIVE;
    my_wire.incarnation = 1;
    my_wire.mem_pages   = (uint32_t)(ctx->cfg.node_mem / ctx->cfg.page_size);

    uint8_t join_payload[DSM_MEMBERSHIP_WIRE_LEN];
    dsm_pack_membership(&my_wire, join_payload);

    for (uint32_t i = 0; i < w->count; i++) {
        const membership_wire_t *m = &w->members[i];
        if (m->slot == ctx->memb.self_slot) continue;

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = m->ip;
        addr.sin_port        = htons(m->port);

        int fd = dsm_connect_timeout(&addr, ctx->cfg.connect_timeout_ms);
        if (fd < 0) {
            continue;
        }

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_JOIN;
        hdr.src_slot      = ctx->memb.self_slot;
        hdr.dst_slot      = m->slot;
        hdr.epoch         = w->epoch;
        hdr.payload_len   = DSM_MEMBERSHIP_WIRE_LEN;

        uint8_t raw_hdr[DSM_HDR_LEN];
        dsm_wire_finish(&hdr, raw_hdr, join_payload, DSM_MEMBERSHIP_WIRE_LEN);

        if (dsm_send_full(fd, raw_hdr, DSM_HDR_LEN) != 0 ||
            dsm_send_full(fd, join_payload, DSM_MEMBERSHIP_WIRE_LEN) != 0) {
            close(fd);
            continue;
        }

        /* Esperar JOIN_ACK o NACK */
        uint8_t rx_hdr_buf[DSM_HDR_LEN];
        if (dsm_read_full_deadline(fd, rx_hdr_buf, DSM_HDR_LEN, ctx->cfg.request_timeout_ms) == DSM_OK) {
            msg_header_t rx_hdr;
            if (dsm_wire_unpack(rx_hdr_buf, &rx_hdr) == DSM_OK) {
                if (rx_hdr.type == MSG_NACK && rx_hdr.status == DSM_ST_SLOT_TAKEN) {
                    *status = DSM_ST_SLOT_TAKEN;
                    close(fd);
                    return DSM_OK;
                }
                if (rx_hdr.type == MSG_JOIN_ACK) {
                    if (rx_hdr.epoch > ctx->memb.epoch) {
                        ctx->memb.epoch = rx_hdr.epoch;
                    }
                }
            }
        }
        close(fd);
    }

    return DSM_OK;
}

int discovery_join(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    int rc;

    if ((rc = uuid_load_or_create(ctx, m->self_uuid)) != DSM_OK) {
        return rc;
    }

    char uuid_str[37];
    uuid_to_str(m->self_uuid, uuid_str);
    DSM_LOG_INFO("Identificador del nodo (UUID): %s", uuid_str);

    if ((rc = discovery_bind_first_free(ctx)) != DSM_OK) {
        return rc;
    }

    if ((rc = net_start_listener(ctx)) != DSM_OK) {
        return rc;
    }

    welcome_t w;
    memset(&w, 0, sizeof(w));
    rc = discovery_scan(ctx, &w);

    if (rc == DSM_ERR_NOT_FOUND) {
        DSM_LOG_INFO("No se detectaron pares activos. Asumiendo rol de primer nodo (Slot 0, Epoch 1)");
        m->self_slot = 0;
        m->epoch     = 1;
        membership_add_self(ctx);
        hb_start(ctx);
        return DSM_OK;
    }
    if (rc != DSM_OK) {
        return rc;
    }

    /* Fusionar lo recibido en el WELCOME */
    membership_merge_wire(m, &w);
    m->self_slot = membership_first_free_slot(ctx);

    for (uint32_t attempt = 0; attempt < MAX_NODES; attempt++) {
        uint16_t status = DSM_ST_OK;
        rc = discovery_send_join_all(ctx, &w, &status);
        if (rc != DSM_OK) {
            return rc;
        }
        if (status != DSM_ST_SLOT_TAKEN) {
            break;
        }
        DSM_LOG_WARN("Slot %u ocupado. Solicitando siguiente slot libre...", m->self_slot);
        m->self_slot = membership_next_free_slot(ctx, m->self_slot + 1);
    }

    if (m->self_slot == DSM_SLOT_NONE) {
        DSM_LOG_ERR("No hay slots libres en el cluster");
        return DSM_ERR_NO_SPACE;
    }

    if (m->epoch <= w.epoch) {
        m->epoch = w.epoch + 1;
    }
    membership_add_self(ctx);

    /* Establecer de inmediato conexiones salientes con los pares descubiertos */
    for (uint32_t i = 0; i < w.count; i++) {
        uint32_t s = w.members[i].slot;
        if (s != m->self_slot && membership_slot_present(ctx, s)) {
            peer_ensure_connected(ctx, s);
        }
    }

    DSM_LOG_INFO("Union completada exitosamente: Slot asignado = %u, Epoch = %lu",
                 m->self_slot, (unsigned long)m->epoch);

    hb_start(ctx);
    return DSM_OK;
}

void discovery_leave(dsm_ctx_t *ctx)
{
    hb_stop(ctx);

    dsm_membership_t *m = &ctx->memb;
    uint32_t dst[MAX_NODES];
    uint32_t ndst = 0;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s != m->self_slot && membership_slot_present(ctx, s)) {
            dst[ndst++] = s;
        }
    }
    pthread_rwlock_unlock(&m->rw);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_LEAVE;
    hdr.src_slot      = m->self_slot;
    hdr.epoch         = m->epoch;

    for (uint32_t i = 0; i < ndst; i++) {
        uint32_t s = dst[i];
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;

        pthread_rwlock_rdlock(&m->rw);
        addr.sin_addr.s_addr = m->nodes[s].ip;
        addr.sin_port        = htons(m->nodes[s].port);
        pthread_rwlock_unlock(&m->rw);

        int fd = dsm_connect_timeout(&addr, ctx->cfg.connect_timeout_ms);
        if (fd >= 0) {
            hdr.dst_slot = s;
            uint8_t raw_hdr[DSM_HDR_LEN];
            dsm_wire_finish(&hdr, raw_hdr, NULL, 0);
            dsm_send_full(fd, raw_hdr, DSM_HDR_LEN);
            close(fd);
        }
    }

    /* Marcarse localmente como salido */
    pthread_rwlock_wrlock(&m->rw);
    if (m->self_slot < MAX_NODES) {
        m->nodes[m->self_slot].state = DSM_NODE_DEAD;
        m->present[m->self_slot / 8] &= (uint8_t)~(1u << (m->self_slot % 8));
    }
    pthread_rwlock_unlock(&m->rw);
}

/* --- Tabla de membresía local --- */

int membership_init(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    memset(m, 0, sizeof(*m));
    pthread_rwlock_init(&m->rw, NULL);
    m->self_slot = DSM_SLOT_NONE;
    return DSM_OK;
}

void membership_destroy(dsm_ctx_t *ctx)
{
    pthread_rwlock_destroy(&ctx->memb.rw);
}

void membership_add_self(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    pthread_rwlock_wrlock(&m->rw);

    uint32_t s = m->self_slot;
    uint32_t ps = (ctx->cfg.page_size > 0) ? ctx->cfg.page_size : 4096;
    if (s < MAX_NODES) {
        node_info_t *ni = &m->nodes[s];
        memcpy(ni->uuid, m->self_uuid, 16);
        ni->slot        = s;
        ni->ip          = htonl(INADDR_LOOPBACK);
        ni->port        = ctx->net.listen_port;
        ni->state       = DSM_NODE_ALIVE;
        ni->incarnation = 1;
        ni->mem_pages   = (uint32_t)(ctx->cfg.node_mem / ps);
        ni->free_pages  = ni->mem_pages;
        ni->last_hb_ms  = mono_ms();
        ni->missed_hb   = 0;

        m->present[s / 8] |= (uint8_t)(1u << (s % 8));
    }

    pthread_rwlock_unlock(&m->rw);
}

uint32_t membership_first_free_slot(dsm_ctx_t *ctx)
{
    return membership_next_free_slot(ctx, 0);
}

uint32_t membership_next_free_slot(dsm_ctx_t *ctx, uint32_t from)
{
    dsm_membership_t *m = &ctx->memb;
    uint64_t now = mono_ms();
    uint32_t max_nodes = ctx->cfg.max_runtime_nodes;
    if (max_nodes > MAX_NODES) max_nodes = MAX_NODES;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = from; s < max_nodes; s++) {
        bool in_use = (m->present[s / 8] & (1u << (s % 8))) != 0;
        if (!in_use && m->reusable_after_ms[s] <= now) {
            pthread_rwlock_unlock(&m->rw);
            return s;
        }
    }
    pthread_rwlock_unlock(&m->rw);
    return DSM_SLOT_NONE;
}

bool membership_slot_present(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot >= MAX_NODES) return false;
    return (ctx->memb.present[slot / 8] & (1u << (slot % 8))) != 0;
}

bool membership_is_alive(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot >= MAX_NODES) return false;
    dsm_membership_t *m = &ctx->memb;
    pthread_rwlock_rdlock(&m->rw);
    bool alive = (membership_slot_present(ctx, slot) && m->nodes[slot].state == DSM_NODE_ALIVE);
    pthread_rwlock_unlock(&m->rw);
    return alive;
}

void membership_merge_wire(dsm_membership_t *m, const welcome_t *w)
{
    pthread_rwlock_wrlock(&m->rw);
    if (w->epoch > m->epoch) {
        m->epoch = w->epoch;
    }

    for (uint32_t i = 0; i < w->count; i++) {
        const membership_wire_t *wire = &w->members[i];
        uint32_t s = wire->slot;
        if (s >= MAX_NODES) continue;

        node_info_t *ni = &m->nodes[s];
        memcpy(ni->uuid, wire->uuid, 16);
        ni->slot        = s;
        ni->ip          = wire->ip;
        ni->port        = wire->port;
        ni->state       = wire->state;
        ni->incarnation = wire->incarnation;
        ni->mem_pages   = wire->mem_pages;
        ni->free_pages  = wire->mem_pages;
        ni->last_hb_ms  = mono_ms();
        ni->missed_hb   = 0;

        m->present[s / 8] |= (uint8_t)(1u << (s % 8));
    }
    pthread_rwlock_unlock(&m->rw);
}

void membership_on_heartbeat(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.src_slot;
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        node_info_t *ni = &ctx->memb.nodes[s];
        ni->last_hb_ms = mono_ms();
        ni->missed_hb  = 0;
        if (ni->state == DSM_NODE_SUSPECT) {
            ni->state = DSM_NODE_ALIVE;
            DSM_LOG_INFO("Nodo en slot %u recupero estado ALIVE", s);
        }
        if (m->payload && m->hdr.payload_len >= 12) {
            uint32_t free_p = 0;
            memcpy(&free_p, (const uint8_t *)m->payload + 8, 4);
            ni->free_pages = ntohl(free_p);
        }
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
}

void membership_on_join_fd(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd)
{
    if (!m->payload || m->hdr.payload_len < DSM_MEMBERSHIP_WIRE_LEN) return;

    membership_wire_t wire;
    dsm_unpack_membership(m->payload, &wire);

    uint32_t s = wire.slot;
    if (s >= MAX_NODES) {
        if (cfd >= 0) {
            msg_header_t nack_hdr;
            memset(&nack_hdr, 0, sizeof(nack_hdr));
            nack_hdr.magic         = DSM_MAGIC;
            nack_hdr.cluster_id    = ctx->cfg.cluster_id;
            nack_hdr.proto_version = DSM_PROTO_VERSION;
            nack_hdr.type          = MSG_NACK;
            nack_hdr.src_slot      = ctx->memb.self_slot;
            nack_hdr.dst_slot      = m->hdr.src_slot;
            nack_hdr.req_id        = m->hdr.req_id;
            nack_hdr.status        = DSM_ST_NO_SPACE;
            uint8_t nack_raw[DSM_HDR_LEN];
            dsm_wire_finish(&nack_hdr, nack_raw, NULL, 0);
            dsm_send_full(cfd, nack_raw, DSM_HDR_LEN);
        } else {
            net_send_nack(ctx, m->hdr.src_slot, m->hdr.req_id, DSM_ST_NO_SPACE);
        }
        return;
    }

    pthread_rwlock_wrlock(&ctx->memb.rw);
    bool taken = membership_slot_present(ctx, s) &&
                 (memcmp(ctx->memb.nodes[s].uuid, wire.uuid, 16) != 0);

    if (taken) {
        pthread_rwlock_unlock(&ctx->memb.rw);
        if (cfd >= 0) {
            msg_header_t nack_hdr;
            memset(&nack_hdr, 0, sizeof(nack_hdr));
            nack_hdr.magic         = DSM_MAGIC;
            nack_hdr.cluster_id    = ctx->cfg.cluster_id;
            nack_hdr.proto_version = DSM_PROTO_VERSION;
            nack_hdr.type          = MSG_NACK;
            nack_hdr.src_slot      = ctx->memb.self_slot;
            nack_hdr.dst_slot      = m->hdr.src_slot;
            nack_hdr.req_id        = m->hdr.req_id;
            nack_hdr.status        = DSM_ST_SLOT_TAKEN;
            uint8_t nack_raw[DSM_HDR_LEN];
            dsm_wire_finish(&nack_hdr, nack_raw, NULL, 0);
            dsm_send_full(cfd, nack_raw, DSM_HDR_LEN);
        } else {
            net_send_nack(ctx, m->hdr.src_slot, m->hdr.req_id, DSM_ST_SLOT_TAKEN);
        }
        return;
    }

    node_info_t *ni = &ctx->memb.nodes[s];
    memcpy(ni->uuid, wire.uuid, 16);
    ni->slot        = s;
    ni->ip          = wire.ip;
    ni->port        = wire.port;
    ni->state       = DSM_NODE_ALIVE;
    ni->incarnation = wire.incarnation;
    ni->mem_pages   = wire.mem_pages;
    ni->free_pages  = wire.mem_pages;
    ni->last_hb_ms  = mono_ms();
    ni->missed_hb   = 0;
    ctx->memb.present[s / 8] |= (uint8_t)(1u << (s % 8));

    if (m->hdr.epoch >= ctx->memb.epoch) {
        ctx->memb.epoch = m->hdr.epoch + 1;
    }

    pthread_rwlock_unlock(&ctx->memb.rw);

    /* Establecer conexion saliente de vuelta hacia el nuevo nodo */
    peer_ensure_connected(ctx, s);

    /* Responder con JOIN_ACK directamente en el socket abierto si existe */
    msg_header_t ack_hdr;
    memset(&ack_hdr, 0, sizeof(ack_hdr));
    ack_hdr.magic         = DSM_MAGIC;
    ack_hdr.cluster_id    = ctx->cfg.cluster_id;
    ack_hdr.proto_version = DSM_PROTO_VERSION;
    ack_hdr.type          = MSG_JOIN_ACK;
    ack_hdr.src_slot      = ctx->memb.self_slot;
    ack_hdr.dst_slot      = s;
    ack_hdr.epoch         = ctx->memb.epoch;
    ack_hdr.req_id        = m->hdr.req_id;
    ack_hdr.status        = DSM_ST_OK;

    if (cfd >= 0) {
        uint8_t ack_raw[DSM_HDR_LEN];
        dsm_wire_finish(&ack_hdr, ack_raw, NULL, 0);
        dsm_send_full(cfd, ack_raw, DSM_HDR_LEN);
        atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN, memory_order_relaxed);
    } else {
        dsm_net_send(ctx, s, &ack_hdr, NULL);
    }
    DSM_LOG_INFO("Nuevo nodo incorporado a la membresia: Slot %u (Port %u)", s, wire.port);
}

void membership_on_join(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    membership_on_join_fd(ctx, m, -1);
}

void membership_on_join_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

void membership_on_node_down(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.page_id; /* slot afectado en page_id */
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        ctx->memb.nodes[s].state = DSM_NODE_DEAD;
        ctx->memb.present[s / 8] &= (uint8_t)~(1u << (s % 8));
        ctx->memb.reusable_after_ms[s] = mono_ms() + ctx->cfg.reconnect_max_ms;
        DSM_LOG_WARN("Notificacion NODE_DOWN procesada para slot %u", s);
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
}

void membership_on_leave(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.src_slot;
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        ctx->memb.nodes[s].state = DSM_NODE_DEAD;
        ctx->memb.present[s / 8] &= (uint8_t)~(1u << (s % 8));
        ctx->memb.reusable_after_ms[s] = mono_ms() + ctx->cfg.reconnect_max_ms;
        DSM_LOG_INFO("Salida ordenada (LEAVE) del nodo en slot %u", s);
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
}

void membership_on_resp(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

void membership_send_resp(dsm_ctx_t *ctx, uint32_t dst)
{
    (void)ctx;
    (void)dst;
}

uint32_t membership_total_pages(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    uint32_t total = 0;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (membership_slot_present(ctx, s) && m->nodes[s].state != DSM_NODE_DEAD) {
            total += m->nodes[s].mem_pages;
        }
    }
    pthread_rwlock_unlock(&m->rw);
    return total;
}

void membership_evaluate(dsm_ctx_t *ctx, uint64_t now_ms)
{
    dsm_membership_t *m = &ctx->memb;
    uint32_t dead_slots[MAX_NODES];
    uint32_t dead_count = 0;

    pthread_rwlock_wrlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == m->self_slot || !membership_slot_present(ctx, s)) {
            continue;
        }
        node_info_t *ni = &m->nodes[s];
        if (ni->state == DSM_NODE_DEAD) {
            continue;
        }

        uint64_t elapsed = now_ms - ni->last_hb_ms;
        uint32_t missed = (uint32_t)(elapsed / ctx->cfg.hb_interval_ms);
        ni->missed_hb = (uint8_t)(missed > 255 ? 255 : missed);

        if (ni->missed_hb >= ctx->cfg.hb_dead) {
            ni->state = DSM_NODE_DEAD;
            m->present[s / 8] &= (uint8_t)~(1u << (s % 8));
            m->reusable_after_ms[s] = now_ms + ctx->cfg.reconnect_max_ms;
            dead_slots[dead_count++] = s;
            DSM_LOG_WARN("Nodo en slot %u dictaminado DEAD (latidos perdidos: %u)", s, ni->missed_hb);
        } else if (ni->missed_hb >= ctx->cfg.hb_suspect && ni->state == DSM_NODE_ALIVE) {
            ni->state = DSM_NODE_SUSPECT;
            DSM_LOG_WARN("Nodo en slot %u degradado a SUSPECT (latidos perdidos: %u)", s, ni->missed_hb);
        }
    }
    pthread_rwlock_unlock(&m->rw);

    /* Difundir NODE_DOWN para nodos declarados muertos */
    for (uint32_t d = 0; d < dead_count; d++) {
        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_NODE_DOWN;
        hdr.src_slot      = m->self_slot;
        hdr.page_id       = dead_slots[d];

        for (uint32_t s = 0; s < MAX_NODES; s++) {
            if (s != m->self_slot && membership_slot_present(ctx, s)) {
                dsm_net_send(ctx, s, &hdr, NULL);
            }
        }
    }
}

/* --- Hilo de Latidos (Heartbeat) --- */

static void *hb_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;

    while (atomic_load(&ctx->running)) {
        /* Despertar en intervalos cortos (50ms) para respuesta rápida ante parada */
        uint32_t slept = 0;
        while (atomic_load(&ctx->running) && slept < ctx->cfg.hb_interval_ms) {
            uint32_t step = (ctx->cfg.hb_interval_ms - slept > 50) ? 50 : (ctx->cfg.hb_interval_ms - slept);
            sleep_ms(step);
            slept += step;
        }
        if (!atomic_load(&ctx->running)) break;

        uint64_t now = mono_ms();
        membership_evaluate(ctx, now);

        uint32_t dst[MAX_NODES];
        uint32_t ndst = 0;

        pthread_rwlock_rdlock(&ctx->memb.rw);
        for (uint32_t s = 0; s < MAX_NODES; s++) {
            if (s != ctx->memb.self_slot && membership_slot_present(ctx, s)) {
                dst[ndst++] = s;
            }
        }
        pthread_rwlock_unlock(&ctx->memb.rw);

        for (uint32_t i = 0; i < ndst; i++) {
            net_send_heartbeat(ctx, dst[i], 0);
        }
    }

    return NULL;
}

int hb_start(dsm_ctx_t *ctx)
{
    if (pthread_create(&ctx->hb_thread, NULL, hb_thread_main, ctx) != 0) {
        DSM_LOG_ERR("No se pudo iniciar hilo de heartbeat: %s", strerror(errno));
        return DSM_ERR_MEM;
    }
    return DSM_OK;
}

void hb_stop(dsm_ctx_t *ctx)
{
    if (ctx->hb_thread) {
        pthread_join(ctx->hb_thread, NULL);
        ctx->hb_thread = 0;
    }
}
