#include "discovery.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "wire.h"
#include "log.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <fcntl.h>
#include <poll.h>

/*
 * Vueltas maximas del reparto de ranuras. Cada vuelta sondea los pares
 * anunciados por UDP y envia un JOIN por cada uno, de modo que el reparto
 * converge en tantas vueltas como nodos simultaneos haya; el limite solo
 * acota un caso patologico y lo reporta como error.
 */
#define DISCOVERY_RESOLVE_ROUNDS 32u

/* Pausa máxima entre pasadas de la ventana de asentamiento del primer nodo.
 * La espera real es por condición de progreso de la membresía; este plazo solo
 * acota el reescaneo cuando el aviso proviene de un par que aún no publicó su
 * ranura. */
#define DISCOVERY_SETTLE_STEP_MS 20u

/*
 * Plazo mínimo que se concede a un nodo recién incorporado antes de evaluar sus
 * latidos. Un JOIN puede recibirse mientras el nuevo nodo todavía recorre el
 * rango de puertos y resuelve sus vecinos, de modo que su primer latido puede
 * tardar más que HB_DEAD. El plazo se aplica solo mientras el nodo no haya
 * enviado ningún latido; en cuanto llega el primero, rige la detección normal.
 */
#define DISCOVERY_JOIN_GRACE_MS 30000u

/* Cadencia de los anuncios UDP y plazo de sondeo del hilo de descubrimiento. */
#define DISCOVERY_ANNOUNCE_MS 500u
#define DISCOVERY_POLL_MS     100u

/* Intervalo mínimo entre avisos por un mismo par con configuración distinta.
 * El anuncio se repite cada DISCOVERY_ANNOUNCE_MS; sin este límite el aviso
 * enterraría el resto del registro. */
#define DISCOVERY_HASH_WARN_MS 5000u

/*
 * Cerrojo de descubrimiento del cluster.
 *
 * El reparto de ranuras es una decision sobre una vista compartida: cuantos mas
 * nodos la consultan a la vez, mas vueltas necesita para converger. Serializar
 * el descubrimiento entre los nodos de un mismo equipo convierte un arranque
 * simultaneo en una secuencia de uniones, de modo que cada nodo ve las ranuras
 * ya reclamadas y elige la siguiente sin competir. Entre equipos distintos el
 * cerrojo no aplica y el reparto se resuelve igualmente con el desempate por
 * UUID y los JOIN rechazados.
 *
 * El cerrojo se toma solo durante discovery_join y lo libera el sistema al
 * cerrar el descriptor, incluso si el proceso muere.
 */
static int discovery_lock_acquire(dsm_ctx_t *ctx)
{
    char dir[256];
    const char *home = getenv("HOME");
    snprintf(dir, sizeof(dir), "%s/.dsm", home ? home : "/tmp");

    char path[320];
    snprintf(path, sizeof(path), "%s/%08x-%u-%u.lock", dir,
             ctx->cfg.cluster_id, ctx->cfg.port_min, ctx->cfg.port_max);

    int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) {
        return -1; /* sin cerrojo: el reparto sigue por el desempate por UUID */
    }

    int rc;
    do {
        rc = flock(fd, LOCK_EX);
    } while (rc != 0 && errno == EINTR);

    if (rc != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static void discovery_lock_release(int fd)
{
    if (fd >= 0) {
        close(fd);
    }
}

/* Direccion IPv4 en orden de red. INADDR_ANY no identifica un destino
 * alcanzable y se trata como desconocida. */
static bool ip_is_usable(uint32_t ip)
{
    return ip != 0 && ip != htonl(INADDR_ANY);
}

static bool ip_is_loopback(uint32_t ip)
{
    return (ip & htonl(0xFF000000u)) == htonl(0x7F000000u);
}

/* Una observacion loopback no reemplaza una direccion ya alcanzable: sirve
 * para el mismo equipo, pero no debe publicarse al resto del cluster. */
static bool ip_replaceable(uint32_t current, uint32_t observed)
{
    if (!ip_is_usable(observed)) {
        return false;
    }
    if (!ip_is_usable(current)) {
        return true;
    }
    return !(ip_is_loopback(observed) && !ip_is_loopback(current));
}

/* IP del extremo remoto de un socket ya conectado, en orden de red. */
static uint32_t socket_peer_ip(int fd)
{
    struct sockaddr_in sa;
    socklen_t len = sizeof(sa);

    if (fd < 0 || getpeername(fd, (struct sockaddr *)&sa, &len) != 0 ||
        sa.sin_family != AF_INET) {
        return 0;
    }
    return sa.sin_addr.s_addr;
}

/* IP local que el kernel eligio para un socket, en orden de red. Es la
 * direccion con la que el par alcanza a este nodo. */
static uint32_t socket_local_ip(int fd)
{
    struct sockaddr_in sa;
    socklen_t len = sizeof(sa);

    if (fd < 0 || getsockname(fd, (struct sockaddr *)&sa, &len) != 0 ||
        sa.sin_family != AF_INET) {
        return 0;
    }
    return sa.sin_addr.s_addr;
}

/* Dirección legible para el diagnóstico, en orden de red. */
static const char *ip_to_text(uint32_t ip, char *out, size_t cap)
{
    struct in_addr addr;
    addr.s_addr = ip;
    if (!inet_ntop(AF_INET, &addr, out, cap)) {
        snprintf(out, cap, "0.0.0.0");
    }
    return out;
}

/* Interfaz IPv4 principal, descubierta sin enviar tráfico: connect sobre un
 * datagrama solo fija el destino por defecto. Cero significa "la que elija el
 * kernel", que es el respaldo cuando no hay una ruta fuera de la máquina. */
static uint32_t discovery_principal_iface(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family      = AF_INET;
    dst.sin_port        = htons(9u);
    dst.sin_addr.s_addr = htonl(0x08080808u);

    uint32_t ip = 0;
    if (connect(fd, (struct sockaddr *)&dst, sizeof(dst)) == 0) {
        ip = socket_local_ip(fd);
    }
    close(fd);
    return ip;
}

/* Nombre que se publica en el anuncio: etiqueta del proceso o nombre de la
 * máquina. El campo del cable tiene longitud fija y siempre termina en cero. */
static void discovery_node_name(char out[32])
{
    const char *tag = getenv("DSM_NODE_TAG");
    if (tag && tag[0] != '\0') {
        snprintf(out, 32, "%s", tag);
    } else if (gethostname(out, 32) != 0) {
        snprintf(out, 32, "dsm");
    }
    out[31] = '\0';
}

/* Construye el anuncio campo por campo con el estado vigente del nodo. */
static size_t discovery_build_announcement(dsm_ctx_t *ctx,
                                           uint8_t out[DSM_DISCOVERY_WIRE_LEN])
{
    discovery_wire_t w;
    memset(&w, 0, sizeof(w));

    w.magic         = DSM_MAGIC;
    w.proto_version = DSM_PROTO_VERSION;
    w.cluster_id    = ctx->cfg.cluster_id;
    w.shared_hash   = ctx->cfg.shared_hash;
    memcpy(w.uuid, ctx->memb.self_uuid, 16);
    discovery_node_name((char *)w.name);

    pthread_rwlock_rdlock(&ctx->memb.rw);
    w.slot = ctx->memb.self_slot;
    /* INADDR_ANY nunca es un destino alcanzable; mientras no se observe una IP
     * real se publica el loopback, suficiente para el clúster local. */
    w.ip = ip_is_usable(ctx->memb.self_ip)
               ? ctx->memb.self_ip
               : htonl(INADDR_LOOPBACK);
    pthread_rwlock_unlock(&ctx->memb.rw);

    w.port      = ctx->net.listen_port;
    uint32_t ps = (ctx->cfg.page_size > 0) ? ctx->cfg.page_size : 4096u;
    w.mem_pages = ctx->cfg.client_mode ? 0u : (uint32_t)(ctx->cfg.node_mem / ps);
    w.role      = ctx->cfg.client_mode ? 1u : 0u;

    return dsm_pack_discovery(&w, out);
}

/* Envía un anuncio por la interfaz indicada. Un entorno sin ruta multicast o
 * sin broadcast no detiene el hilo: el próximo periodo vuelve a intentarlo. */
static void discovery_send_packet(dsm_ctx_t *ctx, const uint8_t *buf, size_t len,
                                  uint32_t iface, bool multicast,
                                  const struct sockaddr_in *dst)
{
    if (multicast && iface != 0) {
        if (setsockopt(ctx->disc.fd, IPPROTO_IP, IP_MULTICAST_IF,
                       &iface, sizeof(iface)) != 0) {
            return;
        }
    }
    (void)sendto(ctx->disc.fd, buf, len, 0,
                 (const struct sockaddr *)dst, sizeof(*dst));
}

/* Emite el anuncio por todas las vías disponibles. La copia por loopback hace
 * que dos procesos de la misma máquina se vean aunque la ruta multicast por
 * defecto no devuelva los paquetes; la copia por la interfaz principal sirve
 * para nodos en equipos distintos. El broadcast es el respaldo. */
static void discovery_announce_once(dsm_ctx_t *ctx)
{
    uint8_t buf[DSM_DISCOVERY_WIRE_LEN];
    size_t len = discovery_build_announcement(ctx, buf);

    struct sockaddr_in dest;
    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port   = htons(ctx->cfg.discovery_port);
    dest.sin_addr.s_addr = ctx->disc.multicast
                               ? htonl(ctx->cfg.discovery_group)
                               : htonl(INADDR_BROADCAST);

    if (ctx->disc.multicast) {
        discovery_send_packet(ctx, buf, len, htonl(INADDR_LOOPBACK), true, &dest);

        uint32_t principal = discovery_principal_iface();
        uint32_t loopback  = htonl(INADDR_LOOPBACK);
        if (ip_is_usable(principal) && principal != loopback) {
            discovery_send_packet(ctx, buf, len, principal, true, &dest);
            uint32_t restore = ctx->disc.mcast_if;
            (void)setsockopt(ctx->disc.fd, IPPROTO_IP, IP_MULTICAST_IF,
                             &restore, sizeof(restore));
        }
    }

    dest.sin_addr.s_addr = htonl(INADDR_BROADCAST);
    discovery_send_packet(ctx, buf, len, 0, false, &dest);
}

/* Avisa que un par quedó fuera del clúster por configuración compartida
 * distinta. El anuncio se repite, así que el aviso se limita por UUID. */
static void discovery_warn_hash_mismatch(dsm_ctx_t *ctx,
                                         const discovery_wire_t *w,
                                         uint32_t ip)
{
    char uuid_str[37];
    char ip_str[INET_ADDRSTRLEN];
    char name[33];

    uuid_to_str(w->uuid, uuid_str);
    memcpy(name, w->name, sizeof(w->name));
    name[sizeof(name) - 1] = '\0';

    struct in_addr addr;
    memset(&addr, 0, sizeof(addr));
    addr.s_addr = ip;
    if (!inet_ntop(AF_INET, &addr, ip_str, sizeof(ip_str))) {
        snprintf(ip_str, sizeof(ip_str), "?");
    }

    uint64_t now = mono_ms();
    bool warn = false;

    pthread_mutex_lock(&ctx->disc.mu);
    uint32_t i = 0;
    while (i < ctx->disc.nconflicts &&
           memcmp(ctx->disc.conflicts[i].uuid, w->uuid, 16) != 0) {
        i++;
    }
    if (i == ctx->disc.nconflicts) {
        if (ctx->disc.nconflicts < MAX_NODES) {
            i = ctx->disc.nconflicts++;
            memcpy(ctx->disc.conflicts[i].uuid, w->uuid, 16);
            ctx->disc.conflicts[i].last_warn_ms = now;
            warn = true;
        }
    } else if (now - ctx->disc.conflicts[i].last_warn_ms >=
               DISCOVERY_HASH_WARN_MS) {
        ctx->disc.conflicts[i].last_warn_ms = now;
        warn = true;
    }
    pthread_mutex_unlock(&ctx->disc.mu);

    if (warn) {
        DSM_LOG_WARN("Anuncio del par '%s' (%s, uuid %s) rechazado: "
                     "configuración compartida distinta (shared_hash "
                     "0x%08x, propio 0x%08x); no se incorpora al clúster",
                     name, ip_str, uuid_str, w->shared_hash,
                     ctx->cfg.shared_hash);
    }
}

/* Registra un anuncio válido. La IP del remitente observada en el socket
 * sustituye a una dirección loopback publicada por el propio nodo. */
static void discovery_on_announcement(dsm_ctx_t *ctx, const uint8_t *buf,
                                      size_t len, const struct sockaddr_in *src)
{
    if (len != DSM_DISCOVERY_WIRE_LEN) {
        return;
    }

    discovery_wire_t w;
    dsm_unpack_discovery(buf, &w);

    if (w.magic != DSM_MAGIC || w.proto_version != DSM_PROTO_VERSION ||
        w.cluster_id != ctx->cfg.cluster_id) {
        return;
    }
    if (memcmp(w.uuid, ctx->memb.self_uuid, 16) == 0) {
        return;
    }

    uint32_t ip = w.ip;
    uint32_t src_ip = (src && src->sin_family == AF_INET)
                          ? src->sin_addr.s_addr
                          : 0;
    if (ip_replaceable(ip, src_ip)) {
        ip = src_ip;
    }

    if (w.shared_hash != ctx->cfg.shared_hash) {
        discovery_warn_hash_mismatch(ctx, &w, ip);
        return;
    }
    if (w.port == 0 || w.role > 1u) {
        return;
    }
    if (!ip_is_usable(ip)) {
        return;
    }

    w.name[sizeof(w.name) - 1] = '\0';

    bool notify = false;
    pthread_mutex_lock(&ctx->disc.mu);

    uint32_t i = 0;
    while (i < ctx->disc.npeers &&
           memcmp(ctx->disc.peers[i].uuid, w.uuid, 16) != 0) {
        i++;
    }

    if (i == ctx->disc.npeers && ctx->disc.npeers < MAX_NODES) {
        discovery_peer_t *p = &ctx->disc.peers[ctx->disc.npeers++];
        memset(p, 0, sizeof(*p));
        memcpy(p->uuid, w.uuid, 16);
        memcpy(p->name, w.name, sizeof(p->name) - 1);
        p->name[sizeof(p->name) - 1] = '\0';
        notify = true;
    } else if (i < ctx->disc.npeers) {
        notify = (ctx->disc.peers[i].slot != w.slot ||
                  ctx->disc.peers[i].port != w.port);
    }

    if (i < ctx->disc.npeers) {
        discovery_peer_t *p = &ctx->disc.peers[i];
        p->ip         = ip;
        p->port       = w.port;
        p->mem_pages  = w.mem_pages;
        p->slot       = w.slot;
        p->role       = w.role;
        p->last_seen_ms = mono_ms();
        memcpy(p->name, w.name, sizeof(p->name) - 1);
        p->name[sizeof(p->name) - 1] = '\0';
    }

    pthread_mutex_unlock(&ctx->disc.mu);

    membership_observe_peer_ip(ctx, w.uuid, ip);
    if (notify) {
        membership_progress_notify(&ctx->memb);
    }
}

static void *discovery_udp_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    uint64_t next_announce = mono_ms();

    while (atomic_load(&ctx->disc.running)) {
        uint64_t now = mono_ms();
        if (now >= next_announce) {
            discovery_announce_once(ctx);
            next_announce = now + DISCOVERY_ANNOUNCE_MS;
        }

        struct pollfd pfd = { .fd = ctx->disc.fd, .events = POLLIN, .revents = 0 };
        int pr = poll(&pfd, 1, DISCOVERY_POLL_MS);
        if (pr < 0) {
            if (errno == EINTR) continue;
            DSM_LOG_ERR("poll del socket de descubrimiento falló: %s", strerror(errno));
            break;
        }
        if (pr == 0) continue;
        if (pfd.revents & POLLNVAL) break;
        if (!(pfd.revents & POLLIN)) continue;

        for (;;) {
            uint8_t buf[DSM_DISCOVERY_WIRE_LEN];
            struct sockaddr_in src;
            socklen_t slen = sizeof(src);
            memset(&src, 0, sizeof(src));

            ssize_t n = recvfrom(ctx->disc.fd, buf, sizeof(buf), MSG_DONTWAIT,
                                 (struct sockaddr *)&src, &slen);
            if (n < 0) break;
            if (n == (ssize_t)DSM_DISCOVERY_WIRE_LEN) {
                discovery_on_announcement(ctx, buf, (size_t)n, &src);
            }
        }
    }
    return NULL;
}

int discovery_mcast_start(dsm_ctx_t *ctx)
{
    dsm_discovery_t *d = &ctx->disc;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        DSM_LOG_ERR("No se pudo crear el socket UDP de descubrimiento: %s",
                    strerror(errno));
        return DSM_ERR_NET;
    }
    set_cloexec(fd);

    int opt = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    (void)setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));
    (void)setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &opt, sizeof(opt));
    uint8_t ttl = 1;
    (void)setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(ctx->cfg.discovery_port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        DSM_LOG_ERR("No se pudo enlazar el socket UDP de descubrimiento en el puerto %u: %s",
                    ctx->cfg.discovery_port, strerror(errno));
        close(fd);
        return DSM_ERR_NET;
    }

    bool joined = false;
    d->mcast_if = 0;
    uint32_t group = htonl(ctx->cfg.discovery_group);
    bool is_multicast = (ctx->cfg.discovery_group & 0xF0000000u) == 0xE0000000u;

    if (is_multicast) {
        struct ip_mreq mreq;
        memset(&mreq, 0, sizeof(mreq));
        mreq.imr_multiaddr.s_addr = group;
        mreq.imr_interface.s_addr = htonl(INADDR_ANY);

        if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                       &mreq, sizeof(mreq)) == 0) {
            joined = true;
            /* Segunda suscripción por loopback: permite que otro proceso de la
             * misma máquina sea visto aunque la interfaz por defecto no
             * devuelva los datágramas multicast. */
            mreq.imr_interface.s_addr = htonl(INADDR_LOOPBACK);
            (void)setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                             &mreq, sizeof(mreq));
        } else {
            mreq.imr_interface.s_addr = htonl(INADDR_LOOPBACK);
            if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                           &mreq, sizeof(mreq)) == 0) {
                joined = true;
                d->mcast_if = htonl(INADDR_LOOPBACK);
            }
        }
    }

    d->multicast = joined;
    d->fd        = fd;
    d->npeers    = 0;
    atomic_store(&d->running, true);

    int rc = pthread_create(&d->thread, NULL, discovery_udp_thread_main, ctx);
    if (rc != 0) {
        DSM_LOG_ERR("No se pudo crear el hilo de descubrimiento UDP: %d", rc);
        atomic_store(&d->running, false);
        close(fd);
        d->fd = -1;
        d->multicast = false;
        return DSM_ERR_MEM;
    }
    d->thread_started = true;

    char group_text[INET_ADDRSTRLEN];
    DSM_LOG_INFO("Descubrimiento UDP en %s:%u (%s)",
                 ip_to_text(group, group_text, sizeof(group_text)),
                 ctx->cfg.discovery_port,
                 joined ? "multicast" : "broadcast");
    return DSM_OK;
}

void discovery_mcast_stop(dsm_ctx_t *ctx)
{
    dsm_discovery_t *d = &ctx->disc;

    atomic_store(&d->running, false);
    if (d->thread_started) {
        (void)pthread_join(d->thread, NULL);
        d->thread_started = false;
    }
    if (d->fd >= 0) {
        close(d->fd);
        d->fd = -1;
    }
    d->multicast = false;
}

int uuid_load_or_create(dsm_ctx_t *ctx, uint8_t out[16])
{
    const char *tag = getenv("DSM_NODE_TAG");

    /*
     * Un cliente no representa una identidad estable del cluster: mientras no
     * se le asigne una etiqueta explicita, cada ejecucion recibe un UUID
     * efimero. Si reutilizara el archivo del nodo local, el descubrimiento lo
     * descartaria por duplicado y no podria unirse.
     */
    if (ctx->cfg.client_mode && (!tag || tag[0] == '\0')) {
        if (uuid_generate(out) != DSM_OK) {
            return DSM_ERR_MEM;
        }
        return DSM_OK;
    }

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
            /*
             * SO_REUSEADDR permite que dos sockets se enlacen al mismo puerto
             * mientras ninguno este escuchando, de modo que dos nodos que
             * arrancan a la vez pueden salir del bind con el mismo puerto. El
             * listen es lo que reserva el puerto de forma exclusiva: si falla,
             * otro proceso lo gano y se prueba el siguiente.
             */
            if (listen(fd, 128) == 0) {
                ctx->net.listen_fd   = fd;
                ctx->net.listen_port = port;
                DSM_LOG_INFO("Puerto de escucha asignado localmente: %u", port);
                return DSM_OK;
            }
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

    /* La direccion local de esta conexion es una IP real de este nodo y la
     * unica que el par puede usar para responderle. */
    membership_observe_self_ip(ctx, socket_local_ip(fd));

    /* Enviar mensaje HELLO: 20 bytes (uuid[16] + shared_hash[4]) */
    uint8_t hello_payload[20];
    memcpy(hello_payload, ctx->memb.self_uuid, 16);
    uint32_t hash_net = htonl(ctx->cfg.shared_hash);
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
        char ip_str[INET_ADDRSTRLEN];
        if (!inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str))) {
            snprintf(ip_str, sizeof(ip_str), "?");
        }
        DSM_LOG_ERR("El par %s rechazó la unión: la configuración compartida "
                    "difiere (shared_hash distinto)", ip_str);
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

/*
 * Reclamacion de ranura observada en una pasada de escaneo.
 *
 * 'seen' evita contar dos veces al mismo UUID cuando llegan anuncios repetidos
 * por varias interfaces. 'claims' reune la ranura que publica cada par
 * en su WELCOME: es la vista con la que se decide el desempate por UUID.
 */
typedef struct {
    uint8_t  uuid[16];
    uint32_t slot;
    uint32_t ip;      /* orden de red */
    uint16_t port;    /* orden de host */
    uint32_t mem_pages;
} slot_claim_t;

/* Direccion de escucha de un par descubierto y la ranura que publica. */
typedef struct {
    uint8_t  uuid[16];
    uint32_t ip;      /* orden de red */
    uint16_t port;    /* orden de host */
    uint32_t slot;    /* ranura que el par publica en su tabla */
} peer_ep_t;

typedef struct {
    welcome_t   *first;
    bool         capture_first;
    bool         have_first;
    uint32_t     peers;
    uint64_t     max_epoch;
    bool         merge;
    uint8_t      seen[MAX_NODES][16];
    slot_claim_t claims[MAX_NODES];
    uint32_t     nclaims;
    peer_ep_t    ep[MAX_NODES];
    uint32_t     nep;
} discovery_walk_t;

/*
 * Registra la reclamacion de ranura de un par.
 *
 * Solo cuenta la fila que el par publica de si mismo: es la unica que el nodo
 * garantiza, porque la escribio tras obtener el visto bueno de sus pares. Las
 * filas de terceros que viajan en el WELCOME son vistas que pueden estar
 * obsoletas, y usarlas como reclamacion bloquearia ranuras de nodos que ya no
 * las ocupan.
 */
static void discovery_record_claim(dsm_ctx_t *ctx, discovery_walk_t *walk,
                                  const welcome_t *w)
{
    if (memcmp(w->origin_uuid, ctx->memb.self_uuid, 16) == 0) {
        return;
    }

    for (uint32_t i = 0; i < walk->nclaims; i++) {
        if (memcmp(walk->claims[i].uuid, w->origin_uuid, 16) == 0) {
            return;
        }
    }
    if (walk->nclaims >= MAX_NODES) {
        return;
    }

    for (uint32_t i = 0; i < w->count; i++) {
        const membership_wire_t *m = &w->members[i];
        if (memcmp(m->uuid, w->origin_uuid, 16) != 0) {
            continue;
        }

        slot_claim_t *c = &walk->claims[walk->nclaims++];
        memcpy(c->uuid, m->uuid, 16);
        c->slot      = m->slot;
        c->ip        = m->ip;
        c->port      = m->port;
        c->mem_pages = m->mem_pages;
        return;
    }
}

static int discovery_visit_peer(dsm_ctx_t *ctx, const welcome_t *w,
                                const struct sockaddr_in *addr,
                                discovery_walk_t *walk)
{
    for (uint32_t i = 0; i < walk->peers && i < MAX_NODES; i++) {
        if (memcmp(walk->seen[i], w->origin_uuid, 16) == 0) {
            return DSM_OK;
        }
    }

    if (walk->peers < MAX_NODES) {
        memcpy(walk->seen[walk->peers], w->origin_uuid, 16);
    }
    walk->peers++;

    if (walk->nep < MAX_NODES) {
        peer_ep_t *ep = &walk->ep[walk->nep++];
        memcpy(ep->uuid, w->origin_uuid, 16);
        ep->ip   = addr->sin_addr.s_addr;
        ep->port = ntohs(addr->sin_port);
        ep->slot = DSM_SLOT_NONE;
        for (uint32_t i = 0; i < w->count; i++) {
            if (memcmp(w->members[i].uuid, w->origin_uuid, 16) == 0) {
                ep->slot = w->members[i].slot;
                break;
            }
        }
    }

    discovery_record_claim(ctx, walk, w);

    if (w->epoch > walk->max_epoch) {
        walk->max_epoch = w->epoch;
    }
    if (walk->merge) {
        membership_merge_wire(&ctx->memb, w);
    }
    /* Cada WELCOME es también un punto de replica del mapa global. La
     * importación avanza la marca de agua y reemplaza los huecos por los de la
     * instantánea vigente, de modo que una vista atrasada no reutilice páginas
     * que el coordinador ya reservó. */
    if (w->alloc_count != 0) {
        dsm_alloc_import(ctx, w->allocs, w->alloc_count);
    }
    /* La direccion desde la que respondio el par es la que este nodo debe
     * usar para enviarle; sustituye cualquier IP loopback que el par haya
     * publicado de si mismo. */
    membership_observe_peer_ip(ctx, w->origin_uuid, addr->sin_addr.s_addr);
    if (walk->capture_first && !walk->have_first) {
        *walk->first = *w;
        walk->have_first = true;
    }
    return DSM_OK;
}

/*
 * Sondea una vez cada par anunciado por UDP. Cada par que responde se entrega a
 * discovery_visit_peer, de modo que la pasada deja la tabla local fusionada con
 * todos los pares descubiertos y no solo con el primero.
 *
 * El anuncio da la dirección y el puerto TCP; el WELCOME posterior da la
 * ranura y la membresía. Un par con la tabla vacía cuenta como par vivo aunque
 * no aporte ninguna reclamación: acaba de abrir su listener y publica su
 * reclamo en su propia unión.
 */
static int discovery_walk(dsm_ctx_t *ctx, discovery_walk_t *walk)
{
    uint64_t deadline = mono_ms() + ctx->cfg.discovery_timeout_ms;
    discovery_peer_t peers[MAX_NODES];
    uint32_t npeers;

    pthread_mutex_lock(&ctx->disc.mu);
    npeers = ctx->disc.npeers;
    if (npeers > MAX_NODES) {
        npeers = MAX_NODES;
    }
    memcpy(peers, ctx->disc.peers, (size_t)npeers * sizeof(peers[0]));
    pthread_mutex_unlock(&ctx->disc.mu);

    for (uint32_t i = 0; i < npeers; i++) {
        if (mono_ms() >= deadline) break;

        const discovery_peer_t *p = &peers[i];
        if (memcmp(p->uuid, ctx->memb.self_uuid, 16) == 0) {
            continue; /* El loopback del propio anuncio no es un par */
        }

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = p->ip;
        addr.sin_port        = htons(p->port);

        welcome_t w;
        int pr = probe_peer(ctx, &addr, &w);
        if (pr == DSM_ERR_CONFIG) {
            char text[INET_ADDRSTRLEN];
            DSM_LOG_ERR("Configuración compartida distinta en el par anunciado %s:%u",
                        ip_to_text(p->ip, text, sizeof(text)), p->port);
            return DSM_ERR_CONFIG;
        }
        if (pr != DSM_OK) {
            continue;
        }

        /*
         * La membresía identifica a cada nodo por su UUID, de modo que dos
         * procesos con el mismo UUID (mismo archivo de identidad, puerto
         * distinto) no se pueden distinguir entre sí. Ese caso se reporta en
         * lugar de ignorar al par: ignorarlo deja a los dos procesos creyéndose
         * el primer nodo del clúster.
         */
        if (memcmp(w.origin_uuid, ctx->memb.self_uuid, 16) == 0) {
            char own_uuid[37];
            uuid_to_str(w.origin_uuid, own_uuid);
            DSM_LOG_ERR("El par anunciado responde con el mismo UUID (%s); "
                         "cada nodo necesita un DSM_NODE_TAG distinto",
                         own_uuid);
            return DSM_ERR_CONFIG;
        }

        int rc = discovery_visit_peer(ctx, &w, &addr, walk);
        if (rc != DSM_OK) {
            return rc;
        }
    }

    return DSM_OK;
}

int discovery_scan(dsm_ctx_t *ctx, welcome_t *out)
{
    discovery_walk_t walk;
    memset(&walk, 0, sizeof(walk));
    walk.first         = out;
    walk.capture_first = true;

    int rc = discovery_walk(ctx, &walk);
    if (rc != DSM_OK) {
        return rc;
    }
    if (!walk.have_first) {
        return DSM_ERR_NOT_FOUND;
    }

    DSM_LOG_INFO("Par descubierto (epoch=%lu, nodos=%u)",
                 (unsigned long)out->epoch, out->count);
    return DSM_OK;
}

/*
 * Pasada de escaneo con fusion: deja en la membresia local la union de las
 * tablas de todos los pares alcanzables y devuelve en 'walk' cuantos pares
 * distintos respondieron y que ranuras reclamaron. 'max_epoch' acumula la
 * epoca mayor observada entre todas las pasadas.
 */
static int discovery_pass(dsm_ctx_t *ctx, discovery_walk_t *walk, uint64_t *max_epoch)
{
    memset(walk, 0, sizeof(*walk));
    walk->merge     = true;
    walk->max_epoch = *max_epoch;

    int rc = discovery_walk(ctx, walk);
    if (rc != DSM_OK) {
        return rc;
    }

    *max_epoch = walk->max_epoch;
    return DSM_OK;
}

/*
 * Envia un JOIN a cada par alcanzable en la pasada y espera su JOIN_ACK o
 * NACK.
 *
 * Los destinos salen de las direcciones escaneadas y no de la tabla local: un
 * nodo que llega a la vez puede haber publicado una ranura que esta tabla aun no
 * registra, y sin el JOIN ese par no sabria que su ranura esta disputada.
 * 'acked' cuenta las respuestas favorables; 'status' conserva el primer rechazo
 * y lo decide el par, que es la unica autoridad sobre su propia tabla.
 */
static int discovery_send_join_all(dsm_ctx_t *ctx, const discovery_walk_t *walk,
                                   uint16_t *status, uint32_t *acked)
{
    *status = DSM_ST_OK;
    *acked  = 0;

    membership_wire_t my_wire;
    memset(&my_wire, 0, sizeof(my_wire));
    memcpy(my_wire.uuid, ctx->memb.self_uuid, 16);
    pthread_rwlock_rdlock(&ctx->memb.rw);
    my_wire.ip   = ctx->memb.self_ip;
    my_wire.slot = (uint8_t)ctx->memb.self_slot;
    pthread_rwlock_unlock(&ctx->memb.rw);
    if (!ip_is_usable(my_wire.ip)) {
        my_wire.ip = htonl(INADDR_LOOPBACK);
    }
    my_wire.port        = ctx->net.listen_port;
    my_wire.state       = DSM_NODE_ALIVE;
    my_wire.incarnation = 1;
    /* El JOIN publica la memoria aportada: un cliente no aporta ninguna y el
     * campo viaja en cero, que es como los pares lo reconocen. */
    my_wire.mem_pages   = ctx->cfg.client_mode
                              ? 0u
                              : (uint32_t)(ctx->cfg.node_mem / ctx->cfg.page_size);

    for (uint32_t i = 0; i < walk->nep; i++) {
        const peer_ep_t *ep = &walk->ep[i];
        if (memcmp(ep->uuid, ctx->memb.self_uuid, 16) == 0) {
            continue;
        }

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = ep->ip;
        addr.sin_port        = htons(ep->port);

        int fd = dsm_connect_timeout(&addr, ctx->cfg.connect_timeout_ms);
        if (fd < 0) {
            continue;
        }

        /* La IP publicada es la que el kernel uso para llegar a este par; la
         * loopback solo queda para un unico nodo que aun no observo otra. */
        uint32_t local_ip = socket_local_ip(fd);
        membership_wire_t wire = my_wire;
        if (ip_is_usable(local_ip)) {
            wire.ip = local_ip;
            membership_observe_self_ip(ctx, local_ip);
        }

        uint8_t join_payload[DSM_MEMBERSHIP_WIRE_LEN];
        dsm_pack_membership(&wire, join_payload);

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_JOIN;
        hdr.src_slot      = ctx->memb.self_slot;
        hdr.dst_slot      = ep->slot;
        hdr.epoch         = atomic_load(&ctx->memb.epoch);
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
        int read_rc = dsm_read_full_deadline(fd, rx_hdr_buf, DSM_HDR_LEN,
                                             ctx->cfg.discovery_timeout_ms);
        if (read_rc == DSM_OK) {
            msg_header_t rx_hdr;
            if (dsm_wire_unpack(rx_hdr_buf, &rx_hdr) == DSM_OK) {
                if (rx_hdr.type == MSG_NACK) {
                    if (*status == DSM_ST_OK) {
                        *status = rx_hdr.status;
                    }
                } else if (rx_hdr.type == MSG_JOIN_ACK) {
                    (*acked)++;
                    /* El aviso despierta a las esperas por condición del
                     * descubrimiento: la ranura propia ya quedó registrada en
                     * el par y el reparto puede reevaluarse sin esperar el
                     * plazo del paso. */
                    membership_progress_notify(&ctx->memb);
                    pthread_rwlock_wrlock(&ctx->memb.rw);
                    if (rx_hdr.epoch > atomic_load(&ctx->memb.epoch)) {
                        ctx->memb.epoch = rx_hdr.epoch;
                    }
                    pthread_rwlock_unlock(&ctx->memb.rw);
                    /* El par ya registró esta ranura: el latido inmediato
                     * confirma que el nodo está vivo antes de que arranque
                     * su hilo periódico. */
                    (void)net_send_heartbeat(ctx, ep->slot,
                                             dsm_alloc_free_pages(ctx));
                }
            }
        }
        close(fd);
    }

    return DSM_OK;
}

/* Reclamacion de menor UUID para 'slot' dentro de una pasada, o NULL si nadie
 * la pide. Varios nodos pueden reclamar la misma ranura a la vez, y el
 * desempate solo es determinista si se compara contra el menor de ellos. */
static const slot_claim_t *walk_claim_for_slot(const discovery_walk_t *walk,
                                               uint32_t slot)
{
    const slot_claim_t *best = NULL;

    for (uint32_t i = 0; i < walk->nclaims; i++) {
        const slot_claim_t *c = &walk->claims[i];
        if (c->slot != slot) {
            continue;
        }
        if (!best || memcmp(c->uuid, best->uuid, 16) < 0) {
            best = c;
        }
    }
    return best;
}

static bool uuid_less(const uint8_t a[16], const uint8_t b[16])
{
    return memcmp(a, b, 16) < 0;
}

/* Ranura que la tabla local asigno a 'uuid', o DSM_SLOT_NONE si no figura. */
static uint32_t membership_slot_of_uuid(dsm_ctx_t *ctx, const uint8_t uuid[16])
{
    uint32_t found = DSM_SLOT_NONE;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (memcmp(ctx->memb.nodes[s].uuid, uuid, 16) != 0) continue;
        found = s;
        break;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
    return found;
}

/* Ranura 's' ocupada por otro nodo vivo en la tabla local. */
static bool slot_occupied_by_other(dsm_ctx_t *ctx, uint32_t s)
{
    bool occupied = false;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s) &&
        ctx->memb.nodes[s].state != DSM_NODE_DEAD &&
        memcmp(ctx->memb.nodes[s].uuid, ctx->memb.self_uuid, 16) != 0) {
        occupied = true;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
    return occupied;
}

/* Ranura propia y epoca bajo el candado de la tabla. */
static uint32_t membership_self_slot(dsm_ctx_t *ctx)
{
    uint32_t slot;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    slot = ctx->memb.self_slot;
    pthread_rwlock_unlock(&ctx->memb.rw);
    return slot;
}

/* Incorpora la reclamación de un par que ocupa la ranura que este nodo deja
 * al reubicarse. La reclamación no publica memoria ni estado completo; el
 * siguiente latido o WELCOME corrige esos campos. */
static void membership_seed_claim(dsm_ctx_t *ctx, const slot_claim_t *claim)
{
    if (!ctx || !claim || claim->slot >= MAX_NODES) return;
    if (memcmp(claim->uuid, ctx->memb.self_uuid, 16) == 0) return;
    if (!ip_is_usable(claim->ip) || claim->port == 0) return;

    dsm_membership_t *m = &ctx->memb;
    uint32_t s = claim->slot;
    bool inserted = false;

    pthread_rwlock_wrlock(&m->rw);
    bool present = membership_slot_present(ctx, s);
    if (!present || memcmp(m->nodes[s].uuid, claim->uuid, 16) == 0) {
        node_info_t *ni = &m->nodes[s];
        memcpy(ni->uuid, claim->uuid, 16);
        ni->slot        = s;
        ni->ip          = claim->ip;
        ni->port        = claim->port;
        ni->state       = DSM_NODE_ALIVE;
        ni->incarnation = 1;
        ni->mem_pages   = claim->mem_pages;
        ni->free_pages  = claim->mem_pages;
        ni->is_client   = (claim->mem_pages == 0) ? 1u : 0u;
        ni->last_hb_ms  = mono_ms();
        ni->joined_ms   = ni->last_hb_ms;
        ni->missed_hb   = 0;
        ni->hb_seen     = 0;
        m->present[s / 8] |= (uint8_t)(1u << (s % 8));
        inserted = true;
    }
    pthread_rwlock_unlock(&m->rw);

    if (inserted) {
        membership_progress_notify(m);
    }
}

/*
 * Ranura que este nodo adopta buscando desde la primera, saltando las que
 * 'rejected' marca como rechazadas por un par confirmado.
 *
 * Una ranura reclamada por otro nodo no se adopta: el reclamo es la prueba de
 * que su titular la esta usando, y pisarlo obligaria a renumerar dos nodos. La
 * ranura propia se conserva si nadie la reclama, de modo que un nodo solo no
 * paga ninguna reubicacion.
 */
static uint32_t discovery_pick_slot(dsm_ctx_t *ctx, const discovery_walk_t *walk,
                                    const bool *rejected)
{
    uint32_t max_nodes = ctx->cfg.max_runtime_nodes;
    if (max_nodes > MAX_NODES) {
        max_nodes = MAX_NODES;
    }

    uint32_t self = membership_self_slot(ctx);

    for (uint32_t s = 0; s < max_nodes; s++) {
        if (rejected && rejected[s]) {
            continue;
        }
        if (walk_claim_for_slot(walk, s) != NULL) {
            continue;
        }
        if (s == self) {
            return s;
        }
        if (slot_occupied_by_other(ctx, s)) {
            continue;
        }
        return s;
    }

    return DSM_SLOT_NONE;
}

/*
 * Ventana de asentamiento del primer nodo.
 *
 * Un nodo que no ve ningun par publica su reclamo de la ranura 0 y vuelve a
 * escanear hasta que vence discovery_timeout_ms. La ventana cubre el caso de
 * dos nodos que abren su listener casi a la vez: el segundo publica su propio
 * reclamo dentro de la ventana del primero y el desempate por UUID ve a los dos
 * candidatos. La espera termina en cuanto un par responde, y un nodo solo paga
 * como maximo discovery_timeout_ms. La correccion no depende de esta ventana:
 * discovery_resolve_slots repara cualquier reclamo que aparezca despues.
 */
static int discovery_settle(dsm_ctx_t *ctx, discovery_walk_t *walk,
                            uint64_t *max_epoch)
{
    /* La ventana debe cubrir varios periodos de anuncio: un plazo de sondeo
     * más corto que la cadencia UDP dejaría al primer nodo creyéndose solo
     * aunque otro estuviera a punto de anunciarse. */
    uint64_t window = ctx->cfg.discovery_timeout_ms;
    uint64_t min_window = 3u * DISCOVERY_ANNOUNCE_MS;
    if (window < min_window) {
        window = min_window;
    }
    uint64_t deadline = mono_ms() + window;

    for (;;) {
        uint64_t mark = membership_progress_mark(&ctx->memb);
        int rc = discovery_pass(ctx, walk, max_epoch);
        if (rc != DSM_OK) {
            return rc;
        }
        if (walk->peers > 0) {
            return DSM_OK;
        }

        uint64_t now = mono_ms();
        if (now >= deadline) {
            return DSM_OK;
        }
        uint64_t wake = now + DISCOVERY_SETTLE_STEP_MS;
        if (wake > deadline) {
            wake = deadline;
        }
        membership_progress_wait(&ctx->memb, mark, wake);
    }
}

/* Todos los pares alcanzables publicaron su propia ranura y este nodo la
 * registra: la vista local es la del cluster y los latidos encuentran entrada
 * en ambos lados. */
static bool discovery_converged(dsm_ctx_t *ctx, const discovery_walk_t *walk)
{
    for (uint32_t i = 0; i < walk->nep; i++) {
        if (walk->ep[i].slot == DSM_SLOT_NONE) {
            return false; /* el par aun no publica su reclamacion */
        }
        uint32_t s = membership_slot_of_uuid(ctx, walk->ep[i].uuid);
        if (s == DSM_SLOT_NONE || s != walk->ep[i].slot) {
            return false;
        }
    }
    return true;
}

/*
 * Espera a que la vista local coincida con la de los pares alcanzables.
 *
 * Un nodo que arranca mientras el resto sigue escaneando ve pares cuyo WELCOME
 * todavia no incluye su propia ranura. Esa tabla no aporta nada a la fusion y,
 * si el nodo termina su union en ese momento, se queda con una vista minima
 * hasta el siguiente descubrimiento. La espera vuelve a escanear hasta que cada
 * par publica su ranura y esta queda registrada aqui, y esta acotada por dos
 * veces discovery_timeout_ms para que un par que se cae no retrase el arranque.
 */
static int discovery_converge(dsm_ctx_t *ctx, uint64_t *max_epoch)
{
    uint64_t deadline = mono_ms() + 2u * ctx->cfg.discovery_timeout_ms;
    discovery_walk_t walk;

    for (;;) {
        uint64_t mark = membership_progress_mark(&ctx->memb);
        int rc = discovery_pass(ctx, &walk, max_epoch);
        if (rc != DSM_OK) {
            return rc;
        }
        if (walk.nep == 0 || discovery_converged(ctx, &walk)) {
            return DSM_OK;
        }

        uint64_t now = mono_ms();
        if (now >= deadline) {
            DSM_LOG_WARN("La vista local no convergio con %u par(es) en %u ms",
                         walk.nep, 2u * ctx->cfg.discovery_timeout_ms);
            return DSM_OK;
        }
        uint64_t wake = now + DISCOVERY_SETTLE_STEP_MS;
        if (wake > deadline) {
            wake = deadline;
        }
        membership_progress_wait(&ctx->memb, mark, wake);
    }
}

/*
 * Reparto definitivo de la ranura propia.
 *
 * Cada vuelta reune la tabla de todos los pares alcanzables y envia un JOIN con
 * la ranura propia. Los dos lados aplican el mismo desempate por UUID:
 *
 *  - Una reclamacion rival de UUID menor se cede en el acto. La cesion la
 *    ejecuta siempre este hilo, de modo que la ranura propia tiene un unico
 *    escritor.
 *  - Una reclamacion rival de UUID mayor se mantiene: su titular responde BUSY
 *    mientras su reparto es provisional y cede en su propia vuelta; cuando ya
 *    confirmo el reparto responde SLOT_TAKEN y es este nodo el que repliega su
 *    reclamacion.
 *
 * 'floor' solo avanza: una ranura cedida o rechazada no se vuelve a pedir, de
 * modo que la busqueda es monotona y el bucle no puede oscilar entre dos
 * ranuras. El reparto termina cuando los pares alcanzables registraron la
 * ranura propia o cuando no queda ninguno.
 */
static int discovery_resolve_slots(dsm_ctx_t *ctx, uint32_t *peers_out)
{
    dsm_membership_t *m = &ctx->memb;
    bool rejected[MAX_NODES];
    memset(rejected, 0, sizeof(rejected));

    for (uint32_t round = 0; round < DISCOVERY_RESOLVE_ROUNDS; round++) {
        uint64_t max_epoch = m->epoch;
        discovery_walk_t walk;
        int rc = discovery_pass(ctx, &walk, &max_epoch);
        if (rc != DSM_OK) {
            return rc;
        }
        if (max_epoch > m->epoch) {
            m->epoch = max_epoch;
        }

        uint32_t self = membership_self_slot(ctx);
        bool cede = false;

        const slot_claim_t *rival = walk_claim_for_slot(&walk, self);
        if (rival && uuid_less(rival->uuid, m->self_uuid)) {
            char rival_uuid[37];
            uuid_to_str(rival->uuid, rival_uuid);
            DSM_LOG_INFO("Ranura %u reclamada por el nodo %s (UUID menor); se cede",
                         self, rival_uuid);
            cede = true;
        } else {
            uint16_t status = DSM_ST_OK;
            uint32_t acked = 0;
            rc = discovery_send_join_all(ctx, &walk, &status, &acked);
            if (rc != DSM_OK) {
                return rc;
            }

            if (walk.nep == 0) {
                /* Sin pares alcanzables la ranura no tiene con quien
                 * confirmarse: el nodo queda como unico miembro. */
                *peers_out = 0;
                return DSM_OK;
            }
            if (acked >= walk.nep) {
                /* Todos los pares alcanzables registraron la ranura propia.
                 * Cada envio abre su propio enlace y lo cierra al terminar la
                 * trama, de modo que no quedan sockets pre-abiertos que el par
                 * pueda cerrar antes del primer latido. */
                *peers_out = walk.peers;
                return DSM_OK;
            }
            if (status == DSM_ST_BUSY) {
                /* El titular de la ranura aun no confirmo su reparto y la cede
                 * en su propia vuelta; se le da tiempo a completarla. La
                 * espera termina antes si su JOIN o cualquier cambio de
                 * membresía avisa del progreso. */
                uint64_t mark = membership_progress_mark(&ctx->memb);
                membership_progress_wait(&ctx->memb, mark,
                                         mono_ms() + DISCOVERY_SETTLE_STEP_MS);
                continue;
            }
            if (status == DSM_ST_NO_SPACE) {
                DSM_LOG_ERR("El cluster no tiene ranuras libres");
                return DSM_ERR_NO_SPACE;
            }
            if (status != DSM_ST_OK && status != DSM_ST_SLOT_TAKEN) {
                DSM_LOG_ERR("Rechazo inesperado al solicitar la ranura %u: %u",
                            self, status);
                return DSM_ERR_PROTO;
            }
            /* Un par confirmado rechazo la ranura: se descarta para siempre,
             * porque volver a pedirla solo repite el intercambio. */
            rejected[self] = true;
            cede = true;
        }

        if (!cede) {
            return DSM_ERR_BUSY;
        }

        uint32_t next = discovery_pick_slot(ctx, &walk, rejected);
        if (next == DSM_SLOT_NONE) {
            DSM_LOG_ERR("No hay slots libres en el cluster");
            return DSM_ERR_NO_SPACE;
        }
        DSM_LOG_INFO("Ranura %u no disponible; se solicita la ranura %u", self, next);
        const slot_claim_t *vacated = walk_claim_for_slot(&walk, self);
        membership_claim_slot(ctx, next);
        if (vacated) {
            membership_seed_claim(ctx, vacated);
        }
    }

    DSM_LOG_ERR("El reparto de ranuras no convergio tras %u vueltas",
                (unsigned)DISCOVERY_RESOLVE_ROUNDS);
    return DSM_ERR_BUSY;
}

/* Hay al menos un miembro del cluster que aporta memoria y puede custodiar
 * páginas. Un cliente solo puede operar contra ese cluster: el anillo necesita
 * al menos un nodo con memoria. Un nodo completo cuenta por si mismo aunque su
 * ranura todavia no figure en la tabla. */
static bool membership_has_memory_node(dsm_ctx_t *ctx)
{
    if (!ctx->cfg.client_mode) {
        return true;
    }

    dsm_membership_t *m = &ctx->memb;
    bool found = false;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (m->nodes[s].state == DSM_NODE_DEAD) continue;
        if (m->nodes[s].is_client || m->nodes[s].mem_pages == 0) continue;
        found = true;
        break;
    }
    pthread_rwlock_unlock(&m->rw);
    return found;
}

/* Con redundancia activa la franja necesita al menos dos custodios de datos y
 * uno de paridad. La comprobación se hace aquí, cuando la membresía ya existe,
 * y no en config_validate, que solo ve el archivo.
 *
 * La espera no se limita a mirar la tabla: vuelve a escanear a los pares
 * anunciados y fusiona sus vistas. Un nodo que arranca después del JOIN
 * original puede así aparecer en esta tabla y completar el mínimo. Solo se
 * falla cuando el plazo vence sin que existan tres nodos alcanzables. */
static int discovery_wait_redundancy_members(dsm_ctx_t *ctx)
{
    const uint32_t min_nodes = 3u;
    uint64_t wait_ms = (uint64_t)ctx->cfg.discovery_timeout_ms * 8u;
    if (wait_ms < 15000u) wait_ms = 15000u;
    if (wait_ms > 30000u) wait_ms = 30000u;

    uint64_t deadline = mono_ms() + wait_ms;
    uint64_t max_epoch = atomic_load(&ctx->memb.epoch);
    uint32_t count = membership_alive_count(ctx);

    while (count < min_nodes) {
        uint64_t now = mono_ms();
        if (now >= deadline) break;

        discovery_walk_t walk;
        uint64_t pass_epoch = max_epoch;
        if (discovery_pass(ctx, &walk, &pass_epoch) == DSM_OK &&
            pass_epoch > max_epoch) {
            max_epoch = pass_epoch;
        }
        if (max_epoch > atomic_load(&ctx->memb.epoch)) {
            atomic_store(&ctx->memb.epoch, max_epoch);
        }

        count = membership_alive_count(ctx);
        if (count >= min_nodes) break;

        now = mono_ms();
        if (now >= deadline) break;
        uint64_t mark = membership_progress_mark(&ctx->memb);
        uint64_t wake = now + DISCOVERY_SETTLE_STEP_MS;
        if (wake > deadline) wake = deadline;
        membership_progress_wait(&ctx->memb, mark, wake);
        count = membership_alive_count(ctx);
    }

    if (count < min_nodes) {
        DSM_LOG_ERR("REDUNDANCY=true requiere al menos %u nodos vivos que "
                    "aporten memoria; hay %u tras %llu ms de descubrimiento. "
                    "El nodo no se une al clúster.",
                    min_nodes, count, (unsigned long long)wait_ms);
        return DSM_ERR_CONFIG;
    }
    return DSM_OK;
}

/* Cuerpo de discovery_join, con el cerrojo de descubrimiento ya tomado. */
static int discovery_join_locked(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    int rc;

    atomic_store(&m->claim_pending, true);

    uint64_t max_epoch = 0;
    discovery_walk_t walk;
    rc = discovery_pass(ctx, &walk, &max_epoch);
    if (rc != DSM_OK) {
        return rc;
    }

    if (walk.peers == 0) {
        if (ctx->cfg.client_mode) {
            /* Un cliente no puede asumir el rol de primer nodo: no aporta
             * memoria y ninguna página tendria custodio. Se le concede la
             * misma ventana de asentamiento que a un nodo para que un par que
             * arranca a la vez alcance a publicar su tabla. */
            rc = discovery_settle(ctx, &walk, &max_epoch);
            if (rc != DSM_OK) {
                return rc;
            }
        } else {
            DSM_LOG_INFO("No se detectaron pares activos: se reclama la ranura 0 y se "
                         "verifica que siga libre durante %u ms",
                         ctx->cfg.discovery_timeout_ms);
            if (max_epoch >= m->epoch) {
                m->epoch = max_epoch + 1;
            }
            membership_claim_slot(ctx, 0);

            rc = discovery_settle(ctx, &walk, &max_epoch);
            if (rc != DSM_OK) {
                return rc;
            }
        }
    }

    if (!membership_has_memory_node(ctx)) {
        if (ctx->cfg.client_mode) {
            DSM_LOG_ERR("modo cliente: no se encontro ningun nodo con memoria en el cluster");
            return DSM_ERR_NO_SPACE;
        }
        DSM_LOG_ERR("No hay ningun nodo con memoria en el cluster");
        return DSM_ERR_NO_SPACE;
    }

    if (max_epoch >= m->epoch) {
        m->epoch = max_epoch + 1;
    }

    uint32_t peers = walk.peers;
    uint32_t old_slot = m->self_slot;
    uint32_t slot = discovery_pick_slot(ctx, &walk, NULL);
    if (slot == DSM_SLOT_NONE) {
        DSM_LOG_ERR("No hay slots libres en el cluster");
        return DSM_ERR_NO_SPACE;
    }
    if (slot != old_slot) {
        /* Si la ranura que se abandona estaba ocupada por otro nodo, su
         * reclamación se conserva. La fusión previa no pudo escribirla porque
         * coincidía con la ranura propia provisional. */
        const slot_claim_t *vacated = walk_claim_for_slot(&walk, old_slot);
        membership_claim_slot(ctx, slot);
        if (vacated) {
            membership_seed_claim(ctx, vacated);
        }
    }

    /* El hilo de latidos arranca antes de resolver el reparto: la ranura ya
     * está reclamada y los pares que registraron al nodo reciben latidos
     * mientras este termina de escanear y converger. */
    if ((rc = hb_start(ctx)) != DSM_OK) {
        return rc;
    }

    rc = discovery_resolve_slots(ctx, &peers);
    if (rc != DSM_OK) {
        return rc;
    }

    atomic_store(&m->claim_pending, false);

    uint64_t converge_epoch = m->epoch;
    rc = discovery_converge(ctx, &converge_epoch);
    if (rc != DSM_OK) {
        return rc;
    }
    if (converge_epoch > m->epoch) {
        m->epoch = converge_epoch;
    }

    DSM_LOG_INFO("Union completada exitosamente: Slot asignado = %u, Epoch = %lu, "
                 "pares alcanzables = %u",
                 m->self_slot, (unsigned long)m->epoch, peers);

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

    /* El listener queda activo con el cerrojo aun sin tomar: los nodos que ya
     * lo tienen necesitan que este responda sus HELLO para verlo candidato. */
    if ((rc = net_start_listener(ctx)) != DSM_OK) {
        return rc;
    }

    /* El descubrimiento UDP arranca junto al listener y sigue anunciando
     * mientras el nodo está vivo, sin bloquear la aceptación de conexiones. */
    if ((rc = discovery_mcast_start(ctx)) != DSM_OK) {
        return rc;
    }

    int lock_fd = discovery_lock_acquire(ctx);
    if (lock_fd < 0) {
        DSM_LOG_WARN("Sin cerrojo de descubrimiento; el reparto depende del desempate por UUID");
    }

    rc = discovery_join_locked(ctx);

    if (rc != DSM_OK) {
        /* El hilo de latidos pudo arrancar antes de que el reparto fallara. */
        hb_stop(ctx);
    }

    discovery_lock_release(lock_fd);

    /* La espera de miembros y el ajuste de K ocurren sin el cerrojo de
     * descubrimiento: los demás nodos necesitan tomarlo para completar su
     * propia unión. */
    if (rc == DSM_OK && ctx->cfg.redundancy) {
        rc = discovery_wait_redundancy_members(ctx);
        if (rc == DSM_OK) {
            rc = raid_reconfigure(ctx);
            if (rc != DSM_OK) {
                DSM_LOG_ERR("No se pudo ajustar la paridad al tamaño del clúster: %s",
                            dsm_strerror((dsm_status_t)rc));
            }
        }
        if (rc != DSM_OK) {
            hb_stop(ctx);
        }
    }

    /* Un fallo en cualquier fase deja el socket y el hilo UDP cerrados; en el
     * arranque correcto siguen anunciando hasta discovery_leave. */
    if (rc != DSM_OK) {
        discovery_mcast_stop(ctx);
    }

    return rc;
}

void discovery_leave(dsm_ctx_t *ctx)
{
    discovery_mcast_stop(ctx);
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

/* Las esperas por condición usan CLOCK_MONOTONIC para comparar sus plazos
 * contra mono_ms(). Un condvar con el reloj de tiempo real interpretaría el
 * plazo monotónico como una fecha de calendario. */
static int membership_wait_init(dsm_membership_t *m)
{
    pthread_condattr_t attr;
    int rc = pthread_condattr_init(&attr);
    if (rc != 0) {
        return -1;
    }

    rc = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (rc == 0 && pthread_mutex_init(&m->wait_mu, NULL) != 0) {
        rc = -1;
    }
    if (rc == 0 && pthread_cond_init(&m->progress_cv, &attr) != 0) {
        pthread_mutex_destroy(&m->wait_mu);
        rc = -1;
    }
    if (rc == 0 && pthread_mutex_init(&m->hb_mu, NULL) != 0) {
        pthread_cond_destroy(&m->progress_cv);
        pthread_mutex_destroy(&m->wait_mu);
        rc = -1;
    }
    if (rc == 0 && pthread_cond_init(&m->hb_cv, &attr) != 0) {
        pthread_mutex_destroy(&m->hb_mu);
        pthread_cond_destroy(&m->progress_cv);
        pthread_mutex_destroy(&m->wait_mu);
        rc = -1;
    }
    pthread_condattr_destroy(&attr);
    return rc;
}

int membership_init(dsm_ctx_t *ctx)
{
    dsm_membership_t *m = &ctx->memb;
    memset(m, 0, sizeof(*m));
    pthread_rwlock_init(&m->rw, NULL);
    if (membership_wait_init(m) != 0) {
        pthread_rwlock_destroy(&m->rw);
        return DSM_ERR_MEM;
    }
    if (pthread_mutex_init(&ctx->disc.mu, NULL) != 0) {
        pthread_cond_destroy(&m->hb_cv);
        pthread_mutex_destroy(&m->hb_mu);
        pthread_cond_destroy(&m->progress_cv);
        pthread_mutex_destroy(&m->wait_mu);
        pthread_rwlock_destroy(&m->rw);
        return DSM_ERR_MEM;
    }
    ctx->disc.fd = -1;
    ctx->disc.npeers = 0;
    ctx->disc.nconflicts = 0;
    ctx->disc.multicast = false;
    ctx->disc.thread_started = false;
    atomic_store(&ctx->disc.running, false);
    m->self_slot = DSM_SLOT_NONE;
    m->cfg       = &ctx->cfg;
    return DSM_OK;
}

void membership_destroy(dsm_ctx_t *ctx)
{
    discovery_mcast_stop(ctx);
    pthread_mutex_destroy(&ctx->disc.mu);
    pthread_cond_destroy(&ctx->memb.hb_cv);
    pthread_mutex_destroy(&ctx->memb.hb_mu);
    pthread_cond_destroy(&ctx->memb.progress_cv);
    pthread_mutex_destroy(&ctx->memb.wait_mu);
    pthread_rwlock_destroy(&ctx->memb.rw);
}

uint64_t membership_progress_mark(dsm_membership_t *m)
{
    pthread_mutex_lock(&m->wait_mu);
    uint64_t mark = m->progress_seq;
    pthread_mutex_unlock(&m->wait_mu);
    return mark;
}

void membership_progress_notify(dsm_membership_t *m)
{
    pthread_mutex_lock(&m->wait_mu);
    m->progress_seq++;
    pthread_cond_broadcast(&m->progress_cv);
    pthread_mutex_unlock(&m->wait_mu);
}

void membership_progress_wait(dsm_membership_t *m, uint64_t mark,
                              uint64_t deadline_ms)
{
    struct timespec ts;

    pthread_mutex_lock(&m->wait_mu);
    while (m->progress_seq == mark) {
        uint64_t now = mono_ms();
        if (now >= deadline_ms) {
            break;
        }
        mono_deadline_ts(deadline_ms, &ts);
        if (pthread_cond_timedwait(&m->progress_cv, &m->wait_mu, &ts) != 0) {
            break;
        }
    }
    pthread_mutex_unlock(&m->wait_mu);
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
        /* Solo se publica loopback como IP propia mientras no se haya
         * observado una direccion alcanzable en una conexion real. */
        ni->ip          = ip_is_usable(m->self_ip) ? m->self_ip
                                                   : htonl(INADDR_LOOPBACK);
        ni->port        = ctx->net.listen_port;
        ni->state       = DSM_NODE_ALIVE;
        ni->incarnation = 1;
        /* Un cliente no aporta memoria: ni páginas propias ni libres. */
        ni->mem_pages   = ctx->cfg.client_mode ? 0u : (uint32_t)(ctx->cfg.node_mem / ps);
        ni->free_pages  = ni->mem_pages;
        ni->is_client   = ctx->cfg.client_mode ? 1u : 0u;
        ni->last_hb_ms  = mono_ms();
        ni->joined_ms   = ni->last_hb_ms;
        ni->missed_hb   = 0;
        ni->hb_seen     = 1;

        m->present[s / 8] |= (uint8_t)(1u << (s % 8));
    }

    pthread_rwlock_unlock(&m->rw);
    membership_progress_notify(m);
}

/* Registra la IP con la que este nodo alcanza a un par. La observacion sale
 * de la conexion, no de lo que el par publica: la tabla del receptor es la
 * autoridad sobre su propia direccion. */
void membership_observe_peer_ip(dsm_ctx_t *ctx, const uint8_t uuid[16],
                                uint32_t ip)
{
    if (!ctx || !uuid || !ip_is_usable(ip)) {
        return;
    }

    dsm_membership_t *m = &ctx->memb;
    pthread_rwlock_wrlock(&m->rw);

    if (memcmp(uuid, m->self_uuid, 16) == 0) {
        if (ip_replaceable(m->self_ip, ip)) {
            m->self_ip = ip;
            uint32_t s = m->self_slot;
            if (s < MAX_NODES && membership_slot_present(ctx, s) &&
                memcmp(m->nodes[s].uuid, m->self_uuid, 16) == 0) {
                m->nodes[s].ip = ip;
            }
        }
        pthread_rwlock_unlock(&m->rw);
        return;
    }

    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (memcmp(m->nodes[s].uuid, uuid, 16) != 0) continue;
        if (ip_replaceable(m->nodes[s].ip, ip)) {
            m->nodes[s].ip = ip;
        }
        break;
    }

    pthread_rwlock_unlock(&m->rw);
}

/* Registra la IP local observada en un socket. No cambia el puerto de
 * escucha: en un socket saliente el puerto local es efimero. */
void membership_observe_self_ip(dsm_ctx_t *ctx, uint32_t ip)
{
    if (!ctx || !ip_is_usable(ip)) {
        return;
    }

    dsm_membership_t *m = &ctx->memb;
    pthread_rwlock_wrlock(&m->rw);

    if (ip_replaceable(m->self_ip, ip)) {
        m->self_ip = ip;
        uint32_t s = m->self_slot;
        if (s < MAX_NODES && membership_slot_present(ctx, s) &&
            memcmp(m->nodes[s].uuid, m->self_uuid, 16) == 0) {
            m->nodes[s].ip = ip;
        }
    }

    pthread_rwlock_unlock(&m->rw);
}

/*
 * Publica la ranura propia: retira la entrada previa y escribe la nueva. La
 * tabla no puede quedar con la entrada vieja presente, o el nodo apareceria dos
 * veces ante los pares. La epoca la fija el llamador, que es quien conoce la
 * vista del cluster con la que se reclama.
 */
void membership_claim_slot(dsm_ctx_t *ctx, uint32_t slot)
{
    dsm_membership_t *m = &ctx->memb;
    pthread_rwlock_wrlock(&m->rw);

    uint32_t prev = m->self_slot;
    if (prev < MAX_NODES && prev != slot) {
        if ((m->present[prev / 8] & (1u << (prev % 8))) &&
            memcmp(m->nodes[prev].uuid, m->self_uuid, 16) == 0) {
            m->nodes[prev].state = DSM_NODE_DEAD;
            m->present[prev / 8] &= (uint8_t)~(1u << (prev % 8));
            m->reusable_after_ms[prev] = 0;
        }
        /* Reubicarse cambia el reparto de la tabla. Si la ranura previa ya
         * estaba ocupada por un par, su fila se conserva y también debe
         * avanzar la época para que las vistas anteriores no la pisen. */
        m->epoch++;
    }

    m->self_slot = slot;
    if (slot < MAX_NODES) {
        m->present[slot / 8] |= (uint8_t)(1u << (slot % 8));
    }

    pthread_rwlock_unlock(&m->rw);

    membership_add_self(ctx);
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

uint32_t membership_alive_count(dsm_ctx_t *ctx)
{
    if (!ctx) return 0;
    dsm_membership_t *m = &ctx->memb;
    uint32_t count = 0;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (m->nodes[s].state == DSM_NODE_DEAD) continue;
        if (m->nodes[s].is_client || m->nodes[s].mem_pages == 0) continue;
        count++;
    }
    pthread_rwlock_unlock(&m->rw);
    return count;
}

/*
 * Fusiona la tabla que publico un par en su WELCOME.
 *
 * La tabla de membresia es una vista replicada y la fusion tiene que ser
 * conmutativa para que todos los nodos converjan al mismo mapa de uuid a
 * ranura; los latidos solo refrescan entradas existentes, asi que la vista
 * completa de cada par es la unica via por la que un nodo descubre a los demas.
 *
 * Las tres reglas de la fusion:
 *  - La entrada propia no se copia: la ranura de este nodo la decide el mismo.
 *  - Un uuid aparece una sola vez; la fila de su ranura previa se retira.
 *  - Ante dos nodos que reclaman la misma ranura se conserva el de UUID
 *    lexicograficamente menor, igual que en el JOIN.
 */
void membership_merge_wire(dsm_membership_t *m, const welcome_t *w)
{
    pthread_rwlock_wrlock(&m->rw);

    for (uint32_t i = 0; i < w->count; i++) {
        const membership_wire_t *wire = &w->members[i];
        uint32_t s = wire->slot;
        if (s >= MAX_NODES) continue;
        if (memcmp(wire->uuid, m->self_uuid, 16) == 0) continue;
        if (s == m->self_slot) {
            /* Una ranura propia provisional puede estar ocupada de hecho por
             * un nodo que ya la reclamó. Si su UUID es menor, la vista del par
             * manda y esta fila se reemplaza; así el nodo no pierde al par al
             * reubicarse. Ya confirmado el reparto, la ranura propia no se
             * toca. */
            if (!atomic_load(&m->claim_pending) ||
                memcmp(m->self_uuid, wire->uuid, 16) < 0) {
                continue;
            }
        }

        /* El uuid se reubico: la fila de su ranura previa es una vista vieja. */
        for (uint32_t other = 0; other < MAX_NODES; other++) {
            if (other == s || other == m->self_slot) continue;
            if (!(m->present[other / 8] & (1u << (other % 8)))) continue;
            if (memcmp(m->nodes[other].uuid, wire->uuid, 16) != 0) continue;
            m->nodes[other].state = DSM_NODE_DEAD;
            m->present[other / 8] &= (uint8_t)~(1u << (other % 8));
            m->reusable_after_ms[other] = 0;
        }

        bool present = (m->present[s / 8] & (1u << (s % 8))) != 0;
        if (present && memcmp(m->nodes[s].uuid, wire->uuid, 16) != 0) {
            if (memcmp(m->nodes[s].uuid, wire->uuid, 16) < 0) {
                continue; /* la ranura pertenece al nodo de UUID menor */
            }
        }

        node_info_t *ni = &m->nodes[s];
        uint32_t previous_ip = present ? ni->ip : 0;
        memcpy(ni->uuid, wire->uuid, 16);
        ni->slot        = s;
        /* Una vista que aun publica loopback no degrada la IP observada por
         * una conexion directa con ese mismo par. */
        ni->ip          = (!present || ip_replaceable(previous_ip, wire->ip))
                              ? wire->ip
                              : previous_ip;
        ni->port        = wire->port;
        ni->state       = wire->state;
        ni->incarnation = wire->incarnation;
        ni->mem_pages   = wire->mem_pages;
        ni->free_pages  = wire->mem_pages;
        /* El rol no viaja en el cable: un nodo que publica cero páginas es un
         * cliente, que usa la memoria del cluster pero no la aporta. */
        ni->is_client   = (wire->mem_pages == 0) ? 1u : 0u;
        ni->last_hb_ms  = mono_ms();
        ni->joined_ms   = ni->last_hb_ms;
        ni->missed_hb   = 0;
        ni->hb_seen     = 0;

        m->present[s / 8] |= (uint8_t)(1u << (s % 8));
    }

    pthread_rwlock_unlock(&m->rw);
}

/*
 * Replica la tabla de membresía vigente a todos los pares vivos.
 *
 * Se usa al aceptar un JOIN y al aprender una ranura por un latido. Sin esta
 * réplica, solo el nodo que atendió el JOIN incorpora al recién llegado y los
 * demás siguen con una vista incompleta hasta que su propio escaneo los une.
 * El mensaje es un WELCOME no solicitado: la tabla es la misma que viaja en el
 * saludo y el receptor la fusiona con las reglas de siempre.
 */
static void membership_replicate(dsm_ctx_t *ctx)
{
    if (!ctx) return;

    dsm_membership_t *m = &ctx->memb;
    welcome_t w;
    memset(&w, 0, sizeof(w));
    w.epoch = atomic_load(&m->epoch);
    memcpy(w.origin_uuid, m->self_uuid, 16);

    uint32_t count = 0;
    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        node_info_t *ni = &m->nodes[s];
        membership_wire_t *mw = &w.members[count++];
        memcpy(mw->uuid, ni->uuid, 16);
        mw->ip          = ni->ip;
        mw->port        = ni->port;
        mw->slot        = (uint8_t)s;
        mw->state       = ni->state;
        mw->incarnation = ni->incarnation;
        mw->mem_pages   = ni->mem_pages;
    }
    pthread_rwlock_unlock(&m->rw);
    w.count = count;
    w.alloc_count = 0;

    uint8_t payload[DSM_WELCOME_WIRE_MAX];
    size_t len = dsm_pack_welcome(&w, payload, sizeof(payload));
    if (len == 0) {
        return;
    }

    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == m->self_slot) continue;
        if (!membership_is_alive(ctx, s)) continue;

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_WELCOME;
        hdr.src_slot      = m->self_slot;
        hdr.dst_slot      = s;
        hdr.epoch         = w.epoch;
        hdr.payload_len   = (uint32_t)len;
        (void)dsm_net_send(ctx, s, &hdr, payload);
    }
}

/* WELCOME no solicitado: réplica de membresía de un par. Se fusiona con la
 * tabla local y se avisa a las esperas de descubrimiento. */
void membership_on_welcome(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload || m->hdr.payload_len < 32u) {
        return;
    }

    welcome_t w;
    memset(&w, 0, sizeof(w));
    if (dsm_unpack_welcome((const uint8_t *)m->payload, m->hdr.payload_len,
                           &w) != DSM_OK) {
        return;
    }
    if (memcmp(w.origin_uuid, ctx->memb.self_uuid, 16) == 0) {
        return;
    }

    membership_merge_wire(&ctx->memb, &w);
    uint64_t epoch = atomic_load(&ctx->memb.epoch);
    if (w.epoch > epoch) {
        atomic_store(&ctx->memb.epoch, w.epoch);
    }
    membership_progress_notify(&ctx->memb);

    /* La réplica puede incorporar un nodo que aún no estaba en la vista. Con
     * redundancia activa, el K derivado de la membresía cambia y todas las
     * vistas deben ajustarlo, no solo la que atendió el JOIN. */
    if (ctx->cfg.redundancy && ctx->raid.stripes) {
        int rrc = raid_reconfigure(ctx);
        if (rrc != DSM_OK) {
            DSM_LOG_WARN("No se pudo ajustar K tras fusionar la membresía: %s",
                         dsm_strerror((dsm_status_t)rrc));
        }
    }
}

/* UUID sin inicializar: un latido extendido solo se aprende si trae identidad. */
static bool membership_uuid_zero(const uint8_t uuid[16])
{
    for (uint32_t i = 0; i < 16; i++) {
        if (uuid[i] != 0) return false;
    }
    return true;
}

void membership_on_heartbeat(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.src_slot;
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    membership_wire_t ident;
    memset(&ident, 0, sizeof(ident));
    bool have_ident = false;

    if (m->payload && m->hdr.payload_len >= DSM_HEARTBEAT_ID_WIRE_LEN) {
        dsm_unpack_membership((const uint8_t *)m->payload +
                                  DSM_HEARTBEAT_WIRE_LEN,
                              &ident);
        have_ident = ident.slot == s && !membership_uuid_zero(ident.uuid) &&
                     memcmp(ident.uuid, ctx->memb.self_uuid, 16) != 0;
    }

    bool learned = false;

    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        node_info_t *ni = &ctx->memb.nodes[s];
        ni->last_hb_ms = mono_ms();
        ni->missed_hb  = 0;
        ni->hb_seen    = 1;
        if (ni->state == DSM_NODE_SUSPECT) {
            ni->state = DSM_NODE_ALIVE;
            DSM_LOG_INFO("Nodo en slot %u recupero estado ALIVE", s);
        }
        if (m->payload && m->hdr.payload_len >= 12) {
            uint32_t free_p = 0;
            memcpy(&free_p, (const uint8_t *)m->payload + 8, 4);
            ni->free_pages = ntohl(free_p);
        }
        if (have_ident && memcmp(ni->uuid, ident.uuid, 16) == 0) {
            /* El latido también refresca la dirección de escucha: el JOIN
             * pudo llegar antes de que el par publicara su puerto final. */
            if (ip_replaceable(ni->ip, ident.ip)) {
                ni->ip = ident.ip;
            }
            if (ident.port != 0) {
                ni->port = ident.port;
            }
            ni->mem_pages = ident.mem_pages;
            ni->is_client = (ident.mem_pages == 0) ? 1u : 0u;
        }
    } else if (have_ident) {
        /* Un latido de una ranura que la tabla no conoce significa que este
         * nodo se perdió el JOIN original. La identidad viaja en el latido;
         * se retira cualquier fila vieja del mismo UUID y se incorpora el
         * nodo en su ranura. */
        for (uint32_t other = 0; other < MAX_NODES; other++) {
            if (other == s || other == ctx->memb.self_slot) continue;
            if (!membership_slot_present(ctx, other)) continue;
            if (memcmp(ctx->memb.nodes[other].uuid, ident.uuid, 16) != 0) {
                continue;
            }
            ctx->memb.nodes[other].state = DSM_NODE_DEAD;
            ctx->memb.present[other / 8] &=
                (uint8_t)~(1u << (other % 8));
            ctx->memb.reusable_after_ms[other] = 0;
        }

        node_info_t *ni = &ctx->memb.nodes[s];
        memcpy(ni->uuid, ident.uuid, 16);
        ni->slot        = s;
        ni->ip          = ip_is_usable(ident.ip) ? ident.ip : 0;
        ni->port        = ident.port;
        ni->state       = DSM_NODE_ALIVE;
        ni->incarnation = ident.incarnation;
        ni->mem_pages   = ident.mem_pages;
        ni->free_pages  = ident.mem_pages;
        ni->is_client   = (ident.mem_pages == 0) ? 1u : 0u;
        ni->last_hb_ms  = mono_ms();
        ni->joined_ms   = ni->last_hb_ms;
        ni->missed_hb   = 0;
        ni->hb_seen     = 1;
        ctx->memb.present[s / 8] |= (uint8_t)(1u << (s % 8));
        learned = true;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (learned) {
        char uuid_str[37];
        uuid_to_str(ident.uuid, uuid_str);
        DSM_LOG_INFO("Latido de una ranura desconocida (%u): se incorpora el "
                     "nodo %s", s, uuid_str);
        membership_progress_notify(&ctx->memb);
        membership_replicate(ctx);
        if (ctx->cfg.redundancy && ctx->raid.stripes) {
            int rrc = raid_reconfigure(ctx);
            if (rrc != DSM_OK) {
                DSM_LOG_WARN("No se pudo ajustar K tras incorporar el slot %u: %s",
                             s, dsm_strerror((dsm_status_t)rrc));
            }
        }
    }
}

/* Responde al solicitante de un JOIN con el rechazo indicado. */
static void join_reply_nack(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd,
                            uint16_t status)
{
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
        nack_hdr.status        = status;
        uint8_t nack_raw[DSM_HDR_LEN];
        dsm_wire_finish(&nack_hdr, nack_raw, NULL, 0);
        dsm_send_full(cfd, nack_raw, DSM_HDR_LEN);
    } else {
        net_send_nack(ctx, m->hdr.src_slot, m->hdr.req_id, status);
    }
}

/*
 * Procesa un JOIN de otro nodo.
 *
 * El receptor es la autoridad sobre su propia tabla, y aplica el mismo
 * desempate por UUID que el solicitante:
 *
 *  - La ranura esta libre: se registra al nodo y se responde JOIN_ACK.
 *  - La ranura la ocupa este nodo y el solicitante tiene UUID menor, mientras
 *    el reparto local sigue provisional: se responde BUSY. El titular cede la
 *    ranura en su propio bucle de descubrimiento y el solicitante reintenta,
 *    de modo que la ranura propia tiene un unico escritor.
 *  - Cualquier otra colision se rechaza con SLOT_TAKEN, y el solicitante toma
 *    la siguiente ranura libre. Una ranura ya confirmada no se renumera: el
 *    cluster en marcha conserva el reparto que sus pares ya publicaron.
 */
void membership_on_join_fd(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd)
{
    if (!m->payload || m->hdr.payload_len < DSM_MEMBERSHIP_WIRE_LEN) return;

    membership_wire_t wire;
    dsm_unpack_membership(m->payload, &wire);

    uint32_t s = wire.slot;
    if (s >= MAX_NODES) {
        join_reply_nack(ctx, m, cfd, DSM_ST_NO_SPACE);
        return;
    }

    /* La IP del solicitante se toma del socket aceptado y no del payload:
     * getpeername devuelve la direccion observada por el kernel. El puerto de
     * escucha si viaja en el JOIN, porque el puerto remoto de esta conexion es
     * efimero. */
    uint32_t observed_ip = socket_peer_ip(cfd);
    if (cfd >= 0) {
        membership_observe_self_ip(ctx, socket_local_ip(cfd));
    }

    pthread_rwlock_wrlock(&ctx->memb.rw);

    /* El nodo que se reubica deja una vista vieja en su ranura previa: se
     * retira para que no figure dos veces. La ranura propia nunca se toca. */
    for (uint32_t other = 0; other < MAX_NODES; other++) {
        if (other == s || other == ctx->memb.self_slot) continue;
        if (!membership_slot_present(ctx, other)) continue;
        if (memcmp(ctx->memb.nodes[other].uuid, wire.uuid, 16) != 0) continue;
        ctx->memb.nodes[other].state = DSM_NODE_DEAD;
        ctx->memb.present[other / 8] &= (uint8_t)~(1u << (other % 8));
        ctx->memb.reusable_after_ms[other] = 0;
    }

    bool taken = membership_slot_present(ctx, s) &&
                 (memcmp(ctx->memb.nodes[s].uuid, wire.uuid, 16) != 0);
    bool own = (s == ctx->memb.self_slot);
    bool busy = taken && own && atomic_load(&ctx->memb.claim_pending) &&
                memcmp(wire.uuid, ctx->memb.self_uuid, 16) < 0;

    if (taken) {
        pthread_rwlock_unlock(&ctx->memb.rw);
        join_reply_nack(ctx, m, cfd, busy ? DSM_ST_BUSY : DSM_ST_SLOT_TAKEN);
        return;
    }

    node_info_t *ni = &ctx->memb.nodes[s];
    memcpy(ni->uuid, wire.uuid, 16);
    ni->slot        = s;
    ni->ip          = ip_is_usable(observed_ip) ? observed_ip : wire.ip;
    ni->port        = wire.port;
    ni->state       = DSM_NODE_ALIVE;
    ni->incarnation = wire.incarnation;
    ni->mem_pages   = wire.mem_pages;
    ni->free_pages  = wire.mem_pages;
    /* Cero páginas publicadas identifica a un cliente en la membresía local. */
    ni->is_client   = (wire.mem_pages == 0) ? 1u : 0u;
    ni->last_hb_ms  = mono_ms();
    ni->joined_ms   = ni->last_hb_ms;
    ni->missed_hb   = 0;
    ni->hb_seen     = 0;
    ctx->memb.present[s / 8] |= (uint8_t)(1u << (s % 8));

    if (m->hdr.epoch >= ctx->memb.epoch) {
        ctx->memb.epoch = m->hdr.epoch + 1;
    }
    uint64_t epoch = atomic_load(&ctx->memb.epoch);

    pthread_rwlock_unlock(&ctx->memb.rw);
    membership_progress_notify(&ctx->memb);

    /* Responder con JOIN_ACK directamente en el socket abierto si existe. No
     * se pre-abre un enlace de vuelta: cada envio posterior conecta bajo
     * demanda y cierra su descriptor al terminar la trama. */
    msg_header_t ack_hdr;
    memset(&ack_hdr, 0, sizeof(ack_hdr));
    ack_hdr.magic         = DSM_MAGIC;
    ack_hdr.cluster_id    = ctx->cfg.cluster_id;
    ack_hdr.proto_version = DSM_PROTO_VERSION;
    ack_hdr.type          = MSG_JOIN_ACK;
    ack_hdr.src_slot      = membership_self_slot(ctx);
    ack_hdr.dst_slot      = s;
    ack_hdr.epoch         = epoch;
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

    /* El JOIN solo lo recibió este nodo. Se replica la tabla a los demás
     * miembros vivos para que el recién llegado aparezca en todas las vistas
     * sin esperar a que cada uno lo descubra por su cuenta. */
    membership_replicate(ctx);

    /* Mientras la paridad no tenga contenido, un nodo nuevo cambia el K
     * derivado de la membresía. Si ya hay franjas escritas, raid_reconfigure
     * conserva el K con el que se formaron. */
    if (ctx->cfg.redundancy && ctx->raid.stripes) {
        int rrc = raid_reconfigure(ctx);
        if (rrc != DSM_OK) {
            DSM_LOG_WARN("No se pudo recalcular K tras la unión del slot %u: %s",
                         s, dsm_strerror((dsm_status_t)rrc));
        }
    }
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

/* Recuperación de coherencia tras una baja: reasigna páginas custodiadas por
 * el slot caído, pasa sus cerrojos al nuevo custodio y programa la
 * reconstrucción de paridad. La ejecuta un hilo worker: sus RPC por página
 * no pueden correr en el listener, que es el único que completa las
 * respuestas, ni en el hilo de latidos, cuyo retraso declararía muertos a
 * los demás nodos. */
void membership_node_down_recover(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES || slot == ctx->memb.self_slot) return;

    msi_on_node_down(ctx, slot);
    sync_on_node_down(ctx, slot);
    /* La membresia ya esta cerrada: raid_on_node_down consulta el anillo
     * antes de tomar el mutex de franja, sin invertir el orden de locks. */
    if (ctx->cfg.redundancy) {
        raid_on_node_down(ctx, slot);
    }
}

/* Encola la recuperación de una ranura recién cerrada. Quien detecta la
 * muerte solo actualiza la membresía y deja el trabajo pesado a un worker,
 * de modo que el listener y el hilo de latidos nunca se bloquean en RPC. */
static void node_down_recover_enqueue(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES) return;

    dsm_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.magic       = DSM_MAGIC;
    msg.hdr.cluster_id  = ctx->cfg.cluster_id;
    msg.hdr.proto_version = DSM_PROTO_VERSION;
    msg.hdr.type        = MSG_NODE_DOWN;
    msg.hdr.src_slot    = ctx->memb.self_slot;
    msg.hdr.page_id     = slot;

    if (worker_enqueue(ctx, &msg, NULL) != DSM_OK) {
        DSM_LOG_WARN("Recuperación del slot %u aplazada: sin capacidad de "
                     "cola; los fallos de página la completarán", slot);
    }
}

void membership_on_node_down(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.page_id; /* slot afectado en page_id */
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    bool changed = false;
    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        ctx->memb.nodes[s].state = DSM_NODE_DEAD;
        ctx->memb.present[s / 8] &= (uint8_t)~(1u << (s % 8));
        ctx->memb.reusable_after_ms[s] = mono_ms() + ctx->cfg.reconnect_max_ms;
        changed = true;
        DSM_LOG_WARN("Notificacion NODE_DOWN procesada para slot %u", s);
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    /* Solo quien cierra la ranura recupera: los receptores del broadcast
     * ya la encuentran cerrada y su paso es un no-op. */
    if (changed) {
        membership_progress_notify(&ctx->memb);
        node_down_recover_enqueue(ctx, s);
    }
}

void membership_on_leave(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t s = m->hdr.src_slot;
    if (s >= MAX_NODES || s == ctx->memb.self_slot) return;

    bool changed = false;
    pthread_rwlock_wrlock(&ctx->memb.rw);
    if (membership_slot_present(ctx, s)) {
        ctx->memb.nodes[s].state = DSM_NODE_DEAD;
        ctx->memb.present[s / 8] &= (uint8_t)~(1u << (s % 8));
        ctx->memb.reusable_after_ms[s] = mono_ms() + ctx->cfg.reconnect_max_ms;
        changed = true;
        DSM_LOG_INFO("Salida ordenada (LEAVE) del nodo en slot %u", s);
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (changed) {
        membership_progress_notify(&ctx->memb);
        node_down_recover_enqueue(ctx, s);
    }
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

/* --- Servicio de consultas de diagnóstico (dsmctl) ---
 *
 * El nodo responde QUERY_REQ sobre la conexión efímera en la que llegó la
 * petición. No se valida el epoch ni el slot del solicitante: dsmctl no es un
 * miembro del clúster y la respuesta es de solo lectura, por lo que no puede
 * alterar el estado. La validación de magic, cluster_id y CRC32 ya la hizo la
 * capa de red. */

#define QUERY_RESP_MAX_RECS 128u

/* Registro más grande del protocolo de consulta; dimensiona el búfer de
 * respuesta sin reservar el máximo de 1 MiB del cable. */
#define QUERY_RESP_MAX_BYTES \
    (DSM_QUERY_RESP_HDR_LEN + QUERY_RESP_MAX_RECS * DSM_NODE_REC_LEN)

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
    bool     overflow;
} query_builder_t;

static void query_builder_put(query_builder_t *b, const uint8_t *src, size_t n)
{
    if (b->len + n > b->cap) {
        b->overflow = true;
        return;
    }
    memcpy(b->buf + b->len, src, n);
    b->len += n;
}

static void query_builder_put_node(query_builder_t *b, const node_info_t *ni,
                                   uint32_t slot, uint64_t epoch)
{
    node_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.slot       = slot;
    memcpy(rec.uuid, ni->uuid, 16);
    rec.ip         = ni->ip;
    rec.port       = ni->port;
    rec.state      = ni->state;
    rec.missed_hb  = ni->missed_hb;
    rec.mem_pages  = ni->mem_pages;
    rec.free_pages = ni->free_pages;
    rec.epoch      = epoch;
    rec.last_hb_ms = ni->last_hb_ms;

    uint8_t raw[DSM_NODE_REC_LEN];
    dsm_pack_node_rec(&rec, raw);
    query_builder_put(b, raw, sizeof(raw));
}

/* Copia hasta 'limit' nodos de la membresía, en orden de slot. */
static uint32_t query_collect_nodes(dsm_ctx_t *ctx, uint32_t first, uint32_t max,
                                    query_builder_t *b)
{
    uint32_t emitted = 0;
    uint32_t seen = 0;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES && emitted < max; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (seen++ < first) continue;
        query_builder_put_node(b, &ctx->memb.nodes[s], s, ctx->memb.epoch);
        emitted++;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
    return emitted;
}

/* Copia hasta 'limit' páginas de la tabla local en el rango pedido. */
static uint32_t query_collect_pages(dsm_ctx_t *ctx, uint32_t first, uint32_t max,
                                    query_builder_t *b)
{
    uint32_t emitted = 0;
    uint32_t total = ctx->mem.npages;
    if (first >= total) return 0;

    uint32_t last = first + max;
    if (last > total || last < first) last = total;

    for (uint32_t p = first; p < last; p++) {
        page_local_t *loc = dsm_mem_local(&ctx->mem, p);
        if (!loc) break;

        /* El registro se copia bajo el cerrojo de franja para no observar una
         * transacción a medias. El home se calcula después, con la franja
         * liberada: ring_home_of consulta el anillo y la membresía y no debe
         * llamarse con la franja tomada. */
        page_local_t snap;
        dsm_mem_stripe_lock(&ctx->mem, p);
        snap = *loc;
        dsm_mem_stripe_unlock(&ctx->mem, p);

        uint32_t home = ring_home_of(ctx, p);

        page_rec_t rec;
        memset(&rec, 0, sizeof(rec));
        rec.page_id    = p;
        rec.home       = home;
        rec.version    = snap.version;
        rec.state      = snap.state;
        rec.pend_inval = snap.pend_inval;
        rec.pend_down  = snap.pend_down;
        rec.pid        = snap.pid;
        rec.order      = snap.order;

        uint8_t raw[DSM_PAGE_REC_LEN];
        dsm_pack_page_rec(&rec, raw);
        query_builder_put(b, raw, sizeof(raw));
        emitted++;
    }
    return emitted;
}

static uint16_t query_fill_global(dsm_ctx_t *ctx, query_builder_t *b)
{
    (void)ctx;
    dsm_stats_t st;
    if (dsm_stats(&st) != DSM_OK) return DSM_ST_BUSY;

    stats_wire_t sw;
    memset(&sw, 0, sizeof(sw));
    sw.read_faults       = st.read_faults;
    sw.write_faults      = st.write_faults;
    sw.msgs_sent         = st.msgs_sent;
    sw.msgs_recv         = st.msgs_recv;
    sw.bytes_sent        = st.bytes_sent;
    sw.bytes_recv        = st.bytes_recv;
    sw.dist_tasks_run    = st.dist_tasks_run;
    sw.dist_tasks_failed = st.dist_tasks_failed;
    sw.parity_deltas     = st.parity_deltas;
    sw.stripe_rebuilds   = st.stripe_rebuilds;
    sw.tseq_discarded    = st.tseq_discarded;
    sw.hb_missed         = st.hb_missed;

    uint8_t raw[DSM_STATS_WIRE_LEN];
    dsm_pack_stats(&sw, raw);
    query_builder_put(b, raw, sizeof(raw));
    return DSM_ST_OK;
}

/* Tabla global de directorio: dsm_pages_global produce las filas con
 * propietario, proceso, orden, copyset y franja para el rango pedido. */
static uint16_t query_fill_dir(dsm_ctx_t *ctx, const query_req_t *q,
                               query_builder_t *b, uint32_t *out_count,
                               uint8_t *out_more)
{
    uint32_t total = ctx->mem.npages;
    if (q->first >= total) {
        *out_count = 0;
        *out_more  = 0;
        return DSM_ST_OK;
    }

    uint32_t max = q->max ? q->max : 1u;
    if (max > 16u) max = 16u;
    uint32_t rows = total - q->first;
    if (rows > max) rows = max;

    char *text = (char *)b->buf + b->len;
    size_t cap = b->cap - b->len;
    int rc = dsm_pages_global(q->first, rows, text, cap);
    if (rc != DSM_OK) return (uint16_t)rc;

    size_t len = strlen(text);
    b->len += len;
    *out_count = rows;
    *out_more  = (q->first + rows < total) ? 1 : 0;
    return DSM_ST_OK;
}

/* Bloques reservados del mapa global. Los huecos se recortan del intervalo
 * [0, marca de agua) y el PID se toma de la tabla local del nodo consultado
 * cuando conoce la reserva; el resto queda sin proceso. */
static uint16_t query_fill_alloc(dsm_ctx_t *ctx, const query_req_t *q,
                                 query_builder_t *b, uint32_t *out_count,
                                 uint8_t *out_more)
{
    uint32_t max = q->max ? q->max : 1u;
    if (max > QUERY_RESP_MAX_RECS) max = QUERY_RESP_MAX_RECS;

    uint32_t emitted = 0;
    uint32_t seen = 0;
    uint8_t more = 0;

    pthread_mutex_lock(&ctx->alloc.mu);

    if (!ctx->alloc.map_valid) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        *out_count = 0;
        *out_more  = 0;
        return DSM_ST_OK;
    }

    uint64_t cursor = 0;
    uint32_t holes = ctx->alloc.nholes;
    for (uint32_t i = 0; i <= holes; i++) {
        uint64_t start;
        uint64_t end;

        if (i < holes) {
            start = cursor;
            end   = ctx->alloc.holes[i].first_page;
            if (end < cursor) end = cursor;
        } else {
            start = cursor;
            end   = ctx->alloc.next_page;
            if (end < cursor) end = cursor;
        }

        if (end > start) {
            if (seen++ >= q->first) {
                if (emitted >= max) {
                    more = 1;
                    break;
                }
                alloc_rec_t rec;
                memset(&rec, 0, sizeof(rec));
                rec.first_page = (uint32_t)start;
                rec.npages     = (uint32_t)(end - start);
                rec.slot       = ctx->memb.self_slot;
                rec.reserved   = DSM_ALLOC_REC_LIVE;
                for (uint32_t k = 0; k < ctx->alloc.depth; k++) {
                    alloc_block_t *blk = &ctx->alloc.stack[k];
                    if (blk->npages == 0 || blk->retiring) continue;
                    if (blk->first_page != rec.first_page ||
                        blk->npages != rec.npages) continue;
                    rec.pid      = blk->pid;
                    rec.alloc_id = blk->alloc_id;
                    break;
                }
                uint8_t raw[DSM_ALLOC_REC_LEN];
                dsm_pack_alloc_rec(&rec, raw);
                query_builder_put(b, raw, sizeof(raw));
                emitted++;
            }
        }

        if (i < holes) {
            uint64_t h_end = (uint64_t)ctx->alloc.holes[i].first_page +
                             ctx->alloc.holes[i].npages;
            if (h_end > cursor) cursor = h_end;
        }
    }

    pthread_mutex_unlock(&ctx->alloc.mu);

    *out_count = emitted;
    *out_more  = more;
    return DSM_ST_OK;
}

static uint16_t query_build_resp(dsm_ctx_t *ctx, const query_req_t *q,
                                 query_builder_t *b, uint32_t *out_count,
                                 uint8_t *out_more)
{
    uint32_t max = q->max;
    if (max == 0 || max > QUERY_RESP_MAX_RECS) {
        max = QUERY_RESP_MAX_RECS;
    }

    uint32_t count = 0;
    uint8_t more = 0;

    switch (q->kind) {
    case DSM_QUERY_NODES:
        count = query_collect_nodes(ctx, q->first, max, b);
        if (count == max) {
            /* Queda al menos un nodo si el recorrido completo supera max. */
            uint32_t total = 0;
            pthread_rwlock_rdlock(&ctx->memb.rw);
            for (uint32_t s = 0; s < MAX_NODES; s++) {
                if (membership_slot_present(ctx, s)) total++;
            }
            pthread_rwlock_unlock(&ctx->memb.rw);
            more = (q->first + count < total) ? 1 : 0;
        }
        break;
    case DSM_QUERY_STATS:
        if (q->scope == DSM_QUERY_GLOBAL) {
            uint16_t st = query_fill_global(ctx, b);
            if (st != DSM_ST_OK) return st;
            count = 1;
        }
        break;
    case DSM_QUERY_PAGES:
        count = query_collect_pages(ctx, q->first, max, b);
        more = (q->first + count < ctx->mem.npages) ? 1 : 0;
        break;
    case DSM_QUERY_DIR: {
        uint16_t st = query_fill_dir(ctx, q, b, &count, &more);
        if (st != DSM_ST_OK) return st;
        break;
    }
    case DSM_QUERY_ALLOC: {
        uint16_t st = query_fill_alloc(ctx, q, b, &count, &more);
        if (st != DSM_ST_OK) return st;
        break;
    }
    default:
        return DSM_ST_BAD_STATE;
    }

    if (b->overflow) return DSM_ST_NO_SPACE;
    *out_count = count;
    *out_more  = more;
    return DSM_ST_OK;
}

void membership_handle_query_fd(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd)
{
    if (cfd < 0) return;

    msg_header_t resp;
    memset(&resp, 0, sizeof(resp));
    resp.magic         = DSM_MAGIC;
    resp.cluster_id    = ctx->cfg.cluster_id;
    resp.proto_version = DSM_PROTO_VERSION;
    resp.type          = MSG_QUERY_RESP;
    resp.src_slot      = ctx->memb.self_slot;
    resp.dst_slot      = m->hdr.src_slot;
    resp.req_id        = m->hdr.req_id;

    uint8_t payload[QUERY_RESP_MAX_BYTES];
    query_resp_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    size_t data_len = 0;

    if (!m->payload || m->hdr.payload_len < DSM_QUERY_REQ_LEN ||
        m->hdr.payload_len > DSM_QUERY_REQ_LEN + DSM_QUERY_HTML_PATH_MAX) {
        resp.status = DSM_ST_BAD_STATE;
    } else {
        query_req_t q;
        dsm_unpack_query_req(m->payload, &q);

        if (q.kind == DSM_QUERY_HTML) {
            char path[DSM_QUERY_HTML_PATH_MAX + 1];
            uint32_t plen = m->hdr.payload_len - DSM_QUERY_REQ_LEN;
            memcpy(path, (const uint8_t *)m->payload + DSM_QUERY_REQ_LEN, plen);
            path[plen] = '\0';

            int hr = dsm_html_report(plen ? path : NULL);
            resp.status = (hr == DSM_OK) ? DSM_ST_OK : DSM_ST_NO_SPACE;
            if (resp.status == DSM_ST_OK) {
                hdr.kind  = q.kind;
                hdr.count = 0;
                hdr.more  = 0;
            }
        } else if (m->hdr.payload_len != DSM_QUERY_REQ_LEN) {
            resp.status = DSM_ST_BAD_STATE;
        } else {
            query_builder_t b = {
                .buf      = payload + DSM_QUERY_RESP_HDR_LEN,
                .len      = 0,
                .cap      = sizeof(payload) - DSM_QUERY_RESP_HDR_LEN,
                .overflow = false
            };

            uint32_t count = 0;
            uint8_t more = 0;
            resp.status = query_build_resp(ctx, &q, &b, &count, &more);
            if (resp.status == DSM_ST_OK) {
                hdr.kind  = q.kind;
                hdr.more  = more;
                hdr.count = count;
                data_len  = b.len;
            }
        }
    }

    size_t total = DSM_QUERY_RESP_HDR_LEN;
    if (resp.status == DSM_ST_OK) {
        dsm_pack_query_resp(&hdr, payload);
        total += data_len;
    }

    uint8_t raw_hdr[DSM_HDR_LEN];
    dsm_wire_finish(&resp, raw_hdr, payload, (uint32_t)total);
    if (dsm_send_full(cfd, raw_hdr, DSM_HDR_LEN) == 0) {
        dsm_send_full(cfd, payload, total);
    }

    atomic_fetch_add_explicit(&ctx->ctr.msgs_sent, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->ctr.bytes_sent, DSM_HDR_LEN + total, memory_order_relaxed);
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

        /* Un nodo incorporado por JOIN puede tardar en arrancar su hilo de
         * latidos más que HB_DEAD: mientras no haya enviado ninguno, se le
         * concede el plazo de unión y no se evalúa su silencio. */
        if (!ni->hb_seen) {
            uint64_t grace = (uint64_t)ctx->cfg.hb_dead *
                             (uint64_t)ctx->cfg.hb_interval_ms;
            if (grace < DISCOVERY_JOIN_GRACE_MS) {
                grace = DISCOVERY_JOIN_GRACE_MS;
            }
            if (now_ms >= ni->joined_ms && now_ms - ni->joined_ms < grace) {
                continue;
            }
            ni->state = DSM_NODE_DEAD;
            m->present[s / 8] &= (uint8_t)~(1u << (s % 8));
            m->reusable_after_ms[s] = now_ms + ctx->cfg.reconnect_max_ms;
            dead_slots[dead_count++] = s;
            DSM_LOG_WARN("Nodo en slot %u sin latidos desde su unión; "
                         "se dictamina DEAD", s);
            continue;
        }

        uint64_t elapsed = now_ms - ni->last_hb_ms;
        uint32_t missed = (uint32_t)(elapsed / ctx->cfg.hb_interval_ms);
        /* Cada intervalo perdido se cuenta una sola vez: el acumulador
         * crece con el incremento respecto de la evaluación anterior. */
        if (missed > ni->missed_hb) {
            atomic_fetch_add_explicit(&ctx->ctr.hb_missed,
                                      (uint64_t)(missed - ni->missed_hb),
                                      memory_order_relaxed);
        }
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

    if (dead_count > 0) {
        membership_progress_notify(m);
    }

    /* Quien detecta la muerte también recupera la coherencia de su lado:
     * confiar solo en el broadcast dejaría al detector con páginas huérfanas
     * si el aviso se pierde. La recuperación corre en un worker, nunca en
     * este hilo de latidos. */
    for (uint32_t d = 0; d < dead_count; d++) {
        node_down_recover_enqueue(ctx, dead_slots[d]);
    }

    /* Difundir NODE_DOWN para los nodos declarados muertos */
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
    dsm_membership_t *m = &ctx->memb;

    while (atomic_load(&ctx->hb_running)) {
        /* El periodo del latido se espera sobre hb_cv con plazo absoluto. El
         * apagado señaliza la condvar y el hilo despierta en el acto, sin
         * trocear la espera en pasos fijos. */
        uint64_t period_end = mono_ms() + ctx->cfg.hb_interval_ms;
        struct timespec ts;

        pthread_mutex_lock(&m->hb_mu);
        while (atomic_load(&ctx->hb_running)) {
            uint64_t now = mono_ms();
            if (now >= period_end) {
                break;
            }
            mono_deadline_ts(period_end, &ts);
            if (pthread_cond_timedwait(&m->hb_cv, &m->hb_mu, &ts) != 0) {
                break;
            }
        }
        pthread_mutex_unlock(&m->hb_mu);
        if (!atomic_load(&ctx->hb_running)) break;

        uint64_t now = mono_ms();
        membership_evaluate(ctx, now);
        sync_migrate_tick(ctx);

        uint32_t dst[MAX_NODES];
        uint32_t ndst = 0;

        pthread_rwlock_rdlock(&ctx->memb.rw);
        for (uint32_t s = 0; s < MAX_NODES; s++) {
            if (s != ctx->memb.self_slot && membership_slot_present(ctx, s)) {
                dst[ndst++] = s;
            }
        }
        pthread_rwlock_unlock(&ctx->memb.rw);

        uint32_t free_pages = dsm_alloc_free_pages(ctx);
        pthread_rwlock_wrlock(&ctx->memb.rw);
        if (ctx->memb.self_slot < MAX_NODES) {
            ctx->memb.nodes[ctx->memb.self_slot].free_pages = free_pages;
        }
        pthread_rwlock_unlock(&ctx->memb.rw);
        for (uint32_t i = 0; i < ndst; i++) {
            net_send_heartbeat(ctx, dst[i], free_pages);
        }
    }

    return NULL;
}

int hb_start(dsm_ctx_t *ctx)
{
    if (ctx->hb_thread && atomic_load(&ctx->hb_running)) {
        return DSM_OK;
    }

    atomic_store(&ctx->hb_running, true);
    if (pthread_create(&ctx->hb_thread, NULL, hb_thread_main, ctx) != 0) {
        atomic_store(&ctx->hb_running, false);
        ctx->hb_thread = 0;
        DSM_LOG_ERR("No se pudo iniciar hilo de heartbeat: %s", strerror(errno));
        return DSM_ERR_MEM;
    }
    return DSM_OK;
}

void hb_stop(dsm_ctx_t *ctx)
{
    atomic_store(&ctx->hb_running, false);
    pthread_mutex_lock(&ctx->memb.hb_mu);
    pthread_cond_broadcast(&ctx->memb.hb_cv);
    pthread_mutex_unlock(&ctx->memb.hb_mu);
    if (ctx->hb_thread) {
        pthread_join(ctx->hb_thread, NULL);
        ctx->hb_thread = 0;
    }
}
