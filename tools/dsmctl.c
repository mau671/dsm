#include "dsm.h"
#include "config.h"
#include "wire.h"
#include "util.h"

#include <errno.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

/*
 * dsmctl es un cliente de diagnóstico que no integra la membresía del
 * clúster: abre una conexión TCP efímera al puerto de escucha de un nodo,
 * envía QUERY_REQ y lee la QUERY_RESP. El nodo nunca lo registra como
 * miembro ni le reserva marcos de memoria, por lo que consultar no altera
 * el estado del sistema.
 */

#define DEFAULT_CONF  "dsm.conf"
#define DEFAULT_HOST  "127.0.0.1"
#define DEFAULT_PORT  27600u
#define MAX_RECORDS   128u
/* Un byte extra para terminar en cero el texto de DSM_QUERY_DIR. */
#define RX_CAPACITY   (DSM_QUERY_RESP_HDR_LEN + \
                       MAX_RECORDS * DSM_NODE_REC_LEN + 1u)

/* Plazos del cliente: acotados para que un nodo saturado no deje a dsmctl
 * esperando indefinidamente. */
#define CONNECT_TIMEOUT_MS  2000u
#define RESPONSE_TIMEOUT_MS 5000u

typedef struct {
    const char *conf_path;
    const char *host;
    uint16_t    port;
    bool        port_set;
    uint32_t    first;
    uint32_t    limit;
} cli_opts_t;

typedef struct {
    const char *host;
    uint16_t    port;
    uint32_t    next_req_id;
    uint32_t    cluster_id;
} query_conn_t;

/* --- Utilidades de salida --- */

static void print_rule(char c, int width)
{
    for (int i = 0; i < width; i++) putchar(c);
    putchar('\n');
}

static const char *node_state_name(uint8_t state)
{
    switch (state) {
    case DSM_NODE_ALIVE:   return "ALIVE";
    case DSM_NODE_SUSPECT: return "SUSPECT";
    case DSM_NODE_DEAD:    return "DEAD";
    default:               return "Desconocido";
    }
}

static void format_bytes(uint64_t bytes, char *out, size_t out_len)
{
    static const char *units[] = { "B", "KiB", "MiB", "GiB", "TiB" };
    double value = (double)bytes;
    uint32_t unit = 0;

    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        unit++;
    }
    snprintf(out, out_len, "%.2f %s", value, units[unit]);
}

static void format_ip(uint32_t ip_net, char *out, size_t out_len)
{
    struct in_addr in;
    in.s_addr = ip_net;
    if (!inet_ntop(AF_INET, &in, out, (socklen_t)out_len)) {
        snprintf(out, out_len, "0.0.0.0");
    }
}

/* --- Conexión y protocolo --- */

/* El listener del nodo atiende una única trama por conexión y la cierra al
 * responder. Por eso cada consulta abre su propio socket efímero. */
static int query_connect(const query_conn_t *c)
{
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    int fd = DSM_ERR_NET;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[8];
    snprintf(port_str, sizeof(port_str), "%u", c->port);

    int gr = getaddrinfo(c->host, port_str, &hints, &res);
    if (gr != 0 || !res) {
        fprintf(stderr, "dsmctl: no se pudo resolver '%s': %s\n",
                c->host, gai_strerror(gr));
        return DSM_ERR_NET;
    }

    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = dsm_connect_timeout((const struct sockaddr_in *)ai->ai_addr,
                                 CONNECT_TIMEOUT_MS);
        if (fd >= 0) break;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        fprintf(stderr,
                "dsmctl: no hay un nodo DSM escuchando en %s:%u\n",
                c->host, c->port);
    }
    return fd;
}

/* Envía una QUERY_REQ y recibe la QUERY_RESP completa en rx. El bloque
 * 'extra' se añade después de los 12 bytes de la petición; solo la usa
 * DSM_QUERY_HTML para transportar la ruta de salida. */
static int query_exchange_full(query_conn_t *c, uint8_t kind, uint8_t scope,
                               uint32_t first, uint32_t max,
                               const void *extra, uint32_t extra_len,
                               query_resp_t *resp, uint8_t *rx,
                               uint32_t *rx_len)
{
    int fd = query_connect(c);
    if (fd < 0) return DSM_ERR_NET;

    if (extra_len > DSM_QUERY_HTML_PATH_MAX) {
        close(fd);
        return DSM_ERR_INVALID;
    }

    query_req_t q;
    memset(&q, 0, sizeof(q));
    q.kind  = kind;
    q.scope = scope;
    q.first = first;
    q.max   = max;

    uint8_t req_payload[DSM_QUERY_REQ_LEN + DSM_QUERY_HTML_PATH_MAX];
    dsm_pack_query_req(&q, req_payload);
    if (extra_len > 0) {
        memcpy(req_payload + DSM_QUERY_REQ_LEN, extra, extra_len);
    }
    uint32_t req_len = DSM_QUERY_REQ_LEN + extra_len;

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = c->cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_QUERY_REQ;
    hdr.src_slot      = DSM_SLOT_NONE;
    hdr.dst_slot      = DSM_SLOT_NONE;
    hdr.req_id        = c->next_req_id++;

    uint8_t raw_hdr[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw_hdr, req_payload, req_len);

    int rc = DSM_OK;
    if (dsm_send_full(fd, raw_hdr, DSM_HDR_LEN) != 0 ||
        dsm_send_full(fd, req_payload, req_len) != 0) {
        fprintf(stderr, "dsmctl: fallo al enviar la consulta al nodo\n");
        rc = DSM_ERR_NET;
        goto out;
    }

    uint8_t rx_hdr_raw[DSM_HDR_LEN];
    rc = dsm_read_full_deadline(fd, rx_hdr_raw, DSM_HDR_LEN,
                                RESPONSE_TIMEOUT_MS);
    if (rc != DSM_OK) {
        fprintf(stderr, "dsmctl: el nodo no respondió a la consulta\n");
        goto out;
    }

    msg_header_t rx_hdr;
    if (dsm_wire_unpack(rx_hdr_raw, &rx_hdr) != DSM_OK) {
        fprintf(stderr, "dsmctl: respuesta con cabecera inválida\n");
        rc = DSM_ERR_PROTO;
        goto out;
    }
    if (rx_hdr.type != MSG_QUERY_RESP) {
        fprintf(stderr, "dsmctl: respuesta inesperada del nodo: %s\n",
                dsm_msg_type_name(rx_hdr.type));
        rc = DSM_ERR_PROTO;
        goto out;
    }
    if (rx_hdr.req_id != hdr.req_id) {
        fprintf(stderr, "dsmctl: respuesta con req_id ajeno\n");
        rc = DSM_ERR_PROTO;
        goto out;
    }
    if (rx_hdr.status != DSM_ST_OK) {
        fprintf(stderr, "dsmctl: el nodo rechazó la consulta con estado %u\n",
                (unsigned)rx_hdr.status);
        rc = DSM_ERR_PROTO;
        goto out;
    }
    if (rx_hdr.payload_len < DSM_QUERY_RESP_HDR_LEN ||
        rx_hdr.payload_len > *rx_len) {
        fprintf(stderr, "dsmctl: respuesta de tamaño inconsistente\n");
        rc = DSM_ERR_PROTO;
        goto out;
    }

    rc = dsm_read_full_deadline(fd, rx, rx_hdr.payload_len,
                                RESPONSE_TIMEOUT_MS);
    if (rc != DSM_OK) {
        fprintf(stderr, "dsmctl: respuesta truncada del nodo\n");
        goto out;
    }
    if (dsm_wire_validate(&rx_hdr, rx_hdr_raw, rx, c->cluster_id) != DSM_OK) {
        fprintf(stderr, "dsmctl: CRC32 o identificador de clúster inválidos en la respuesta\n");
        rc = DSM_ERR_PROTO;
        goto out;
    }

    dsm_unpack_query_resp(rx, resp);
    *rx_len = rx_hdr.payload_len;

out:
    close(fd);
    return rc;
}

static int query_exchange(query_conn_t *c, uint8_t kind, uint8_t scope,
                          uint32_t first, uint32_t max,
                          query_resp_t *resp, uint8_t *rx, uint32_t *rx_len)
{
    return query_exchange_full(c, kind, scope, first, max, NULL, 0,
                               resp, rx, rx_len);
}

/* --- Comandos --- */

/* Reúne la tabla de membresía del nodo contactado. */
static int fetch_nodes(query_conn_t *c, node_rec_t *out, uint32_t cap,
                       uint32_t *out_n)
{
    uint8_t rx[RX_CAPACITY];
    uint32_t first = 0;
    uint32_t n = 0;
    int rc = DSM_OK;

    while (n < cap) {
        uint32_t rx_len = sizeof(rx) - 1u;
        query_resp_t resp;
        rc = query_exchange(c, DSM_QUERY_NODES, DSM_QUERY_GLOBAL, first,
                            MAX_RECORDS, &resp, rx, &rx_len);
        if (rc != DSM_OK) return rc;
        if (resp.kind != DSM_QUERY_NODES || resp.count > MAX_RECORDS ||
            rx_len != DSM_QUERY_RESP_HDR_LEN + resp.count * DSM_NODE_REC_LEN) {
            return DSM_ERR_PROTO;
        }
        if (resp.count == 0) break;

        for (uint32_t i = 0; i < resp.count && n < cap; i++) {
            dsm_unpack_node_rec(rx + DSM_QUERY_RESP_HDR_LEN + i * DSM_NODE_REC_LEN,
                                &out[n++]);
        }
        if (!resp.more) break;
        first += resp.count;
    }

    *out_n = n;
    return DSM_OK;
}

/* Consulta las estadísticas de un miembro concreto. */
static int fetch_stats_at(uint32_t cluster_id, const node_rec_t *node,
                          stats_wire_t *out)
{
    char host[INET_ADDRSTRLEN];
    format_ip(node->ip, host, sizeof(host));

    query_conn_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.host       = host;
    conn.port       = node->port;
    conn.cluster_id = cluster_id;

    uint8_t rx[RX_CAPACITY];
    uint32_t rx_len = sizeof(rx) - 1u;
    query_resp_t resp;
    int rc = query_exchange(&conn, DSM_QUERY_STATS, DSM_QUERY_GLOBAL, 0, 1,
                            &resp, rx, &rx_len);
    if (rc != DSM_OK) return rc;
    if (resp.kind != DSM_QUERY_STATS || resp.count != 1 ||
        rx_len != DSM_QUERY_RESP_HDR_LEN + DSM_STATS_WIRE_LEN) {
        return DSM_ERR_PROTO;
    }

    dsm_unpack_stats(rx + DSM_QUERY_RESP_HDR_LEN, out);
    return DSM_OK;
}

static int cmd_stats(query_conn_t *c)
{
    node_rec_t nodes[MAX_RECORDS];
    uint32_t nnodes = 0;
    int rc = fetch_nodes(c, nodes, MAX_RECORDS, &nnodes);
    if (rc != DSM_OK) {
        fprintf(stderr, "dsmctl: no se pudo leer la tabla de nodos: %s\n",
                dsm_strerror((dsm_status_t)rc));
        return rc;
    }

    stats_wire_t total;
    memset(&total, 0, sizeof(total));

    print_rule('=', 128);
    printf("%-4s  %-7s  %-10s  %-10s  %-10s  %-10s  %-14s  %s\n",
           "Ranura", "Rol", "Fallos L", "Fallos E", "Mensajes",
           "Deltas", "Reconstruidas", "Latidos perdidos");
    print_rule('=', 128);

    uint32_t answered = 0;
    for (uint32_t i = 0; i < nnodes; i++) {
        stats_wire_t st;
        memset(&st, 0, sizeof(st));
        rc = fetch_stats_at(c->cluster_id, &nodes[i], &st);
        if (rc != DSM_OK) {
            fprintf(stderr, "dsmctl: el nodo %u no respondió: %s\n",
                    nodes[i].slot, dsm_strerror((dsm_status_t)rc));
            continue;
        }
        answered++;

        total.read_faults       += st.read_faults;
        total.write_faults      += st.write_faults;
        total.msgs_sent         += st.msgs_sent;
        total.msgs_recv         += st.msgs_recv;
        total.bytes_sent        += st.bytes_sent;
        total.bytes_recv        += st.bytes_recv;
        total.dist_tasks_run    += st.dist_tasks_run;
        total.dist_tasks_failed += st.dist_tasks_failed;
        total.parity_deltas     += st.parity_deltas;
        total.stripe_rebuilds   += st.stripe_rebuilds;
        total.tseq_discarded    += st.tseq_discarded;
        total.hb_missed         += st.hb_missed;

        printf("%-4u  %-7s  %-10lu  %-10lu  %-10lu  %-10lu  %-14lu  %lu\n",
               nodes[i].slot,
               nodes[i].mem_pages == 0 ? "cliente" : "nodo",
               (unsigned long)st.read_faults,
               (unsigned long)st.write_faults,
               (unsigned long)(st.msgs_sent + st.msgs_recv),
               (unsigned long)st.parity_deltas,
               (unsigned long)st.stripe_rebuilds,
               (unsigned long)st.hb_missed);
    }

    print_rule('-', 128);
    printf("Nodos consultados: %u de %u\n", answered, nnodes);
    if (answered == 0) return DSM_ERR_NET;

    char sent[32], recv[32];
    format_bytes(total.bytes_sent, sent, sizeof(sent));
    format_bytes(total.bytes_recv, recv, sizeof(recv));

    printf("\nEstadísticas agregadas del clúster\n");
    printf("  Fallos de lectura        : %lu\n", (unsigned long)total.read_faults);
    printf("  Fallos de escritura      : %lu\n", (unsigned long)total.write_faults);
    printf("  Fallos totales           : %lu\n",
           (unsigned long)(total.read_faults + total.write_faults));
    printf("  Mensajes enviados        : %lu\n", (unsigned long)total.msgs_sent);
    printf("  Mensajes recibidos       : %lu\n", (unsigned long)total.msgs_recv);
    printf("  Bytes enviados           : %lu, equivalente a %s\n",
           (unsigned long)total.bytes_sent, sent);
    printf("  Bytes recibidos          : %lu, equivalente a %s\n",
           (unsigned long)total.bytes_recv, recv);
    printf("  Tareas distribuidas      : %lu correctas, %lu fallidas\n",
           (unsigned long)total.dist_tasks_run,
           (unsigned long)total.dist_tasks_failed);
    printf("  Deltas de paridad        : %lu\n",
           (unsigned long)total.parity_deltas);
    printf("  Franjas reconstruidas    : %lu\n",
           (unsigned long)total.stripe_rebuilds);
    printf("  Respuestas descartadas   : %lu\n",
           (unsigned long)total.tseq_discarded);
    printf("  Latidos perdidos         : %lu\n",
           (unsigned long)total.hb_missed);
    return DSM_OK;
}

static int cmd_nodes(query_conn_t *c, const cli_opts_t *opts)
{
    uint8_t rx[RX_CAPACITY];
    uint32_t first = opts->first;
    uint32_t limit = opts->limit ? opts->limit : MAX_RECORDS;
    uint32_t printed = 0;
    uint32_t clients = 0;
    bool header_printed = false;
    int rc = DSM_OK;

    for (;;) {
        uint32_t rx_len = sizeof(rx);
        query_resp_t resp;
        rc = query_exchange(c, DSM_QUERY_NODES, DSM_QUERY_GLOBAL, first,
                            MAX_RECORDS, &resp, rx, &rx_len);
        if (rc != DSM_OK) return rc;
        if (resp.kind != DSM_QUERY_NODES) {
            fprintf(stderr, "dsmctl: tipo de respuesta inesperado\n");
            return DSM_ERR_PROTO;
        }

        uint32_t count = resp.count;
        if (count > MAX_RECORDS ||
            rx_len != DSM_QUERY_RESP_HDR_LEN + count * DSM_NODE_REC_LEN) {
            fprintf(stderr, "dsmctl: número de registros inconsistente\n");
            return DSM_ERR_PROTO;
        }

        /* La cabecera se imprime una vez confirmada la primera respuesta,
         * para no mostrar una tabla vacía si el nodo no contesta. */
        if (!header_printed) {
            print_rule('=', 132);
            printf("%-4s  %-36s  %-15s  %-5s  %-8s  %-15s  %-9s  %-13s  %-7s  %s\n",
                   "Ranura", "UUID", "IP", "Puerto", "Rol", "Estado",
                   "Aportadas", "Libres clúster", "Latidos", "Época");
            print_rule('=', 132);
            header_printed = true;
        }

        for (uint32_t i = 0; i < count && printed < limit; i++) {
            node_rec_t rec;
            dsm_unpack_node_rec(rx + DSM_QUERY_RESP_HDR_LEN + i * DSM_NODE_REC_LEN,
                                &rec);

            char uuid[37];
            char ip[INET_ADDRSTRLEN];
            char estado[32];
            const char *rol;
            uuid_to_str(rec.uuid, uuid);
            format_ip(rec.ip, ip, sizeof(ip));
            snprintf(estado, sizeof(estado), "%s", node_state_name(rec.state));

            /* Un miembro que no aporta páginas es un cliente: usa la memoria
             * del cluster sin custodiar ninguna página. */
            if (rec.mem_pages == 0) {
                rol = "cliente";
                clients++;
            } else {
                rol = "nodo";
            }

            printf("%-4u  %-36s  %-15s  %-5u  %-8s  %-15s  %-9u  %-13u  %-7u  %lu\n",
                   rec.slot, uuid, ip, (unsigned)rec.port,
                   rol, estado, rec.mem_pages,
                   rec.free_pages, (unsigned)rec.missed_hb,
                   (unsigned long)rec.epoch);
            printed++;
        }

        if (!resp.more || count == 0 || printed >= limit) break;
        first += count;
    }

    print_rule('-', 132);
    printf("Nodos listados: %u\n", printed - clients);
    if (clients > 0) {
        printf("Clientes listados: %u\n", clients);
    }
    return DSM_OK;
}

static int print_dir_range(query_conn_t *c, uint32_t first, uint32_t limit)
{
    uint8_t rx[RX_CAPACITY];
    uint32_t printed = 0;

    while (printed < limit) {
        uint32_t chunk = limit - printed;
        if (chunk > 8u) chunk = 8u;

        uint32_t rx_len = sizeof(rx) - 1u;
        query_resp_t resp;
        int rc = query_exchange(c, DSM_QUERY_DIR, DSM_QUERY_GLOBAL, first,
                                chunk, &resp, rx, &rx_len);
        if (rc != DSM_OK) return rc;
        if (resp.kind != DSM_QUERY_DIR ||
            rx_len < DSM_QUERY_RESP_HDR_LEN) {
            fprintf(stderr, "dsmctl: respuesta de directorio inconsistente\n");
            return DSM_ERR_PROTO;
        }

        rx[rx_len] = '\0';
        const char *text = (const char *)(rx + DSM_QUERY_RESP_HDR_LEN);
        const char *body = text;
        for (int line = 0; line < 2; line++) {
            const char *nl = strchr(body, '\n');
            if (!nl) {
                body = text + strlen(text);
                break;
            }
            body = nl + 1;
        }
        fputs(body, stdout);

        printed += resp.count;
        first += resp.count;
        if (!resp.more || resp.count == 0) break;
    }

    print_rule('-', 92);
    printf("Páginas mostradas: %u\n", printed);
    return DSM_OK;
}

static int cmd_dir(query_conn_t *c, const cli_opts_t *opts)
{
    uint32_t first = opts->first;
    uint32_t limit = opts->limit ? opts->limit : 16u;

    printf("Tabla de directorio global [%u, %u)\n", first, first + limit);
    printf("%-6s  %-11s  %-7s  %-5s  %-7s  %-6s\n",
           "Página", "Propietario", "Proceso", "Orden", "Copyset", "Franja");
    print_rule('-', 92);
    return print_dir_range(c, first, limit);
}

static int cmd_pages(query_conn_t *c, const cli_opts_t *opts)
{
    uint8_t rx[RX_CAPACITY];
    uint32_t total = 0;
    uint32_t counts[6] = { 0 };
    int rc = DSM_OK;

    /* Recorrido completo: el resumen cuenta todas las páginas, no solo el
     * rango que se imprime. */
    for (;;) {
        uint32_t rx_len = sizeof(rx) - 1u;
        query_resp_t resp;
        rc = query_exchange(c, DSM_QUERY_PAGES, DSM_QUERY_LOCAL, total,
                            MAX_RECORDS, &resp, rx, &rx_len);
        if (rc != DSM_OK) return rc;
        if (resp.kind != DSM_QUERY_PAGES) {
            fprintf(stderr, "dsmctl: tipo de respuesta inesperado\n");
            return DSM_ERR_PROTO;
        }

        uint32_t count = resp.count;
        if (count > MAX_RECORDS ||
            rx_len != DSM_QUERY_RESP_HDR_LEN + count * DSM_PAGE_REC_LEN) {
            fprintf(stderr, "dsmctl: número de registros inconsistente\n");
            return DSM_ERR_PROTO;
        }
        if (count == 0) break;

        for (uint32_t i = 0; i < count; i++) {
            page_rec_t rec;
            dsm_unpack_page_rec(rx + DSM_QUERY_RESP_HDR_LEN + i * DSM_PAGE_REC_LEN,
                                &rec);
            if (rec.state < sizeof(counts) / sizeof(counts[0])) {
                counts[rec.state]++;
            }
        }
        total += count;
        if (!resp.more) break;
    }

    printf("Tabla de páginas local\n");
    printf("  Páginas totales          : %u\n", total);
    printf("  Modificadas              : %u\n", counts[DSM_PAGE_M]);
    printf("  Compartidas              : %u\n", counts[DSM_PAGE_S]);
    printf("  Inválidas                : %u\n", counts[DSM_PAGE_I]);
    printf("  Transiciones pendientes  : %u\n",
           counts[DSM_PAGE_IS_PEND] + counts[DSM_PAGE_IM_PEND] +
           counts[DSM_PAGE_SM_PEND]);

    if (!opts->limit) return DSM_OK;

    printf("\n");
    printf("Directorio global [%u, %u)\n", opts->first,
           opts->first + opts->limit);
    printf("%-6s  %-11s  %-7s  %-5s  %-7s  %-6s\n",
           "Página", "Propietario", "Proceso", "Orden", "Copyset", "Franja");
    print_rule('-', 92);
    return print_dir_range(c, opts->first, opts->limit);
}

static int cmd_alloc(query_conn_t *c, const cli_opts_t *opts)
{
    uint8_t rx[RX_CAPACITY];
    uint32_t first = opts->first;
    uint32_t limit = opts->limit ? opts->limit : 32u;
    uint32_t printed = 0;
    int rc = DSM_OK;

    printf("Bloques reservados del espacio global\n");
    printf("%-14s  %-8s  %-8s  %-7s  %s\n",
           "Primera página", "Páginas", "Proceso", "Ranura", "Identificador");
    print_rule('-', 68);

    while (printed < limit) {
        uint32_t chunk = limit - printed;
        if (chunk > MAX_RECORDS) chunk = MAX_RECORDS;

        uint32_t rx_len = sizeof(rx) - 1u;
        query_resp_t resp;
        rc = query_exchange(c, DSM_QUERY_ALLOC, DSM_QUERY_GLOBAL, first,
                            chunk, &resp, rx, &rx_len);
        if (rc != DSM_OK) return rc;
        if (resp.kind != DSM_QUERY_ALLOC || resp.count > MAX_RECORDS ||
            rx_len != DSM_QUERY_RESP_HDR_LEN +
                      resp.count * DSM_ALLOC_REC_LEN) {
            fprintf(stderr, "dsmctl: respuesta de reservas inconsistente\n");
            return DSM_ERR_PROTO;
        }

        for (uint32_t i = 0; i < resp.count; i++) {
            alloc_rec_t rec;
            dsm_unpack_alloc_rec(rx + DSM_QUERY_RESP_HDR_LEN +
                                     i * DSM_ALLOC_REC_LEN, &rec);
            if (rec.reserved != DSM_ALLOC_REC_LIVE) continue;

            char pid[16];
            if (rec.pid == 0) {
                snprintf(pid, sizeof(pid), "-");
            } else {
                snprintf(pid, sizeof(pid), "%u", rec.pid);
            }
            printf("%-14u  %-8u  %-8s  %-7u  %u\n",
                   rec.first_page, rec.npages, pid, rec.slot, rec.alloc_id);
            printed++;
        }

        if (!resp.more || resp.count == 0) break;
        first += resp.count;
    }

    print_rule('-', 68);
    printf("Bloques mostrados: %u\n", printed);
    return DSM_OK;
}

static int cmd_html(query_conn_t *c, const cli_opts_t *opts,
                    const dsm_config_t *cfg, const char *dir)
{
    (void)opts;
    uint8_t rx[RX_CAPACITY];
    uint32_t rx_len = sizeof(rx) - 1u;
    query_resp_t resp;

    /* Sin argumento, el destino es el OUTPUT_DIR de la configuración con la
     * que se invoca dsmctl. Se envía siempre de forma explícita para que el
     * nodo escriba en ese mismo directorio y el mensaje no anuncie otro. */
    const char *dest = (dir && *dir) ? dir : cfg->output_dir;
    if (!dest || *dest == '\0') {
        fprintf(stderr, "dsmctl: la configuración no define OUTPUT_DIR\n");
        return DSM_ERR_INVALID;
    }
    uint32_t len = (uint32_t)strlen(dest);

    int rc = query_exchange_full(c, DSM_QUERY_HTML, DSM_QUERY_GLOBAL, 0, 0,
                                 dest, len, &resp, rx, &rx_len);
    if (rc != DSM_OK) return rc;
    if (resp.kind != DSM_QUERY_HTML) {
        fprintf(stderr, "dsmctl: respuesta de informe inesperada\n");
        return DSM_ERR_PROTO;
    }

    printf("Informe HTML generado en %s\n", dest);
    printf("Archivos: %s/global.html y un archivo nodo-<ranura>.html por nodo\n",
           dest);
    return DSM_OK;
}

static int cmd_config(const cli_opts_t *opts, const dsm_config_t *cfg)
{
    char view[32], node_mem[32];
    format_bytes(cfg->view_size, view, sizeof(view));
    format_bytes(cfg->node_mem, node_mem, sizeof(node_mem));

    printf("Configuración cargada: %s\n",
           opts->conf_path ? opts->conf_path : DEFAULT_CONF);
    printf("  Identificador de clúster           : 0x%08X\n", cfg->cluster_id);
    printf("  Tamaño de página                   : %u bytes\n", cfg->page_size);
    printf("  Dirección base                     : 0x%lX\n", (unsigned long)cfg->base_addr);
    printf("  Tamaño de vista                    : %lu, equivalente a %s\n",
           (unsigned long)cfg->view_size, view);
    printf("  Memoria por nodo                   : %lu, equivalente a %s\n",
           (unsigned long)cfg->node_mem, node_mem);
    printf("  Nodos máximos en ejecución         : %u\n", cfg->max_runtime_nodes);
    printf("  Nodos virtuales por MiB            : %u\n", cfg->vnodes_per_mb);
    printf("  Redundancia                        : %s\n",
           cfg->redundancy ? "true" : "false");
    printf("  Rango de puertos                   : %u - %u\n", cfg->port_min, cfg->port_max);
    printf("  Grupo de descubrimiento            : 0x%08X\n", cfg->discovery_group);
    printf("  Puerto de descubrimiento           : %u\n", cfg->discovery_port);
    printf("  Conexión                           : %u ms\n", cfg->connect_timeout_ms);
    printf("  Descubrimiento                     : %u ms\n", cfg->discovery_timeout_ms);
    printf("  Envío                              : %u ms\n", cfg->send_timeout_ms);
    printf("  Solicitud                          : %u ms\n", cfg->request_timeout_ms);
    printf("  Reintentos                         : %u\n", cfg->request_retries);
    printf("  Latidos                            : %u ms\n", cfg->hb_interval_ms);
    printf("  Sospecha                           : %u\n", cfg->hb_suspect);
    printf("  Caído                              : %u\n", cfg->hb_dead);
    printf("  Hilos                              : %u trabajadores y %u despachadores\n",
           cfg->worker_threads, cfg->dispatcher_threads);
    printf("  Franjas de cerrojo de página       : %u\n", cfg->page_lock_stripes);
    printf("  Margen de custodio                 : %u.%03u\n",
           cfg->home_slack_permille / 1000, cfg->home_slack_permille % 1000);
    printf("  Plazo de cerrojo                   : %u ms\n", cfg->lock_timeout_ms);
    printf("  Capacidad de cola de trabajadores  : %u\n", cfg->worker_queue_cap);
    printf("  Nivel de registro DSM              : %s\n", cfg->log_level);
    printf("  Hash de configuración compartida   : 0x%08X\n", cfg->shared_hash);

    if (opts->port_set && opts->port != cfg->port_min) {
        printf("\nAviso: se consulta el puerto %u, fuera del rango %u-%u "
               "declarado en la configuración.\n",
               opts->port, cfg->port_min, cfg->port_max);
    }
    return DSM_OK;
}

static void print_usage(const char *prog)
{
    printf("Uso: %s [opciones] <comando> [opciones]\n", prog);
    printf("\n");
    printf("Comandos:\n");
    printf("  stats              Estadísticas agregadas de todos los nodos:\n");
    printf("                     fallos, mensajes, tareas distribuidas,\n");
    printf("                     paridad, reconstrucciones y latidos perdidos.\n");
    printf("  nodes              Tabla de membresía del clúster: ranura, UUID,\n");
    printf("                     IP, puerto, rol, estado, páginas aportadas,\n");
    printf("                     páginas libres del clúster y latidos perdidos.\n");
    printf("  pages [inicio] [n] Resumen de la tabla de páginas local; con\n");
    printf("                     inicio y n imprime el directorio global del\n");
    printf("                     rango, con copyset y franja.\n");
    printf("  dir [inicio] [n]   Tabla de directorio global: propietario,\n");
    printf("                     proceso, orden, copyset y franja.\n");
    printf("  alloc [inicio] [n] Bloques reservados del espacio global.\n");
    printf("  html [directorio]  Genera el informe HTML del clúster en el\n");
    printf("                     nodo consultado, o en el OUTPUT_DIR de la\n");
    printf("                     configuración si se omite el directorio.\n");
    printf("  config             Configuración cargada desde dsm.conf.\n");
    printf("  help               Muestra esta ayuda.\n");
    printf("\n");
    printf("Opciones:\n");
    printf("  -c, --conf <ruta>  Archivo de configuración; por defecto %s.\n",
           DEFAULT_CONF);
    printf("  -H, --host <host>  Nodo a consultar; por defecto %s.\n", DEFAULT_HOST);
    printf("  -p, --port <puerto> Puerto de escucha del nodo; por defecto %u.\n",
           DEFAULT_PORT);
    printf("  -h, --help         Muestra esta ayuda.\n");
    printf("\n");
    printf("  Las opciones se aceptan antes o después del comando.\n");
    printf("\n");
    printf("Funcionamiento:\n");
    printf("  dsmctl no se une al clúster. Abre una conexión TCP efímera al\n");
    printf("  puerto de escucha del nodo indicado y le envía una petición\n");
    printf("  QUERY_REQ; el nodo responde QUERY_RESP sin registrarlo como\n");
    printf("  miembro ni reservarle memoria. El puerto por defecto es el\n");
    printf("  PORT_MIN de la configuración, que es el que toma el primer nodo\n");
    printf("  de la máquina. Para consultar otro nodo use --port.\n");
    printf("\n");
    printf("Limitaciones:\n");
    printf("  - pages muestra el resumen de la vista local del nodo contactado;\n");
    printf("    el detalle y dir consultan la tabla global de todos los nodos.\n");
    printf("  - dir y pages enumeran la vista completa: el tamaño de la vista\n");
    printf("    entre el tamaño de página; en la configuración por defecto son\n");
    printf("    más de un millón de páginas. Use un rango para acotar la salida.\n");
    printf("  - nodes solo muestra los nodos presentes en la membresía del\n");
    printf("    nodo contactado; un nodo caído desaparece de esa tabla.\n");
    printf("  - html escribe los archivos en el sistema de archivos del nodo\n");
    printf("    consultado.\n");
}

/* --- Análisis de argumentos --- */

static bool parse_u32_arg(const char *text, uint32_t *out)
{
    if (!text || *text == '\0' || *text == '-') return false;
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0' || v > UINT32_MAX) {
        return false;
    }
    *out = (uint32_t)v;
    return true;
}

static bool parse_port_arg(const char *text, uint16_t *out)
{
    uint32_t v = 0;
    if (!parse_u32_arg(text, &v) || v == 0 || v > UINT16_MAX) return false;
    *out = (uint16_t)v;
    return true;
}

/* Devuelve el número de posicionales consumidos o -1 ante error. El primer
 * argumento que no sea opción es el comando; el resto son posicionales. Las
 * opciones se aceptan antes o después del comando. */
static int parse_args(int argc, char **argv, cli_opts_t *opts,
                      const char **command, const char **positional,
                      int max_positional)
{
    int npos = 0;
    *command = NULL;

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];

        if (strcmp(arg, "-c") == 0 || strcmp(arg, "--conf") == 0) {
            if (i + 1 >= argc) return -1;
            opts->conf_path = argv[++i];
        } else if (strncmp(arg, "--conf=", 7) == 0) {
            opts->conf_path = arg + 7;
        } else if (strcmp(arg, "-H") == 0 || strcmp(arg, "--host") == 0) {
            if (i + 1 >= argc) return -1;
            opts->host = argv[++i];
        } else if (strncmp(arg, "--host=", 7) == 0) {
            opts->host = arg + 7;
        } else if (strcmp(arg, "-p") == 0 || strcmp(arg, "--port") == 0) {
            if (i + 1 >= argc || !parse_port_arg(argv[i + 1], &opts->port)) {
                return -1;
            }
            opts->port_set = true;
            i++;
        } else if (strncmp(arg, "--port=", 7) == 0) {
            if (!parse_port_arg(arg + 7, &opts->port)) return -1;
            opts->port_set = true;
        } else if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
            *command = "help";
        } else if (arg[0] == '-' && arg[1] != '\0') {
            fprintf(stderr, "dsmctl: opción desconocida '%s'\n", arg);
            return -1;
        } else if (*command == NULL) {
            *command = arg;
        } else {
            if (npos >= max_positional) {
                fprintf(stderr, "dsmctl: argumento sobrante '%s'\n", arg);
                return -1;
            }
            positional[npos++] = arg;
        }
    }

    if (*command == NULL) *command = "help";
    return npos;
}

int main(int argc, char **argv)
{
    cli_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.conf_path = DEFAULT_CONF;
    opts.host      = DEFAULT_HOST;

    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    const char *command = NULL;
    const char *positional[2] = { NULL, NULL };

    int npos = parse_args(argc, argv, &opts, &command, positional, 2);
    if (npos < 0) {
        fprintf(stderr, "Use '%s help' para ver las opciones.\n", argv[0]);
        return 1;
    }

    if (strcmp(command, "help") == 0 || strcmp(command, "--help") == 0) {
        print_usage(argv[0]);
        return 0;
    }

    bool is_config = strcmp(command, "config") == 0;
    bool is_stats  = strcmp(command, "stats") == 0;
    bool is_nodes  = strcmp(command, "nodes") == 0;
    bool is_pages  = strcmp(command, "pages") == 0;
    bool is_dir    = strcmp(command, "dir") == 0;
    bool is_alloc  = strcmp(command, "alloc") == 0;
    bool is_html   = strcmp(command, "html") == 0;

    if (!is_config && !is_stats && !is_nodes && !is_pages &&
        !is_dir && !is_alloc && !is_html) {
        fprintf(stderr, "dsmctl: comando desconocido '%s'\n", command);
        fprintf(stderr, "Comandos disponibles: stats, nodes, pages, dir, alloc, "
                        "html, config, help\n");
        return 1;
    }

    if ((is_pages || is_dir || is_alloc) && npos > 0) {
        if (!parse_u32_arg(positional[0], &opts.first)) {
            fprintf(stderr, "dsmctl: página inicial inválida '%s'\n",
                    positional[0]);
            return 1;
        }
        if (npos > 1) {
            if (!parse_u32_arg(positional[1], &opts.limit) || opts.limit == 0) {
                fprintf(stderr, "dsmctl: cantidad de páginas inválida '%s'\n",
                        positional[1]);
                return 1;
            }
        } else {
            opts.limit = is_pages ? 16u : (is_dir ? 16u : 32u);
        }
    } else if (is_html && npos > 0) {
        /* El directorio de salida es el único argumento. */
    } else if (npos > 0) {
        fprintf(stderr, "dsmctl: '%s' no acepta argumentos posicionales\n",
                command);
        return 1;
    }

    /* La configuración aporta el cluster_id que valida las respuestas y el
     * puerto por defecto. */
    dsm_config_t cfg;
    int rc = config_load(&cfg, opts.conf_path);
    if (rc != DSM_OK) {
        fprintf(stderr, "dsmctl: no se pudo cargar '%s'\n", opts.conf_path);
        return 1;
    }

    if (is_config) {
        return (cmd_config(&opts, &cfg) == DSM_OK) ? 0 : 1;
    }

    if (!opts.port_set) {
        opts.port = cfg.port_min;
    }

    query_conn_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.host       = opts.host;
    conn.port       = opts.port;
    conn.cluster_id = cfg.cluster_id;

    if (is_stats) {
        rc = cmd_stats(&conn);
    } else if (is_nodes) {
        rc = cmd_nodes(&conn, &opts);
    } else if (is_pages) {
        rc = cmd_pages(&conn, &opts);
    } else if (is_dir) {
        rc = cmd_dir(&conn, &opts);
    } else if (is_alloc) {
        rc = cmd_alloc(&conn, &opts);
    } else {
        rc = cmd_html(&conn, &opts, &cfg, npos > 0 ? positional[0] : NULL);
    }

    return (rc == DSM_OK) ? 0 : 1;
}
