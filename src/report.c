#include "dsm.h"
#include "dsm_internal.h"
#include "discovery.h"
#include "ring.h"
#include "wire.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* Registros por consulta remota: el servicio QUERY_RESP entrega como maximo
 * 128 registros por respuesta. */
#define REPORT_QUERY_RECS   128u
/* Paginas procesadas por bloque al recorrer la tabla global: acota la memoria
 * de trabajo del reporte sin reservar una entrada por pagina de la vista. */
#define REPORT_CHUNK_PAGES  4096u
/* Tope de filas y de rangos que escribe el informe HTML. Evita que una vista
 * grande con pocas páginas materializadas se recorra completa y que un clúster
 * con millones de páginas activas convierta el informe en una consulta sin
 * fin. */
#define REPORT_MAX_ROWS     4096u
#define REPORT_MAX_RUNS     1024u
#define REPORT_COPY_BYTES   (MAX_NODES / 8u)

/* Rango contiguo de páginas materializadas en la vista local. */
typedef struct {
    uint32_t first_page;
    uint32_t npages;
} report_run_t;

typedef struct {
    uint32_t slot;
    uint32_t ip;              /* orden de red */
    uint16_t port;            /* orden de host */
    uint32_t mem_pages;
    uint64_t epoch;
    uint8_t  state;
    bool     is_client;
} report_node_t;

typedef struct {
    FILE       *fp;
    uint32_t    slot;
} report_file_t;

/* --- Instantanea de la membresia ---------------------------------------- */

/* Nodos que aportan memoria y participan del anillo. Un cliente usa la vista
 * pero no custodia paginas: nunca aparece como nodo ni como propietario. */
static uint32_t report_collect_nodes(dsm_ctx_t *ctx, report_node_t *out,
                                     uint32_t cap)
{
    uint32_t n = 0;
    dsm_membership_t *m = &ctx->memb;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES && n < cap; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (m->nodes[s].state == DSM_NODE_DEAD) continue;
        if (m->nodes[s].is_client || m->nodes[s].mem_pages == 0) continue;

        out[n].slot      = s;
        out[n].ip        = m->nodes[s].ip;
        out[n].port      = m->nodes[s].port;
        out[n].mem_pages = m->nodes[s].mem_pages;
        out[n].epoch     = m->epoch;
        out[n].state     = m->nodes[s].state;
        out[n].is_client = false;
        n++;
    }
    pthread_rwlock_unlock(&m->rw);
    return n;
}

/* Clientes: miembros que usan la vista pero no aportan memoria. Se listan con
 * su rol y nunca cuentan como nodos ni como propietarios de páginas. */
static uint32_t report_collect_clients(dsm_ctx_t *ctx, report_node_t *out,
                                       uint32_t cap)
{
    uint32_t n = 0;
    dsm_membership_t *m = &ctx->memb;

    pthread_rwlock_rdlock(&m->rw);
    for (uint32_t s = 0; s < MAX_NODES && n < cap; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (m->nodes[s].state == DSM_NODE_DEAD) continue;
        if (!m->nodes[s].is_client && m->nodes[s].mem_pages != 0) continue;

        out[n].slot      = s;
        out[n].ip        = m->nodes[s].ip;
        out[n].port      = m->nodes[s].port;
        out[n].mem_pages = m->nodes[s].mem_pages;
        out[n].epoch     = m->epoch;
        out[n].state     = m->nodes[s].state;
        out[n].is_client = true;
        n++;
    }
    pthread_rwlock_unlock(&m->rw);
    return n;
}

static void report_count_roles(dsm_ctx_t *ctx, uint32_t *nodes,
                               uint32_t *clients, uint64_t *epoch)
{
    uint32_t nn = 0;
    uint32_t nc = 0;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        if (ctx->memb.nodes[s].is_client ||
            ctx->memb.nodes[s].mem_pages == 0) {
            nc++;
        } else {
            nn++;
        }
    }
    if (epoch) *epoch = ctx->memb.epoch;
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (nodes) *nodes = nn;
    if (clients) *clients = nc;
}

/* --- Consulta de la tabla de paginas de un nodo ------------------------- */

/* Abre la conexion efimera, envia QUERY_REQ y lee la QUERY_RESP completa. El
 * listener del nodo atiende una sola trama por conexion y la cierra. */
static int report_query_remote(dsm_ctx_t *ctx, const report_node_t *n,
                               uint32_t first, uint32_t max,
                               page_rec_t *out, uint32_t *out_count,
                               uint8_t *out_more)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = n->ip;
    addr.sin_port        = htons(n->port);

    uint32_t connect_ms = ctx->cfg.connect_timeout_ms ? ctx->cfg.connect_timeout_ms
                                                      : 2000u;
    uint32_t send_ms    = ctx->cfg.send_timeout_ms ? ctx->cfg.send_timeout_ms
                                                   : 2000u;
    uint32_t recv_ms    = ctx->cfg.request_timeout_ms ? ctx->cfg.request_timeout_ms
                                                      : 5000u;

    int fd = dsm_connect_timeout(&addr, connect_ms);
    if (fd < 0) return DSM_ERR_NET;

    query_req_t q;
    memset(&q, 0, sizeof(q));
    q.kind  = DSM_QUERY_PAGES;
    q.scope = DSM_QUERY_LOCAL;
    q.first = first;
    q.max   = max;

    uint8_t req[DSM_QUERY_REQ_LEN];
    dsm_pack_query_req(&q, req);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_QUERY_REQ;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = n->slot;
    hdr.req_id        = dsm_net_next_req(ctx);
    hdr.payload_len   = DSM_QUERY_REQ_LEN;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw, req, DSM_QUERY_REQ_LEN);

    int rc = DSM_OK;
    if (dsm_send_full_deadline(fd, raw, DSM_HDR_LEN, send_ms) != 0 ||
        dsm_send_full_deadline(fd, req, DSM_QUERY_REQ_LEN, send_ms) != 0) {
        rc = DSM_ERR_NET;
        goto out;
    }

    uint8_t rx_hdr_raw[DSM_HDR_LEN];
    if (dsm_read_full_deadline(fd, rx_hdr_raw, DSM_HDR_LEN, recv_ms) != DSM_OK) {
        rc = DSM_ERR_NET;
        goto out;
    }

    msg_header_t rx_hdr;
    if (dsm_wire_unpack(rx_hdr_raw, &rx_hdr) != DSM_OK ||
        rx_hdr.type != MSG_QUERY_RESP ||
        rx_hdr.req_id != hdr.req_id ||
        rx_hdr.src_slot != n->slot ||
        rx_hdr.cluster_id != ctx->cfg.cluster_id) {
        rc = DSM_ERR_PROTO;
        goto out;
    }
    if (rx_hdr.status != DSM_ST_OK ||
        rx_hdr.payload_len < DSM_QUERY_RESP_HDR_LEN ||
        rx_hdr.payload_len > DSM_QUERY_RESP_HDR_LEN +
                             REPORT_QUERY_RECS * DSM_PAGE_REC_LEN) {
        rc = DSM_ERR_PROTO;
        goto out;
    }

    uint8_t payload[DSM_QUERY_RESP_HDR_LEN +
                    REPORT_QUERY_RECS * DSM_PAGE_REC_LEN];
    if (dsm_read_full_deadline(fd, payload, rx_hdr.payload_len, recv_ms) != DSM_OK) {
        rc = DSM_ERR_NET;
        goto out;
    }
    if (dsm_wire_validate(&rx_hdr, rx_hdr_raw, payload,
                          ctx->cfg.cluster_id) != DSM_OK) {
        rc = DSM_ERR_PROTO;
        goto out;
    }

    query_resp_t resp;
    dsm_unpack_query_resp(payload, &resp);
    if (resp.kind != DSM_QUERY_PAGES || resp.count > max ||
        rx_hdr.payload_len != DSM_QUERY_RESP_HDR_LEN +
                              resp.count * DSM_PAGE_REC_LEN) {
        rc = DSM_ERR_PROTO;
        goto out;
    }

    for (uint32_t i = 0; i < resp.count; i++) {
        dsm_unpack_page_rec(payload + DSM_QUERY_RESP_HDR_LEN +
                                i * DSM_PAGE_REC_LEN, &out[i]);
    }
    *out_count = resp.count;
    *out_more  = resp.more;

out:
    close(fd);
    return rc;
}

static uint32_t report_fetch_local(dsm_ctx_t *ctx, uint32_t first,
                                   uint32_t count, page_rec_t *out)
{
    uint32_t total = ctx->mem.npages;
    if (first >= total) return 0;

    uint32_t last = first + count;
    if (last > total || last < first) last = total;

    uint32_t got = 0;
    for (uint32_t p = first; p < last; p++) {
        page_local_t *loc = dsm_mem_local(&ctx->mem, p);
        if (!loc) break;

        memset(&out[got], 0, sizeof(out[got]));
        out[got].page_id    = p;
        out[got].home       = DSM_SLOT_NONE;
        out[got].version    = loc->version;
        out[got].state      = loc->state;
        out[got].pend_inval = loc->pend_inval;
        out[got].pend_down  = loc->pend_down;
        out[got].pid        = loc->pid;
        out[got].order      = loc->order;
        got++;
    }
    return got;
}

/* Copia 'count' registros de la tabla del nodo a partir de 'first'. Los nodos
 * con menos paginas devuelven menos registros y el resto queda en cero. */
static int report_fetch_node(dsm_ctx_t *ctx, const report_node_t *n,
                             uint32_t first, uint32_t count,
                             page_rec_t *out, uint32_t *out_got)
{
    memset(out, 0, (size_t)count * sizeof(*out));

    if (n->slot == ctx->memb.self_slot) {
        *out_got = report_fetch_local(ctx, first, count, out);
        return DSM_OK;
    }

    uint32_t got = 0;
    while (got < count) {
        uint32_t want = count - got;
        if (want > REPORT_QUERY_RECS) want = REPORT_QUERY_RECS;

        uint32_t recs = 0;
        uint8_t  more = 0;
        int rc = report_query_remote(ctx, n, first + got, want,
                                     out + got, &recs, &more);
        if (rc != DSM_OK) return rc;

        got += recs;
        if (recs == 0 || !more) break;
    }

    *out_got = got;
    return DSM_OK;
}

/* --- Formato de la tabla de texto --------------------------------------- */

static void report_format_copyset(const uint8_t *copyset, char *buf,
                                  size_t cap)
{
    size_t off = 0;
    bool any = false;

    if (cap == 0) return;
    off += (size_t)snprintf(buf + off, cap - off, "{");
    for (uint32_t s = 0; s < MAX_NODES && off + 16 < cap; s++) {
        if ((copyset[s / 8] & (uint8_t)(1u << (s % 8))) == 0) continue;
        off += (size_t)snprintf(buf + off, cap - off, "%s%u",
                                any ? "," : "", s);
        any = true;
    }
    if (off + 3 < cap) {
        snprintf(buf + off, cap - off, "}");
    } else if (cap > 0) {
        buf[cap - 1] = '\0';
    }
    if (!any && cap >= 3) {
        snprintf(buf, cap, "-");
    }
}

static uint32_t report_stripe(const dsm_ctx_t *ctx, uint32_t page)
{
    uint32_t group = ctx->cfg.chunk_pages ? ctx->cfg.chunk_pages : 1u;
    if (ctx->cfg.redundancy_raid5 && ctx->cfg.raid_k >= 2) {
        group = ctx->cfg.raid_k;
    }
    return page / group;
}

/* Texto de la columna Proceso: el PID real de la reserva o "-" si la página
 * todavía no pertenece a ningún bloque. */
static const char *report_pid_text(uint32_t pid, char *buf, size_t cap)
{
    if (pid == 0) return "-";
    snprintf(buf, cap, "%u", pid);
    return buf;
}

/* --- Tabla global de texto ---------------------------------------------- */

static int report_append(char *out, size_t cap, size_t *len, const char *fmt,
                         ...)
{
    va_list ap;
    va_start(ap, fmt);
    int need = vsnprintf(out + *len, cap - *len, fmt, ap);
    va_end(ap);
    if (need < 0 || (size_t)need >= cap - *len) return DSM_ERR_NO_SPACE;
    *len += (size_t)need;
    return DSM_OK;
}

dsm_status_t dsm_pages_global(uint32_t first, uint32_t max, char *out,
                              size_t cap)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out || cap == 0) return DSM_ERR_INVALID;

    size_t len = 0;
    int rc = report_append(out, cap, &len,
                           "Pagina  Propietario  Proceso  Orden  Copyset  Franja\n"
                           "------  -----------  -------  -----  -------  ------\n");
    if (rc != DSM_OK) return (dsm_status_t)rc;

    uint32_t total = ctx->mem.npages;
    if (max == 0 || first >= total) {
        out[len] = '\0';
        return DSM_OK;
    }

    uint32_t rows = total - first;
    if (rows > max) rows = max;

    report_node_t nodes[MAX_NODES];
    uint32_t nnodes = report_collect_nodes(ctx, nodes, MAX_NODES);

    uint32_t chunk = rows;
    if (chunk > REPORT_CHUNK_PAGES) chunk = REPORT_CHUNK_PAGES;

    size_t entries = (size_t)chunk * (nnodes ? nnodes : 1u);
    uint8_t *states = calloc(entries, 1u);
    uint32_t *pids = calloc(entries, sizeof(uint32_t));
    uint32_t *orders = calloc(entries, sizeof(uint32_t));
    page_rec_t *recs = calloc(chunk, sizeof(page_rec_t));
    if (!states || !pids || !orders || !recs) {
        free(states);
        free(pids);
        free(orders);
        free(recs);
        return DSM_ERR_NO_SPACE;
    }

    uint32_t done = 0;
    while (done < rows) {
        uint32_t n = rows - done;
        if (n > chunk) n = chunk;

        memset(states, 0, (size_t)n * (nnodes ? nnodes : 1u));
        memset(pids, 0, (size_t)n * (nnodes ? nnodes : 1u) * sizeof(uint32_t));
        memset(orders, 0, (size_t)n * (nnodes ? nnodes : 1u) * sizeof(uint32_t));
        for (uint32_t k = 0; k < nnodes; k++) {
            uint32_t got = 0;
            int frc = report_fetch_node(ctx, &nodes[k], first + done, n,
                                        recs, &got);
            if (frc != DSM_OK) {
                free(states);
                free(pids);
                free(orders);
                free(recs);
                return (dsm_status_t)frc;
            }
            for (uint32_t i = 0; i < got; i++) {
                size_t idx = (size_t)k * n + i;
                states[idx] = recs[i].state;
                pids[idx]   = recs[i].pid;
                orders[idx] = recs[i].order;
            }
        }

        for (uint32_t i = 0; i < n; i++) {
            uint32_t page = first + done + i;
            uint8_t copyset[MAX_NODES / 8];
            memset(copyset, 0, sizeof(copyset));

            uint32_t pid = 0;
            uint32_t order = 0;
            for (uint32_t k = 0; k < nnodes; k++) {
                size_t idx = (size_t)k * n + i;
                uint8_t st = states[idx];
                if (st == DSM_PAGE_S || st == DSM_PAGE_M) {
                    uint32_t s = nodes[k].slot;
                    copyset[s / 8] |= (uint8_t)(1u << (s % 8));
                    if (pid == 0 && pids[idx] != 0) {
                        pid   = pids[idx];
                        order = orders[idx];
                    }
                }
            }

            char cps[8 * MAX_NODES + 4];
            char pidbuf[16];
            report_format_copyset(copyset, cps, sizeof(cps));
            rc = report_append(out, cap, &len,
                               "%-6u  %-11u  %-7s  %-5u  %-7s  %-6u\n",
                               page, ring_home_of(ctx, page),
                               report_pid_text(pid, pidbuf, sizeof(pidbuf)),
                               order, cps, report_stripe(ctx, page));
            if (rc != DSM_OK) {
                free(states);
                free(pids);
                free(orders);
                free(recs);
                return (dsm_status_t)rc;
            }
        }

        done += n;
    }

    free(states);
    free(pids);
    free(orders);
    free(recs);
    out[len] = '\0';
    return DSM_OK;
}

/* --- Informe HTML ------------------------------------------------------- */

static const char *report_state_name(uint8_t state)
{
    switch (state) {
    case DSM_PAGE_I:       return "I";
    case DSM_PAGE_S:       return "S";
    case DSM_PAGE_M:       return "M";
    case DSM_PAGE_IS_PEND: return "IS_PEND";
    case DSM_PAGE_IM_PEND: return "IM_PEND";
    case DSM_PAGE_SM_PEND: return "SM_PEND";
    default:               return "?";
    }
}

static const char *report_node_state_name(uint8_t state)
{
    switch (state) {
    case DSM_NODE_ALIVE:   return "ALIVE";
    case DSM_NODE_SUSPECT: return "SUSPECT";
    case DSM_NODE_DEAD:    return "DEAD";
    default:               return "DESCONOCIDO";
    }
}

static void report_html_head(FILE *fp, const char *title)
{
    fprintf(fp,
            "<!DOCTYPE html>\n"
            "<html lang=\"es\">\n<head>\n"
            "<meta charset=\"utf-8\">\n"
            "<meta name=\"viewport\" content=\"width=device-width, "
            "initial-scale=1\">\n"
            "<title>%s</title>\n"
            "<style>\n"
            "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,"
            "sans-serif;margin:2rem;color:#1f2328;background:#f6f8fa}\n"
            "h1{font-size:1.6rem}h2{font-size:1.2rem;margin-top:2rem}\n"
            "table{border-collapse:collapse;background:#fff;"
            "box-shadow:0 1px 2px rgba(0,0,0,.12);font-size:.9rem}\n"
            "th,td{border:1px solid #d0d7de;padding:.35rem .6rem;"
            "text-align:left}th{background:#eaeef2}\n"
            ".meta{display:flex;flex-wrap:wrap;gap:1.5rem;margin:1rem 0}\n"
            ".meta div{background:#fff;border:1px solid #d0d7de;"
            "border-radius:6px;padding:.6rem .9rem}\n"
            "code{background:#eaeef2;padding:.1rem .3rem;border-radius:4px}\n"
            "footer{margin-top:2rem;color:#57606a;font-size:.85rem}\n"
            "</style>\n</head>\n<body>\n",
            title);
    fprintf(fp, "<h1>%s</h1>\n", title);
}

static void report_html_tail(FILE *fp)
{
    fprintf(fp, "<footer>Generado por la biblioteca DSM.</footer>\n"
                "</body>\n</html>\n");
}

static int report_ensure_dir(const char *path)
{
    size_t plen = strlen(path);
    if (plen == 0 || plen >= 512u) return DSM_ERR_NO_SPACE;

    char tmp[512];
    memcpy(tmp, path, plen + 1);

    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(tmp, 0777) != 0 && errno != EEXIST) return DSM_ERR_NO_SPACE;
        *p = '/';
    }
    if (mkdir(tmp, 0777) != 0 && errno != EEXIST) return DSM_ERR_NO_SPACE;
    return DSM_OK;
}

/* Reúne los rangos contiguos de páginas materializadas en la vista local. El
 * informe consulta solo esos rangos: recorrer la vista completa con una
 * consulta remota por bloque no terminaría en un tiempo razonable cuando la
 * configuración por defecto mapea un millón de páginas y solo unas pocas
 * tienen datos. */
static uint32_t report_collect_runs(dsm_ctx_t *ctx, report_run_t *out,
                                    uint32_t cap, uint32_t max_rows)
{
    uint32_t runs = 0;
    uint32_t rows = 0;
    uint32_t p = 0;

    while (p < ctx->mem.npages && rows < max_rows) {
        dsm_mem_stripe_lock(&ctx->mem, p);
        uint8_t state = ctx->mem.local[p].state;
        dsm_mem_stripe_unlock(&ctx->mem, p);
        if (state == DSM_PAGE_I) {
            p++;
            continue;
        }

        uint32_t first = p;
        while (p < ctx->mem.npages && rows < max_rows) {
            dsm_mem_stripe_lock(&ctx->mem, p);
            state = ctx->mem.local[p].state;
            dsm_mem_stripe_unlock(&ctx->mem, p);
            if (state == DSM_PAGE_I) break;
            p++;
            rows++;
        }

        if (runs >= cap) break;
        out[runs].first_page = first;
        out[runs].npages     = p - first;
        runs++;
    }
    return runs;
}

dsm_status_t dsm_html_report(const char *out_dir)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    const char *dir = out_dir;
    if (!dir || dir[0] == '\0') {
        dir = ctx->cfg.output_dir;
    }
    if (!dir || dir[0] == '\0') return DSM_ERR_INVALID;

    int rc = report_ensure_dir(dir);
    if (rc != DSM_OK) return (dsm_status_t)rc;

    report_node_t nodes[MAX_NODES];
    uint32_t nnodes = report_collect_nodes(ctx, nodes, MAX_NODES);
    report_node_t clients[MAX_NODES];
    uint32_t nclients = report_collect_clients(ctx, clients, MAX_NODES);

    report_file_t files[MAX_NODES + 1];
    memset(files, 0, sizeof(files));

    dsm_status_t result = DSM_OK;
    uint32_t nfiles = 0;

    char path[600];
    snprintf(path, sizeof(path), "%s/global.html", dir);
    FILE *global = fopen(path, "w");
    if (!global) return DSM_ERR_NO_SPACE;
    report_html_head(global, "Tabla global de paginas DSM");

    uint32_t nodos = 0;
    uint32_t clientes = 0;
    uint64_t epoch = 0;
    report_count_roles(ctx, &nodos, &clientes, &epoch);

    dsm_stats_t st;
    memset(&st, 0, sizeof(st));
    (void)dsm_stats(&st);

    fprintf(global,
            "<div class=\"meta\">"
            "<div><strong>Nodos:</strong> %u</div>"
            "<div><strong>Clientes:</strong> %u</div>"
            "<div><strong>Epoca:</strong> %llu</div>"
            "<div><strong>Tamano de pagina:</strong> %u B</div>"
            "<div><strong>Paginas de la vista:</strong> %u</div>"
            "<div><strong>Particiones distribuidas:</strong> %llu ok, "
            "%llu fallidas</div>"
            "</div>\n",
            nodos, clientes, (unsigned long long)epoch,
            ctx->cfg.page_size, ctx->mem.npages,
            (unsigned long long)st.dist_tasks_run,
            (unsigned long long)st.dist_tasks_failed);

    fprintf(global,
            "<h2>Nodos y clientes del cluster</h2>\n"
            "<table>\n<tr><th>Ranura</th><th>Rol</th><th>Estado</th>"
            "<th>Memoria</th><th>Direccion</th><th>Informe</th></tr>\n");
    for (uint32_t k = 0; k < nnodes; k++) {
        char ip[INET_ADDRSTRLEN] = "0.0.0.0";
        struct in_addr in;
        in.s_addr = nodes[k].ip;
        if (!inet_ntop(AF_INET, &in, ip, sizeof(ip))) {
            snprintf(ip, sizeof(ip), "0.0.0.0");
        }
        fprintf(global,
                "<tr><td>%u</td><td>nodo</td><td>%s</td>"
                "<td>%u paginas</td><td>%s:%u</td>"
                "<td><a href=\"nodo-%u.html\">nodo-%u.html</a></td></tr>\n",
                nodes[k].slot, report_node_state_name(nodes[k].state),
                nodes[k].mem_pages, ip, (unsigned)nodes[k].port,
                nodes[k].slot, nodes[k].slot);
    }
    for (uint32_t k = 0; k < nclients; k++) {
        char ip[INET_ADDRSTRLEN] = "0.0.0.0";
        struct in_addr in;
        in.s_addr = clients[k].ip;
        if (!inet_ntop(AF_INET, &in, ip, sizeof(ip))) {
            snprintf(ip, sizeof(ip), "0.0.0.0");
        }
        fprintf(global,
                "<tr><td>%u</td><td>cliente</td><td>%s</td>"
                "<td>0 paginas</td><td>%s:%u</td><td>-</td></tr>\n",
                clients[k].slot, report_node_state_name(clients[k].state),
                ip, (unsigned)clients[k].port);
    }
    fprintf(global, "</table>\n");

    fprintf(global,
            "<h2>Tabla global de paginas</h2>\n"
            "<table>\n<tr><th>Pagina</th><th>Propietario</th>"
            "<th>Proceso</th><th>Orden</th>"
            "<th>Copyset</th><th>Franja</th></tr>\n");

    for (uint32_t k = 0; k < nnodes; k++) {
        snprintf(path, sizeof(path), "%s/nodo-%u.html", dir, nodes[k].slot);
        FILE *fp = fopen(path, "w");
        if (!fp) {
            result = DSM_ERR_NO_SPACE;
            goto cleanup;
        }
        files[nfiles].fp    = fp;
        files[nfiles].slot  = nodes[k].slot;
        nfiles++;

        char title[96];
        snprintf(title, sizeof(title), "Nodo %u", nodes[k].slot);
        report_html_head(fp, title);

        char ip[INET_ADDRSTRLEN] = "0.0.0.0";
        struct in_addr in;
        in.s_addr = nodes[k].ip;
        if (!inet_ntop(AF_INET, &in, ip, sizeof(ip))) {
            snprintf(ip, sizeof(ip), "0.0.0.0");
        }

        fprintf(fp,
                "<div class=\"meta\">"
                "<div><strong>Ranura:</strong> %u</div>"
                "<div><strong>Rol:</strong> nodo</div>"
                "<div><strong>Estado:</strong> %s</div>"
                "<div><strong>Memoria aportada:</strong> %u paginas</div>"
                "<div><strong>Direccion:</strong> %s:%u</div>"
                "</div>\n",
                nodes[k].slot, report_node_state_name(nodes[k].state),
                nodes[k].mem_pages, ip, (unsigned)nodes[k].port);

        if (nodes[k].slot == ctx->memb.self_slot) {
            fprintf(fp,
                    "<h2>Estadisticas del nodo local</h2>\n"
                    "<table>\n"
                    "<tr><th>Metrica</th><th>Valor</th></tr>\n"
                    "<tr><td>Fallos de lectura</td><td>%llu</td></tr>\n"
                    "<tr><td>Fallos de escritura</td><td>%llu</td></tr>\n"
                    "<tr><td>Mensajes enviados</td><td>%llu</td></tr>\n"
                    "<tr><td>Mensajes recibidos</td><td>%llu</td></tr>\n"
                    "<tr><td>Bytes enviados</td><td>%llu</td></tr>\n"
                    "<tr><td>Bytes recibidos</td><td>%llu</td></tr>\n"
                    "<tr><td>Particiones ejecutadas</td><td>%llu</td></tr>\n"
                    "<tr><td>Particiones fallidas</td><td>%llu</td></tr>\n"
                    "</table>\n",
                    (unsigned long long)st.read_faults,
                    (unsigned long long)st.write_faults,
                    (unsigned long long)st.msgs_sent,
                    (unsigned long long)st.msgs_recv,
                    (unsigned long long)st.bytes_sent,
                    (unsigned long long)st.bytes_recv,
                    (unsigned long long)st.dist_tasks_run,
                    (unsigned long long)st.dist_tasks_failed);
        }

        fprintf(fp,
                "<h2>Tabla de paginas</h2>\n"
                "<table>\n<tr><th>Pagina</th><th>Estado</th>"
                "<th>Propietario</th><th>Proceso</th><th>Orden</th>"
                "<th>Copyset</th><th>Franja</th></tr>\n");
    }

    report_run_t *runs = calloc(REPORT_MAX_RUNS, sizeof(report_run_t));
    if (!runs) {
        result = DSM_ERR_NO_SPACE;
        goto cleanup;
    }
    uint32_t nruns = report_collect_runs(ctx, runs, REPORT_MAX_RUNS,
                                         REPORT_MAX_ROWS);

    uint32_t chunk = REPORT_CHUNK_PAGES;
    size_t entries = (size_t)chunk * (nnodes ? nnodes : 1u);
    uint8_t *states = calloc(entries, 1u);
    uint32_t *pids = calloc(entries, sizeof(uint32_t));
    uint32_t *orders = calloc(entries, sizeof(uint32_t));
    page_rec_t *recs = calloc(chunk, sizeof(page_rec_t));
    if (!states || !pids || !orders || !recs) {
        free(runs);
        free(states);
        free(pids);
        free(orders);
        free(recs);
        result = DSM_ERR_NO_SPACE;
        goto cleanup;
    }

    uint32_t materialized = 0;
    uint32_t row_budget = REPORT_MAX_ROWS;

    for (uint32_t r = 0; r < nruns && row_budget > 0; r++) {
        uint32_t run_done = 0;
        while (run_done < runs[r].npages && row_budget > 0) {
            uint32_t n = runs[r].npages - run_done;
            if (n > chunk) n = chunk;
            if (n > row_budget) n = row_budget;

            uint32_t first = runs[r].first_page + run_done;

            memset(states, 0, (size_t)n * (nnodes ? nnodes : 1u));
            memset(pids, 0, (size_t)n * (nnodes ? nnodes : 1u) * sizeof(uint32_t));
            memset(orders, 0, (size_t)n * (nnodes ? nnodes : 1u) * sizeof(uint32_t));
            for (uint32_t k = 0; k < nnodes; k++) {
                uint32_t got = 0;
                int frc = report_fetch_node(ctx, &nodes[k], first, n,
                                            recs, &got);
                if (frc != DSM_OK) {
                    free(runs);
                    free(states);
                    free(pids);
                    free(orders);
                    free(recs);
                    result = (dsm_status_t)frc;
                    goto cleanup;
                }
                for (uint32_t i = 0; i < got; i++) {
                    size_t idx = (size_t)k * n + i;
                    states[idx] = recs[i].state;
                    pids[idx]   = recs[i].pid;
                    orders[idx] = recs[i].order;
                }
            }

            for (uint32_t i = 0; i < n; i++) {
                uint32_t page = first + i;
                uint8_t copyset[MAX_NODES / 8];
                memset(copyset, 0, sizeof(copyset));

                uint32_t pid = 0;
                uint32_t order = 0;
                for (uint32_t k = 0; k < nnodes; k++) {
                    size_t idx = (size_t)k * n + i;
                    uint8_t s = states[idx];
                    if (s == DSM_PAGE_S || s == DSM_PAGE_M) {
                        uint32_t slot = nodes[k].slot;
                        copyset[slot / 8] |= (uint8_t)(1u << (slot % 8));
                        if (pid == 0 && pids[idx] != 0) {
                            pid   = pids[idx];
                            order = orders[idx];
                        }
                    }
                }

                char cps[8 * MAX_NODES + 4];
                char pidbuf[16];
                report_format_copyset(copyset, cps, sizeof(cps));

                if (cps[0] != '-') {
                    materialized++;
                    fprintf(global,
                            "<tr><td>%u</td><td>%u</td><td>%s</td><td>%u</td>"
                            "<td>%s</td><td>%u</td></tr>\n",
                            page, ring_home_of(ctx, page),
                            report_pid_text(pid, pidbuf, sizeof(pidbuf)),
                            order, cps, report_stripe(ctx, page));
                }

                for (uint32_t k = 0; k < nfiles; k++) {
                    size_t idx = (size_t)k * n + i;
                    uint8_t s = states[idx];
                    if (s == DSM_PAGE_I) continue;
                    fprintf(files[k].fp,
                            "<tr><td>%u</td><td>%s</td><td>%u</td>"
                            "<td>%s</td><td>%u</td><td>%s</td><td>%u</td></tr>\n",
                            page, report_state_name(s),
                            ring_home_of(ctx, page),
                            report_pid_text(pids[idx], pidbuf, sizeof(pidbuf)),
                            orders[idx], cps, report_stripe(ctx, page));
                }
            }

            run_done += n;
            row_budget -= n;
        }
    }

    free(runs);
    free(states);
    free(pids);
    free(orders);
    free(recs);

    if (materialized == 0) {
        fprintf(global,
                "<tr><td colspan=\"6\">No hay paginas materializadas en la "
                "vista.</td></tr>\n");
    }

cleanup:
    if (global) {
        fprintf(global, "</table>\n");
        report_html_tail(global);
        if (fclose(global) != 0 && result == DSM_OK) {
            result = DSM_ERR_NO_SPACE;
        }
    }
    for (uint32_t k = 0; k < nfiles; k++) {
        if (!files[k].fp) continue;
        fprintf(files[k].fp, "</table>\n");
        report_html_tail(files[k].fp);
        if (fclose(files[k].fp) != 0 && result == DSM_OK) {
            result = DSM_ERR_NO_SPACE;
        }
    }
    return result;
}
