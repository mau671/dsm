#include "dsm.h"
#include "dsm_internal.h"
#include "sigsegv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/resource.h>

static _Atomic(dsm_ctx_t *) g_ctxp = NULL;

dsm_ctx_t *dsm_ctx_get(void)
{
    return atomic_load(&g_ctxp);
}

bool dsm_is_initialized(void)
{
    return (atomic_load(&g_ctxp) != NULL);
}

void dsm_ctx_reset(void)
{
    atomic_store(&g_ctxp, NULL);
}

/* El mapa global se espera con un plazo medido sobre CLOCK_MONOTONIC. El
 * condvar debe usar el mismo reloj, de lo contrario el deadline monotónico se
 * interpreta como tiempo real y la espera vence de inmediato. */
static int alloc_sync_cv_init(dsm_ctx_t *ctx)
{
    pthread_condattr_t attr;
    int rc = pthread_condattr_init(&attr);
    if (rc != 0) return rc;

    rc = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    if (rc == 0) {
        rc = pthread_cond_init(&ctx->alloc.sync_cv, &attr);
    }
    pthread_condattr_destroy(&attr);
    return rc;
}

#if defined(DSM_TEST_HOOKS)
dsm_ctx_t *dsm_ctx_create_test(void)
{
    dsm_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (ctx) {
        if (membership_init(ctx) != DSM_OK) {
            free(ctx);
            return NULL;
        }
        net_init(ctx);
        sync_init(ctx);
        if (dist_init(ctx) != DSM_OK) {
            sync_destroy(ctx);
            net_destroy(ctx);
            membership_destroy(ctx);
            free(ctx);
            return NULL;
        }
        if (pthread_mutex_init(&ctx->alloc.mu, NULL) != 0) {
            dist_destroy(ctx);
            sync_destroy(ctx);
            net_destroy(ctx);
            membership_destroy(ctx);
            free(ctx);
            return NULL;
        }
        if (alloc_sync_cv_init(ctx) != 0) {
            pthread_mutex_destroy(&ctx->alloc.mu);
            dist_destroy(ctx);
            sync_destroy(ctx);
            net_destroy(ctx);
            membership_destroy(ctx);
            free(ctx);
            return NULL;
        }
    }
    return ctx;
}

void dsm_ctx_install_test(dsm_ctx_t *ctx)
{
    atomic_store(&g_ctxp, ctx);
}
#endif

static void raise_nofile_limit(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

dsm_status_t dsm_init(const char *conf_path, uint64_t node_mem_override)
{
    if (dsm_is_initialized()) {
        DSM_LOG_WARN("dsm_init: el subsistema ya se encuentra inicializado");
        return DSM_OK;
    }

    dsm_log_init_from_env();

    dsm_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        return DSM_ERR_MEM;
    }

    /* 1. Cargar configuracion */
    int rc = config_load(&ctx->cfg, conf_path);
    if (rc != DSM_OK) {
        free(ctx);
        return rc;
    }

    if (node_mem_override > 0) {
        ctx->cfg.node_mem = node_mem_override;
        rc = config_validate(&ctx->cfg);
        if (rc != DSM_OK) {
            free(ctx);
            return rc;
        }
    }

    config_dump(&ctx->cfg);

    /* 2. Ignorar SIGPIPE para evitar terminacion por desconexiones TCP */
    signal(SIGPIPE, SIG_IGN);

    /* 3. Elevar límite de descriptores de archivo */
    raise_nofile_limit();

    atomic_store(&ctx->running, true);

    /* 4. Inicializar cola de fallos y subsistema de señales */
    if (faultq_init() != 0) {
        free(ctx);
        return DSM_ERR_MEM;
    }

    /* 5. Inicializar estructuras internas */
    if (membership_init(ctx) != DSM_OK) {
        faultq_destroy();
        free(ctx);
        return DSM_ERR_MEM;
    }
    net_init(ctx);
    if (pthread_mutex_init(&ctx->alloc.mu, NULL) != 0) {
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return DSM_ERR_MEM;
    }
    if (alloc_sync_cv_init(ctx) != 0) {
        pthread_mutex_destroy(&ctx->alloc.mu);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return DSM_ERR_MEM;
    }
    rc = msi_init(ctx);
    if (rc != DSM_OK) {
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }
    sync_init(ctx);

    rc = dist_init(ctx);
    if (rc != DSM_OK) {
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }

    /* 6. Inicializar memoria virtual y manejador de fallos */
    rc = dsm_mem_init(&ctx->mem, &ctx->cfg);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al inicializar subsistema de memoria virtual: %s", dsm_strerror(rc));
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }
    rc = raid_init(ctx);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al inicializar el subsistema de redundancia RAID: %s", dsm_strerror(rc));
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }

    rc = dsm_sig_install();
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al registrar manejador de señales SIGSEGV");
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }

    /* 7. Iniciar workers y despachadores de fallos */
    rc = worker_start(ctx);
    if (rc != DSM_OK) {
        /* worker_start ya detuvo los hilos que llegó a crear: el apagado
         * sigue el orden de dsm_finalize, sin listener ni reconstrucción
         * pendiente que necesiten hilos vivos. */
        dsm_sig_restore();
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        faultq_destroy();
        free(ctx);
        return rc;
    }
    rc = dispatcher_start(ctx);
    if (rc != DSM_OK) {
        /* Como en dsm_finalize: la reconstrucción pendiente se une primero,
         * con workers y listener aún vivos, y la red se corta antes de
         * liberar las estructuras que sus hilos aún usan. */
        raid_shutdown(ctx);
        atomic_store(&ctx->running, false);
        msi_abort_waits(ctx);
        worker_stop(ctx);
        dist_destroy(ctx);
        net_stop_listener(ctx);
        net_destroy(ctx);
        dsm_sig_restore();
        dsm_mem_destroy(&ctx->mem);
        msi_destroy(ctx);
        faultq_destroy();
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    /* 8. Descubrimiento y adhesion al cluster */
    rc = discovery_join(ctx);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo durante el proceso de descubrimiento y unión al clúster: %s", dsm_strerror(rc));
        /* discovery_join ya creo el hilo listener, que itera mientras
         * ctx->running sea true. El apagado sigue el orden de dsm_finalize:
         * primero se une la reconstrucción pendiente —necesita workers y
         * listener vivos—, luego se detienen despachadores y workers, y la
         * red se corta antes de destruir memoria, directorio y distribuidor,
         * cuyos semáforos siguen en uso hasta ese punto. */
        raid_shutdown(ctx);
        atomic_store(&ctx->running, false);
        msi_abort_waits(ctx);
        dispatcher_stop(ctx);
        worker_stop(ctx);
        dist_destroy(ctx);
        net_stop_listener(ctx);
        net_destroy(ctx);
        dsm_sig_restore();
        dsm_mem_destroy(&ctx->mem);
        msi_destroy(ctx);
        faultq_destroy();
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    atomic_store(&g_ctxp, ctx);
    DSM_LOG_INFO("Sistema DSM inicializado exitosamente. Nodo %u, clúster 0x%08X.",
                 ctx->memb.self_slot, ctx->cfg.cluster_id);
    return DSM_OK;
}

dsm_status_t dsm_finalize(void)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) {
        return DSM_ERR_NOT_INIT;
    }

    DSM_LOG_INFO("Finalizando subsistema DSM en nodo %u", ctx->memb.self_slot);

    /* 1. Unir el hilo de reconstruccion antes de cortar la red: puede estar
     * esperando STRIPE_DATA y necesita workers y listener todavia vivos. */
    raid_shutdown(ctx);

    /* 2. Detener ciclo de ejecucion primero para que hb_thread pare */
    atomic_store(&ctx->running, false);
    msi_abort_waits(ctx);

    /* 3. Anunciar salida al cluster y detener hilo de heartbeat */
    discovery_leave(ctx);

    /* 4. Detener despachadores y workers */
    dispatcher_stop(ctx);
    worker_stop(ctx);
    dist_destroy(ctx);

    /* 5. Detener red */
    net_stop_listener(ctx);
    net_destroy(ctx);

    /* 6. Restaurar señales y liberar memoria */
    dsm_sig_restore();
    dsm_mem_destroy(&ctx->mem);

    /* 6. Destruir modulos restantes */
    msi_destroy(ctx);
    sync_destroy(ctx);
    ring_free(&ctx->ring);
    membership_destroy(ctx);
    pthread_cond_destroy(&ctx->alloc.sync_cv);
    pthread_mutex_destroy(&ctx->alloc.mu);

    /* La cola de fallos se destruye al final: el manejador de SIGSEGV ya se
     * restauró y los despachadores se unieron antes, así que ningún hilo
     * puede hacer sem_post o sem_timedwait sobre el semáforo liberado. */
    faultq_destroy();

    atomic_store(&g_ctxp, NULL);
    free(ctx);

    DSM_LOG_INFO("Subsistema DSM finalizado.");
    return DSM_OK;
}

/* --- Hilos de usuario --- */

dsm_status_t dsm_thread_attach(void)
{
    if (!dsm_is_initialized()) return DSM_ERR_NOT_INIT;
    return dsm_sig_thread_attach();
}

dsm_status_t dsm_thread_detach(void)
{
    if (!dsm_is_initialized()) return DSM_ERR_NOT_INIT;
    return dsm_sig_thread_detach();
}

/* --- Gestión de memoria compartida --- */

/* Paginas que ocupa un bloque de size bytes redondeado al alza. */
static uint32_t alloc_pages_for(uint64_t size, uint32_t page_size)
{
    return (uint32_t)((size + page_size - 1) / page_size);
}

/* Capacidad real del espacio compartido: la suma de las paginas aportadas por
 * los miembros vivos. VIEW_SIZE solo acota el espacio de direcciones de la
 * vista; un rango mas alla de esta cota no tendria respaldo en ningun nodo. */
static uint32_t alloc_capacity_pages(dsm_ctx_t *ctx)
{
    return membership_total_pages(ctx);
}

/* Deja una pagina en cero, inaccesible y sin copia valida antes de devolverla
 * al espacio libre, de modo que la proxima reserva que la ocupe no observe el
 * contenido del bloque anterior. */
static void alloc_page_release(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    memset(dsm_mem_page_ptr(&ctx->mem, page), 0, ctx->mem.page_size);
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    (void)dsm_mem_backing_drop(&ctx->mem, page);
    ctx->mem.local[page].state = DSM_PAGE_I;
    ctx->mem.local[page].pid   = 0;
    ctx->mem.local[page].order = 0;
    dsm_mem_stripe_unlock(&ctx->mem, page);
}

/* Anota el proceso y el orden de cada pagina de un bloque recien reservado.
 * El estado MSI lo sigue gobernando el manejador de fallos; estos dos campos
 * solo describen a que reserva pertenece la pagina. */
static void alloc_tag_pages(dsm_ctx_t *ctx, uint32_t first, uint32_t npages)
{
    uint32_t pid = (uint32_t)getpid();

    for (uint32_t i = 0; i < npages; i++) {
        uint32_t page = first + i;
        dsm_mem_stripe_lock(&ctx->mem, page);
        ctx->mem.local[page].pid   = pid;
        ctx->mem.local[page].order = i;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    }
}

/* Une los huecos contiguos que hayan quedado tras una insercion, de modo que
 * dos liberaciones consecutivas se comporten como un solo rango libre. */
static void alloc_holes_normalize(dsm_alloc_t *a)
{
    uint32_t out = 0;

    for (uint32_t i = 0; i < a->nholes; i++) {
        uint32_t prev_end = 0;
        if (out > 0) {
            prev_end = a->holes[out - 1].first_page + a->holes[out - 1].npages;
        }
        if (out > 0 && prev_end == a->holes[i].first_page) {
            a->holes[out - 1].npages += a->holes[i].npages;
            continue;
        }
        a->holes[out++] = a->holes[i];
    }
    a->nholes = out;
}

/* Inserta un rango en la lista ordenada por first_page. El llamador comprueba
 * antes que quede capacidad para una entrada mas. */
static void alloc_hole_add(dsm_alloc_t *a, uint32_t first, uint32_t npages)
{
    uint32_t pos = a->nholes;
    while (pos > 0 && a->holes[pos - 1].first_page > first) {
        a->holes[pos] = a->holes[pos - 1];
        pos--;
    }
    a->holes[pos].first_page = first;
    a->holes[pos].npages     = npages;
    a->nholes++;

    alloc_holes_normalize(a);
}

/* Toma npages de la lista de huecos con first-fit. Devuelve la primera pagina
 * del rango o DSM_PAGE_ID_NONE si ningun hueco alcanza el tamaño pedido; si el
 * hueco es mayor, el resto queda como hueco de menor tamaño. */
static uint32_t alloc_hole_take(dsm_alloc_t *a, uint32_t npages)
{
    for (uint32_t i = 0; i < a->nholes; i++) {
        alloc_hole_t *h = &a->holes[i];
        if (h->npages < npages) continue;

        uint32_t first = h->first_page;
        if (h->npages == npages) {
            for (uint32_t j = i + 1; j < a->nholes; j++) {
                a->holes[j - 1] = a->holes[j];
            }
            a->nholes--;
        } else {
            h->first_page += npages;
            h->npages     -= npages;
        }
        return first;
    }
    return DSM_PAGE_ID_NONE;
}

/* Entrada libre de la pila de bloques o ALLOC_STACK_MAX si no hay ninguna.
 * Una entrada que dsm_free() está retirando conserva sus páginas hasta que la
 * liberación termina y no se entrega a otra reserva. */
static uint32_t alloc_stack_slot(dsm_alloc_t *a)
{
    for (uint32_t i = 0; i < a->depth; i++) {
        if (a->stack[i].npages == 0 && !a->stack[i].retiring) return i;
    }
    if (a->depth < ALLOC_STACK_MAX) return a->depth;
    return ALLOC_STACK_MAX;
}

/* Coordinador del espacio global; se define más abajo. */
static uint32_t alloc_coordinator_slot(dsm_ctx_t *ctx);

/* Quita de la lista de huecos el rango [first, first+npages). Un hueco que lo
 * contiene deja dos restos o desaparece. Se usa al confirmar una reserva en la
 * copia cliente del mapa, para que una liberación posterior no reutilice un
 * rango que otro nodo ya ocupa. */
static void alloc_hole_carve(dsm_alloc_t *a, uint32_t first, uint32_t npages)
{
    uint64_t target_end = (uint64_t)first + npages;
    uint32_t out = 0;

    for (uint32_t i = 0; i < a->nholes; i++) {
        uint64_t h_first = a->holes[i].first_page;
        uint64_t h_end   = h_first + a->holes[i].npages;
        if (h_end <= first || h_first >= target_end) {
            a->holes[out++] = a->holes[i];
            continue;
        }
        if (h_first < first) {
            a->holes[out].first_page = (uint32_t)h_first;
            a->holes[out].npages     = first - (uint32_t)h_first;
            out++;
        }
        if (h_end > target_end) {
            a->holes[out].first_page = (uint32_t)target_end;
            a->holes[out].npages     = (uint32_t)(h_end - target_end);
            out++;
        }
    }
    a->nholes = out;
    alloc_holes_normalize(a);
}

/* Confirma en la copia local del mapa que un rango quedó reservado. */
static void alloc_note_reserved(dsm_ctx_t *ctx, uint32_t first, uint32_t npages)
{
    pthread_mutex_lock(&ctx->alloc.mu);
    uint64_t end = (uint64_t)first + npages;
    if (end > ctx->alloc.next_page) ctx->alloc.next_page = (uint32_t)end;
    alloc_hole_carve(&ctx->alloc, first, npages);
    pthread_mutex_unlock(&ctx->alloc.mu);
}

/* Confirma en la copia local del mapa que un rango volvió a estar libre. */
static void alloc_note_freed(dsm_ctx_t *ctx, uint32_t first, uint32_t npages)
{
    pthread_mutex_lock(&ctx->alloc.mu);
    if (ctx->alloc.nholes < ALLOC_HOLE_MAX) {
        alloc_hole_add(&ctx->alloc, first, npages);
    }
    pthread_mutex_unlock(&ctx->alloc.mu);
}

/* Instantánea del mapa global para WELCOME. La marca de agua ocupa el primer
 * registro y los huecos los siguientes; los bloques vivos quedan implícitos
 * como el complemento de los huecos por debajo de la marca. */
uint32_t dsm_alloc_export(dsm_ctx_t *ctx, alloc_rec_t *recs, uint32_t cap)
{
    if (!ctx || !recs || cap == 0) return 0;

    uint32_t count = 0;
    pthread_mutex_lock(&ctx->alloc.mu);
    if (!ctx->alloc.map_valid) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return 0;
    }

    memset(recs, 0, (size_t)cap * sizeof(*recs));
    recs[count].first_page = ctx->alloc.next_page;
    recs[count].npages     = 0;
    recs[count].slot       = ctx->memb.self_slot;
    recs[count].alloc_id   = ctx->alloc.map_seq;
    recs[count].reserved   = DSM_ALLOC_REC_WATERMARK;
    count++;

    for (uint32_t i = 0; i < ctx->alloc.nholes && count < cap; i++) {
        recs[count].first_page = ctx->alloc.holes[i].first_page;
        recs[count].npages     = ctx->alloc.holes[i].npages;
        recs[count].slot       = ctx->memb.self_slot;
        recs[count].reserved   = DSM_ALLOC_REC_HOLE;
        count++;
    }
    pthread_mutex_unlock(&ctx->alloc.mu);
    return count;
}

/* Incorpora la instantánea de un coordinador. La marca de agua solo avanza y
 * los huecos se reemplazan por los de la instantánea vigente: reintroducir el
 * hueco de una vista atrasada permitiría repartir una página ya reservada. */
void dsm_alloc_import(dsm_ctx_t *ctx, const alloc_rec_t *recs, uint32_t count)
{
    if (!ctx || !recs || count == 0) return;

    pthread_mutex_lock(&ctx->alloc.mu);

    uint32_t origin = DSM_SLOT_NONE;
    uint32_t seq = 0;
    for (uint32_t i = 0; i < count; i++) {
        if (recs[i].reserved == DSM_ALLOC_REC_WATERMARK) {
            origin = recs[i].slot;
            seq    = recs[i].alloc_id;
            break;
        }
    }
    if (origin >= MAX_NODES || origin == ctx->memb.self_slot) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return;
    }

    if (ctx->alloc.map_valid && ctx->alloc.map_origin == origin &&
        (int32_t)(seq - ctx->alloc.map_seq) <= 0) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return; /* vista atrasada del mismo coordinador */
    }

    ctx->alloc.map_origin = origin;
    ctx->alloc.map_seq    = seq;
    ctx->alloc.map_valid  = true;
    ctx->alloc.nholes     = 0;

    for (uint32_t i = 0; i < count; i++) {
        if (recs[i].reserved == DSM_ALLOC_REC_WATERMARK) {
            if (recs[i].first_page > ctx->alloc.next_page) {
                ctx->alloc.next_page = recs[i].first_page;
            }
            continue;
        }
        if (recs[i].reserved != DSM_ALLOC_REC_HOLE) continue;
        if (recs[i].npages == 0) continue;
        uint64_t end = (uint64_t)recs[i].first_page + recs[i].npages;
        if (end > ctx->alloc.next_page) continue;
        if (ctx->alloc.nholes >= ALLOC_HOLE_MAX) break;
        alloc_hole_add(&ctx->alloc, recs[i].first_page, recs[i].npages);
    }
    pthread_mutex_unlock(&ctx->alloc.mu);
}

/* Difunde la instantánea del mapa a los pares vivos. El coordinador la envía
 * tras cada cambio para que el relevo no dependa de la vista que cada nodo
 * tenía en su JOIN. */
static void alloc_broadcast_map(dsm_ctx_t *ctx)
{
    if (ctx->memb.self_slot >= MAX_NODES ||
        alloc_coordinator_slot(ctx) != ctx->memb.self_slot) {
        return;
    }

    welcome_t w;
    memset(&w, 0, sizeof(w));
    w.epoch = atomic_load(&ctx->memb.epoch);
    memcpy(w.origin_uuid, ctx->memb.self_uuid, 16);
    w.alloc_count = dsm_alloc_export(ctx, w.allocs, 64u);
    if (w.alloc_count == 0) return;

    uint8_t *payload = malloc(DSM_WELCOME_WIRE_MAX);
    if (!payload) return;
    size_t len = dsm_pack_welcome(&w, payload, DSM_WELCOME_WIRE_MAX);
    if (len == 0) {
        free(payload);
        return;
    }

    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == ctx->memb.self_slot) continue;
        if (!membership_is_alive(ctx, s)) continue;

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_WELCOME;
        hdr.src_slot      = ctx->memb.self_slot;
        hdr.dst_slot      = s;
        hdr.epoch         = atomic_load(&ctx->memb.epoch);
        hdr.payload_len   = (uint32_t)len;
        (void)dsm_net_send(ctx, s, &hdr, payload);
    }
    free(payload);
}

/* WELCOME no solicitado: el coordinador replica su mapa global. */
void dsm_alloc_on_welcome(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    welcome_t w;
    if (!ctx || !m || !m->payload || m->hdr.payload_len == 0) return;
    if (dsm_unpack_welcome(m->payload, m->hdr.payload_len, &w) != DSM_OK) return;
    dsm_alloc_import(ctx, w.allocs, w.alloc_count);
}

/* --- Sincronización del mapa al asumir la coordinación --- */

#define ALLOC_MAP_SYNC_TIMEOUT_MS 500u

/* Un coordinador de relevo no puede confiar en la marca de agua de su último
 * WELCOME: entre esa vista y la baja pudieron repartirse páginas. Antes de
 * servir pregunta a los pares vivos y adopta la marca de agua mayor. Si algún
 * par no responde, no reparte: prefiere BUSY a entregar una página viva. */
static dsm_status_t alloc_takeover_sync(dsm_ctx_t *ctx)
{
    uint32_t peers[MAX_NODES];
    uint32_t npeers = 0;
    uint32_t self = ctx->memb.self_slot;

    if (self >= MAX_NODES) return DSM_ERR_NOT_INIT;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == self) continue;
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        peers[npeers++] = s;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    pthread_mutex_lock(&ctx->alloc.mu);
    if (ctx->alloc.map_synced) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_OK;
    }
    if (npeers != 0) {
        /* Los huecos de una vista anterior pueden haber sido entregados por el
         * coordinador caído. Se descartan; el máximo de marcas de agua impide
         * repetir una página viva y las liberaciones posteriores reconstruyen
         * la lista. */
        ctx->alloc.nholes = 0;
    }
    ctx->alloc.map_sync_pending = npeers;
    pthread_mutex_unlock(&ctx->alloc.mu);

    for (uint32_t i = 0; i < npeers; i++) {
        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_ALLOC_SYNC;
        hdr.src_slot      = self;
        hdr.dst_slot      = peers[i];
        hdr.epoch         = atomic_load(&ctx->memb.epoch);
        if (dsm_net_send(ctx, peers[i], &hdr, NULL) != DSM_OK) {
            pthread_mutex_lock(&ctx->alloc.mu);
            if (ctx->alloc.map_sync_pending > 0) {
                ctx->alloc.map_sync_pending--;
                if (ctx->alloc.map_sync_pending == 0) {
                    pthread_cond_broadcast(&ctx->alloc.sync_cv);
                }
            }
            pthread_mutex_unlock(&ctx->alloc.mu);
        }
    }

    uint64_t deadline = mono_ms() + ALLOC_MAP_SYNC_TIMEOUT_MS;
    pthread_mutex_lock(&ctx->alloc.mu);
    while (ctx->alloc.map_sync_pending != 0) {
        uint64_t now = mono_ms();
        if (now >= deadline) break;
        uint64_t remain_ms = deadline - now;
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        ts.tv_sec  += (time_t)(remain_ms / 1000u);
        ts.tv_nsec += (long)(remain_ms % 1000u) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        if (pthread_cond_timedwait(&ctx->alloc.sync_cv, &ctx->alloc.mu, &ts) != 0) {
            break;
        }
    }

    bool complete = (ctx->alloc.map_sync_pending == 0);
    if (complete) {
        ctx->alloc.map_synced = true;
        ctx->alloc.map_valid  = true;
        ctx->alloc.map_origin = self;
    }
    pthread_mutex_unlock(&ctx->alloc.mu);

    return complete ? DSM_OK : DSM_ERR_BUSY;
}

void dsm_alloc_on_sync_request(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.src_slot >= MAX_NODES) return;

    welcome_t w;
    memset(&w, 0, sizeof(w));
    w.epoch = atomic_load(&ctx->memb.epoch);
    memcpy(w.origin_uuid, ctx->memb.self_uuid, 16);
    w.alloc_count = dsm_alloc_export(ctx, w.allocs, 64u);

    uint8_t *payload = malloc(DSM_WELCOME_WIRE_MAX);
    if (!payload) return;
    size_t len = dsm_pack_welcome(&w, payload, DSM_WELCOME_WIRE_MAX);
    if (len == 0) {
        free(payload);
        return;
    }

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_ALLOC_STATE;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = m->hdr.src_slot;
    hdr.req_id        = m->hdr.req_id;
    hdr.epoch         = atomic_load(&ctx->memb.epoch);
    hdr.payload_len   = (uint32_t)len;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &hdr, payload);
    free(payload);
}

void dsm_alloc_on_sync_state(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    welcome_t w;
    if (!ctx || !m || !m->payload || m->hdr.payload_len == 0) return;
    if (dsm_unpack_welcome(m->payload, m->hdr.payload_len, &w) != DSM_OK) return;

    uint32_t watermark = 0;
    bool have = false;
    for (uint32_t i = 0; i < w.alloc_count; i++) {
        if (w.allocs[i].reserved != DSM_ALLOC_REC_WATERMARK) continue;
        if (!have || w.allocs[i].first_page > watermark) {
            watermark = w.allocs[i].first_page;
        }
        have = true;
    }

    pthread_mutex_lock(&ctx->alloc.mu);
    if (have) {
        if (watermark > ctx->alloc.next_page) {
            ctx->alloc.next_page = watermark;
        }
        ctx->alloc.map_valid = true;
    }
    if (ctx->alloc.map_sync_pending > 0) {
        ctx->alloc.map_sync_pending--;
        if (ctx->alloc.map_sync_pending == 0) {
            pthread_cond_broadcast(&ctx->alloc.sync_cv);
        }
    }
    pthread_mutex_unlock(&ctx->alloc.mu);
}

/* Ranura propia. Un contexto de prueba aislado debe fijarla antes de usar el
 * asignador; sin membresía no hay identidad y el mapa global no puede decidir
 * quién coordina. */
static dsm_status_t alloc_self_slot(dsm_ctx_t *ctx, uint32_t *out)
{
    if (!out) return DSM_ERR_INVALID;
    uint32_t slot = ctx->memb.self_slot;
    if (slot >= MAX_NODES) return DSM_ERR_NOT_INIT;
    *out = slot;
    return DSM_OK;
}

/* Coordinador del espacio global: el miembro vivo de menor ranura que aporta
 * memoria. Un cliente usa la vista pero no custodia páginas, así que pide su
 * rango al coordinador igual que cualquier otro nodo. La consulta toma el
 * cerrojo de la membresía en modo lectura; la tabla cambia con los latidos y
 * la unión de nodos, así que la decisión se repite en cada llamada. */
static uint32_t alloc_coordinator_slot(dsm_ctx_t *ctx)
{
    uint32_t best     = DSM_SLOT_NONE;
    uint32_t fallback = DSM_SLOT_NONE;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        if (ctx->memb.nodes[s].is_client || ctx->memb.nodes[s].mem_pages == 0) {
            if (fallback == DSM_SLOT_NONE) fallback = s;
            continue;
        }
        best = s;
        break;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (best == DSM_SLOT_NONE) {
        best = (fallback != DSM_SLOT_NONE) ? fallback : ctx->memb.self_slot;
    }
    return (best < MAX_NODES) ? best : DSM_SLOT_NONE;
}

/* Cantidad de miembros vivos. Un coordinador recién arrancado sin pares
 * puede inicializar su mapa vacío; uno que asume la coordinación tras una
 * baja debe esperar la instantánea del coordinador anterior. */
static uint32_t alloc_live_members(dsm_ctx_t *ctx)
{
    uint32_t count = 0;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        count++;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
    return count;
}

/* Registra un bloque vivo en la pila local. El llamador ya verificó que hay
 * una entrada libre con alloc_stack_slot(). */
static void alloc_block_register(dsm_ctx_t *ctx, uint32_t slot, uint32_t first,
                                 uint32_t npages)
{
    dsm_alloc_t *a = &ctx->alloc;

    a->alloc_id++;
    if (slot == a->depth) {
        a->depth++;
    }
    alloc_block_t *b = &a->stack[slot];
    b->first_page = first;
    b->npages     = npages;
    b->alloc_id   = a->alloc_id;
    b->pid        = (uint32_t)getpid();
    b->retiring   = false;
}

/* Envía una petición de espacio al coordinador y espera su respuesta. Un
 * fallo de transporte no se sustituye por una asignación local: el llamador
 * recibe DSM_ERR_NET. */
static dsm_status_t alloc_remote_call(dsm_ctx_t *ctx, uint32_t dst,
                                      uint16_t type, const alloc_msg_t *req,
                                      alloc_msg_t *resp)
{
    uint8_t tx[DSM_ALLOC_MSG_LEN];
    uint8_t rx[DSM_ALLOC_MSG_LEN];
    uint16_t hdr_status = DSM_ST_OK;
    uint32_t rx_len = sizeof(rx);

    dsm_pack_alloc_msg(req, tx);
    int rc = dsm_net_rpc(ctx, dst, type, 0, 0, 0, 0, tx, sizeof(tx),
                         &hdr_status, rx, &rx_len);
    if (rc != DSM_OK || rx_len != DSM_ALLOC_MSG_LEN) {
        return DSM_ERR_NET;
    }

    dsm_unpack_alloc_msg(rx, resp);
    if (hdr_status != DSM_ST_OK) return DSM_ERR_PROTO;
    if (resp->status == DSM_ST_NO_SPACE) return DSM_ERR_NO_SPACE;
    if (resp->status == DSM_ST_NOT_HOME) return DSM_ERR_AGAIN;
    if (resp->status == DSM_ST_BUSY) return DSM_ERR_BUSY;
    if (resp->status != DSM_ST_OK) return DSM_ERR_PROTO;
    return DSM_OK;
}

static dsm_status_t alloc_retire_pages(dsm_ctx_t *ctx, uint32_t first,
                                       uint32_t npages);

dsm_status_t dsm_alloc(uint64_t size, void **out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out || size == 0) return DSM_ERR_INVALID;
    if (ctx->cfg.page_size == 0 || ctx->mem.npages == 0) return DSM_ERR_INVALID;
    if (size > ctx->mem.view_size) return DSM_ERR_NO_SPACE;

    uint32_t npages = alloc_pages_for(size, ctx->cfg.page_size);
    if (npages == 0) return DSM_ERR_NO_SPACE;
    if (npages > alloc_capacity_pages(ctx)) return DSM_ERR_NO_SPACE;

    uint32_t self = 0;
    dsm_status_t src = alloc_self_slot(ctx, &self);
    if (src != DSM_OK) return src;
    uint32_t coord = alloc_coordinator_slot(ctx);
    if (coord == DSM_SLOT_NONE) return DSM_ERR_NOT_INIT;
    bool is_coord  = (coord == self);

    if (is_coord && !ctx->alloc.map_synced) {
        dsm_status_t sync_rc = alloc_takeover_sync(ctx);
        if (sync_rc != DSM_OK) return sync_rc;
    }

    uint32_t slot = 0;
    uint32_t first = DSM_PAGE_ID_NONE;

    if (is_coord) {
        pthread_mutex_lock(&ctx->alloc.mu);

        /* Un coordinador que tomó el relevo sin instantánea del anterior no
         * puede repartir espacio: cualquier rango que eligiera podría estar
         * vivo. Solo un nodo sin pares arranca con el mapa vacío. */
        if (!ctx->alloc.map_valid) {
            if (alloc_live_members(ctx) > 1) {
                pthread_mutex_unlock(&ctx->alloc.mu);
                return DSM_ERR_BUSY;
            }
            ctx->alloc.map_valid  = true;
            ctx->alloc.map_origin = self;
            ctx->alloc.map_seq    = 0;
        }

        ctx->alloc.coordinator      = true;
        ctx->alloc.coordinator_slot = self;
        ctx->alloc.map_origin       = self;

        slot = alloc_stack_slot(&ctx->alloc);
        if (slot == ALLOC_STACK_MAX) {
            pthread_mutex_unlock(&ctx->alloc.mu);
            return DSM_ERR_NO_SPACE;
        }

        /* La cota se vuelve a consultar bajo el mutex: un hueco creado cuando
         * habia mas nodos puede quedar fuera de la capacidad vigente. */
        uint32_t capacity = alloc_capacity_pages(ctx);
        first = alloc_hole_take(&ctx->alloc, npages);
        if (first != DSM_PAGE_ID_NONE &&
            (uint64_t)first + npages > capacity) {
            if (ctx->alloc.nholes < ALLOC_HOLE_MAX) {
                alloc_hole_add(&ctx->alloc, first, npages);
            }
            first = DSM_PAGE_ID_NONE;
        }
        if (first == DSM_PAGE_ID_NONE) {
            uint64_t end = (uint64_t)ctx->alloc.next_page + npages;
            if (end > ctx->mem.npages || end > capacity) {
                pthread_mutex_unlock(&ctx->alloc.mu);
                return DSM_ERR_NO_SPACE;
            }
            first = ctx->alloc.next_page;
            ctx->alloc.next_page += npages;
        }

        alloc_block_register(ctx, slot, first, npages);
        ctx->alloc.map_seq++;
        pthread_mutex_unlock(&ctx->alloc.mu);
        alloc_broadcast_map(ctx);
    } else {
        alloc_msg_t req;
        alloc_msg_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        req.npages     = npages;
        req.first_page = DSM_PAGE_ID_NONE;
        req.status     = DSM_ST_OK;
        req.pid        = (uint32_t)getpid();

        dsm_status_t rc = alloc_remote_call(ctx, coord, MSG_ALLOC_REQ,
                                            &req, &resp);
        if (rc == DSM_ERR_AGAIN && resp.first_page < MAX_NODES &&
            resp.first_page != coord) {
            /* El nodo consultado ya no coordina: nos indicó la ranura
             * vigente. Se reintenta una vez contra el coordinador actual. */
            uint32_t next_coord = resp.first_page;
            memset(&resp, 0, sizeof(resp));
            rc = alloc_remote_call(ctx, next_coord, MSG_ALLOC_REQ, &req, &resp);
            if (rc == DSM_OK) coord = next_coord;
        }
        if (rc != DSM_OK) return rc;

        /* La respuesta se valida antes de tocar el estado local: el rango
         * debe caber en la vista que mapea dsm_mem y en la memoria efectivamente
         * aportada al cluster, aunque el coordinador tenga una vista antigua. */
        if (resp.npages != npages || resp.first_page == DSM_PAGE_ID_NONE ||
            resp.first_page >= ctx->mem.npages ||
            (uint64_t)resp.first_page + npages > ctx->mem.npages) {
            return DSM_ERR_PROTO;
        }
        if ((uint64_t)resp.first_page + npages > alloc_capacity_pages(ctx)) {
            return DSM_ERR_NO_SPACE;
        }
        first = resp.first_page;

        pthread_mutex_lock(&ctx->alloc.mu);
        ctx->alloc.coordinator      = false;
        ctx->alloc.coordinator_slot = coord;

        slot = alloc_stack_slot(&ctx->alloc);
        if (slot == ALLOC_STACK_MAX) {
            pthread_mutex_unlock(&ctx->alloc.mu);
            /* Rollback: el rango ya está reservado en el mapa global y no
             * puede quedar huérfano por un fallo local de capacidad. */
            alloc_msg_t undo;
            alloc_msg_t undone;
            memset(&undo, 0, sizeof(undo));
            memset(&undone, 0, sizeof(undone));
            undo.npages     = npages;
            undo.first_page = first;
            undo.status     = DSM_ST_OK;
            undo.pid        = (uint32_t)getpid();
            dsm_status_t undo_rc = alloc_remote_call(ctx, coord, MSG_FREE_REQ,
                                                     &undo, &undone);
            if (undo_rc != DSM_OK) {
                DSM_LOG_ERR("No se pudo devolver al mapa global el rango %u+%u "
                            "tras fallar la tabla local de bloques",
                            first, npages);
                return DSM_ERR_NET;
            }
            return DSM_ERR_NO_SPACE;
        }

        alloc_block_register(ctx, slot, first, npages);
        /* Cota superior observada, útil si este nodo llega a coordinar. */
        if ((uint64_t)first + npages > ctx->alloc.next_page) {
            ctx->alloc.next_page = first + npages;
        }
        pthread_mutex_unlock(&ctx->alloc.mu);
        alloc_note_reserved(ctx, first, npages);
    }

    alloc_tag_pages(ctx, first, npages);

    *out = ctx->mem.view + ((uint64_t)first * ctx->cfg.page_size);
    return DSM_OK;
}

dsm_status_t dsm_free(void *ptr, uint64_t size)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!ptr || size == 0 || size > ctx->mem.view_size) return DSM_ERR_INVALID;
    if (ctx->cfg.page_size == 0 || ctx->mem.npages == 0) return DSM_ERR_INVALID;

    uintptr_t base = (uintptr_t)ctx->mem.view;
    uintptr_t addr = (uintptr_t)ptr;
    if (addr < base || addr - base >= ctx->mem.view_size) return DSM_ERR_INVALID;

    uint64_t off = (uint64_t)(addr - base);
    if (off % ctx->cfg.page_size != 0) return DSM_ERR_INVALID;

    uint32_t npages = alloc_pages_for(size, ctx->cfg.page_size);
    uint32_t first  = (uint32_t)(off / ctx->cfg.page_size);
    if ((uint64_t)first + npages > ctx->mem.npages) return DSM_ERR_INVALID;

    uint32_t self = 0;
    dsm_status_t src = alloc_self_slot(ctx, &self);
    if (src != DSM_OK) return src;
    uint32_t coord = alloc_coordinator_slot(ctx);
    if (coord == DSM_SLOT_NONE) return DSM_ERR_NOT_INIT;
    bool is_coord  = (coord == self);

    if (is_coord && !ctx->alloc.map_synced) {
        dsm_status_t sync_rc = alloc_takeover_sync(ctx);
        if (sync_rc != DSM_OK) return sync_rc;
    }

    pthread_mutex_lock(&ctx->alloc.mu);
    ctx->alloc.coordinator      = is_coord;
    ctx->alloc.coordinator_slot = coord;
    if (is_coord && ctx->alloc.map_valid) {
        ctx->alloc.map_origin = self;
    }

    alloc_block_t *blk = NULL;
    for (uint32_t i = 0; i < ctx->alloc.depth; i++) {
        if (ctx->alloc.stack[i].npages != 0 &&
            !ctx->alloc.stack[i].retiring &&
            ctx->alloc.stack[i].first_page == first) {
            blk = &ctx->alloc.stack[i];
            break;
        }
    }
    if (!blk || blk->npages != npages) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_ERR_INVALID;
    }

    /* La entrada se reserva bajo el mutex y se publica al final. El retiro de
     * las páginas y el RPC corren sin el mutex del mapa: msi_release_page toma
     * franjas y consulta la red, y home_txn_acquire puede esperar un turno. */
    blk->retiring = true;

    if (is_coord) {
        /* La capacidad se comprueba antes de tocar las paginas: una vez
         * marcadas como libres, la insercion del rango no puede fallar. */
        if (ctx->alloc.nholes >= ALLOC_HOLE_MAX) {
            blk->retiring = false;
            pthread_mutex_unlock(&ctx->alloc.mu);
            return DSM_ERR_NO_SPACE;
        }
        pthread_mutex_unlock(&ctx->alloc.mu);

        dsm_status_t rrc = alloc_retire_pages(ctx, first, npages);
        if (rrc != DSM_OK) {
            pthread_mutex_lock(&ctx->alloc.mu);
            blk->retiring = false;
            pthread_mutex_unlock(&ctx->alloc.mu);
            return rrc;
        }

        pthread_mutex_lock(&ctx->alloc.mu);
        blk->npages   = 0;
        blk->retiring = false;
        if (ctx->alloc.nholes < ALLOC_HOLE_MAX) {
            (void)alloc_hole_add(&ctx->alloc, first, npages);
        } else {
            DSM_LOG_WARN("El mapa global no admite más huecos; el rango %u+%u "
                         "queda retirado sin reutilización", first, npages);
        }
        ctx->alloc.map_seq++;
        pthread_mutex_unlock(&ctx->alloc.mu);
        alloc_broadcast_map(ctx);
    } else {
        /* El rango vuelve al mapa global del coordinador. Solo si él lo
         * acepta se libera la entrada local; si no responde, el bloque sigue
         * vivo y el llamador puede reintentar. */
        pthread_mutex_unlock(&ctx->alloc.mu);

        dsm_status_t rrc = alloc_retire_pages(ctx, first, npages);
        if (rrc != DSM_OK) {
            pthread_mutex_lock(&ctx->alloc.mu);
            blk->retiring = false;
            pthread_mutex_unlock(&ctx->alloc.mu);
            return rrc;
        }

        alloc_msg_t req;
        alloc_msg_t resp;
        memset(&req, 0, sizeof(req));
        memset(&resp, 0, sizeof(resp));
        req.npages     = npages;
        req.first_page = first;
        req.status     = DSM_ST_OK;
        req.pid        = (uint32_t)getpid();

        dsm_status_t rc = alloc_remote_call(ctx, coord, MSG_FREE_REQ,
                                            &req, &resp);
        if (rc == DSM_ERR_AGAIN && resp.first_page < MAX_NODES &&
            resp.first_page != coord) {
            uint32_t next_coord = resp.first_page;
            memset(&resp, 0, sizeof(resp));
            rc = alloc_remote_call(ctx, next_coord, MSG_FREE_REQ, &req, &resp);
        }
        if (rc != DSM_OK) {
            pthread_mutex_lock(&ctx->alloc.mu);
            blk->retiring = false;
            pthread_mutex_unlock(&ctx->alloc.mu);
            return rc;
        }

        pthread_mutex_lock(&ctx->alloc.mu);
        blk->npages   = 0;
        blk->retiring = false;
        pthread_mutex_unlock(&ctx->alloc.mu);
        alloc_note_freed(ctx, first, npages);
    }

    return DSM_OK;
}

uint32_t dsm_alloc_free_pages(dsm_ctx_t *ctx)
{
    if (!ctx || ctx->cfg.client_mode) return 0;

    /* La cuenta se hace sobre las páginas aportadas por los miembros vivos,
     * no sobre el tamaño de la vista: VIEW_SIZE solo acota el espacio de
     * direcciones y no todo él tiene respaldo en el clúster. */
    uint64_t total = alloc_capacity_pages(ctx);

    pthread_mutex_lock(&ctx->alloc.mu);
    uint64_t reserved = ctx->alloc.next_page;
    for (uint32_t i = 0; i < ctx->alloc.nholes; i++) {
        uint64_t n = ctx->alloc.holes[i].npages;
        reserved = (reserved > n) ? reserved - n : 0;
    }
    uint64_t free_pages = (total > reserved) ? total - reserved : 0;
    pthread_mutex_unlock(&ctx->alloc.mu);

    return (free_pages > UINT32_MAX) ? UINT32_MAX : (uint32_t)free_pages;
}

/* Retira cada página del protocolo de coherencia y después borra su contenido,
 * la marca Invalid y le quita permisos. El retiro se completa antes de tocar
 * el estado local: si una página no puede liberarse, el bloque queda intacto y
 * dsm_free puede reintentarse. */
static dsm_status_t alloc_retire_pages(dsm_ctx_t *ctx, uint32_t first,
                                       uint32_t npages)
{
    for (uint32_t p = first; p < first + npages; p++) {
        int rc = msi_release_page(ctx, p);
        if (rc != DSM_OK) return (dsm_status_t)rc;
    }
    for (uint32_t p = first; p < first + npages; p++) {
        alloc_page_release(ctx, p);
    }
    return DSM_OK;
}

/* --- Atención del coordinador para el espacio global --- */

/* Responde sobre el mismo req_id de la petición: el emisor espera la trama en
 * su tabla de pendientes y descarta cualquier otra. */
static void alloc_send_response(dsm_ctx_t *ctx, const dsm_msg_t *req,
                                uint16_t type, const alloc_msg_t *resp)
{
    if (!req || req->hdr.src_slot >= MAX_NODES) return;

    uint8_t payload[DSM_ALLOC_MSG_LEN];
    dsm_pack_alloc_msg(resp, payload);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = type;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = req->hdr.src_slot;
    hdr.req_id        = req->hdr.req_id;
    hdr.epoch         = atomic_load(&ctx->memb.epoch);
    hdr.status        = DSM_ST_OK;
    hdr.payload_len   = DSM_ALLOC_MSG_LEN;

    (void)dsm_net_send(ctx, req->hdr.src_slot, &hdr, payload);
}

/* Un rango está completamente libre si queda cubierto por uno o varios huecos
 * contiguos. Sirve para que un FREE_REQ repetido por un reintento de
 * transporte se responda igual que el primero en lugar de duplicar el hueco. */
static bool alloc_range_is_free(const dsm_alloc_t *a, uint32_t first,
                                uint32_t npages)
{
    uint64_t cursor = first;
    uint64_t end    = (uint64_t)first + npages;

    for (uint32_t i = 0; i < a->nholes && cursor < end; i++) {
        uint64_t h_first = a->holes[i].first_page;
        uint64_t h_end   = h_first + a->holes[i].npages;
        if (h_first > cursor) return false;
        if (h_end > cursor) cursor = h_end;
    }
    return cursor >= end;
}

static bool alloc_serve_allowed(dsm_ctx_t *ctx, alloc_msg_t *out)
{
    uint32_t self  = ctx->memb.self_slot;
    uint32_t coord = alloc_coordinator_slot(ctx);

    if (self >= MAX_NODES || coord != self) {
        /* Otro nodo es el coordinador vigente. Se informa cuál para que el
         * solicitante descarte esta respuesta en lugar de reinterpretarla
         * como un rango válido. */
        out->status     = DSM_ST_NOT_HOME;
        out->first_page = (coord < MAX_NODES) ? coord : DSM_PAGE_ID_NONE;
        return false;
    }
    return true;
}

void dsm_alloc_on_request(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    alloc_msg_t out;
    memset(&out, 0, sizeof(out));
    out.first_page = DSM_PAGE_ID_NONE;
    out.status     = DSM_ST_BAD_STATE;

    if (m->payload && m->hdr.payload_len == DSM_ALLOC_MSG_LEN) {
        alloc_msg_t in;
        dsm_unpack_alloc_msg(m->payload, &in);
        out.npages = in.npages;
        out.pid    = in.pid;

        if (!alloc_serve_allowed(ctx, &out)) {
            alloc_send_response(ctx, m, MSG_ALLOC_RESP, &out);
            return;
        }
        if (!ctx->alloc.map_synced &&
            alloc_takeover_sync(ctx) != DSM_OK) {
            out.status     = DSM_ST_BUSY;
            out.first_page = ctx->memb.self_slot;
            alloc_send_response(ctx, m, MSG_ALLOC_RESP, &out);
            return;
        }

        uint32_t capacity = alloc_capacity_pages(ctx);
        if (in.npages == 0 || in.npages > ctx->mem.npages ||
            in.npages > capacity) {
            out.status = DSM_ST_NO_SPACE;
        } else {
            pthread_mutex_lock(&ctx->alloc.mu);
            ctx->alloc.coordinator      = true;
            ctx->alloc.coordinator_slot = ctx->memb.self_slot;
            ctx->alloc.map_origin       = ctx->memb.self_slot;

            uint32_t first = alloc_hole_take(&ctx->alloc, in.npages);
            if (first != DSM_PAGE_ID_NONE &&
                (uint64_t)first + in.npages > capacity) {
                if (ctx->alloc.nholes < ALLOC_HOLE_MAX) {
                    alloc_hole_add(&ctx->alloc, first, in.npages);
                }
                first = DSM_PAGE_ID_NONE;
            }
            uint64_t end = (uint64_t)ctx->alloc.next_page + in.npages;
            if (first == DSM_PAGE_ID_NONE && end <= ctx->mem.npages &&
                end <= capacity) {
                first = ctx->alloc.next_page;
                ctx->alloc.next_page += in.npages;
            }

            if (first == DSM_PAGE_ID_NONE) {
                out.status = DSM_ST_NO_SPACE;
            } else {
                out.first_page = first;
                out.status     = DSM_ST_OK;
                ctx->alloc.alloc_id++;
                ctx->alloc.map_seq++;
            }
            pthread_mutex_unlock(&ctx->alloc.mu);
            if (out.status == DSM_ST_OK) alloc_broadcast_map(ctx);
        }
    }

    alloc_send_response(ctx, m, MSG_ALLOC_RESP, &out);
}

void dsm_free_on_request(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    alloc_msg_t out;
    memset(&out, 0, sizeof(out));
    out.first_page = DSM_PAGE_ID_NONE;
    out.status     = DSM_ST_BAD_STATE;

    if (m->payload && m->hdr.payload_len == DSM_ALLOC_MSG_LEN) {
        alloc_msg_t in;
        dsm_unpack_alloc_msg(m->payload, &in);
        out.npages = in.npages;
        out.pid    = in.pid;

        if (!alloc_serve_allowed(ctx, &out)) {
            alloc_send_response(ctx, m, MSG_FREE_RESP, &out);
            return;
        }
        if (!ctx->alloc.map_synced &&
            alloc_takeover_sync(ctx) != DSM_OK) {
            out.status     = DSM_ST_BUSY;
            out.first_page = ctx->memb.self_slot;
            alloc_send_response(ctx, m, MSG_FREE_RESP, &out);
            return;
        }

        if (in.npages == 0 || in.first_page == DSM_PAGE_ID_NONE ||
            (uint64_t)in.first_page + in.npages > ctx->mem.npages) {
            out.status = DSM_ST_BAD_STATE;
        } else {
            pthread_mutex_lock(&ctx->alloc.mu);
            ctx->alloc.coordinator      = true;
            ctx->alloc.coordinator_slot = ctx->memb.self_slot;
            ctx->alloc.map_origin       = ctx->memb.self_slot;

            if (alloc_range_is_free(&ctx->alloc, in.first_page, in.npages)) {
                out.status = DSM_ST_OK;
            } else if (ctx->alloc.nholes >= ALLOC_HOLE_MAX) {
                out.status = DSM_ST_NO_SPACE;
            } else {
                /* Un rango liberado puede superar la marca de agua conocida
                 * tras un cambio de coordinador: se extiende para no volver a
                 * entregar esas páginas. */
                uint64_t end = (uint64_t)in.first_page + in.npages;
                if (end > ctx->alloc.next_page) {
                    ctx->alloc.next_page = (uint32_t)end;
                }
                /* Si el coordinador había reservado el rango para sí, su
                 * entrada de la pila también queda libre. */
                for (uint32_t i = 0; i < ctx->alloc.depth; i++) {
                    alloc_block_t *b = &ctx->alloc.stack[i];
                    if (!b->retiring && b->npages == in.npages &&
                        b->first_page == in.first_page) {
                        b->npages = 0;
                        break;
                    }
                }
                (void)alloc_hole_add(&ctx->alloc, in.first_page, in.npages);
                out.status = DSM_ST_OK;
                ctx->alloc.map_seq++;
            }
            pthread_mutex_unlock(&ctx->alloc.mu);
            if (out.status == DSM_ST_OK) alloc_broadcast_map(ctx);
        }
    }

    alloc_send_response(ctx, m, MSG_FREE_RESP, &out);
}

dsm_status_t dsm_sync(void)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    /*
     * Persistir todas las paginas que este nodo tenga en estado M. Un fallo
     * en cualquiera de ellas deja el respaldo del home desactualizado, asi que
     * se propaga al llamador en vez de reportar exito parcial.
     */
    dsm_status_t result = DSM_OK;
    for (uint32_t p = 0; p < ctx->mem.npages; p++) {
        page_local_t *loc = dsm_mem_local(&ctx->mem, p);
        if (!loc) continue;

        /* El estado se copia bajo el cerrojo de franja y la escritura al
         * respaldo corre sin él: msi_writeback_page toma sus propios cerrojos
         * y consulta la red, cosas que no deben ocurrir con la franja tomada. */
        dsm_mem_stripe_lock(&ctx->mem, p);
        uint8_t state = loc->state;
        dsm_mem_stripe_unlock(&ctx->mem, p);

        if (state == DSM_PAGE_M) {
            int rc = msi_writeback_page(ctx, p);
            if (rc != DSM_OK && result == DSM_OK) {
                result = (dsm_status_t)rc;
            }
        }
    }
    return result;
}

/* --- Sincronización distribuida --- */

dsm_status_t dsm_lock(uint32_t lock_id)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    return (dsm_status_t)sync_lock(ctx, lock_id);
}

dsm_status_t dsm_unlock(uint32_t lock_id)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    return (dsm_status_t)sync_unlock(ctx, lock_id);
}

dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    return (dsm_status_t)sync_barrier(ctx, barrier_id, n);
}

/* --- Consultas y diagnósticos --- */

dsm_status_t dsm_node_id(uint32_t *out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out) return DSM_ERR_INVALID;
    *out = ctx->memb.self_slot;
    return DSM_OK;
}

dsm_status_t dsm_node_count(uint32_t *out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out) return DSM_ERR_INVALID;

    uint32_t count = 0;
    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        /* Un cliente usa la memoria del cluster pero no la aporta: no cuenta
         * como nodo del cluster en las consultas. */
        if (ctx->memb.nodes[s].is_client || ctx->memb.nodes[s].mem_pages == 0) continue;
        count++;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    *out = count;
    return DSM_OK;
}

dsm_status_t dsm_wait_members(uint32_t min_nodes, uint32_t timeout_ms)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    uint64_t deadline = mono_ms() + timeout_ms;

    for (;;) {
        /* La marca se toma antes de contar: un aviso emitido entre la cuenta y
         * la espera cambia el contador y la espera no se bloquea. */
        uint64_t mark = membership_progress_mark(&ctx->memb);
        uint32_t count = 0;
        dsm_status_t rc = dsm_node_count(&count);
        if (rc != DSM_OK) return rc;
        if (count >= min_nodes) return DSM_OK;

        uint64_t now = mono_ms();
        if (now >= deadline) return DSM_ERR_TIMEOUT;
        membership_progress_wait(&ctx->memb, mark, deadline);
    }
}

dsm_status_t dsm_global_size(uint64_t *out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out) return DSM_ERR_INVALID;

    uint32_t total_pages = membership_total_pages(ctx);
    *out = (uint64_t)total_pages * ctx->cfg.page_size;
    return DSM_OK;
}

dsm_status_t dsm_stats(dsm_stats_t *out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out) return DSM_ERR_INVALID;

    memset(out, 0, sizeof(*out));
    out->read_faults            = atomic_load_explicit(&ctx->ctr.read_faults, memory_order_relaxed);
    out->write_faults           = atomic_load_explicit(&ctx->ctr.write_faults, memory_order_relaxed);
    out->msgs_sent              = atomic_load_explicit(&ctx->ctr.msgs_sent, memory_order_relaxed);
    out->msgs_recv              = atomic_load_explicit(&ctx->ctr.msgs_recv, memory_order_relaxed);
    out->bytes_sent             = atomic_load_explicit(&ctx->ctr.bytes_sent, memory_order_relaxed);
    out->bytes_recv             = atomic_load_explicit(&ctx->ctr.bytes_recv, memory_order_relaxed);
    out->invalidations          = atomic_load_explicit(&ctx->ctr.invalidations, memory_order_relaxed);
    out->fault_latency_us_total = atomic_load_explicit(&ctx->ctr.fault_latency_us_total, memory_order_relaxed);
    out->faults_serviced        = atomic_load_explicit(&ctx->ctr.faults_serviced, memory_order_relaxed);
    out->tseq_discarded         = atomic_load_explicit(&ctx->ctr.tseq_discarded, memory_order_relaxed);
    out->fault_latency_us_avg   = (out->faults_serviced == 0)
                                      ? 0
                                      : out->fault_latency_us_total / out->faults_serviced;
    out->pending_count          = atomic_load_explicit(&ctx->ctr.pending_count, memory_order_relaxed);
    out->wait_pool_used         = atomic_load_explicit(&ctx->ctr.wait_pool_used, memory_order_relaxed);
    out->slots_enqueued         = atomic_load_explicit(&ctx->ctr.slots_enqueued, memory_order_relaxed);
    out->slots_woken            = atomic_load_explicit(&ctx->ctr.slots_woken, memory_order_relaxed);
    out->parity_deltas          = atomic_load_explicit(&ctx->ctr.parity_deltas, memory_order_relaxed);
    out->parity_bytes           = atomic_load_explicit(&ctx->ctr.parity_bytes, memory_order_relaxed);
    out->stripe_rebuilds        = atomic_load_explicit(&ctx->ctr.stripe_rebuilds, memory_order_relaxed);
    out->parity_latency_us      = atomic_load_explicit(&ctx->ctr.parity_latency_us, memory_order_relaxed);
    out->dist_tasks_run         = atomic_load_explicit(&ctx->ctr.dist_tasks_run, memory_order_relaxed);
    out->dist_tasks_failed      = atomic_load_explicit(&ctx->ctr.dist_tasks_failed, memory_order_relaxed);
    out->hb_missed              = atomic_load_explicit(&ctx->ctr.hb_missed, memory_order_relaxed);

    msi_counters(ctx, &out->pages_m, &out->pages_s, &out->pages_i);
    return DSM_OK;
}

const char *dsm_strerror(dsm_status_t st)
{
    switch (st) {
    case DSM_OK:            return "Operación exitosa";
    case DSM_ERR_CONFIG:    return "Error en archivo o parámetros de configuración";
    case DSM_ERR_NOT_INIT:  return "El subsistema DSM no ha sido inicializado";
    case DSM_ERR_MEM:       return "Error en reserva de memoria virtual o local";
    case DSM_ERR_NET:       return "Error de enlace de red o socket TCP";
    case DSM_ERR_TIMEOUT:   return "Tiempo de espera agotado";
    case DSM_ERR_NO_SPACE:  return "Capacidad de memoria compartida o ranuras agotadas";
    case DSM_ERR_INVALID:   return "Parámetro o puntero inválido";
    case DSM_ERR_NODE_DOWN: return "Nodo custodio o par inaccesible";
    case DSM_ERR_BUSY:      return "Recurso transitoriamente bloqueado";
    case DSM_ERR_PROTO:     return "Error de protocolo, versión o corrupción CRC32";
    case DSM_ERR_INTR:      return "Llamada interrumpida por señal";
    case DSM_ERR_AGAIN:     return "Recurso no disponible de inmediato; reintentar";
    case DSM_ERR_NOT_FOUND: return "No se encontraron pares en el clúster";
    default:                return "Error desconocido";
    }
}

dsm_status_t dsm_debug_dump_fd(int fd)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    dprintf(fd, "Volcado de depuración DSM. Nodo %u, época %lu.\n",
            ctx->memb.self_slot, (unsigned long)atomic_load(&ctx->memb.epoch));
    dprintf(fd, "Vista base: %p, tamaño: %lu MiB\n",
            (void *)ctx->mem.view, (unsigned long)(ctx->mem.view_size >> 20));

    uint32_t active_nodes = 0;
    dsm_node_count(&active_nodes);
    dprintf(fd, "Nodos activos: %u\n", active_nodes);

    return DSM_OK;
}
