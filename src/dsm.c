#include "dsm.h"
#include "dsm_internal.h"
#include "sigsegv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
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

#if defined(DSM_TEST_HOOKS)
dsm_ctx_t *dsm_ctx_create_test(void)
{
    dsm_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (ctx) {
        membership_init(ctx);
        net_init(ctx);
        sync_init(ctx);
        if (dist_init(ctx) != DSM_OK) {
            net_destroy(ctx);
            membership_destroy(ctx);
            free(ctx);
            return NULL;
        }
        pthread_mutex_init(&ctx->alloc.mu, NULL);
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
    membership_init(ctx);
    net_init(ctx);
    pthread_mutex_init(&ctx->alloc.mu, NULL);
    rc = msi_init(ctx);
    if (rc != DSM_OK) {
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }
    sync_init(ctx);

    rc = dist_init(ctx);
    if (rc != DSM_OK) {
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    /* 6. Inicializar memoria virtual y manejador de fallos */
    rc = dsm_mem_init(&ctx->mem, &ctx->cfg);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al inicializar subsistema de memoria virtual: %d", rc);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }
    raid_init(ctx);

    rc = dsm_sig_install();
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al registrar manejador de señales SIGSEGV");
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    /* 7. Iniciar workers y despachadores de fallos */
    rc = worker_start(ctx);
    if (rc != DSM_OK) {
        dsm_sig_restore();
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }
    rc = dispatcher_start(ctx);
    if (rc != DSM_OK) {
        atomic_store(&ctx->running, false);
        worker_stop(ctx);
        dsm_sig_restore();
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    /* 8. Descubrimiento y adhesion al cluster */
    rc = discovery_join(ctx);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo durante el proceso de descubrimiento y union al cluster: %d", rc);
        /* discovery_join ya creo el hilo listener, que itera mientras
         * ctx->running sea true. Hay que apagarlo y unirlo antes de liberar
         * ctx, o el hilo sigue leyendo ctx->cfg (use-after-free). */
        atomic_store(&ctx->running, false);
        dispatcher_stop(ctx);
        worker_stop(ctx);
        dsm_sig_restore();
        raid_shutdown(ctx);
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        msi_destroy(ctx);
        net_stop_listener(ctx);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    atomic_store(&g_ctxp, ctx);
    DSM_LOG_INFO("Sistema DSM inicializado exitosamente (Nodo %u, Cluster 0x%08X)",
                 ctx->memb.self_slot, ctx->cfg.cluster_id);
    return DSM_OK;
}

dsm_status_t dsm_finalize(void)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) {
        return DSM_ERR_NOT_INIT;
    }

    DSM_LOG_INFO("Finalizando subsistema DSM en nodo %u...", ctx->memb.self_slot);

    /* 1. Unir el hilo de reconstruccion antes de cortar la red: puede estar
     * esperando STRIPE_DATA y necesita workers y listener todavia vivos. */
    raid_shutdown(ctx);

    /* 2. Detener ciclo de ejecucion primero para que hb_thread pare */
    atomic_store(&ctx->running, false);

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
    pthread_mutex_destroy(&ctx->alloc.mu);

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

/* Deja una pagina en cero, inaccesible y sin copia valida antes de devolverla
 * al espacio libre, de modo que la proxima reserva que la ocupe no observe el
 * contenido del bloque anterior. */
static void alloc_page_release(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    memset(dsm_mem_page_ptr(&ctx->mem, page), 0, ctx->mem.page_size);
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    dsm_mem_stripe_unlock(&ctx->mem, page);
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

/* Entrada libre de la pila de bloques o ALLOC_STACK_MAX si no hay ninguna. */
static uint32_t alloc_stack_slot(dsm_alloc_t *a)
{
    for (uint32_t i = 0; i < a->depth; i++) {
        if (a->stack[i].npages == 0) return i;
    }
    if (a->depth < ALLOC_STACK_MAX) return a->depth;
    return ALLOC_STACK_MAX;
}

dsm_status_t dsm_alloc(uint64_t size, void **out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out || size == 0) return DSM_ERR_INVALID;
    if (ctx->cfg.page_size == 0 || ctx->mem.npages == 0) return DSM_ERR_INVALID;
    if (size > ctx->mem.view_size) return DSM_ERR_NO_SPACE;

    uint32_t npages = alloc_pages_for(size, ctx->cfg.page_size);
    if (npages == 0) return DSM_ERR_NO_SPACE;

    pthread_mutex_lock(&ctx->alloc.mu);

    uint32_t slot = alloc_stack_slot(&ctx->alloc);
    if (slot == ALLOC_STACK_MAX) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_ERR_NO_SPACE;
    }

    uint32_t first = alloc_hole_take(&ctx->alloc, npages);
    if (first == DSM_PAGE_ID_NONE) {
        if ((uint64_t)ctx->alloc.next_page + npages > ctx->mem.npages) {
            pthread_mutex_unlock(&ctx->alloc.mu);
            return DSM_ERR_NO_SPACE;
        }
        first = ctx->alloc.next_page;
        ctx->alloc.next_page += npages;
    }

    ctx->alloc.alloc_id++;
    if (slot == ctx->alloc.depth) {
        ctx->alloc.depth++;
    }
    alloc_block_t *b = &ctx->alloc.stack[slot];
    b->first_page = first;
    b->npages     = npages;
    b->alloc_id   = ctx->alloc.alloc_id;

    pthread_mutex_unlock(&ctx->alloc.mu);

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

    pthread_mutex_lock(&ctx->alloc.mu);

    alloc_block_t *blk = NULL;
    for (uint32_t i = 0; i < ctx->alloc.depth; i++) {
        if (ctx->alloc.stack[i].npages != 0 &&
            ctx->alloc.stack[i].first_page == first) {
            blk = &ctx->alloc.stack[i];
            break;
        }
    }
    if (!blk || blk->npages != npages) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_ERR_INVALID;
    }

    /* La capacidad se comprueba antes de tocar las paginas: una vez marcadas
     * como libres, la insercion del rango no puede fallar. */
    if (ctx->alloc.nholes >= ALLOC_HOLE_MAX) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_ERR_NO_SPACE;
    }

    blk->npages = 0;
    for (uint32_t p = first; p < first + npages; p++) {
        alloc_page_release(ctx, p);
    }
    (void)alloc_hole_add(&ctx->alloc, first, npages);

    pthread_mutex_unlock(&ctx->alloc.mu);
    return DSM_OK;
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
        if (loc && loc->state == DSM_PAGE_M) {
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
    case DSM_OK:            return "Operacion exitosa";
    case DSM_ERR_CONFIG:    return "Error en archivo o parametros de configuracion";
    case DSM_ERR_NOT_INIT:  return "El subsistema DSM no ha sido inicializado";
    case DSM_ERR_MEM:       return "Error en reserva de memoria virtual o local";
    case DSM_ERR_NET:       return "Error de enlace de red o socket TCP";
    case DSM_ERR_TIMEOUT:   return "Tiempo de espera agotado";
    case DSM_ERR_NO_SPACE:  return "Capacidad de memoria compartida o ranuras agotadas";
    case DSM_ERR_INVALID:   return "Parametro o puntero invalido";
    case DSM_ERR_NODE_DOWN: return "Nodo custodio o par inaccesible";
    case DSM_ERR_BUSY:      return "Recurso transitoriamente bloqueado";
    case DSM_ERR_PROTO:     return "Error de protocolo, version o corrupcion CRC32";
    case DSM_ERR_INTR:      return "Llamada interrumpida por señal";
    case DSM_ERR_AGAIN:     return "Recurso no disponible de inmediato; reintentar";
    case DSM_ERR_NOT_FOUND: return "No se encontraron pares en el cluster";
    default:                return "Error desconocido";
    }
}

dsm_status_t dsm_debug_dump_fd(int fd)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    dprintf(fd, "=== DSM DEBUG DUMP (Nodo %u, Epoch %lu) ===\n",
            ctx->memb.self_slot, (unsigned long)ctx->memb.epoch);
    dprintf(fd, "Vista base: %p, Tamaño: %lu MB\n",
            (void *)ctx->mem.view, (unsigned long)(ctx->mem.view_size >> 20));

    uint32_t active_nodes = 0;
    dsm_node_count(&active_nodes);
    dprintf(fd, "Nodos activos: %u\n", active_nodes);

    return DSM_OK;
}
