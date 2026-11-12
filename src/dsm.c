#include "dsm.h"
#include "dsm_internal.h"
#include "sigsegv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
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
    }
    return ctx;
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
    msi_init(ctx);
    sync_init(ctx);

    /* 6. Inicializar memoria virtual y manejador de fallos */
    rc = dsm_mem_init(&ctx->mem, &ctx->cfg);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al inicializar subsistema de memoria virtual: %d", rc);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    rc = dsm_sig_install();
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo al registrar manejador de señales SIGSEGV");
        dsm_mem_destroy(&ctx->mem);
        net_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return rc;
    }

    /* 7. Iniciar workers y despachadores de fallos */
    worker_start(ctx);
    dispatcher_start(ctx);

    /* 8. Descubrimiento y adhesion al cluster */
    rc = discovery_join(ctx);
    if (rc != DSM_OK) {
        DSM_LOG_ERR("Fallo durante el proceso de descubrimiento y union al cluster: %d", rc);
        /* discovery_join ya creo el hilo listener, que itera mientras
         * ctx->running sea true. Hay que apagarlo y unirlo ANTES de liberar
         * ctx, o el hilo sigue leyendo ctx->cfg (use-after-free). */
        atomic_store(&ctx->running, false);
        dispatcher_stop(ctx);
        worker_stop(ctx);
        dsm_sig_restore();
        dsm_mem_destroy(&ctx->mem);
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

    /* 1. Detener ciclo de ejecucion primero para que hb_thread pare */
    atomic_store(&ctx->running, false);

    /* 2. Anunciar salida al cluster y detener hilo de heartbeat */
    discovery_leave(ctx);

    /* 3. Detener despachadores y workers */
    dispatcher_stop(ctx);
    worker_stop(ctx);

    /* 4. Detener red */
    net_stop_listener(ctx);
    net_destroy(ctx);

    /* 5. Restaurar señales y liberar memoria */
    dsm_sig_restore();
    dsm_mem_destroy(&ctx->mem);

    /* 6. Destruir modulos restantes */
    msi_destroy(ctx);
    sync_destroy(ctx);
    membership_destroy(ctx);

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

dsm_status_t dsm_alloc(uint64_t size, void **out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out || size == 0) return DSM_ERR_INVALID;

    uint32_t npages = (uint32_t)((size + ctx->cfg.page_size - 1) / ctx->cfg.page_size);

    pthread_mutex_lock(&ctx->alloc.mu);
    if (ctx->alloc.next_page + npages > ctx->mem.npages) {
        pthread_mutex_unlock(&ctx->alloc.mu);
        return DSM_ERR_NO_SPACE;
    }

    uint32_t first = ctx->alloc.next_page;
    ctx->alloc.next_page += npages;
    ctx->alloc.alloc_id++;

    if (ctx->alloc.depth < ALLOC_STACK_MAX) {
        alloc_block_t *b = &ctx->alloc.stack[ctx->alloc.depth++];
        b->first_page = first;
        b->npages     = npages;
        b->alloc_id   = ctx->alloc.alloc_id;
    }
    pthread_mutex_unlock(&ctx->alloc.mu);

    *out = ctx->mem.view + ((uint64_t)first * ctx->cfg.page_size);
    return DSM_OK;
}

dsm_status_t dsm_free(void *ptr, uint64_t size)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    (void)ptr;
    (void)size;
    return DSM_OK;
}

dsm_status_t dsm_sync(void)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    /* Persistir todas las páginas que este nodo tenga en estado M */
    for (uint32_t p = 0; p < ctx->mem.npages; p++) {
        page_local_t *loc = dsm_mem_local(&ctx->mem, p);
        if (loc && loc->state == DSM_PAGE_M) {
            msi_writeback_page(ctx, p);
        }
    }
    return DSM_OK;
}

/* --- Sincronización distribuida --- */

dsm_status_t dsm_lock(uint32_t lock_id)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    (void)lock_id;
    return DSM_OK;
}

dsm_status_t dsm_unlock(uint32_t lock_id)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    (void)lock_id;
    return DSM_OK;
}

dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    (void)barrier_id;
    (void)n;
    return DSM_OK;
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
        if (membership_slot_present(ctx, s) && ctx->memb.nodes[s].state != DSM_NODE_DEAD) {
            count++;
        }
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
    out->pending_count          = atomic_load_explicit(&ctx->ctr.pending_count, memory_order_relaxed);
    out->wait_pool_used         = atomic_load_explicit(&ctx->ctr.wait_pool_used, memory_order_relaxed);
    out->slots_enqueued         = atomic_load_explicit(&ctx->ctr.slots_enqueued, memory_order_relaxed);
    out->slots_woken            = atomic_load_explicit(&ctx->ctr.slots_woken, memory_order_relaxed);

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
