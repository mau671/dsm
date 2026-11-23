#include "dist.h"
#include "dsm_internal.h"
#include "sigsegv.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* --- Utilidades internas ------------------------------------------------- */

static uint32_t dist_capacity(const dsm_ctx_t *ctx)
{
    uint32_t cap = ctx->cfg.wait_pool_entries;
    if (cap == 0) cap = DSM_DIST_DEFAULT_SLOTS;
    if (cap > DSM_DIST_PARTS_MAX) cap = DSM_DIST_PARTS_MAX;
    return cap;
}

static uint32_t dist_timeout(const dsm_ctx_t *ctx)
{
    return ctx->cfg.dist_timeout_ms ? ctx->cfg.dist_timeout_ms
                                    : 5000u;
}

static void dist_deadline(struct timespec *ts, uint32_t timeout_ms)
{
    /* pthread_cond_timedwait usa por defecto CLOCK_REALTIME, igual que el
     * resto de esperas del proyecto. */
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec  += (time_t)(timeout_ms / 1000u);
    ts->tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec++;
        ts->tv_nsec -= 1000000000L;
    }
}

static dist_kernel_slot_t *kernel_find_locked(dsm_dist_t *d, uint32_t kernel_id)
{
    for (uint32_t i = 0; i < DSM_DIST_KERNELS_MAX; i++) {
        if (d->kernels[i].used && d->kernels[i].kernel_id == kernel_id) {
            return &d->kernels[i];
        }
    }
    return NULL;
}

static dist_job_slot_t *job_find_locked(dsm_dist_t *d, uint32_t job_id)
{
    for (uint32_t i = 0; i < DSM_DIST_JOBS_MAX; i++) {
        if (d->jobs[i].active && d->jobs[i].job_id == job_id) {
            return &d->jobs[i];
        }
    }
    return NULL;
}

static void job_release_locked(dist_job_slot_t *j)
{
    free(j->results);
    free(j->part_slot);
    memset(j, 0, sizeof(*j));
}

/* Reserva una entrada de trabajo. Si no hay libres, recicla la mas antigua
 * que ya entrego todos sus resultados: su trabajo esta terminado y ninguna
 * llamada posterior puede necesitar la tabla. */
static dist_job_slot_t *job_alloc_locked(dsm_dist_t *d)
{
    for (uint32_t i = 0; i < DSM_DIST_JOBS_MAX; i++) {
        if (!d->jobs[i].active) {
            return &d->jobs[i];
        }
    }
    for (uint32_t i = 0; i < DSM_DIST_JOBS_MAX; i++) {
        dist_job_slot_t *j = &d->jobs[i];
        if (j->done && j->collected == j->received) {
            job_release_locked(j);
            return j;
        }
    }
    return NULL;
}

/* Despierta a los que esperan resultados de cualquier trabajo. */
static void dist_wake_all(dsm_dist_t *d)
{
    pthread_cond_broadcast(&d->cond);
}

/* --- Ciclo de vida ------------------------------------------------------- */

/* Kernel de demostración compilado en todos los nodos. Copia el rango de
 * entrada a la salida; un src_va cero indica relleno con ceros. */
static void dist_kernel_demo(void *arg, const dsm_dist_part_t *part,
                             dsm_dist_result_t *out)
{
    (void)arg;

    uint8_t *dst = (uint8_t *)(uintptr_t)part->dst_va;
    if (part->src_va != 0) {
        const uint8_t *src = (const uint8_t *)(uintptr_t)part->src_va;
        memmove(dst, src, (size_t)part->nbytes);
    } else {
        memset(dst, 0, (size_t)part->nbytes);
    }
    out->items = part->nbytes;
}

int dist_init(dsm_ctx_t *ctx)
{
    dsm_dist_t *d = &ctx->dist;
    memset(d, 0, sizeof(*d));

    if (pthread_mutex_init(&d->mu, NULL) != 0) {
        return DSM_ERR_MEM;
    }
    if (pthread_cond_init(&d->cond, NULL) != 0) {
        pthread_mutex_destroy(&d->mu);
        return DSM_ERR_MEM;
    }

    /* El registro es por proceso y no viaja por el cable. El kernel de
     * demostración queda disponible en cada nodo para que un DIST_RUN a un
     * daemon sin kernels de aplicación no se rechace por falta de registro. */
    d->kernels[0].used      = true;
    d->kernels[0].kernel_id = DSM_DIST_KERNEL_DEMO;
    d->kernels[0].fn        = dist_kernel_demo;
    d->kernels[0].arg       = NULL;

    return DSM_OK;
}

void dist_destroy(dsm_ctx_t *ctx)
{
    dsm_dist_t *d = &ctx->dist;

    pthread_mutex_lock(&d->mu);
    for (uint32_t i = 0; i < DSM_DIST_JOBS_MAX; i++) {
        job_release_locked(&d->jobs[i]);
    }
    pthread_mutex_unlock(&d->mu);

    pthread_cond_destroy(&d->cond);
    pthread_mutex_destroy(&d->mu);
}

/* --- Registro de kernels ------------------------------------------------- */

dsm_status_t dsm_dist_register(uint32_t kernel_id, dsm_dist_kernel_fn fn,
                               void *arg)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!fn) return DSM_ERR_INVALID;

    dsm_dist_t *d = &ctx->dist;
    pthread_mutex_lock(&d->mu);

    dist_kernel_slot_t *k = kernel_find_locked(d, kernel_id);
    if (k) {
        k->fn  = fn;
        k->arg = arg;
        pthread_mutex_unlock(&d->mu);
        return DSM_OK;
    }

    for (uint32_t i = 0; i < DSM_DIST_KERNELS_MAX; i++) {
        if (!d->kernels[i].used) {
            d->kernels[i].used      = true;
            d->kernels[i].kernel_id = kernel_id;
            d->kernels[i].fn        = fn;
            d->kernels[i].arg       = arg;
            pthread_mutex_unlock(&d->mu);
            return DSM_OK;
        }
    }

    pthread_mutex_unlock(&d->mu);
    return DSM_ERR_NO_SPACE;
}

/* --- Lanzamiento del trabajo -------------------------------------------- */

dsm_status_t dsm_dist_launch(uint32_t job_id, uint32_t kernel_id,
                             const dsm_dist_part_t *parts, uint32_t nparts,
                             const uint32_t *workers, uint32_t nworkers,
                             uint32_t timeout_ms)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;

    if (!parts || !workers) return DSM_ERR_INVALID;
    if (nparts == 0 || nworkers == 0 || timeout_ms == 0) {
        return DSM_ERR_INVALID;
    }

    uint32_t cap = dist_capacity(ctx);
    if (nparts > cap) return DSM_ERR_NO_SPACE;

    /* Una tabla de particiones con identificadores fuera de rango o repetidos
     * haria ambiguo el correlato de los resultados. */
    uint8_t *seen = calloc(nparts, sizeof(uint8_t));
    if (!seen) return DSM_ERR_NO_SPACE;
    for (uint32_t i = 0; i < nparts; i++) {
        uint32_t id = parts[i].id;
        if (id >= nparts || seen[id]) {
            free(seen);
            return DSM_ERR_INVALID;
        }
        seen[id] = 1;
    }
    free(seen);

    uint32_t self;
    pthread_rwlock_rdlock(&ctx->memb.rw);
    self = ctx->memb.self_slot;
    for (uint32_t i = 0; i < nworkers; i++) {
        uint32_t w = workers[i];
        if (w >= MAX_NODES || !membership_slot_present(ctx, w) ||
            ctx->memb.nodes[w].state == DSM_NODE_DEAD ||
            ctx->memb.nodes[w].is_client ||
            ctx->memb.nodes[w].mem_pages == 0) {
            pthread_rwlock_unlock(&ctx->memb.rw);
            return DSM_ERR_INVALID;
        }
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    dsm_dist_t *d = &ctx->dist;
    pthread_mutex_lock(&d->mu);

    if (!kernel_find_locked(d, kernel_id)) {
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_INVALID;
    }
    if (job_find_locked(d, job_id)) {
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_INVALID;
    }

    dist_job_slot_t *job = job_alloc_locked(d);
    if (!job) {
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_NO_SPACE;
    }

    job->results = calloc(nparts, sizeof(dsm_dist_result_t));
    if (!job->results) {
        job_release_locked(job);
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_NO_SPACE;
    }
    job->part_slot = calloc(nparts, sizeof(uint32_t));
    if (!job->part_slot) {
        job_release_locked(job);
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_NO_SPACE;
    }
    job->job_id      = job_id;
    job->kernel_id   = kernel_id;
    job->nparts      = nparts;
    job->nworkers    = nworkers;
    job->capacity    = nparts;
    d->next_launch_seq++;
    if (d->next_launch_seq == 0) d->next_launch_seq = 1u;
    job->launch_seq  = d->next_launch_seq;
    job->received    = 0;
    job->collected   = 0;
    job->active      = true;
    job->done        = false;
    d->parts_dispatched += nparts;

    /* Se registra que trabajador recibio cada particion: un resultado que
     * no venga de su ranura asignada no pertenece a este lanzamiento. */
    for (uint32_t i = 0; i < nparts; i++) {
        job->part_slot[parts[i].id] = workers[i % nworkers];
    }

    const uint32_t launch_seq = job->launch_seq;

    pthread_mutex_unlock(&d->mu);

    dsm_status_t result = DSM_OK;

    /* Primero se despachan las particiones remotas y al final las locales:
     * asi los nodos trabajadores arrancan en paralelo con el kernel que
     * ejecuta el coordinador, en vez de esperar a que este termine. */
    for (uint32_t pass = 0; pass < 2 && result == DSM_OK; pass++) {
        for (uint32_t i = 0; i < nparts; i++) {
            uint32_t dst = workers[i % nworkers];
            bool local = (dst == self);

            if ((pass == 0 && local) || (pass == 1 && !local)) continue;

            dist_run_wire_t rw;
            memset(&rw, 0, sizeof(rw));
            rw.src_va      = parts[i].src_va;
            rw.dst_va      = parts[i].dst_va;
            rw.nbytes      = parts[i].nbytes;
            rw.job         = job_id;
            rw.kernel      = kernel_id;
            rw.part_id     = parts[i].id;
            rw.nparts      = nparts;
            rw.deadline_ms = timeout_ms;
            rw.launch_seq  = launch_seq;

            uint8_t payload[DSM_DIST_RUN_WIRE_LEN];
            dsm_pack_dist_run(&rw, payload);

            msg_header_t hdr;
            memset(&hdr, 0, sizeof(hdr));
            hdr.magic         = DSM_MAGIC;
            hdr.cluster_id    = ctx->cfg.cluster_id;
            hdr.proto_version = DSM_PROTO_VERSION;
            hdr.type          = MSG_DIST_RUN;
            hdr.epoch         = atomic_load(&ctx->memb.epoch);
            hdr.src_slot      = self;
            hdr.dst_slot      = dst;
            hdr.req_id        = dsm_net_next_req(ctx);
            hdr.page_id       = parts[i].id;
            hdr.payload_len   = DSM_DIST_RUN_WIRE_LEN;

            if (local) {
                /* El coordinador tambien puede aportar trabajo: se ejecuta
                 * aqui mismo sin abrir una conexion TCP contra el listener
                 * propio. */
                dsm_msg_t lm = { .hdr = hdr, .payload = payload };
                dist_on_run(ctx, &lm);
            } else if (dsm_net_send(ctx, dst, &hdr, payload) != DSM_OK) {
                result = DSM_ERR_NET;
                break;
            }
        }
    }

    if (result != DSM_OK) {
        pthread_mutex_lock(&d->mu);
        dist_job_slot_t *failed = job_find_locked(d, job_id);
        if (failed) job_release_locked(failed);
        dist_wake_all(d);
        pthread_mutex_unlock(&d->mu);
    }

    return result;
}

/* --- Receptor en el nodo trabajador ------------------------------------- */

/* Comprueba que un rango pedido por un MSG_DIST_RUN caiga dentro de un
 * mapeo del memfd compartido. La vista y su alias proyectan la misma
 * memoria, asi que ambos son destinos legibles y escribibles para un
 * kernel; cualquier otra direccion es un intento de acceso arbitrario. */
static bool dist_range_inside(const dsm_ctx_t *ctx, uint64_t va,
                              uint64_t nbytes)
{
    if (nbytes == 0 || nbytes > (uint64_t)ctx->mem.view_size) return false;

    const uint64_t bases[2] = {
        (uint64_t)(uintptr_t)ctx->mem.view,
        (uint64_t)(uintptr_t)ctx->mem.alias,
    };
    for (uint32_t i = 0; i < 2; i++) {
        if (bases[i] == 0) continue;
        uint64_t base = bases[i];
        uint64_t end  = base + (uint64_t)ctx->mem.view_size;
        if (va < base || va >= end) continue;
        /* end - va no desborda porque va >= base y end acota la suma. */
        if (end - va < nbytes) continue;
        return true;
    }
    return false;
}

void dist_on_run(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_DIST_RUN_WIRE_LEN) {
        return;
    }

    dist_run_wire_t rw;
    dsm_unpack_dist_run((const uint8_t *)m->payload, &rw);

    dsm_dist_kernel_fn fn  = NULL;
    void              *arg = NULL;
    dsm_dist_t *d = &ctx->dist;

    pthread_mutex_lock(&d->mu);
    dist_kernel_slot_t *k = kernel_find_locked(d, rw.kernel);
    if (k) {
        fn  = k->fn;
        arg = k->arg;
    }
    pthread_mutex_unlock(&d->mu);

    dsm_dist_part_t part;
    memset(&part, 0, sizeof(part));
    part.id     = rw.part_id;
    part.src_va = rw.src_va;
    part.dst_va = rw.dst_va;
    part.nbytes = rw.nbytes;

    dsm_dist_result_t res;
    memset(&res, 0, sizeof(res));
    res.slot    = ctx->memb.self_slot;
    res.part_id = rw.part_id;

    uint64_t rf0 = atomic_load_explicit(&ctx->ctr.read_faults,
                                        memory_order_relaxed);
    uint64_t wf0 = atomic_load_explicit(&ctx->ctr.write_faults,
                                        memory_order_relaxed);

    /* Una direccion fuera de la vista provoca un fallo de segmentacion o
     * una lectura arbitraria: se rechaza la particion antes de tocar
     * memoria. Un src_va en cero indica un kernel que solo escribe su
     * salida. */
    bool src_ok = (rw.src_va == 0) || dist_range_inside(ctx, rw.src_va,
                                                        rw.nbytes);
    bool dst_ok = dist_range_inside(ctx, rw.dst_va, rw.nbytes);

    if (!fn) {
        /* El registro de kernels es por proceso: el coordinador no transporta
         * codigo. Un daemon solo ejecuta kernels compilados o registrados en
         * su propio arranque; DSM_DIST_KERNEL_DEMO esta siempre disponible. */
        res.status = (uint32_t)DSM_ERR_INVALID;
    } else if (!src_ok || !dst_ok) {
        DSM_LOG_WARN("DIST_RUN del trabajo %u con direcciones fuera de la "
                     "vista (part %u, dst 0x%llx, %llu bytes)",
                     rw.job, rw.part_id,
                     (unsigned long long)rw.dst_va,
                     (unsigned long long)rw.nbytes);
        res.status = (uint32_t)DSM_ERR_INVALID;
    } else {
        /* El kernel toca la vista DSM: el hilo worker necesita pila
         * alternativa para que un fallo de pagina lo atienda el despachador
         * sin bloquear el listener. */
        (void)dsm_sig_thread_attach();

        uint64_t t0 = mono_us();
        fn(arg, &part, &res);
        res.us = mono_us() - t0;

        /* Una particion fallida aporta CRC cero, como promete la
         * documentacion de dsm_dist_verify; el CRC resume el contenido
         * que el kernel dejo en la vista. */
        if (res.status != 0) {
            res.crc32 = 0;
        } else if (rw.dst_va != 0) {
            res.crc32 = dsm_crc32(0, (const void *)(uintptr_t)rw.dst_va,
                                  (size_t)rw.nbytes);
        }
    }

    uint64_t rf1 = atomic_load_explicit(&ctx->ctr.read_faults,
                                        memory_order_relaxed);
    uint64_t wf1 = atomic_load_explicit(&ctx->ctr.write_faults,
                                        memory_order_relaxed);
    uint64_t dfr = rf1 - rf0;
    uint64_t dfw = wf1 - wf0;
    res.faults_r = (dfr > UINT32_MAX) ? UINT32_MAX : (uint32_t)dfr;
    res.faults_w = (dfw > UINT32_MAX) ? UINT32_MAX : (uint32_t)dfw;

    if (res.status == 0) {
        atomic_fetch_add_explicit(&ctx->ctr.dist_tasks_run, 1,
                                  memory_order_relaxed);
    } else {
        atomic_fetch_add_explicit(&ctx->ctr.dist_tasks_failed, 1,
                                  memory_order_relaxed);
    }

    pthread_mutex_lock(&d->mu);
    d->parts_completed++;
    pthread_mutex_unlock(&d->mu);

    dist_done_wire_t dw;
    memset(&dw, 0, sizeof(dw));
    dw.job        = rw.job;
    dw.part_id    = rw.part_id;
    dw.status     = res.status;
    dw.items      = (uint32_t)res.items;
    dw.crc        = res.crc32;
    dw.us         = res.us;
    dw.launch_seq = rw.launch_seq;
    dw.faults_r   = res.faults_r;
    dw.faults_w   = res.faults_w;

    uint8_t payload[DSM_DIST_DONE_WIRE_LEN];
    dsm_pack_dist_done(&dw, payload);

    msg_header_t rh;
    memset(&rh, 0, sizeof(rh));
    rh.magic         = DSM_MAGIC;
    rh.cluster_id    = ctx->cfg.cluster_id;
    rh.proto_version = DSM_PROTO_VERSION;
    rh.type          = MSG_DIST_DONE;
    rh.epoch         = atomic_load(&ctx->memb.epoch);
    rh.src_slot      = ctx->memb.self_slot;
    rh.dst_slot      = m->hdr.src_slot;
    rh.req_id        = m->hdr.req_id;
    rh.page_id       = rw.part_id;
    rh.payload_len   = DSM_DIST_DONE_WIRE_LEN;

    if (m->hdr.src_slot == ctx->memb.self_slot) {
        dsm_msg_t local = { .hdr = rh, .payload = payload };
        dist_on_done(ctx, &local);
    } else if (dsm_net_send(ctx, m->hdr.src_slot, &rh, payload) != DSM_OK) {
        DSM_LOG_WARN("No se pudo enviar DIST_DONE del trabajo %u al nodo %u",
                     rw.job, m->hdr.src_slot);
        pthread_mutex_lock(&d->mu);
        d->parts_rejected++;
        pthread_mutex_unlock(&d->mu);
    }
}

/* --- Recepcion de resultados en el coordinador -------------------------- */

void dist_on_done(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || !m->payload ||
        m->hdr.payload_len != DSM_DIST_DONE_WIRE_LEN) {
        return;
    }

    dist_done_wire_t dw;
    dsm_unpack_dist_done((const uint8_t *)m->payload, &dw);

    dsm_dist_t *d = &ctx->dist;
    pthread_mutex_lock(&d->mu);

    dist_job_slot_t *job = job_find_locked(d, dw.job);
    if (!job || dw.part_id >= job->nparts || job->received >= job->capacity) {
        d->parts_rejected++;
        pthread_mutex_unlock(&d->mu);
        return;
    }

    /* El correlato del lanzamiento descarta resultados rezagados de un
     * trabajo anterior con el mismo identificador, y la ranura asignada
     * descarta a un nodo que contesta por una particion ajena. */
    if (dw.launch_seq != job->launch_seq ||
        m->hdr.src_slot >= MAX_NODES ||
        m->hdr.src_slot != job->part_slot[dw.part_id]) {
        d->parts_rejected++;
        pthread_mutex_unlock(&d->mu);
        return;
    }

    for (uint32_t i = 0; i < job->received; i++) {
        if (job->results[i].part_id == dw.part_id) {
            /* Un duplicado o un reenvio tardio no altera la cuenta. */
            d->parts_rejected++;
            pthread_mutex_unlock(&d->mu);
            return;
        }
    }

    dsm_dist_result_t *r = &job->results[job->received++];
    memset(r, 0, sizeof(*r));
    r->slot     = m->hdr.src_slot;
    r->part_id  = dw.part_id;
    r->status   = dw.status;
    r->items    = dw.items;
    r->crc32    = dw.crc;
    r->us       = dw.us;
    r->faults_r = dw.faults_r;
    r->faults_w = dw.faults_w;
    /* Una particion fallida aporta CRC cero al agregado del trabajo. */
    if (r->status != 0) {
        r->crc32 = 0;
    }

    if (job->received >= job->nparts) {
        job->done = true;
    }

    dist_wake_all(d);
    pthread_mutex_unlock(&d->mu);
}

/* --- Recogida y verificacion -------------------------------------------- */

dsm_status_t dsm_dist_collect(uint32_t job_id, dsm_dist_result_t *out,
                              uint32_t *n, uint32_t timeout_ms)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!out || !n) return DSM_ERR_INVALID;

    dsm_dist_t *d = &ctx->dist;
    pthread_mutex_lock(&d->mu);

    dist_job_slot_t *job = job_find_locked(d, job_id);
    if (!job) {
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_INVALID;
    }

    uint32_t capacity = *n;
    if (capacity == 0) {
        pthread_mutex_unlock(&d->mu);
        if (timeout_ms != 0) return DSM_ERR_INVALID;
        *n = 0;
        return DSM_OK;
    }

    if (timeout_ms > 0 && job->received == job->collected) {
        struct timespec deadline;
        dist_deadline(&deadline, timeout_ms);

        while (job->received == job->collected) {
            int rc = pthread_cond_timedwait(&d->cond, &d->mu, &deadline);
            /* La ranura puede reciclarse mientras se espera: se re-resuelve
             * el trabajo por identificador antes de volver a tocarlo. */
            job = job_find_locked(d, job_id);
            if (!job) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_INVALID;
            }
            if (rc == ETIMEDOUT) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_TIMEOUT;
            }
            if (rc != 0) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_INVALID;
            }
        }
    }

    uint32_t available = job->received - job->collected;
    uint32_t copied    = (available < capacity) ? available : capacity;
    for (uint32_t i = 0; i < copied; i++) {
        out[i] = job->results[job->collected + i];
    }
    job->collected += copied;
    *n = copied;

    pthread_mutex_unlock(&d->mu);
    return DSM_OK;
}

dsm_status_t dsm_dist_verify(uint32_t job_id, uint64_t expected_crc32,
                             bool *ok)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return DSM_ERR_NOT_INIT;
    if (!ok) return DSM_ERR_INVALID;

    dsm_dist_t *d = &ctx->dist;
    pthread_mutex_lock(&d->mu);

    dist_job_slot_t *job = job_find_locked(d, job_id);
    if (!job) {
        pthread_mutex_unlock(&d->mu);
        return DSM_ERR_INVALID;
    }

    if (!job->done) {
        struct timespec deadline;
        dist_deadline(&deadline, dist_timeout(ctx));

        while (!job->done) {
            int rc = pthread_cond_timedwait(&d->cond, &d->mu, &deadline);
            /* La ranura puede reciclarse mientras se espera: se re-resuelve
             * el trabajo por identificador antes de volver a tocarlo. */
            job = job_find_locked(d, job_id);
            if (!job) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_INVALID;
            }
            if (rc == ETIMEDOUT) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_TIMEOUT;
            }
            if (rc != 0) {
                pthread_mutex_unlock(&d->mu);
                return DSM_ERR_INVALID;
            }
        }
    }

    /* Agregado del trabajo: XOR de los CRC32 de cada particion. XOR es
     * conmutativo, asi que el valor no depende del orden de llegada. Una
     * particion fallida aporta cero. */
    uint64_t aggregate = 0;
    for (uint32_t i = 0; i < job->received; i++) {
        aggregate ^= job->results[i].crc32;
    }

    *ok = (aggregate == expected_crc32);
    pthread_mutex_unlock(&d->mu);
    return DSM_OK;
}
