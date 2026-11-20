#include "worker.h"
#include "dsm_internal.h"
#include "sigsegv.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

/* La espera del hilo de aplicacion ocurre dentro del manejador de SIGSEGV, y
 * ahi solo se pueden usar funciones async-signal-safe. sem_wait no lo es, en
 * cambio futex_wait si: la ranura de fallo se bloquea con futex sobre done y
 * el despachador la despierta con futex_wake. */
static void fault_slot_wake(fault_slot_t *slot)
{
    __atomic_store_n(&slot->done, 1, __ATOMIC_RELEASE);
    syscall(SYS_futex, (int *)&slot->done, FUTEX_WAKE, 1, NULL, NULL, 0);
}

static void *dispatcher_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    uint32_t jitter_seed = (uint32_t)(uintptr_t)pthread_self();
    if (jitter_seed == 0) jitter_seed = 1;

    while (atomic_load(&ctx->running)) {
        fault_slot_t *slot = faultq_pop();
        if (!slot) {
            sleep_ms(1);
            continue;
        }

        /*
         * Localmente el fallo solo puede quedar pendiente si otro despachador
         * ya lo esta resolviendo; la ranura se libera cuando ese despachador
         * instala los permisos, asi que se reintenta hasta salir de PEND.
         * La latencia abarca la resolucion completa, reintentos incluidos.
         */
        uint64_t t0 = mono_us();
        int rc;
        do {
            rc = msi_local_fault(ctx, slot);
            if (rc == DSM_ERR_BUSY) {
                /* Limitación temporal: hasta que exista retry_at diferido,
                 * este despachador reintenta con jitter sin conservar locks.
                 * Cada hilo usa su propia semilla; no usa rand() global. */
                jitter_seed ^= jitter_seed << 13;
                jitter_seed ^= jitter_seed >> 17;
                jitter_seed ^= jitter_seed << 5;
                uint32_t us = 500u + (jitter_seed % 1501u);
                struct timespec ts;
                ts.tv_sec = 0;
                ts.tv_nsec = (long)us * 1000L;
                while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
                    /* EINTR conserva el tiempo restante en ts. */
                }
            }
        } while (rc == DSM_ERR_BUSY && atomic_load(&ctx->running));
        uint64_t elapsed = mono_us() - t0;

        atomic_fetch_add_explicit(&ctx->ctr.fault_latency_us_total, elapsed,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->ctr.faults_serviced, 1,
                                  memory_order_relaxed);

        fault_slot_wake(slot);
        atomic_fetch_add_explicit(&ctx->ctr.slots_woken, 1, memory_order_relaxed);
    }

    return NULL;
}

static void *worker_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    dsm_worker_t *w = &ctx->worker;

    while (atomic_load(&ctx->running)) {
        /* EINTR se reintenta: el hilo solo abandona por worker_stop. */
        while (sem_wait(&w->items) != 0) {
            if (errno != EINTR) {
                return NULL;
            }
        }

        pthread_mutex_lock(&w->mu);
        if (w->stopping) {
            pthread_mutex_unlock(&w->mu);
            break;
        }

        work_item_t *item = w->head;
        w->head = item->next;
        if (!w->head) w->tail = NULL;
        w->len--;
        pthread_mutex_unlock(&w->mu);
        sem_post(&w->free_slots);

        switch (item->msg.hdr.type) {
        case MSG_PAGE_REQUEST:
            msi_home_handle_request(ctx, &item->msg);
            break;
        case MSG_PAGE_DATA:
            msi_on_page_data(ctx, &item->msg);
            break;
        case MSG_GRANT:
            msi_on_grant(ctx, &item->msg);
            break;
        case MSG_INVALIDATE:
            msi_on_invalidate(ctx, &item->msg);
            break;
        case MSG_INVALIDATE_ACK:
            msi_on_invalidate_ack(ctx, &item->msg);
            break;
        case MSG_DOWNGRADE:
            msi_on_downgrade(ctx, &item->msg);
            break;
        case MSG_DOWNGRADE_ACK:
            msi_on_downgrade_ack(ctx, &item->msg);
            break;
        case MSG_WRITEBACK:
            msi_on_writeback(ctx, &item->msg);
            break;
        case MSG_LOCK_REQ:
            sync_on_lock_req(ctx, &item->msg);
            break;
        case MSG_LOCK_REL:
            sync_on_lock_rel(ctx, &item->msg);
            break;
        case MSG_LOCK_GRANT:
            sync_on_lock_grant(ctx, &item->msg);
            break;
        case MSG_BARRIER_ARRIVE:
            sync_on_barrier_arrive(ctx, &item->msg);
            break;
        case MSG_BARRIER_GO:
            sync_on_barrier_go(ctx, &item->msg);
            break;
        default:
            break;
        }

        if (item->owned) {
            free(item->owned);
        }
        free(item);
    }
    return NULL;
}

int worker_start(dsm_ctx_t *ctx)
{
    dsm_worker_t *w = &ctx->worker;
    memset(w, 0, sizeof(*w));
    pthread_mutex_init(&w->mu, NULL);

    w->cap      = ctx->cfg.worker_queue_cap ? ctx->cfg.worker_queue_cap : 4096;
    w->nthreads = ctx->cfg.worker_threads ? ctx->cfg.worker_threads : 4;

    if (sem_init(&w->free_slots, 0, (unsigned)w->cap) != 0) {
        pthread_mutex_destroy(&w->mu);
        return DSM_ERR_MEM;
    }
    if (sem_init(&w->items, 0, 0) != 0) {
        sem_destroy(&w->free_slots);
        pthread_mutex_destroy(&w->mu);
        return DSM_ERR_MEM;
    }

    w->threads = calloc(w->nthreads, sizeof(pthread_t));
    if (!w->threads) {
        sem_destroy(&w->items);
        sem_destroy(&w->free_slots);
        pthread_mutex_destroy(&w->mu);
        return DSM_ERR_MEM;
    }

    for (uint32_t i = 0; i < w->nthreads; i++) {
        if (pthread_create(&w->threads[i], NULL, worker_thread_main, ctx) != 0) {
            /* Detener los hilos ya creados antes de liberar sus recursos. */
            w->nthreads = i;
            worker_stop(ctx);
            return DSM_ERR_MEM;
        }
    }
    return DSM_OK;
}

void worker_stop(dsm_ctx_t *ctx)
{
    dsm_worker_t *w = &ctx->worker;
    pthread_mutex_lock(&w->mu);
    w->stopping = true;
    pthread_mutex_unlock(&w->mu);

    /* Un sem_post por hilo despierta a todos los consumidores, que observan
     * stopping y salen del bucle. */
    for (uint32_t i = 0; i < w->nthreads; i++) {
        sem_post(&w->items);
    }

    if (w->threads) {
        for (uint32_t i = 0; i < w->nthreads; i++) {
            pthread_join(w->threads[i], NULL);
        }
        free(w->threads);
        w->threads = NULL;
    }

    /* Los elementos que quedaron encolados no se procesan: se liberan aqui.
     * Se toma mu para que ningun productor los vea a medio liberar; todo
     * productor posterior observa stopping y se retira sin tocar la lista. */
    pthread_mutex_lock(&w->mu);
    work_item_t *item = w->head;
    while (item) {
        work_item_t *next = item->next;
        free(item->owned);
        free(item);
        item = next;
    }
    w->head = NULL;
    w->tail = NULL;
    w->len  = 0;
    pthread_mutex_unlock(&w->mu);

    /* El mutex y los semaforos se dejan inicializados. El hilo listener puede
     * seguir encolando mientras su descriptor siga abierto, y un productor
     * tardio debe encontrar la cola en estado stopping en lugar de primitivas
     * destruidas. worker_enqueue nunca toca la lista en ese caso. */
}

int worker_enqueue(dsm_ctx_t *ctx, const dsm_msg_t *m, uint8_t *owned)
{
    dsm_worker_t *w = &ctx->worker;

    /* El productor es el hilo listener: si el ciclo de ejecucion ya se apago,
     * el mensaje se descarta de inmediato y no se encola trabajo nuevo. */
    if (!atomic_load(&ctx->running)) {
        return DSM_ERR_AGAIN;
    }

    pthread_mutex_lock(&w->mu);
    if (w->stopping) {
        pthread_mutex_unlock(&w->mu);
        return DSM_ERR_AGAIN;
    }

    /* Reserva de capacidad: el consumidor devuelve el espacio al retirar.
     * sem_trywait no bloquea, por lo que es seguro hacerlo con mu tomado. */
    while (sem_trywait(&w->free_slots) != 0) {
        if (errno != EINTR) {
            pthread_mutex_unlock(&w->mu);
            return DSM_ERR_AGAIN;
        }
    }

    work_item_t *item = malloc(sizeof(*item));
    if (!item) {
        pthread_mutex_unlock(&w->mu);
        sem_post(&w->free_slots);
        return DSM_ERR_MEM;
    }

    item->msg   = *m;
    item->owned = owned;
    item->next  = NULL;

    if (w->tail) {
        w->tail->next = item;
    } else {
        w->head = item;
    }
    w->tail = item;
    w->len++;
    pthread_mutex_unlock(&w->mu);

    sem_post(&w->items);
    return DSM_OK;
}

int dispatcher_start(dsm_ctx_t *ctx)
{
    dsm_dispatcher_t *d = &ctx->dispatcher;
    memset(d, 0, sizeof(*d));

    d->nthreads = ctx->cfg.dispatcher_threads ? ctx->cfg.dispatcher_threads : 1;
    d->threads  = calloc(d->nthreads, sizeof(pthread_t));
    if (!d->threads) {
        d->nthreads = 0;
        return DSM_ERR_MEM;
    }

    for (uint32_t i = 0; i < d->nthreads; i++) {
        if (pthread_create(&d->threads[i], NULL,
                           dispatcher_thread_main, ctx) != 0) {
            /* Los hilos ya creados salen solos al apagarse el ciclo. */
            atomic_store(&ctx->running, false);
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(d->threads[j], NULL);
            }
            free(d->threads);
            d->threads  = NULL;
            d->nthreads = 0;
            return DSM_ERR_MEM;
        }
    }
    return DSM_OK;
}

void dispatcher_stop(dsm_ctx_t *ctx)
{
    dsm_dispatcher_t *d = &ctx->dispatcher;

    if (d->threads) {
        for (uint32_t i = 0; i < d->nthreads; i++) {
            pthread_join(d->threads[i], NULL);
        }
        free(d->threads);
        d->threads = NULL;
    }
    d->nthreads = 0;
}
