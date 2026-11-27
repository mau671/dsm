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

/* Límite de reintentos consecutivos por ranura de fallo. Es la red de
 * seguridad de la espera bloqueada: una página cuya transacción nunca
 * señala su fin no debe monopolizar al despachador. Al agotarlo se despierta
 * al hilo de aplicación, que vuelve a fallar y reencolar, y el despachador
 * queda libre para otras páginas. */
#define DSM_FAULT_BUSY_ATTEMPTS 64u

static void *dispatcher_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;

    while (atomic_load(&ctx->running)) {
        /* La cola de fallos avisa con su semáforo contador: el hilo se
         * bloquea en faultq_pop y no sondea con suspensiones temporizadas. */
        fault_slot_t *slot = faultq_pop();
        if (!slot) {
            continue;
        }

        /*
         * Un BUSY transitorio significa que otra transición de la página está
         * en curso. La ranura se encola en la cola FIFO de esa página y el
         * despachador aguarda en su condvar con plazo LOCK_TIMEOUT_MS; cada
         * finalización difunde la señal y provoca un solo reintento. No hay
         * suspensiones de duración fija ni bucles de rechazo.
         *
         * El plazo es absoluto para toda la estancia en la cola: al vencer se
         * devuelve el error una sola vez y se libera la ranura. El tope de
         * reintentos subsiste como red de seguridad.
         *
         * Los errores terminales no se resuelven con otro intento: el dueño
         * muerto en M, una ranura inválida, la falta de espacio y la falta de
         * memoria local. El ciclo no espera ni repite en esos estados; el
         * motivo se publica en la ranura y el manejador de SIGSEGV reenvía la
         * señal en lugar de dejar al hilo fallar en bucle. La latencia abarca
         * la resolución completa, reintentos incluidos.
         */
        uint32_t timeout_ms = ctx->cfg.lock_timeout_ms
                                  ? ctx->cfg.lock_timeout_ms : 10000u;
        uint64_t t0 = mono_us();
        uint64_t deadline_us = 0;
        int rc;
        uint32_t busy_attempts = 0;
        do {
            uint64_t generation = msi_page_generation(ctx, slot->page);
            rc = msi_local_fault(ctx, slot);
            if (rc != DSM_ERR_BUSY) {
                break;
            }
            busy_attempts++;
            if (busy_attempts >= DSM_FAULT_BUSY_ATTEMPTS) {
                /* Red de seguridad: una página que nunca publica su fin no
                 * debe monopolizar al despachador. */
                DSM_LOG_WARN("Fallo de la página %u sin progreso tras %u "
                             "reintentos; se libera la ranura",
                             slot->page, busy_attempts);
                break;
            }
            if (deadline_us == 0) {
                deadline_us = mono_us() + (uint64_t)timeout_ms * 1000u;
            }
            uint64_t now = mono_us();
            if (now >= deadline_us) {
                rc = DSM_ERR_TIMEOUT;
                break;
            }
            uint32_t remaining_ms = (uint32_t)((deadline_us - now) / 1000u);
            if (remaining_ms == 0) {
                rc = DSM_ERR_TIMEOUT;
                break;
            }
            int wait_rc = msi_page_wait(ctx, slot->page, slot, generation,
                                        remaining_ms);
            if (wait_rc != DSM_OK) {
                rc = wait_rc;
                break;
            }
        } while (atomic_load(&ctx->running));
        uint64_t elapsed = mono_us() - t0;

        atomic_fetch_add_explicit(&ctx->ctr.fault_latency_us_total, elapsed,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&ctx->ctr.faults_serviced, 1,
                                  memory_order_relaxed);

        /* Un dueño muerto en M, una ranura inválida, la falta de espacio o la
         * falta de memoria no se resuelven con otro intento. El motivo se
         * publica una sola vez en la ranura y el manejador reenvía SIGSEGV: el
         * fallo termina el proceso en lugar de repetirse. */
        if (rc == DSM_ERR_NODE_DOWN || rc == DSM_ERR_INVALID ||
            rc == DSM_ERR_NO_SPACE || rc == DSM_ERR_MEM) {
            DSM_LOG_ERR("Fallo terminal en la página %u (acceso %u): %d; se "
                        "reenvía SIGSEGV al hilo", slot->page, slot->access, rc);
            __atomic_store_n(&slot->error, (uint16_t)(-rc), __ATOMIC_RELEASE);
        }

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
        case MSG_PAGE_MOVE:
            msi_on_page_move(ctx, &item->msg);
            break;
        case MSG_PARITY_DELTA:
            raid_on_parity_delta(ctx, &item->msg);
            break;
        case MSG_PARITY_ACK:
            raid_on_parity_ack(ctx, &item->msg);
            break;
        case MSG_STRIPE_READ:
            raid_on_stripe_read(ctx, &item->msg);
            break;
        case MSG_STRIPE_DATA:
            raid_on_stripe_data(ctx, &item->msg);
            break;
        case MSG_STRIPE_PUT:
            raid_on_stripe_put(ctx, &item->msg);
            break;
        case MSG_STRIPE_PUT_ACK:
            raid_on_stripe_put_ack(ctx, &item->msg);
            break;
        case MSG_STATE_REPORT:
            raid_on_state_report(ctx, &item->msg);
            break;
        case MSG_DIR_DONE:
            raid_on_dir_done(ctx, &item->msg);
            break;
        case MSG_NACK:
            atomic_fetch_add_explicit(&ctx->ctr.nacks_received, 1,
                                      memory_order_relaxed);
            if (!sync_on_barrier_nack(ctx, &item->msg)) {
                DSM_LOG_WARN("NACK del slot %u para la peticion %u (pagina %u, "
                             "estado %u) sin peticion pendiente local",
                             item->msg.hdr.src_slot, item->msg.hdr.req_id,
                             item->msg.hdr.page_id, item->msg.hdr.status);
            }
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
        case MSG_LOCK_MIGRATE:
            sync_on_lock_migrate(ctx, &item->msg);
            break;
        case MSG_LOCK_STATE:
            sync_on_lock_state(ctx, &item->msg);
            break;
        case MSG_BARRIER_ARRIVE:
            sync_on_barrier_arrive(ctx, &item->msg);
            break;
        case MSG_BARRIER_GO:
            sync_on_barrier_go(ctx, &item->msg);
            break;
        case MSG_DIST_RUN:
            dist_on_run(ctx, &item->msg);
            break;
        case MSG_DIST_DONE:
            dist_on_done(ctx, &item->msg);
            break;
        case MSG_ALLOC_REQ:
            dsm_alloc_on_request(ctx, &item->msg);
            break;
        case MSG_FREE_REQ:
            dsm_free_on_request(ctx, &item->msg);
            break;
        case MSG_WELCOME:
            /* WELCOME no solicitado: réplica de membresía y del mapa global
             * del coordinador. */
            membership_on_welcome(ctx, &item->msg);
            dsm_alloc_on_welcome(ctx, &item->msg);
            break;
        case MSG_ALLOC_SYNC:
            dsm_alloc_on_sync_request(ctx, &item->msg);
            break;
        case MSG_ALLOC_STATE:
            dsm_alloc_on_sync_state(ctx, &item->msg);
            break;
        case MSG_NODE_DOWN:
            /* Mensaje sintético encolado por quien cerró la ranura: la
             * recuperación de coherencia corre aquí y no en el listener. */
            membership_node_down_recover(ctx, item->msg.hdr.page_id);
            break;
        default:
            break;
        }

        if (item->owned) {
            free(item->owned);
        }
        free(item);
    }

    /* Solo los hilos que ejecutaron un kernel distribuido tienen pila
     * alternativa que liberar; en el resto es una operacion nula. */
    (void)dsm_sig_thread_detach();
    return NULL;
}

int worker_start(dsm_ctx_t *ctx)
{
    dsm_worker_t *w = &ctx->worker;
    memset(w, 0, sizeof(*w));
    if (pthread_mutex_init(&w->mu, NULL) != 0) {
        return DSM_ERR_MEM;
    }

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
