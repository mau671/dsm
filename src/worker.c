#include "worker.h"
#include "dsm_internal.h"
#include <stdlib.h>
#include <string.h>

static void *worker_thread_main(void *arg)
{
    dsm_ctx_t *ctx = (dsm_ctx_t *)arg;
    dsm_worker_t *w = &ctx->worker;

    while (atomic_load(&ctx->running)) {
        pthread_mutex_lock(&w->mu);
        while (!w->stopping && !w->head) {
            pthread_cond_wait(&w->cv, &w->mu);
        }
        if (w->stopping) {
            pthread_mutex_unlock(&w->mu);
            break;
        }

        work_item_t *item = w->head;
        w->head = item->next;
        if (!w->head) w->tail = NULL;
        w->len--;
        pthread_mutex_unlock(&w->mu);

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
    pthread_cond_init(&w->cv, NULL);

    w->cap      = ctx->cfg.worker_queue_cap ? ctx->cfg.worker_queue_cap : 4096;
    w->nthreads = ctx->cfg.worker_threads ? ctx->cfg.worker_threads : 4;
    w->threads  = calloc(w->nthreads, sizeof(pthread_t));

    for (uint32_t i = 0; i < w->nthreads; i++) {
        pthread_create(&w->threads[i], NULL, worker_thread_main, ctx);
    }
    return DSM_OK;
}

void worker_stop(dsm_ctx_t *ctx)
{
    dsm_worker_t *w = &ctx->worker;
    pthread_mutex_lock(&w->mu);
    w->stopping = true;
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->mu);

    if (w->threads) {
        for (uint32_t i = 0; i < w->nthreads; i++) {
            pthread_join(w->threads[i], NULL);
        }
        free(w->threads);
        w->threads = NULL;
    }
    pthread_mutex_destroy(&w->mu);
    pthread_cond_destroy(&w->cv);
}

int worker_enqueue(dsm_ctx_t *ctx, const dsm_msg_t *m, uint8_t *owned)
{
    dsm_worker_t *w = &ctx->worker;
    pthread_mutex_lock(&w->mu);
    if (w->stopping || w->len >= w->cap) {
        pthread_mutex_unlock(&w->mu);
        return DSM_ERR_AGAIN;
    }

    work_item_t *item = malloc(sizeof(*item));
    if (!item) {
        pthread_mutex_unlock(&w->mu);
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

    pthread_cond_signal(&w->cv);
    pthread_mutex_unlock(&w->mu);
    return DSM_OK;
}

int dispatcher_start(dsm_ctx_t *ctx)
{
    (void)ctx;
    return DSM_OK;
}

void dispatcher_stop(dsm_ctx_t *ctx)
{
    (void)ctx;
}
