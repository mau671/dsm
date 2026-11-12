#ifndef DSM_WORKER_H
#define DSM_WORKER_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "dsm_types.h"
#include "wire.h"

typedef struct work_item {
    dsm_msg_t         msg;
    uint8_t          *owned;
    struct work_item *next;
} work_item_t;

typedef struct dsm_worker {
    work_item_t    *head;
    work_item_t    *tail;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    size_t          len;
    size_t          cap;
    pthread_t      *threads;
    uint32_t        nthreads;
    bool            stopping;
} dsm_worker_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  worker_start(dsm_ctx_t *ctx);
void worker_stop(dsm_ctx_t *ctx);
int  worker_enqueue(dsm_ctx_t *ctx, const dsm_msg_t *m, uint8_t *owned);
int  dispatcher_start(dsm_ctx_t *ctx);
void dispatcher_stop(dsm_ctx_t *ctx);

#endif /* DSM_WORKER_H */
