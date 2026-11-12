#ifndef DSM_SYNC_H
#define DSM_SYNC_H

#include <stdint.h>
#include <pthread.h>
#include "dsm_types.h"
#include "wire.h"

typedef struct {
    uint32_t lock_id;
    uint32_t owner_slot;
    uint32_t owner_req;
    uint32_t wait_queue[64];
    uint32_t wait_req[64];
    uint8_t  head, tail, count;
    uint8_t  pad;
} dsm_lock_dir_t;

typedef struct {
    uint32_t barrier_id;
    uint32_t threshold;
    uint32_t count;
    uint32_t generation;
    uint8_t  arrived_map[MAX_NODES / 8];
} dsm_barrier_dir_t;

typedef struct dsm_sync {
    pthread_mutex_t   mu;
    dsm_lock_dir_t    locks[256];
    dsm_barrier_dir_t barriers[64];
} dsm_sync_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  sync_init(dsm_ctx_t *ctx);
void sync_destroy(dsm_ctx_t *ctx);
void sync_on_lock_req(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_lock_rel(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_barrier_arrive(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_barrier_go(dsm_ctx_t *ctx, const dsm_msg_t *m);

#endif /* DSM_SYNC_H */
