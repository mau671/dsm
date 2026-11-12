#ifndef DSM_INTERNAL_H
#define DSM_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

#include "dsm_types.h"
#include "config.h"
#include "discovery.h"
#include "mem.h"
#include "net.h"
#include "msi.h"
#include "sync.h"
#include "worker.h"
#include "ring.h"
#include "log.h"
#include "util.h"

#define ALLOC_STACK_MAX 1024

typedef struct {
    uint32_t first_page;
    uint32_t npages;
    uint32_t alloc_id;
} alloc_block_t;

typedef struct {
    pthread_mutex_t mu;
    uint32_t        next_page;
    uint32_t        total_pages;
    uint64_t        epoch;
    uint32_t        alloc_id;
    alloc_block_t   stack[ALLOC_STACK_MAX];
    uint32_t        depth;
} dsm_alloc_t;

typedef struct {
    _Atomic uint64_t read_faults, write_faults;
    _Atomic uint64_t msgs_sent, msgs_recv;
    _Atomic uint64_t bytes_sent, bytes_recv;
    _Atomic uint64_t invalidations;
    _Atomic uint64_t fault_latency_us_total;
    _Atomic uint32_t pending_count, wait_pool_used;
    _Atomic uint32_t slots_enqueued, slots_woken;
} dsm_counters_t;

typedef struct dsm_ctx {
    dsm_config_t      cfg;
    dsm_mem_t         mem;
    dsm_membership_t  memb;
    dsm_net_t         net;
    dsm_msi_t         msi;
    dsm_sync_t        sync;
    dsm_worker_t      worker;
    dsm_counters_t    ctr;
    dsm_alloc_t       alloc;
    dsm_ring_t        ring;
    atomic_bool       running;
    pthread_t         hb_thread;
    pthread_t         listener_thread;
    pthread_t         dispatcher_thread;
} dsm_ctx_t;

/* Acceso al contexto global del nodo */
dsm_ctx_t *dsm_ctx_get(void);
bool       dsm_is_initialized(void);
void       dsm_ctx_reset(void);

#if defined(DSM_TEST_HOOKS)
dsm_ctx_t *dsm_ctx_create_test(void);
#endif

#endif /* DSM_INTERNAL_H */
