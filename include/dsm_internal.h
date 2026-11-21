#ifndef DSM_INTERNAL_H
#define DSM_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>

#include "dsm_types.h"
#include "config.h"
#include "discovery.h"
#include "mem.h"
#include "net.h"
#include "msi.h"
#include "sync.h"
#include "worker.h"
#include "ring.h"
#include "raid.h"
#include "dist.h"
#include "log.h"
#include "util.h"

#define ALLOC_STACK_MAX 1024
#define ALLOC_HOLE_MAX  1024

/* Bloque vivo registrado por dsm_alloc(). npages == 0 marca la entrada como
 * disponible para una reserva posterior. */
typedef struct {
    uint32_t first_page;
    uint32_t npages;
    uint32_t alloc_id;
} alloc_block_t;

/* Rango contiguo de paginas devuelto a la lista de espacio libre por
 * dsm_free(). Los huecos se mantienen ordenados por first_page y fusionados
 * con sus vecinos. */
typedef struct {
    uint32_t first_page;
    uint32_t npages;
} alloc_hole_t;

typedef struct {
    pthread_mutex_t mu;
    uint32_t        next_page;
    uint32_t        total_pages;
    uint64_t        epoch;
    uint32_t        alloc_id;
    alloc_block_t   stack[ALLOC_STACK_MAX];
    uint32_t        depth;
    alloc_hole_t    holes[ALLOC_HOLE_MAX];
    uint32_t        nholes;
} dsm_alloc_t;

typedef struct {
    _Atomic uint64_t read_faults, write_faults;
    _Atomic uint64_t msgs_sent, msgs_recv;
    _Atomic uint64_t bytes_sent, bytes_recv;
    _Atomic uint64_t invalidations;
    _Atomic uint64_t fault_latency_us_total;
    _Atomic uint64_t faults_serviced;
    _Atomic uint64_t tseq_discarded;
    _Atomic uint64_t nacks_received;
    _Atomic uint32_t pending_count, wait_pool_used;
    _Atomic uint32_t slots_enqueued, slots_woken;
    _Atomic uint64_t parity_deltas, parity_bytes;
    _Atomic uint64_t stripe_rebuilds, parity_latency_us;
    _Atomic uint64_t dist_tasks_run, dist_tasks_failed;
    _Atomic uint64_t hb_missed;
} dsm_counters_t;

typedef struct dsm_ctx {
    dsm_config_t      cfg;
    dsm_mem_t         mem;
    dsm_membership_t  memb;
    dsm_net_t         net;
    dsm_msi_t         msi;
    dsm_sync_t        sync;
    dsm_worker_t      worker;
    dsm_dispatcher_t  dispatcher;
    dsm_counters_t    ctr;
    dsm_alloc_t       alloc;
    dsm_ring_t        ring;
    dsm_raid_t        raid;
    dsm_dist_t        dist;
    atomic_bool       running;
    pthread_t         hb_thread;
} dsm_ctx_t;

/* Acceso al contexto global del nodo */
dsm_ctx_t *dsm_ctx_get(void);
bool        dsm_is_initialized(void);
void        dsm_ctx_reset(void);

/* Marca de tiempo monotónica en microsegundos para medir latencias */
static inline uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

#if defined(DSM_TEST_HOOKS)
dsm_ctx_t *dsm_ctx_create_test(void);
/* Publica un contexto de prueba como contexto global para que las funciones
 * publicas lo resuelvan con dsm_ctx_get(). */
void        dsm_ctx_install_test(dsm_ctx_t *ctx);
#endif

#endif /* DSM_INTERNAL_H */
