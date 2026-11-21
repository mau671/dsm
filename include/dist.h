#ifndef DSM_DIST_H
#define DSM_DIST_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#include "dsm.h"
#include "wire.h"

/* Capacidad de las tablas del subsistema de ejecucion distribuida. */
#define DSM_DIST_KERNELS_MAX 32u
#define DSM_DIST_JOBS_MAX    16u
/* Tope duro de particiones por trabajo: acota la tabla de resultados aunque
 * WAIT_POOL_ENTRIES configure un valor mayor. */
#define DSM_DIST_PARTS_MAX   4096u
/* Valor de WAIT_POOL_ENTRIES cuando la configuracion no lo define. */
#define DSM_DIST_DEFAULT_SLOTS 64u

typedef struct {
    uint32_t           kernel_id;
    dsm_dist_kernel_fn fn;
    void              *arg;
    bool               used;
} dist_kernel_slot_t;

typedef struct {
    uint32_t            job_id;
    uint32_t            kernel_id;
    uint32_t            nparts;
    uint32_t            nworkers;
    uint32_t            received;
    uint32_t            collected;
    uint32_t            capacity;
    uint32_t            launch_seq;
    bool                active;
    bool                done;
    dsm_dist_result_t  *results;
    uint32_t           *part_slot;
} dist_job_slot_t;

typedef struct {
    pthread_mutex_t    mu;
    pthread_cond_t     cond;
    dist_kernel_slot_t kernels[DSM_DIST_KERNELS_MAX];
    dist_job_slot_t    jobs[DSM_DIST_JOBS_MAX];
    uint32_t           next_launch_seq;
    uint64_t           parts_dispatched;
    uint64_t           parts_completed;
    uint64_t           parts_rejected;
} dsm_dist_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

/* Inicializa y destruye el estado del subsistema. No crea hilos propios: los
 * kernels se ejecutan en los hilos worker que ya existen. */
int  dist_init(dsm_ctx_t *ctx);
void dist_destroy(dsm_ctx_t *ctx);

/* Atiende un MSG_DIST_RUN recibido: ejecuta el kernel y responde DIST_DONE.
 * Se invoca desde los hilos worker. */
void dist_on_run(dsm_ctx_t *ctx, const dsm_msg_t *m);

/* Registra un MSG_DIST_DONE recibido en el trabajo correspondiente. */
void dist_on_done(dsm_ctx_t *ctx, const dsm_msg_t *m);

#endif /* DSM_DIST_H */
