#ifndef DSM_SYNC_H
#define DSM_SYNC_H

#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>
#include "dsm_types.h"
#include "wire.h"

#define DSM_LOCK_SLOTS 256
#define DSM_BARRIER_SLOTS 64

/* Capacidad de la cola de espera de cada cerrojo en el custodio */
#define DSM_LOCK_QUEUE 64

/* Peticiones en vuelo: cada espera de la aplicacion ocupa una y tiene su
 * propio semaforo. El mutex del directorio nunca se retiene durante una
 * espera de red. */
#define DSM_SYNC_PENDING 128

/* Cerrojos que un mismo proceso puede sostener a la vez */
#define DSM_MAX_HELD 256

typedef struct {
    uint32_t lock_id;
    uint32_t owner_slot;
    uint32_t owner_req;
    uint32_t wait_queue[DSM_LOCK_QUEUE];
    uint32_t wait_req[DSM_LOCK_QUEUE];
    uint8_t  head, tail, count;
    uint8_t  used;
} dsm_lock_dir_t;

typedef struct {
    uint32_t barrier_id;
    uint32_t threshold;
    uint32_t count;
    uint32_t generation;
    uint8_t  arrived_map[MAX_NODES / 8];
    uint8_t  used;
} dsm_barrier_dir_t;

/* Clase de espera registrada en la tabla de peticiones en vuelo */
enum dsm_sync_wait_kind {
    DSM_SYNC_WAIT_LOCK    = 1,
    DSM_SYNC_WAIT_BARRIER = 2,
    DSM_SYNC_WAIT_ACK     = 3
};

typedef struct {
    uint32_t req_id;      /* Identificador que viaja en la cabecera */
    uint32_t key;         /* lock_id o barrier_id segun kind */
    uint32_t gen;         /* Generacion del custodio al registrarse */
    uint8_t  kind;        /* dsm_sync_wait_kind */
    uint8_t  active;
    uint8_t  done;
    int      result;
    sem_t    sem;
} dsm_sync_pending_t;

/* Cerrojo sostenido por un hilo concreto de este proceso: la propiedad es por
 * hilo, de modo que dos hilos del mismo nodo compiten igual que dos nodos. */
typedef struct {
    uint32_t  lock_id;
    uint32_t  req_id;
    pthread_t owner;
    uint8_t   active;
} dsm_sync_held_t;

typedef struct dsm_sync {
    pthread_mutex_t     mu;
    dsm_lock_dir_t      locks[DSM_LOCK_SLOTS];
    dsm_barrier_dir_t   barriers[DSM_BARRIER_SLOTS];
    dsm_sync_pending_t  pending[DSM_SYNC_PENDING];
    dsm_sync_held_t     held[DSM_MAX_HELD];
} dsm_sync_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  sync_init(dsm_ctx_t *ctx);
void sync_destroy(dsm_ctx_t *ctx);

/* API consumida por dsm.c. Devuelve DSM_OK o un dsm_status_t negativo. */
int  sync_lock(dsm_ctx_t *ctx, uint32_t lock_id);
int  sync_unlock(dsm_ctx_t *ctx, uint32_t lock_id);
int  sync_barrier(dsm_ctx_t *ctx, uint32_t barrier_id, uint32_t n);

/* Manejadores de los mensajes del protocolo, invocados desde los workers */
void sync_on_lock_req(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_lock_grant(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_lock_rel(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_barrier_arrive(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_barrier_go(dsm_ctx_t *ctx, const dsm_msg_t *m);
void sync_on_node_down(dsm_ctx_t *ctx, uint32_t slot);
void sync_send_lock_ack(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                        uint32_t req_id);
void sync_ack_received(dsm_ctx_t *ctx, uint32_t req_id);

#endif /* DSM_SYNC_H */
