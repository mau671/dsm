#ifndef DSM_WORKER_H
#define DSM_WORKER_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include "dsm_types.h"
#include "wire.h"

typedef struct work_item {
    dsm_msg_t         msg;
    uint8_t          *owned;
    struct work_item *next;
} work_item_t;

/* Cola productor/consumidor de mensajes de red.
 *
 * free_slots e items son los contadores de espacio disponible y de elementos
 * encolados. El mutex mu protege unicamente la lista enlazada, los contadores
 * len y cap y la bandera stopping; nunca se sostiene mientras se procesa un
 * mensaje.
 *
 * El lado productor (hilo listener) reserva un espacio con sem_trywait antes
 * de insertar el nodo y, si la insercion no se completa, devuelve el espacio
 * con sem_post. El lado consumidor (hilos worker) espera un elemento con
 * sem_wait y lo retira bajo mu. worker_stop libera a los consumidores
 * publicando stopping y sembrando un sem_post por hilo.
 *
 * worker_stop no destruye mu ni los semaforos: el hilo listener puede seguir
 * encolando hasta que se cierre su descriptor, y un productor tardio debe
 * encontrar stopping activo en lugar de primitivas liberadas. */
typedef struct dsm_worker {
    work_item_t    *head;
    work_item_t    *tail;
    pthread_mutex_t mu;
    sem_t           free_slots;
    sem_t           items;
    size_t          len;
    size_t          cap;
    pthread_t      *threads;
    uint32_t        nthreads;
    bool            stopping;
} dsm_worker_t;

typedef struct dsm_dispatcher {
    pthread_t *threads;
    uint32_t   nthreads;
} dsm_dispatcher_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  worker_start(dsm_ctx_t *ctx);
void worker_stop(dsm_ctx_t *ctx);
int  worker_enqueue(dsm_ctx_t *ctx, const dsm_msg_t *m, uint8_t *owned);
int  dispatcher_start(dsm_ctx_t *ctx);
void dispatcher_stop(dsm_ctx_t *ctx);

#endif /* DSM_WORKER_H */
