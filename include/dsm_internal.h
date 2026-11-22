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
    uint32_t pid;            /* Proceso que hizo la reserva */
    /**
     * dsm_free() retiró la entrada de la pila y libera sus páginas fuera del
     * mutex del mapa. Mientras está en curso, la ranura no se reutiliza.
     */
    bool     retiring;
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
    /* En el coordinador next_page es la marca de agua del mapa global de la
     * vista; en un nodo cliente solo conserva la cota superior observada para
     * no reutilizar rangos ajenos si llega a asumir la coordinación. */
    uint32_t        next_page;
    uint32_t        total_pages;
    uint64_t        epoch;
    uint32_t        alloc_id;
    alloc_block_t   stack[ALLOC_STACK_MAX];
    uint32_t        depth;
    alloc_hole_t    holes[ALLOC_HOLE_MAX];
    uint32_t        nholes;
    /**
     * Verdadero cuando esta copia del mapa global conoce la marca de agua y
     * los huecos vigentes. Un nodo que asume la coordinación sin un mapa
     * importado sincroniza la marca de agua con los pares vivos o rechaza la
     * petición; nunca reparte espacio con una vista incompleta.
     */
    bool            map_valid;
    /** Origen y secuencia de la instantánea vigente del mapa. Un WELCOME con
     *  la misma procedencia y una secuencia menor es una vista atrasada y no
     *  puede reinsertar huecos que el coordinador ya entregó. */
    uint32_t        map_origin;
    uint32_t        map_seq;
    /** Falso hasta que este nodo, al asumir la coordinación, contrastó su
     *  marca de agua con los pares vivos. Un coordinador sin sincronizar no
     *  reparte espacio. */
    bool            map_synced;
    /** Respuestas de sincronización que aún se esperan. */
    uint32_t        map_sync_pending;
    /** Se señaliza cuando map_sync_pending llega a cero. Comparte mu con el
     *  contador, de modo que la comprobación y la espera no pierden avisos. */
    pthread_cond_t  sync_cv;
    /** Verdadero si esta llamada encontró a este nodo como coordinador vivo
     *  de menor ranura; falso si el rango se pidió a otro nodo. */
    bool            coordinator;
    /** Ranura del coordinador consultado cuando coordinator es falso. */
    uint32_t        coordinator_slot;
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

/* Manejadores del coordinador para las peticiones de espacio global. Se
 * ejecutan en un worker a partir de MSG_ALLOC_REQ y MSG_FREE_REQ. */
void dsm_alloc_on_request(dsm_ctx_t *ctx, const dsm_msg_t *m);
void dsm_free_on_request(dsm_ctx_t *ctx, const dsm_msg_t *m);

/* Instantánea del mapa global que viaja en WELCOME. Devuelve la cantidad de
 * registros escritos y deja el mapa intacto si esta copia no es válida. */
uint32_t dsm_alloc_export(dsm_ctx_t *ctx, alloc_rec_t *recs, uint32_t cap);

/* Incorpora una instantánea recibida en WELCOME. La marca de agua solo avanza
 * y los huecos se reemplazan por los de la vista vigente; una instantánea
 * atrasada del mismo origen se descarta. */
void dsm_alloc_import(dsm_ctx_t *ctx, const alloc_rec_t *recs, uint32_t count);

/* WELCOME no solicitado con la instantánea que el coordinador replica tras
 * cada cambio del mapa global. */
void dsm_alloc_on_welcome(dsm_ctx_t *ctx, const dsm_msg_t *m);

/* Sondeo de marca de agua al asumir la coordinación. Cada par responde con su
 * instantánea y el nuevo coordinador toma el máximo antes de repartir. */
void dsm_alloc_on_sync_request(dsm_ctx_t *ctx, const dsm_msg_t *m);
void dsm_alloc_on_sync_state(dsm_ctx_t *ctx, const dsm_msg_t *m);

#endif /* DSM_INTERNAL_H */
