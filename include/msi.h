#ifndef DSM_MSI_H
#define DSM_MSI_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include "dsm_types.h"
#include "wire.h"
#include "sigsegv.h"

/* Nodo de la cola FIFO de espera local de una página. Vive en la pila del hilo
 * despachador mientras este aguarda en la condvar, así que no hay un conjunto
 * de nodos que agotarse ni memoria que liberar al despertar. */
typedef struct wait_rec_t {
    fault_slot_t      *slot;
    uint32_t           page;
    struct wait_rec_t *next;
} wait_rec_t;

/* Cola FIFO de espera por página. Un único mutex protege todas las colas; la
 * condvar se difunde al completar la transición y el contador de generaciones
 * evita perder una finalización publicada entre el encolado y el bloqueo. */
typedef struct {
    pthread_cond_t cv;
    uint64_t       generation;
    wait_rec_t    *head;
    wait_rec_t    *tail;
    uint32_t       count;
} page_wait_t;

/* Cola FIFO de transacciones del home por página. El dueño del turno retiene
 * dir[page].busy. Las solicitudes locales esperan en una condvar con plazo
 * LOCK_TIMEOUT_MS; las remotas no ocupan un worker: se difieren y se
 * reencolan para ejecutarse cuando el turno queda libre. Los números de
 * secuencia conservan el orden FIFO entre ambos tipos. */
typedef struct msi_txn_waiter msi_txn_waiter_t;
typedef struct msi_txn_deferred msi_txn_deferred_t;

typedef struct {
    msi_txn_waiter_t   *wait_head;
    msi_txn_waiter_t   *wait_tail;
    msi_txn_deferred_t *defer_head;
    msi_txn_deferred_t *defer_tail;
    uint64_t            seq_next;
} msi_txn_queue_t;

#define MSI_ACK_WAIT_SLOTS 64u

/* Espera de los acuses de una transaccion que invalida varias copias. ok_mask
 * conserva los remitentes cuyo acuse todavia no llego, de modo que un duplicado
 * o un nodo ajeno al conjunto no altera la cuenta. La entrada permanece
 * activa hasta que su dueño consume el resultado: un acuse que llega antes de
 * la espera no debe perderse. */
typedef struct {
    uint32_t txn;
    uint32_t expected;
    uint32_t received;
    uint8_t  ok_mask[MAX_NODES / 8];
    uint16_t status;
    uint8_t  done;
    bool     active;
    sem_t    sem;
} msi_ack_wait_t;

typedef struct dsm_msi {
    dir_entry_t     *dir;
    size_t           cap;
    /* Último home calculado para cada página (cap entradas). */
    uint32_t        *home_map;
    /* Cola FIFO de espera por página (cap entradas). El despachador que recibe
     * BUSY transitorio encola su ranura y aguarda en la condvar de la página;
     * los puntos de finalización difunden la señal. Sin sondeo ni suspensiones
     * de duración fija. */
    page_wait_t     *page_q;
    /* Cola FIFO de turnos del home (cap entradas). Un solo cerrojo y una sola
     * condvar cubren todas las páginas: los nodos de espera viven en la pila
     * de cada hilo y jamás se retiene el cerrojo a través de la red. */
    msi_txn_queue_t *txn_q;
    pthread_mutex_t  txn_mu;
    pthread_cond_t   txn_cv;
    msi_ack_wait_t   ack_waits[MSI_ACK_WAIT_SLOTS];
    pthread_mutex_t  dir_mu;
    /* Protege exclusivamente las colas de espera. Nunca se retiene mientras se
     * envía o espera un mensaje de red. */
    pthread_mutex_t  wait_mu;
} dsm_msi_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  msi_init(dsm_ctx_t *ctx);
void msi_destroy(dsm_ctx_t *ctx);
int  msi_local_fault(dsm_ctx_t *ctx, fault_slot_t *s);

/* Encola la ranura en la cola FIFO de la página y bloquea hasta que la
 * transición termine —difusión de la condvar— o venza 'timeout_ms'. 'since' es
 * la generación capturada antes del intento que devolvió BUSY; si ya cambió,
 * la espera retorna de inmediato en lugar de perder la finalización. Devuelve
 * DSM_OK cuando hubo finalización y el llamador debe reintentar una sola vez,
 * DSM_ERR_TIMEOUT si venció el plazo y DSM_ERR_INVALID si la página no existe.
 * No retiene cerrojos propios mientras aguarda. */
uint64_t msi_page_generation(dsm_ctx_t *ctx, uint32_t page);

int msi_page_wait(dsm_ctx_t *ctx, uint32_t page, fault_slot_t *slot,
                  uint64_t since, uint32_t timeout_ms);

void msi_home_handle_request(dsm_ctx_t *ctx, const dsm_msg_t *m);

/* Despierta a los hilos que aguardan un turno del home para que abandonen la
 * cola al apagarse el nodo. Se invoca después de marcar running=false y antes
 * de unir despachadores y workers. */
void msi_abort_waits(dsm_ctx_t *ctx);
#if defined(DSM_TEST_HOOKS)
void msi_test_busy_hook_config(uint32_t page, bool fail);
void msi_test_busy_hook_wait_entered(void);
void msi_test_busy_hook_release(void);
bool msi_test_busy_hook_second_seen(void);
void msi_test_downgrade_config(uint16_t status);
uint32_t msi_test_downgrade_calls(void);
void msi_test_downgrade_reset(void);
#endif
void msi_on_page_data(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_grant(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_invalidate(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_invalidate_ack(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_downgrade(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_downgrade_ack(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_writeback(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_page_move(dsm_ctx_t *ctx, const dsm_msg_t *m);
void msi_on_node_down(dsm_ctx_t *ctx, uint32_t slot);

int  msi_writeback_page(dsm_ctx_t *ctx, uint32_t page);
/* Retira la copia local de una página antes de devolver su bloque al espacio
 * libre: persiste el contenido si está en M y, cuando el home es local, deja
 * la entrada del directorio como no cacheada. */
int  msi_release_page(dsm_ctx_t *ctx, uint32_t page);
void msi_counters(dsm_ctx_t *ctx, uint64_t *out_m, uint64_t *out_s, uint64_t *out_i);

bool msi_tseq_stale(dsm_ctx_t *ctx, uint32_t page, uint32_t tseq);

int  msi_ack_wait_begin(dsm_ctx_t *ctx, uint32_t txn,
                        const uint8_t *mask, uint32_t expected);
void msi_ack_wait_cancel(dsm_ctx_t *ctx, uint32_t txn);
int  msi_ack_wait_result(dsm_ctx_t *ctx, uint32_t txn, uint32_t timeout_ms);


#endif /* DSM_MSI_H */
