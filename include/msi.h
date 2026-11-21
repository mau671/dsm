#ifndef DSM_MSI_H
#define DSM_MSI_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include "dsm_types.h"
#include "wire.h"
#include "sigsegv.h"

/* Estructura de transacción de directorio en el home */
typedef struct {
    uint32_t page_id;
    uint32_t requester;
    uint32_t req_id;
    uint8_t  kind;
    uint8_t  pad[3];
    uint8_t  ack_bitmap[MAX_NODES / 8];
} txn_t;

/* Estructura de espera de un nodo remoto en el home */
typedef struct {
    uint32_t requester;
    uint32_t req_id;
    uint32_t page;
    uint8_t  access;
    uint8_t  pad[3];
    uint32_t next;
} home_wait_t;

/* Estructura de espera local */
typedef struct {
    fault_slot_t *slot;
    uint32_t      page;
    uint32_t      next;
} wait_rec_t;

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
    uint32_t        *frames_free;
    uint32_t         nframes_free;
    home_wait_t     *wait_pool;
    uint32_t         home_wait_free;
    wait_rec_t      *wait_local;
    uint32_t         wait_local_free;
    txn_t           *txns;
    uint32_t         ntxns;
    msi_ack_wait_t   ack_waits[MSI_ACK_WAIT_SLOTS];
    pthread_mutex_t  dir_mu;
} dsm_msi_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  msi_init(dsm_ctx_t *ctx);
void msi_destroy(dsm_ctx_t *ctx);
int  msi_local_fault(dsm_ctx_t *ctx, fault_slot_t *s);

void msi_home_handle_request(dsm_ctx_t *ctx, const dsm_msg_t *m);
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
void msi_counters(dsm_ctx_t *ctx, uint64_t *out_m, uint64_t *out_s, uint64_t *out_i);

bool msi_tseq_stale(dsm_ctx_t *ctx, uint32_t page, uint32_t tseq);

int  msi_ack_wait_begin(dsm_ctx_t *ctx, uint32_t txn,
                        const uint8_t *mask, uint32_t expected);
void msi_ack_wait_cancel(dsm_ctx_t *ctx, uint32_t txn);
int  msi_ack_wait_result(dsm_ctx_t *ctx, uint32_t txn, uint32_t timeout_ms);


#endif /* DSM_MSI_H */
