#ifndef DSM_RAID_H
#define DSM_RAID_H

#include <stdint.h>
#include <pthread.h>
#include "wire.h"

/* Numero maximo de paginas de datos por franja (validado en config.c). */
#define DSM_RAID_MAX_K       8u
/* Tamano de la tabla de solicitudes de paridad pendientes. */
#define DSM_RAID_PENDING_MAX 256u

/* Estados de una franja en tiempo de ejecucion. */
enum {
    DSM_STRIPE_ACTIVE   = 0,
    DSM_STRIPE_DEGRADED = 1,
    DSM_STRIPE_REBUILD  = 2,
    /* STRIPE_PUT recibido en el nuevo custodio: espera el marco completo. */
    DSM_STRIPE_INSTALL  = 3
};

/* Variantes de STRIPE_READ: marco de respaldo de una pagina o marco de paridad. */
enum {
    DSM_STRIPE_READ_DATA   = 0,
    DSM_STRIPE_READ_PARITY = 1
};

/* Clases de respuesta esperadas por el emisor. */
enum {
    DSM_RAID_PENDING_ACK  = 0,
    DSM_RAID_PENDING_DATA = 1,
    DSM_RAID_PENDING_PUT  = 2
};

typedef struct {
    uint32_t stripe_id;
    uint32_t k;
    uint32_t data_page[DSM_RAID_MAX_K];
    uint32_t data_slot[DSM_RAID_MAX_K];
    uint32_t parity_slot;
    uint32_t version;
    uint8_t  state;
    uint8_t  parity_frame_valid;
} stripe_rt_t;

typedef struct {
    uint32_t req_id;
    uint64_t t0_us;
    uint32_t rx_len;
    uint32_t data_version;
    uint8_t *rx_buf;
    uint32_t rx_cap;
    uint16_t status;
    uint8_t  used;
    uint8_t  completed;
    uint8_t  consumed;
    uint8_t  kind;
} raid_pending_t;

typedef struct {
    pthread_mutex_t   mu;
    pthread_cond_t    cond;
    stripe_rt_t      *stripes;
    uint8_t          *parity;       /* nstripes * page_size, en el orden de los marcos */
    uint32_t          nstripes;
    uint32_t          page_size;
    uint32_t          k;
    uint32_t          next_pending;
    pthread_mutex_t   rebuild_mu;
    pthread_t         rebuild_thread;
    uint32_t          rebuild_slot;
    bool              rebuild_active;
    bool              rebuild_pending;
    raid_pending_t    pending[DSM_RAID_PENDING_MAX];
} dsm_raid_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

void raid_init(dsm_ctx_t *ctx);
void raid_shutdown(dsm_ctx_t *ctx);

int  raid_on_write(dsm_ctx_t *ctx, uint32_t page,
                   const uint8_t *old_data, const uint8_t *new_data);
void raid_on_node_down(dsm_ctx_t *ctx, uint32_t slot);

void raid_on_parity_delta(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_parity_ack(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_stripe_read(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_stripe_data(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_stripe_put(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_stripe_put_ack(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_state_report(dsm_ctx_t *ctx, const dsm_msg_t *m);
void raid_on_dir_done(dsm_ctx_t *ctx, const dsm_msg_t *m);

/* Reconstruccion XOR de n piezas de ps bytes cada una. */
void raid_xor_rebuild(const uint8_t **parts, uint32_t n, uint32_t ps,
                      uint8_t *out);

#endif /* DSM_RAID_H */
