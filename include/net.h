#ifndef DSM_NET_H
#define DSM_NET_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <semaphore.h>
#include <netinet/in.h>
#include "dsm_types.h"
#include "wire.h"

/* Estructura de par saliente */
typedef struct dsm_peer {
    uint32_t        slot;
    int             out_fd;
    pthread_mutex_t send_mu;
    atomic_bool     connected;
    atomic_uint     generation;
    uint32_t        backoff_ms;
    struct sockaddr_in addr;
} dsm_peer_t;

/* Estructura de conexión entrante */
typedef struct dsm_inbound {
    int             fd;
    uint32_t        peer_slot;
    pthread_t       reader;
    atomic_bool     alive;
} dsm_inbound_t;

/* Solicitud a la espera de respuesta asíncrona */
typedef struct dsm_pending {
    uint32_t            req_id;
    uint32_t            page_id;
    sem_t               sem;
    uint16_t            status;
    uint8_t             completed;
    uint8_t            *rx_payload;
    uint32_t            rx_len;
    uint32_t            rx_capacity;
    uint32_t            rx_version;
    int                 result;
    struct dsm_pending *next;
} pending_req_t;

typedef struct dsm_net {
    int              listen_fd;
    uint16_t         listen_port;
    dsm_peer_t       peers[MAX_NODES];
    dsm_inbound_t    inbound[MAX_NODES];
    pthread_mutex_t  pending_mu;
    pending_req_t   *pending[1024];
    pthread_t        listener;
    atomic_uint_fast64_t req_counter;
} dsm_net_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

int  net_init(dsm_ctx_t *ctx);
void net_destroy(dsm_ctx_t *ctx);
int  net_start_listener(dsm_ctx_t *ctx);
int  net_stop_listener(dsm_ctx_t *ctx);

int  dsm_net_send(dsm_ctx_t *ctx, uint32_t dst, const msg_header_t *hdr, const void *payload);
int  dsm_net_rpc(dsm_ctx_t *ctx, uint32_t dst, uint16_t type, uint32_t page_id,
                 uint32_t tseq, uint8_t access, uint8_t flags,
                 const void *tx_payload, uint32_t tx_len,
                 uint16_t *out_status, void *rx_payload, uint32_t *rx_len);

int  dsm_net_read_one(int fd, uint32_t cluster_id, dsm_msg_t *out, uint8_t **owned);
int  peer_ensure_connected(dsm_ctx_t *ctx, uint32_t slot);
void peer_mark_down(dsm_ctx_t *ctx, uint32_t slot);

int  net_probe(dsm_ctx_t *ctx, const struct sockaddr_in *addr, welcome_t *out);
int  net_send_heartbeat(dsm_ctx_t *ctx, uint32_t dst, uint32_t free_pages);
int  net_send_nack(dsm_ctx_t *ctx, uint32_t dst, uint32_t req_id, uint16_t status);

#endif /* DSM_NET_H */
