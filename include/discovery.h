#ifndef DSM_DISCOVERY_H
#define DSM_DISCOVERY_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "dsm_types.h"
#include "wire.h"

/* Estructura de información de un nodo en la membresía local (56 B) */
typedef struct {
    uint8_t  uuid[16];
    uint64_t incarnation;
    uint64_t last_hb_ms;      /* Marca de tiempo monotónica del último latido */
    uint32_t slot;
    uint32_t ip;              /* Orden de red IPv4 */
    uint32_t mem_pages;
    uint32_t free_pages;
    uint16_t port;            /* Orden de host */
    uint8_t  state;           /* ALIVE, SUSPECT, DEAD */
    uint8_t  missed_hb;
    /* Un cliente se une para usar la memoria del cluster sin aportar páginas:
     * no proyecta vnodes en el anillo y nunca es custodio. */
    uint8_t  is_client;
} node_info_t;

_Static_assert(sizeof(node_info_t) == 56, "node_info_t debe ocupar 56 bytes");

typedef struct {
    node_info_t      nodes[MAX_NODES];
    uint8_t          present[MAX_NODES / 8];
    uint64_t         reusable_after_ms[MAX_NODES];
    uint32_t         self_slot;
    uint8_t          self_uuid[16];
    /* IP de este nodo observada en una conexion entrante o saliente. Cero
     * mientras no se conozca una direccion alcanzable. */
    uint32_t         self_ip;
    uint64_t         epoch;
    uint64_t         election_tiebreak;
    /*
     * Una ranura reclamada mientras discovery_join no confirma el reparto es
     * provisional: el titular la cede a un nodo de UUID menor que todavia esta
     * arrancando. Ya confirmada, la defiende con un rechazo para que un nodo
     * nuevo no reubique al cluster en marcha.
     */
    atomic_bool      claim_pending;
    const struct dsm_config *cfg;   /* Página y vnodes por MiB del cluster */
    pthread_rwlock_t rw;
} dsm_membership_t;

/* Forward declaration del contexto global */
struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

/* --- Gestión de descubrimiento y conexión --- */

int  uuid_load_or_create(dsm_ctx_t *ctx, uint8_t out[16]);
int  discovery_bind_first_free(dsm_ctx_t *ctx);
int  discovery_scan(dsm_ctx_t *ctx, welcome_t *out);
int  discovery_join(dsm_ctx_t *ctx);
void discovery_leave(dsm_ctx_t *ctx);

/* --- Gestión de tabla de membresía --- */

int  membership_init(dsm_ctx_t *ctx);
void membership_destroy(dsm_ctx_t *ctx);
void membership_add_self(dsm_ctx_t *ctx);
void membership_observe_self_ip(dsm_ctx_t *ctx, uint32_t ip);
void membership_observe_peer_ip(dsm_ctx_t *ctx, const uint8_t uuid[16],
                                uint32_t ip);
void membership_claim_slot(dsm_ctx_t *ctx, uint32_t slot);
uint32_t membership_first_free_slot(dsm_ctx_t *ctx);
uint32_t membership_next_free_slot(dsm_ctx_t *ctx, uint32_t from);

void membership_on_heartbeat(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_on_join(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_on_join_fd(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd);
void membership_on_join_ack(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_on_node_down(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_on_leave(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_on_resp(dsm_ctx_t *ctx, const dsm_msg_t *m);
void membership_send_resp(dsm_ctx_t *ctx, uint32_t dst);

/* Recuperación de coherencia tras una baja: reasigna las páginas, los
 * cerrojos y la paridad del slot indicado. La ejecuta un hilo worker a
 * partir del mensaje sintético que encola quien cierra la ranura. */
void membership_node_down_recover(dsm_ctx_t *ctx, uint32_t slot);

/* Atiende una QUERY_REQ de dsmctl y responde por el socket de la petición. */
void membership_handle_query_fd(dsm_ctx_t *ctx, const dsm_msg_t *m, int cfd);

void membership_merge_wire(dsm_membership_t *m, const welcome_t *w);
bool membership_is_alive(dsm_ctx_t *ctx, uint32_t slot);
bool membership_slot_present(dsm_ctx_t *ctx, uint32_t slot);
void membership_evaluate(dsm_ctx_t *ctx, uint64_t now_ms);
uint32_t membership_total_pages(dsm_ctx_t *ctx);

/* --- Hilo de latidos (Heartbeat) --- */

int  hb_start(dsm_ctx_t *ctx);
void hb_stop(dsm_ctx_t *ctx);

#endif /* DSM_DISCOVERY_H */
