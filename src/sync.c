#include "sync.h"
#include "dsm_internal.h"
#include "ring.h"
#include "log.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>

/*
 * Cerrojos y barreras distribuidas con custodio determinista.
 *
 * El custodio de un cerrojo o de una barrera es el nodo que el anillo de
 * dispersion asigna a su identificador, el mismo calculo que usan todos los
 * nodos con la misma membresia. El custodio mantiene el dueno y la cola de
 * espera de cada cerrojo; los demas nodos le piden la concesion y esperan la
 * respuesta.
 *
 * Cada espera de la aplicacion usa un semaforo propio. Los cerrojos remotos
 * reutilizan la tabla de peticiones en vuelo de net.c: MSG_LOCK_GRANT y
 * MSG_NACK son mensajes de respuesta y el hilo listener completa la peticion
 * desde pending_complete. Las barreras y las concesiones locales usan la tabla
 * dsm_sync_pending_t de este modulo, que despierta el manejador del mensaje
 * MSG_BARRIER_GO o la propia concesion local.
 *
 * El mutex del directorio solo cubre el acceso a indices, contadores y colas:
 * nunca se retiene durante un envio ni durante una espera de red. La propiedad
 * de un cerrojo se registra por hilo, de modo que dos hilos del mismo nodo
 * compiten igual que dos nodos distintos.
 */

/* Dominio separado para el custodio de barreras: evita que un cerrojo y una
 * barrera con el mismo identificador compartan nodo custodio. */
#define SYNC_BARRIER_DOMAIN UINT32_C(0x9E3779B9)

/* --- Utilidades de tiempo y de la tabla de peticiones --- */

static uint32_t sync_new_req(dsm_ctx_t *ctx)
{
    return (uint32_t)atomic_fetch_add(&ctx->net.req_counter, 1) + 1u;
}

static void sync_deadline(struct timespec *ts, uint32_t ms)
{
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec  += (time_t)(ms / 1000u);
    ts->tv_nsec += (long)(ms % 1000u) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec++;
        ts->tv_nsec -= 1000000000L;
    }
}

static uint32_t sync_timeout_ms(dsm_ctx_t *ctx)
{
    return ctx->cfg.lock_timeout_ms ? ctx->cfg.lock_timeout_ms : 10000u;
}

/*
 * Registra una espera local en la tabla del modulo. La tabla es fija y las
 * entradas se reutilizan; el semaforo se inicializa una sola vez en sync_init.
 * Devuelve NULL si no queda ninguna entrada libre.
 */
static dsm_sync_pending_t *sync_pending_add(dsm_ctx_t *ctx, uint8_t kind,
                                            uint32_t key)
{
    dsm_sync_pending_t *p = NULL;

    pthread_mutex_lock(&ctx->sync.mu);
    for (int i = 0; i < DSM_SYNC_PENDING; i++) {
        dsm_sync_pending_t *c = &ctx->sync.pending[i];
        if (!c->active) {
            /* Una concesion inmediata puede dejar su permiso en el semaforo
             * sin que el llamador llegue a esperar: se descarta el permiso
             * sobrante para que la siguiente espera no lo confunda con una
             * respuesta. */
            while (sem_trywait(&c->sem) == 0) {
            }
            c->kind   = kind;
            c->key    = key;
            c->req_id = sync_new_req(ctx);
            c->gen    = 0;
            c->done   = 0;
            c->result = DSM_ERR_TIMEOUT;
            c->active = 1;
            p = c;
            break;
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);
    return p;
}

static void sync_pending_release(dsm_ctx_t *ctx, dsm_sync_pending_t *p)
{
    pthread_mutex_lock(&ctx->sync.mu);
    p->active = 0;
    p->done   = 0;
    pthread_mutex_unlock(&ctx->sync.mu);
}

/*
 * Marca la espera como completada y despierta al hilo que la sostiene. Se
 * invoca con sync.mu tomado, igual que pending_complete en net.c: el post solo
 * ocurre mientras el mutex garantiza que la entrada sigue siendo del llamante.
 */
static void sync_pending_complete_locked(dsm_ctx_t *ctx, dsm_sync_pending_t *p,
                                         int result)
{
    (void)ctx;
    p->result = result;
    p->done   = 1;
    if (sem_post(&p->sem) != 0) {
        p->result = DSM_ERR_NET;
    }
}

/* Despierta la espera concreta identificada por req_id. */
static void sync_pending_wake_req(dsm_ctx_t *ctx, uint32_t req_id, int result)
{
    pthread_mutex_lock(&ctx->sync.mu);
    for (int i = 0; i < DSM_SYNC_PENDING; i++) {
        dsm_sync_pending_t *c = &ctx->sync.pending[i];
        if (c->active && c->req_id == req_id && !c->done) {
            sync_pending_complete_locked(ctx, c, result);
            break;
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);
}

/*
 * Despierta las esperas de barrera de un identificador cuya generacion es
 * anterior a la anunciada. Asi un aviso repetido o atrasado no libera a quien
 * todavia no ha llegado a la ronda siguiente.
 */
static void sync_pending_wake_barrier(dsm_ctx_t *ctx, uint32_t key,
                                      uint32_t generation)
{
    pthread_mutex_lock(&ctx->sync.mu);
    for (int i = 0; i < DSM_SYNC_PENDING; i++) {
        dsm_sync_pending_t *c = &ctx->sync.pending[i];
        if (c->active && c->kind == DSM_SYNC_WAIT_BARRIER && c->key == key &&
            !c->done && c->gen < generation) {
            c->gen = generation;
            sync_pending_complete_locked(ctx, c, DSM_OK);
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);
}

/* Espera acotada sobre el semaforo de una entrada local. */
static int sync_pending_wait(dsm_ctx_t *ctx, dsm_sync_pending_t *p,
                             uint32_t timeout_ms)
{
    struct timespec deadline;
    int rc;

    (void)ctx;
    sync_deadline(&deadline, timeout_ms);
    do {
        rc = sem_timedwait(&p->sem, &deadline);
    } while (rc != 0 && errno == EINTR);

    if (rc != 0) {
        return (errno == ETIMEDOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
    }
    return p->result;
}

/* --- Tabla de cerrojos del custodio --- */

/* Slot de directorio de un cerrojo. Con create en cero solo busca uno ya
 * existente y devuelve NULL si no lo hay. */
static dsm_lock_dir_t *sync_lock_slot(dsm_ctx_t *ctx, uint32_t id, int create)
{
    uint32_t h = (id * UINT32_C(2654435761)) % DSM_LOCK_SLOTS;
    dsm_lock_dir_t *free_slot = NULL;

    for (uint32_t i = 0; i < DSM_LOCK_SLOTS; i++) {
        dsm_lock_dir_t *e = &ctx->sync.locks[(h + i) % DSM_LOCK_SLOTS];
        if (e->used && e->lock_id == id) return e;
        if (!e->used && !free_slot) free_slot = e;
    }
    if (!create || !free_slot) return NULL;

    memset(free_slot, 0, sizeof(*free_slot));
    free_slot->used       = 1;
    free_slot->lock_id    = id;
    free_slot->owner_slot = DSM_SLOT_NONE;
    return free_slot;
}

static int sync_lock_queue_push(dsm_lock_dir_t *e, uint32_t slot, uint32_t req)
{
    if (e->count >= DSM_LOCK_QUEUE) return 0;
    e->wait_queue[e->tail] = slot;
    e->wait_req[e->tail]   = req;
    e->tail = (uint8_t)((e->tail + 1) % DSM_LOCK_QUEUE);
    e->count++;
    return 1;
}

static int sync_lock_queue_pop(dsm_lock_dir_t *e, uint32_t *slot, uint32_t *req)
{
    if (e->count == 0) return 0;
    *slot = e->wait_queue[e->head];
    *req  = e->wait_req[e->head];
    e->head = (uint8_t)((e->head + 1) % DSM_LOCK_QUEUE);
    e->count--;
    return 1;
}

static void sync_lock_queue_remove(dsm_lock_dir_t *e, uint32_t slot,
                                   uint32_t req)
{
    uint32_t n = e->count;
    uint32_t out_slot[DSM_LOCK_QUEUE];
    uint32_t out_req[DSM_LOCK_QUEUE];
    uint32_t kept = 0;

    for (uint32_t i = 0; i < n; i++) {
        uint32_t idx = (uint32_t)((e->head + i) % DSM_LOCK_QUEUE);
        if (e->wait_queue[idx] == slot && e->wait_req[idx] == req) {
            continue;
        }
        out_slot[kept] = e->wait_queue[idx];
        out_req[kept]  = e->wait_req[idx];
        kept++;
    }

    for (uint32_t i = 0; i < kept; i++) {
        uint32_t idx = (uint32_t)((e->head + i) % DSM_LOCK_QUEUE);
        e->wait_queue[idx] = out_slot[i];
        e->wait_req[idx]   = out_req[i];
    }
    e->count = (uint8_t)kept;
    e->tail  = (uint8_t)((e->head + kept) % DSM_LOCK_QUEUE);
}

/* --- Envio de mensajes de sincronizacion --- */

/*
 * Envia una trama de sincronizacion sobre un enlace recien abierto. El
 * descubrimiento deja una conexion saliente sin usar hacia el par, y el
 * listener la cierra al vencer su plazo de lectura; un mensaje que reutilizara
 * ese descriptor se perderia sin que el emisor lo note. Cerrar el enlace antes
 * de enviar obliga a peer_ensure_connected a abrir uno nuevo, y el reintento
 * cubre el caso de una conexion caida a mitad del envio.
 */
static int sync_send_msg(dsm_ctx_t *ctx, uint32_t dst, const msg_header_t *hdr,
                         const void *payload)
{
    peer_mark_down(ctx, dst);
    int rc = dsm_net_send(ctx, dst, hdr, payload);
    if (rc != DSM_OK) {
        rc = dsm_net_send(ctx, dst, hdr, payload);
    }
    return rc;
}

static int sync_send_lock_grant(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                                uint32_t req_id)
{
    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_LOCK_GRANT;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.page_id       = id;
    hdr.epoch         = ctx->memb.epoch;

    return sync_send_msg(ctx, dst, &hdr, NULL);
}

static int sync_send_lock_req(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                              uint32_t req_id)
{
    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_LOCK_REQ;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.page_id       = id;
    hdr.epoch         = ctx->memb.epoch;

    return sync_send_msg(ctx, dst, &hdr, NULL);
}

static int sync_send_lock_rel(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                              uint32_t req_id)
{
    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_LOCK_REL;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.page_id       = id;
    hdr.epoch         = ctx->memb.epoch;

    return sync_send_msg(ctx, dst, &hdr, NULL);
}

static int sync_send_barrier_arrive(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                                    uint32_t n, uint32_t req_id)
{
    uint8_t payload[8];
    uint32_t n_net  = htonl(n);
    uint32_t gen_net = htonl(0);
    memcpy(payload, &n_net, 4);
    memcpy(payload + 4, &gen_net, 4);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_BARRIER_ARRIVE;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.req_id        = req_id;
    hdr.page_id       = id;
    hdr.epoch         = ctx->memb.epoch;
    hdr.payload_len   = 8;

    return sync_send_msg(ctx, dst, &hdr, payload);
}

static int sync_send_barrier_go(dsm_ctx_t *ctx, uint32_t dst, uint32_t id,
                                uint32_t generation)
{
    uint8_t payload[4];
    uint32_t gen_net = htonl(generation);
    memcpy(payload, &gen_net, 4);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_BARRIER_GO;
    hdr.src_slot      = ctx->memb.self_slot;
    hdr.dst_slot      = dst;
    hdr.page_id       = id;
    hdr.epoch         = ctx->memb.epoch;
    hdr.payload_len   = 4;

    return sync_send_msg(ctx, dst, &hdr, payload);
}

/* --- Peticiones de cerrojo en la tabla de red --- */

/* Enlaza una peticion en la tabla que consulta pending_complete. */
static void sync_net_pending_add(dsm_ctx_t *ctx, pending_req_t *pending)
{
    uint32_t bucket = pending->req_id % 1024u;
    pthread_mutex_lock(&ctx->net.pending_mu);
    pending->next = ctx->net.pending[bucket];
    ctx->net.pending[bucket] = pending;
    pthread_mutex_unlock(&ctx->net.pending_mu);
}

static void sync_net_pending_del(dsm_ctx_t *ctx, pending_req_t *pending)
{
    uint32_t bucket = pending->req_id % 1024u;

    pthread_mutex_lock(&ctx->net.pending_mu);
    pending_req_t **link = &ctx->net.pending[bucket];
    while (*link) {
        if (*link == pending) {
            *link = pending->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&ctx->net.pending_mu);
}

/* --- Propiedad local del cerrojo --- */

static dsm_sync_held_t *sync_held_find_locked(dsm_ctx_t *ctx, uint32_t id)
{
    pthread_t self = pthread_self();
    for (int i = 0; i < DSM_MAX_HELD; i++) {
        dsm_sync_held_t *h = &ctx->sync.held[i];
        if (h->active && h->lock_id == id && pthread_equal(h->owner, self)) {
            return h;
        }
    }
    return NULL;
}

static int sync_held_add(dsm_ctx_t *ctx, uint32_t id, uint32_t req_id)
{
    int rc = DSM_ERR_NO_SPACE;

    pthread_mutex_lock(&ctx->sync.mu);
    for (int i = 0; i < DSM_MAX_HELD; i++) {
        dsm_sync_held_t *h = &ctx->sync.held[i];
        if (!h->active) {
            h->lock_id = id;
            h->req_id  = req_id;
            h->owner   = pthread_self();
            h->active  = 1;
            rc = DSM_OK;
            break;
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);
    return rc;
}

static int sync_held_remove(dsm_ctx_t *ctx, uint32_t id, uint32_t *out_req)
{
    int rc = DSM_ERR_INVALID;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_sync_held_t *h = sync_held_find_locked(ctx, id);
    if (h) {
        if (out_req) *out_req = h->req_id;
        h->active = 0;
        rc = DSM_OK;
    }
    pthread_mutex_unlock(&ctx->sync.mu);
    return rc;
}

/* --- Concesion en el custodio --- */

/*
 * Entrega el cerrojo al siguiente de la cola o lo deja libre. Se invoca sin el
 * mutex tomado y no lo retiene durante el envio de la concesion.
 */
static void sync_lock_handoff(dsm_ctx_t *ctx, dsm_lock_dir_t *e)
{
    for (;;) {
        uint32_t dst = DSM_SLOT_NONE;
        uint32_t req = 0;
        uint32_t id;

        pthread_mutex_lock(&ctx->sync.mu);
        if (e->owner_slot != DSM_SLOT_NONE) {
            pthread_mutex_unlock(&ctx->sync.mu);
            return;
        }
        if (!sync_lock_queue_pop(e, &dst, &req)) {
            pthread_mutex_unlock(&ctx->sync.mu);
            return;
        }
        e->owner_slot = dst;
        e->owner_req  = req;
        id = e->lock_id;
        pthread_mutex_unlock(&ctx->sync.mu);

        if (dst == ctx->memb.self_slot) {
            sync_pending_wake_req(ctx, req, DSM_OK);
            return;
        }

        if (sync_send_lock_grant(ctx, dst, id, req) == DSM_OK) {
            return;
        }

        /* Sin enlace con el solicitante: se deshace la concesion y se atiende
         * al siguiente para no dejar el cerrojo retenido por una peticion que
         * nunca llego a su destino. */
        pthread_mutex_lock(&ctx->sync.mu);
        if (e->owner_slot == dst && e->owner_req == req) {
            e->owner_slot = DSM_SLOT_NONE;
            e->owner_req  = 0;
        }
        pthread_mutex_unlock(&ctx->sync.mu);
    }
}

static void sync_lock_release(dsm_ctx_t *ctx, dsm_lock_dir_t *e,
                              uint32_t owner_slot, uint32_t owner_req)
{
    int released = 0;

    pthread_mutex_lock(&ctx->sync.mu);
    if (e->owner_slot == owner_slot && e->owner_req == owner_req) {
        e->owner_slot = DSM_SLOT_NONE;
        e->owner_req  = 0;
        released = 1;
    }
    pthread_mutex_unlock(&ctx->sync.mu);

    if (released) {
        sync_lock_handoff(ctx, e);
    }
}

/* --- Concesion local directa --- */

/*
 * Intenta conceder el cerrojo en el custodio local. Devuelve DSM_OK cuando la
 * concesion ya se registro, DSM_ERR_AGAIN cuando la peticion quedo en cola y
 * un error negativo cuando no hay recursos.
 */
static int sync_lock_local_request(dsm_ctx_t *ctx, dsm_sync_pending_t *p,
                                   uint32_t id)
{
    uint32_t self = ctx->memb.self_slot;
    int rc = DSM_ERR_AGAIN;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_lock_dir_t *e = sync_lock_slot(ctx, id, 1);
    if (!e) {
        pthread_mutex_unlock(&ctx->sync.mu);
        return DSM_ERR_NO_SPACE;
    }
    if (e->owner_slot == DSM_SLOT_NONE && e->count == 0) {
        e->owner_slot = self;
        e->owner_req  = p->req_id;
        sync_pending_complete_locked(ctx, p, DSM_OK);
        rc = DSM_OK;
    } else if (!sync_lock_queue_push(e, self, p->req_id)) {
        rc = DSM_ERR_NO_SPACE;
    }
    pthread_mutex_unlock(&ctx->sync.mu);
    return rc;
}

/* Retira una peticion propia de la cola tras agotarse el plazo. */
static void sync_lock_local_withdraw(dsm_ctx_t *ctx, uint32_t id, uint32_t req)
{
    uint32_t self = ctx->memb.self_slot;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_lock_dir_t *e = sync_lock_slot(ctx, id, 0);
    if (e) {
        if (e->owner_slot == self && e->owner_req == req) {
            e->owner_slot = DSM_SLOT_NONE;
            e->owner_req  = 0;
        } else {
            sync_lock_queue_remove(e, self, req);
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);

    if (e) {
        sync_lock_handoff(ctx, e);
    }
}

/* --- API de cerrojos --- */

int sync_lock(dsm_ctx_t *ctx, uint32_t lock_id)
{
    uint32_t self = ctx->memb.self_slot;
    uint32_t home = ring_home_of(ctx, lock_id);
    uint32_t timeout_ms = sync_timeout_ms(ctx);
    dsm_sync_pending_t *p;
    int rc;

    if (self == DSM_SLOT_NONE) return DSM_ERR_NOT_INIT;

    pthread_mutex_lock(&ctx->sync.mu);
    int already = (sync_held_find_locked(ctx, lock_id) != NULL);
    pthread_mutex_unlock(&ctx->sync.mu);
    if (already) return DSM_ERR_BUSY;

    p = sync_pending_add(ctx, DSM_SYNC_WAIT_LOCK, lock_id);
    if (!p) return DSM_ERR_NO_SPACE;

    if (home == self) {
        rc = sync_lock_local_request(ctx, p, lock_id);
        if (rc == DSM_OK) {
            uint32_t req = p->req_id;
            sync_pending_release(ctx, p);
            rc = sync_held_add(ctx, lock_id, req);
            if (rc != DSM_OK) {
                sync_lock_local_withdraw(ctx, lock_id, req);
            }
            return rc;
        }
        if (rc == DSM_ERR_AGAIN) {
            rc = sync_pending_wait(ctx, p, timeout_ms);
            if (rc == DSM_OK) {
                uint32_t req = p->req_id;
                sync_pending_release(ctx, p);
                rc = sync_held_add(ctx, lock_id, req);
                if (rc != DSM_OK) {
                    sync_lock_local_withdraw(ctx, lock_id, req);
                }
                return rc;
            }
            sync_lock_local_withdraw(ctx, lock_id, p->req_id);
        }
        sync_pending_release(ctx, p);
        return rc;
    }

    /* Cerrojo remoto: el semaforo vive en una pending_req_t de la tabla de red,
     * igual que en dsm_net_rpc, y MSG_LOCK_GRANT lo despierta desde el
     * listener. */
    pending_req_t pending;
    memset(&pending, 0, sizeof(pending));
    pending.req_id = p->req_id;
    pending.result = DSM_ERR_TIMEOUT;
    uint32_t req_id = p->req_id;
    sync_pending_release(ctx, p);

    if (sem_init(&pending.sem, 0, 0) != 0) {
        return DSM_ERR_MEM;
    }
    sync_net_pending_add(ctx, &pending);

    rc = sync_send_lock_req(ctx, home, lock_id, req_id);
    if (rc != DSM_OK) {
        sync_net_pending_del(ctx, &pending);
        sem_destroy(&pending.sem);
        return rc;
    }

    struct timespec deadline;
    sync_deadline(&deadline, timeout_ms);
    int wait_rc;
    do {
        wait_rc = sem_timedwait(&pending.sem, &deadline);
    } while (wait_rc != 0 && errno == EINTR);

    if (wait_rc != 0) {
        int late_completed;
        /* Se retira de la tabla y luego se mira el semaforo: una concesion
         * posterior llega al socket y se descarta, mientras que una que ya
         * dejo su post se sigue viendo como permiso pendiente. */
        sync_net_pending_del(ctx, &pending);
        late_completed = (sem_trywait(&pending.sem) == 0);
        sem_destroy(&pending.sem);
        if (late_completed) {
            rc = sync_held_add(ctx, lock_id, req_id);
            if (rc != DSM_OK) {
                (void)sync_send_lock_rel(ctx, home, lock_id, req_id);
            }
            return rc;
        }
        /* El custodio puede habernos concedido tras el plazo: el aviso le
         * permite liberar la concesion vencida o retirar la peticion. */
        (void)sync_send_lock_rel(ctx, home, lock_id, req_id);
        return DSM_ERR_TIMEOUT;
    }

    sync_net_pending_del(ctx, &pending);
    rc = pending.result;
    uint16_t status = pending.status;
    sem_destroy(&pending.sem);

    if (rc != DSM_OK) return rc;
    if (status != DSM_ST_OK) {
        return (status == DSM_ST_BUSY) ? DSM_ERR_BUSY : DSM_ERR_NO_SPACE;
    }
    rc = sync_held_add(ctx, lock_id, req_id);
    if (rc != DSM_OK) {
        /* Sin entrada local no hay forma de liberar el cerrojo despues: se
         * devuelve al custodio y se informa el fallo a continuacion. */
        (void)sync_send_lock_rel(ctx, home, lock_id, req_id);
    }
    return rc;
}

int sync_unlock(dsm_ctx_t *ctx, uint32_t lock_id)
{
    uint32_t req_id = 0;
    uint32_t self = ctx->memb.self_slot;
    int rc = sync_held_remove(ctx, lock_id, &req_id);
    if (rc != DSM_OK) return rc;

    uint32_t home = ring_home_of(ctx, lock_id);
    if (home == self) {
        pthread_mutex_lock(&ctx->sync.mu);
        dsm_lock_dir_t *e = sync_lock_slot(ctx, lock_id, 0);
        pthread_mutex_unlock(&ctx->sync.mu);
        if (e) {
            sync_lock_release(ctx, e, self, req_id);
        }
        return DSM_OK;
    }

    return sync_send_lock_rel(ctx, home, lock_id, req_id);
}

/* --- Barreras --- */

static dsm_barrier_dir_t *sync_barrier_slot(dsm_ctx_t *ctx, uint32_t id)
{
    uint32_t h = (id * UINT32_C(2654435761)) % DSM_BARRIER_SLOTS;

    for (uint32_t i = 0; i < DSM_BARRIER_SLOTS; i++) {
        dsm_barrier_dir_t *b = &ctx->sync.barriers[(h + i) % DSM_BARRIER_SLOTS];
        if (b->used && b->barrier_id == id) return b;
        if (!b->used) {
            memset(b, 0, sizeof(*b));
            b->used       = 1;
            b->barrier_id = id;
            b->threshold  = 0;
            b->count      = 0;
            b->generation = 1;
            return b;
        }
    }
    return NULL;
}

/* Anota un arribo en el custodio y difunde MSG_BARRIER_GO al completarse. */
static int sync_barrier_arrive(dsm_ctx_t *ctx, uint32_t id, uint32_t n,
                               uint32_t src)
{
    if (src >= MAX_NODES || n == 0) return DSM_ERR_INVALID;

    uint32_t release[MAX_NODES];
    uint32_t nrelease = 0;
    uint32_t generation;
    uint32_t self = ctx->memb.self_slot;
    int broadcast = 0;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_barrier_dir_t *b = sync_barrier_slot(ctx, id);
    if (!b) {
        pthread_mutex_unlock(&ctx->sync.mu);
        return DSM_ERR_NO_SPACE;
    }
    if (b->count == 0) {
        b->threshold = n;
        memset(b->arrived_map, 0, sizeof(b->arrived_map));
    }
    if (b->threshold == 0) b->threshold = n;

    uint32_t byte = src / 8u;
    uint8_t  bit  = (uint8_t)(1u << (src % 8u));
    if ((b->arrived_map[byte] & bit) == 0) {
        b->arrived_map[byte] |= bit;
        b->count++;
    }

    if (b->count >= b->threshold) {
        for (uint32_t s = 0; s < MAX_NODES; s++) {
            if ((b->arrived_map[s / 8u] & (1u << (s % 8u))) != 0 &&
                s != self) {
                release[nrelease++] = s;
            }
        }
        broadcast  = 1;
        generation = b->generation;
        b->count      = 0;
        b->threshold  = 0;
        b->generation = generation + 1;
        memset(b->arrived_map, 0, sizeof(b->arrived_map));
    }
    pthread_mutex_unlock(&ctx->sync.mu);

    if (!broadcast) return DSM_OK;

    for (uint32_t i = 0; i < nrelease; i++) {
        if (sync_send_barrier_go(ctx, release[i], id, generation) != DSM_OK) {
            DSM_LOG_WARN("No se pudo difundir BARRIER_GO al slot %u", release[i]);
        }
    }
    /* El custodio tambien participa: su espera local se completa aqui, igual
     * que la de cualquier otro participante de la ronda cerrada. */
    sync_pending_wake_barrier(ctx, id, generation + 1);
    return DSM_OK;
}

int sync_barrier(dsm_ctx_t *ctx, uint32_t barrier_id, uint32_t n)
{
    uint32_t self = ctx->memb.self_slot;
    uint32_t home = ring_home_of(ctx, barrier_id ^ SYNC_BARRIER_DOMAIN);
    uint32_t timeout_ms = sync_timeout_ms(ctx);
    int rc;

    if (n == 0) return DSM_ERR_INVALID;
    if (self == DSM_SLOT_NONE) return DSM_ERR_NOT_INIT;

    dsm_sync_pending_t *p = sync_pending_add(ctx, DSM_SYNC_WAIT_BARRIER,
                                             barrier_id);
    if (!p) return DSM_ERR_NO_SPACE;

    if (home == self) {
        rc = sync_barrier_arrive(ctx, barrier_id, n, self);
    } else {
        rc = sync_send_barrier_arrive(ctx, home, barrier_id, n, p->req_id);
    }

    if (rc != DSM_OK) {
        sync_pending_release(ctx, p);
        return rc;
    }

    rc = sync_pending_wait(ctx, p, timeout_ms);
    sync_pending_release(ctx, p);
    return rc;
}

/* --- Manejadores de mensajes --- */

void sync_on_lock_req(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t id  = m->hdr.page_id;
    uint32_t src = m->hdr.src_slot;
    uint32_t req = m->hdr.req_id;
    uint32_t dst = DSM_SLOT_NONE;

    if (src >= MAX_NODES) return;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_lock_dir_t *e = sync_lock_slot(ctx, id, 1);
    if (!e) {
        pthread_mutex_unlock(&ctx->sync.mu);
        (void)net_send_nack(ctx, src, req, DSM_ST_NO_SPACE);
        return;
    }
    if (e->owner_slot == DSM_SLOT_NONE && e->count == 0) {
        e->owner_slot = src;
        e->owner_req  = req;
        dst = src;
    } else if (!sync_lock_queue_push(e, src, req)) {
        pthread_mutex_unlock(&ctx->sync.mu);
        (void)net_send_nack(ctx, src, req, DSM_ST_BUSY);
        return;
    }
    pthread_mutex_unlock(&ctx->sync.mu);

    if (dst != DSM_SLOT_NONE &&
        sync_send_lock_grant(ctx, dst, id, req) != DSM_OK) {
        pthread_mutex_lock(&ctx->sync.mu);
        if (e->owner_slot == dst && e->owner_req == req) {
            e->owner_slot = DSM_SLOT_NONE;
            e->owner_req  = 0;
        }
        pthread_mutex_unlock(&ctx->sync.mu);
        sync_lock_handoff(ctx, e);
    }
}

void sync_on_lock_grant(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    /* El listener completa las concesiones desde la tabla de red. Una
     * concesion que llega hasta el worker no tiene peticion registrada: el
     * custodio ya la dio por entregada, asi que se devuelve de inmediato para
     * que no quede retenida por un solicitante que ya no la espera. */
    uint32_t src = m->hdr.src_slot;

    if (src >= MAX_NODES || src == ctx->memb.self_slot) return;
    (void)sync_send_lock_rel(ctx, src, m->hdr.page_id, m->hdr.req_id);
}

void sync_on_lock_rel(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t id  = m->hdr.page_id;
    uint32_t src = m->hdr.src_slot;
    uint32_t req = m->hdr.req_id;
    int released = 0;

    if (src >= MAX_NODES) return;

    pthread_mutex_lock(&ctx->sync.mu);
    dsm_lock_dir_t *e = sync_lock_slot(ctx, id, 0);
    if (e) {
        if (e->owner_slot == src && e->owner_req == req) {
            e->owner_slot = DSM_SLOT_NONE;
            e->owner_req  = 0;
            released = 1;
        } else {
            sync_lock_queue_remove(e, src, req);
        }
    }
    pthread_mutex_unlock(&ctx->sync.mu);

    if (e && released) {
        sync_lock_handoff(ctx, e);
    }
}

void sync_on_barrier_arrive(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!m->payload || m->hdr.payload_len < 8) return;

    uint32_t n_net;
    memcpy(&n_net, m->payload, 4);
    uint32_t n = ntohl(n_net);

    (void)sync_barrier_arrive(ctx, m->hdr.page_id, n, m->hdr.src_slot);
}

void sync_on_barrier_go(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t gen = 0;

    if (m->payload && m->hdr.payload_len >= 4) {
        uint32_t gen_net;
        memcpy(&gen_net, m->payload, 4);
        gen = ntohl(gen_net);
    }

    /* El aviso corresponde a la ronda 'gen': libera a los participantes que
     * esperan una generacion anterior y deja intactas las esperas posteriores. */
    sync_pending_wake_barrier(ctx, m->hdr.page_id, gen + 1);
}

/* --- Ciclo de vida --- */

int sync_init(dsm_ctx_t *ctx)
{
    memset(&ctx->sync, 0, sizeof(ctx->sync));
    if (pthread_mutex_init(&ctx->sync.mu, NULL) != 0) {
        return DSM_ERR_MEM;
    }
    for (int i = 0; i < DSM_LOCK_SLOTS; i++) {
        ctx->sync.locks[i].owner_slot = DSM_SLOT_NONE;
    }
    for (int i = 0; i < DSM_SYNC_PENDING; i++) {
        if (sem_init(&ctx->sync.pending[i].sem, 0, 0) != 0) {
            for (int j = 0; j < i; j++) {
                sem_destroy(&ctx->sync.pending[j].sem);
            }
            pthread_mutex_destroy(&ctx->sync.mu);
            return DSM_ERR_MEM;
        }
    }
    return DSM_OK;
}

void sync_destroy(dsm_ctx_t *ctx)
{
    for (int i = 0; i < DSM_SYNC_PENDING; i++) {
        sem_destroy(&ctx->sync.pending[i].sem);
    }
    pthread_mutex_destroy(&ctx->sync.mu);
}
