#include "msi.h"
#include "dsm_internal.h"

#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>

static bool copyset_has(const dir_entry_t *entry, uint32_t slot)
{
    return slot < MAX_NODES &&
           (entry->copyset[slot / 8] & (uint8_t)(1u << (slot % 8))) != 0;
}

static void copyset_set(dir_entry_t *entry, uint32_t slot)
{
    if (slot < MAX_NODES) {
        entry->copyset[slot / 8] |= (uint8_t)(1u << (slot % 8));
    }
}

static void page_invalidate_local(dsm_ctx_t *ctx, uint32_t page);

/* --- Identidad del home de cada página ---
 *
 * home_map guarda el último home calculado para cada página. Cuando el anillo
 * asigna la página a otro nodo, el custodio anterior todavía conserva el
 * contenido y lo entrega al nuevo home; sin ese registro no habría forma de
 * distinguir un nodo que perdió la página de uno que nunca la custodió. */

static uint32_t home_known(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx->msi.home_map || page >= ctx->msi.cap) return DSM_SLOT_NONE;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    uint32_t home = ctx->msi.home_map[page];
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return home;
}

static void home_remember(dsm_ctx_t *ctx, uint32_t page, uint32_t home)
{
    if (!ctx->msi.home_map || page >= ctx->msi.cap) return;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    ctx->msi.home_map[page] = home;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
}

/* Resuelve el home vigente y deja el anterior en 'prev'. */
static uint32_t home_resolve(dsm_ctx_t *ctx, uint32_t page, uint32_t *prev)
{
    uint32_t previous = home_known(ctx, page);
    uint32_t home = ring_home_of(ctx, page);

    if (home != previous) home_remember(ctx, page, home);
    if (prev) *prev = previous;
    return home;
}

/* --- Consultas sobre el contenido local --- */

static dir_entry_t dir_snapshot(dsm_ctx_t *ctx, uint32_t page)
{
    dir_entry_t entry;
    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return entry;
}

static bool owner_alive(dsm_ctx_t *ctx, uint32_t slot)
{
    if (slot == DSM_SLOT_NONE || slot >= MAX_NODES) return false;
    if (slot == ctx->memb.self_slot) return true;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    bool alive = membership_slot_present(ctx, slot) &&
                 ctx->memb.nodes[slot].state != DSM_NODE_DEAD;
    pthread_rwlock_unlock(&ctx->memb.rw);
    return alive;
}

/* Pares vivos distintos del propio, en orden ascendente de ranura. */
static uint32_t live_peers(dsm_ctx_t *ctx, uint32_t *slots)
{
    uint32_t count = 0;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == ctx->memb.self_slot) continue;
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        slots[count++] = s;
    }
    pthread_rwlock_unlock(&ctx->memb.rw);
    return count;
}

static bool page_local_valid(dsm_ctx_t *ctx, uint32_t page)
{
    uint8_t state = ctx->mem.local[page].state;
    return state == DSM_PAGE_S || state == DSM_PAGE_M ||
           dsm_mem_backing_has(&ctx->mem, page);
}

/* Copia el contenido vigente que este nodo puede ofrecer. La copia en M es la
 * escritura pendiente y la copia en S quedó validada por la invalidación previa
 * a toda concesión de escritura. El marco de respaldo solo se usa cuando el
 * dueño en M del directorio ya no está vivo: una escritura posterior en ese nodo
 * deja el marco desactualizado. */
static bool page_can_serve(dsm_ctx_t *ctx, uint32_t page, void *buf)
{
    uint8_t state;
    bool copied = false;

    dsm_mem_stripe_lock(&ctx->mem, page);
    state = ctx->mem.local[page].state;
    if (state == DSM_PAGE_M || state == DSM_PAGE_S) {
        memcpy(buf, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
        copied = true;
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);

    if (state == DSM_PAGE_M) return true;

    if (!copied) {
        dir_entry_t entry = dir_snapshot(ctx, page);
        if (entry.state == DSM_DIR_MODIFIED &&
            entry.owner != ctx->memb.self_slot &&
            owner_alive(ctx, entry.owner)) {
            return false;
        }
    }
    if (copied) return true;

    return dsm_mem_backing_load(&ctx->mem, page, buf) == DSM_OK;
}

/* Persiste en el marco de respaldo el contenido del alias. Solo se invoca desde
 * las rutas en las que este nodo es el home de la página. */
static void page_persist_home(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    (void)dsm_mem_backing_store(&ctx->mem, page,
                                dsm_mem_page_ptr(&ctx->mem, page));
    dsm_mem_stripe_unlock(&ctx->mem, page);
}

/* Suelta el contenido de una página que pasó a otro home: la copia local queda
 * inaccesible y el marco de respaldo se libera, de modo que el nodo deja de ser
 * una fuente posible para esa página. */
static void page_relinquish(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    (void)dsm_mem_backing_drop(&ctx->mem, page);
    dsm_mem_stripe_unlock(&ctx->mem, page);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    dir_entry_t *entry = &ctx->msi.dir[page];
    entry->state = DSM_DIR_UNCACHED;
    entry->owner = DSM_SLOT_NONE;
    memset(entry->copyset, 0, sizeof(entry->copyset));
    pthread_mutex_unlock(&ctx->msi.dir_mu);
}

/* --- Traspaso entre homes --- */

/* Pregunta a los pares vivos por el contenido de una página para la que este
 * nodo acaba de recibir el rol de home. Cualquier copia viva de un par es la
 * versión vigente: las copias en S se invalidan antes de conceder una escritura
 * y la copia en M es la única escritura pendiente. Devuelve DSM_OK si obtuvo
 * datos, DSM_ERR_NOT_FOUND si todos negaron tenerlos y DSM_ERR_NET si alguna
 * consulta no se pudo completar. */
static int page_pull(dsm_ctx_t *ctx, uint32_t page, void *dst)
{
    uint32_t slots[MAX_NODES];
    uint32_t count = live_peers(ctx, slots);
    bool uncertain = false;

    for (uint32_t i = 0; i < count; i++) {
        uint16_t status = DSM_ST_OK;
        uint32_t rx_len = ctx->mem.page_size;
        int rc = dsm_net_rpc(ctx, slots[i], MSG_PAGE_REQUEST, page, 0,
                             DSM_ACCESS_READ,
                             DSM_FLAG_PROBE | DSM_FLAG_WANT_DATA,
                             NULL, 0, &status, dst, &rx_len);
        if (rc == DSM_OK && status == DSM_ST_OK &&
            rx_len == ctx->mem.page_size) {
            return DSM_OK;
        }
        if (rc != DSM_OK) uncertain = true;
    }

    return uncertain ? DSM_ERR_NET : DSM_ERR_NOT_FOUND;
}

/* Entrega al home vigente el contenido de una página que este nodo custodiaba y
 * espera el acuse antes de soltar su copia. Devuelve DSM_OK cuando el nuevo home
 * confirmó que guardó el contenido. */
static int home_transfer(dsm_ctx_t *ctx, uint32_t page, uint32_t home,
                         const void *data)
{
    if (home == DSM_SLOT_NONE || home == ctx->memb.self_slot) {
        return DSM_ERR_INVALID;
    }

    uint16_t status = DSM_ST_OK;
    uint32_t rx_len = 0;
    int rc = dsm_net_rpc(ctx, home, MSG_WRITEBACK, page, 0, DSM_ACCESS_WRITE,
                         DSM_FLAG_HAS_DATA, data, ctx->mem.page_size,
                         &status, NULL, &rx_len);
    if (rc != DSM_OK) return rc;
    return (status == DSM_ST_OK) ? DSM_OK : DSM_ERR_NET;
}

/* Contenido que el home debe servir para una página que no tiene en la vista:
 * su copia viva, el respaldo propio, la de un par que todavía la conserve o
 * ceros si nadie la escribió. 'known' queda en false cuando la consulta a los
 * pares no se pudo completar; el solicitante recibe un rechazo y reintenta. */
static void home_page_content(dsm_ctx_t *ctx, uint32_t page, void *buf,
                              bool *known)
{
    *known = true;

    if (page_can_serve(ctx, page, buf)) return;

    int rc = page_pull(ctx, page, buf);
    if (rc == DSM_OK) {
        (void)dsm_mem_backing_store(&ctx->mem, page, buf);
        dsm_mem_install_page(&ctx->mem, page, buf);
        return;
    }
    if (rc == DSM_ERR_NET) {
        *known = false;
        return;
    }

    memset(buf, 0, ctx->mem.page_size);
}

/* --- Aplicación de contenido y permisos --- */

static int page_apply(dsm_ctx_t *ctx, uint32_t page, dsm_page_state_t state,
                      const void *payload, uint32_t payload_len)
{
    if (page >= ctx->mem.npages) return DSM_ERR_INVALID;
    if (payload_len != 0 && payload_len != ctx->mem.page_size) {
        return DSM_ERR_PROTO;
    }

    dsm_mem_stripe_lock(&ctx->mem, page);
    if (payload_len != 0) {
        dsm_mem_install_page(&ctx->mem, page, payload);
    }

    /* PAGE_DATA/GRANT completan I*_PEND/SM_PEND y abren la vista DSM. */
    int prot = (state == DSM_PAGE_M) ? (PROT_READ | PROT_WRITE) : PROT_READ;
    int rc = dsm_mem_set_prot(&ctx->mem, page, prot);
    if (rc == DSM_OK) {
        ctx->mem.local[page].state = (uint8_t)state;
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);
    return rc;
}

static void page_invalidate_local(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    /* INVALIDATE transforma cualquier copia local en I y la vuelve inaccesible. */
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    dsm_mem_stripe_unlock(&ctx->mem, page);
}

static int invalidate_remote_copies(dsm_ctx_t *ctx, uint32_t page,
                                    const dir_entry_t *entry,
                                    uint32_t keep_slot)
{
    for (uint32_t slot = 0; slot < MAX_NODES; slot++) {
        if (!copyset_has(entry, slot) || slot == keep_slot) {
            continue;
        }
        if (slot == ctx->memb.self_slot) {
            page_invalidate_local(ctx, page);
            atomic_fetch_add_explicit(&ctx->ctr.invalidations, 1,
                                      memory_order_relaxed);
            continue;
        }

        uint16_t status = DSM_ST_OK;
        uint32_t rx_len = 0;
        int rc = dsm_net_rpc(ctx, slot, MSG_INVALIDATE, page, entry->tseq,
                             DSM_ACCESS_WRITE, 0, NULL, 0, &status, NULL,
                             &rx_len);
        if (rc != DSM_OK || status != DSM_ST_OK) {
            return (rc == DSM_OK) ? DSM_ERR_NET : rc;
        }
    }
    return DSM_OK;
}

/* --- Fallo local atendido por el propio home --- */

static int home_local_fault(dsm_ctx_t *ctx, uint32_t page, bool write)
{
    dir_entry_t entry = dir_snapshot(ctx, page);

    uint32_t previous_owner = entry.owner;
    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) return DSM_ERR_MEM;

    bool remote_owner = (entry.state == DSM_DIR_MODIFIED &&
                         entry.owner != DSM_SLOT_NONE &&
                         entry.owner != ctx->memb.self_slot &&
                         owner_alive(ctx, entry.owner));
    bool local_owner = (entry.state == DSM_DIR_MODIFIED &&
                        entry.owner == ctx->memb.self_slot);

    if (remote_owner) {
        uint16_t status = DSM_ST_OK;
        uint32_t data_len = ctx->mem.page_size;
        int rc = dsm_net_rpc(ctx, entry.owner, MSG_DOWNGRADE, page,
                             entry.tseq, write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                             DSM_FLAG_HAS_DATA, NULL, 0, &status, data,
                             &data_len);
        if (rc != DSM_OK || status != DSM_ST_OK ||
            data_len != ctx->mem.page_size) {
            free(data);
            return (rc == DSM_OK) ? DSM_ERR_NET : rc;
        }
    } else if (local_owner) {
        /* Antes de copiar, cerrar escritura impide que un hilo pierda el
         * valor que debe entregar al degradarse de M a S. */
        dsm_mem_stripe_lock(&ctx->mem, page);
        if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            free(data);
            return DSM_ERR_MEM;
        }
        memcpy(data, dsm_mem_page_ptr(&ctx->mem, page),
               ctx->mem.page_size);
        ctx->mem.local[page].state = DSM_PAGE_S;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    } else if (!page_can_serve(ctx, page, data)) {
        /* Página sin copia viva ni respaldo utilizable: el home la recupera de
         * un par que todavía la conserve o la sirve en ceros si nunca se
         * escribió. */
        bool known = true;
        home_page_content(ctx, page, data, &known);
        if (!known) {
            free(data);
            return DSM_ERR_NET;
        }
    }

    if (write) {
        /* Conserva primero una copia propia del contenido. Las copias remotas
         * viejas se invalidan antes de abrir la vista local en M. */
        if (dsm_mem_backing_store(&ctx->mem, page, data) != DSM_OK) {
            free(data);
            return DSM_ERR_NO_SPACE;
        }
        int inv_rc = invalidate_remote_copies(ctx, page, &entry,
                                              ctx->memb.self_slot);
        if (inv_rc != DSM_OK) {
            free(data);
            return inv_rc;
        }

        int rc = page_apply(ctx, page, DSM_PAGE_M,
                            data, ctx->mem.page_size);
        free(data);
        if (rc != DSM_OK) return rc;
    } else {
        int rc = page_apply(ctx, page, DSM_PAGE_S,
                            data, ctx->mem.page_size);
        free(data);
        if (rc != DSM_OK) return rc;

        /* En lectura la copia ya quedó aplicada y puede persistirse con
         * seguridad antes de publicar el directorio. */
        page_persist_home(ctx, page);
    }

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    if (write) {
        /* SM_PEND/IM_PEND -> M: las copias remotas ya fueron invalidadas. */
        entry.state = DSM_DIR_MODIFIED;
        entry.owner = ctx->memb.self_slot;
        memset(entry.copyset, 0, sizeof(entry.copyset));
        copyset_set(&entry, ctx->memb.self_slot);
    } else {
        /* IS_PEND -> S: el home conserva una copia válida para lectura. */
        /* Downgrade M->S: el dueño entrega la versión y conserva lectura. */
        entry.state = DSM_DIR_SHARED;
        entry.owner = DSM_SLOT_NONE;
        copyset_set(&entry, ctx->memb.self_slot);
        if (previous_owner != DSM_SLOT_NONE) {
            copyset_set(&entry, previous_owner);
        }
    }
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return DSM_OK;
}

/* --- Inicialización --- */

int msi_init(dsm_ctx_t *ctx)
{
    dsm_msi_t *msi = &ctx->msi;
    memset(msi, 0, sizeof(*msi));
    pthread_mutex_init(&msi->dir_mu, NULL);

    if (ctx->cfg.page_size == 0 || ctx->cfg.view_size < ctx->cfg.page_size) {
        return DSM_OK;
    }

    msi->cap = (size_t)(ctx->cfg.view_size / ctx->cfg.page_size);
    msi->dir = calloc(msi->cap, sizeof(*msi->dir));
    if (!msi->dir) return DSM_ERR_MEM;

    msi->home_map = malloc(msi->cap * sizeof(*msi->home_map));
    if (!msi->home_map) {
        free(msi->dir);
        msi->dir = NULL;
        msi->cap = 0;
        return DSM_ERR_MEM;
    }

    for (size_t i = 0; i < msi->cap; i++) {
        msi->home_map[i] = DSM_SLOT_NONE;
        msi->dir[i].owner = DSM_SLOT_NONE;
        msi->dir[i].state = DSM_DIR_UNCACHED;
        msi->dir[i].frame = DSM_PAGE_ID_NONE;
        msi->dir[i].wait_head = DSM_WAIT_NONE;
        msi->dir[i].wait_tail = DSM_WAIT_NONE;
        msi->dir[i].txn = DSM_TXN_NONE;
    }
    return DSM_OK;
}

void msi_destroy(dsm_ctx_t *ctx)
{
    free(ctx->msi.home_map);
    ctx->msi.home_map = NULL;
    free(ctx->msi.dir);
    ctx->msi.dir = NULL;
    ctx->msi.cap = 0;
    pthread_mutex_destroy(&ctx->msi.dir_mu);
}

/* --- Fallo local --- */

int msi_local_fault(dsm_ctx_t *ctx, fault_slot_t *s)
{
    if (!ctx || !s || s->page >= ctx->mem.npages) return DSM_ERR_INVALID;

    uint32_t page = s->page;
    bool write = s->access == DSM_ACCESS_WRITE;

    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t current = ctx->mem.local[page].state;
    if ((!write && (current == DSM_PAGE_S || current == DSM_PAGE_M)) ||
        (write && current == DSM_PAGE_M)) {
        dsm_mem_stripe_unlock(&ctx->mem, page);
        return DSM_OK;
    }
    if (current == DSM_PAGE_IS_PEND || current == DSM_PAGE_IM_PEND ||
        current == DSM_PAGE_SM_PEND) {
        dsm_mem_stripe_unlock(&ctx->mem, page);
        return DSM_ERR_BUSY;
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);

    uint32_t previous = DSM_SLOT_NONE;
    uint32_t home = home_resolve(ctx, page, &previous);

    /* Si el anillo le quitó la página a este nodo, el contenido se entrega al
     * nuevo home antes de soltar la copia local. Un traspaso fallido deja la
     * copia intacta y el fallo sigue el camino normal hacia el nuevo home. */
    if (home != ctx->memb.self_slot && previous == ctx->memb.self_slot) {
        uint8_t *moved = malloc(ctx->mem.page_size);
        if (moved) {
            bool have = false;
            dsm_mem_stripe_lock(&ctx->mem, page);
            uint8_t held = ctx->mem.local[page].state;
            if (held == DSM_PAGE_S || held == DSM_PAGE_M) {
                memcpy(moved, dsm_mem_page_ptr(&ctx->mem, page),
                       ctx->mem.page_size);
                have = true;
            }
            dsm_mem_stripe_unlock(&ctx->mem, page);
            if (!have) have = dsm_mem_backing_load(&ctx->mem, page, moved) == DSM_OK;

            if (have && home_transfer(ctx, page, home, moved) == DSM_OK) {
                page_relinquish(ctx, page);
            }
            free(moved);
        }
    }

    /* I/S -> estado pendiente: el acceso queda bloqueado hasta que llegue la
     * copia, y un segundo fallo sobre la pagina devuelve DSM_ERR_BUSY. */
    dsm_mem_stripe_lock(&ctx->mem, page);
    current = ctx->mem.local[page].state;
    if (write) {
        ctx->mem.local[page].state =
            (current == DSM_PAGE_S) ? DSM_PAGE_SM_PEND : DSM_PAGE_IM_PEND;
    } else {
        ctx->mem.local[page].state = DSM_PAGE_IS_PEND;
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);

    if (home == ctx->memb.self_slot) {
        int rc = home_local_fault(ctx, page, write);
        if (rc != DSM_OK) page_invalidate_local(ctx, page);
        return rc;
    }

    uint8_t *rx = malloc(ctx->mem.page_size);
    if (!rx) {
        page_invalidate_local(ctx, page);
        return DSM_ERR_MEM;
    }
    uint32_t rx_len = ctx->mem.page_size;
    uint16_t status = DSM_ST_OK;
    int rc = dsm_net_rpc(ctx, home, MSG_PAGE_REQUEST, page,
                         ctx->mem.local[page].tseq_seen,
                         write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                         DSM_FLAG_WANT_DATA, NULL, 0, &status, rx, &rx_len);
    if (rc == DSM_OK && status == DSM_ST_OK) {
        rc = page_apply(ctx, page, write ? DSM_PAGE_M : DSM_PAGE_S,
                        rx, rx_len);
    } else if (rc == DSM_OK) {
        rc = DSM_ERR_NET;
    }
    free(rx);
    if (rc != DSM_OK) {
        page_invalidate_local(ctx, page);
    }
    return rc;
}

/* --- Atención de solicitudes en el home --- */

/* Responde NACK a una solicitud que el nodo no puede atender. */
static void send_nack(dsm_ctx_t *ctx, const dsm_msg_t *m, uint16_t status)
{
    msg_header_t nack;
    memset(&nack, 0, sizeof(nack));
    nack.magic         = DSM_MAGIC;
    nack.cluster_id    = ctx->cfg.cluster_id;
    nack.proto_version = DSM_PROTO_VERSION;
    nack.type          = MSG_NACK;
    nack.src_slot      = ctx->memb.self_slot;
    nack.dst_slot      = m->hdr.src_slot;
    nack.req_id        = m->hdr.req_id;
    nack.page_id       = m->hdr.page_id;
    nack.status        = status;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &nack, NULL);
}

/* Sonda de un home recién designado: el nodo entrega el contenido que todavía
 * conserva y suelta la página acto seguido, de modo que no quede una copia
 * accesible en un nodo que ya no la custodia. */
static void home_handle_probe(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    uint32_t page = m->hdr.page_id;

    if (!page_local_valid(ctx, page)) {
        send_nack(ctx, m, DSM_ST_BAD_STATE);
        return;
    }

    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) {
        send_nack(ctx, m, DSM_ST_NO_SPACE);
        return;
    }
    if (!page_can_serve(ctx, page, data)) {
        free(data);
        send_nack(ctx, m, DSM_ST_BAD_STATE);
        return;
    }

    msg_header_t response;
    memset(&response, 0, sizeof(response));
    response.magic         = DSM_MAGIC;
    response.cluster_id    = ctx->cfg.cluster_id;
    response.proto_version = DSM_PROTO_VERSION;
    response.type          = MSG_PAGE_DATA;
    response.src_slot      = ctx->memb.self_slot;
    response.dst_slot      = m->hdr.src_slot;
    response.req_id        = m->hdr.req_id;
    response.page_id       = page;
    response.payload_len   = ctx->mem.page_size;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &response, data);
    free(data);

    if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
        page_relinquish(ctx, page);
    }
}

void msi_home_handle_request(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;

    /* Las sondas llegan a nodos que ya no son home y deben contestarse con el
     * contenido que conserven. */
    if (m->hdr.flags & DSM_FLAG_PROBE) {
        home_handle_probe(ctx, m);
        return;
    }

    uint32_t page = m->hdr.page_id;
    uint32_t requester = m->hdr.src_slot;
    bool write = m->hdr.access == DSM_ACCESS_WRITE;

    /* Un nodo que dejó de ser home rechaza la solicitud: el solicitante vuelve
     * a resolver el anillo y no quedan dos custodios sirviendo la página. */
    if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
        send_nack(ctx, m, DSM_ST_NOT_HOME);
        return;
    }

    dir_entry_t entry = dir_snapshot(ctx, page);

    uint8_t *data = NULL;
    uint32_t data_len = 0;
    int rc = DSM_OK;

    bool owner_remote = (entry.state == DSM_DIR_MODIFIED &&
                         entry.owner != DSM_SLOT_NONE &&
                         entry.owner != ctx->memb.self_slot);
    bool owner_local = (entry.state == DSM_DIR_MODIFIED &&
                        entry.owner == ctx->memb.self_slot);

    if (owner_remote) {
        data = malloc(ctx->mem.page_size);
        if (!data) return;
        data_len = ctx->mem.page_size;
        uint16_t status = DSM_ST_OK;
        rc = dsm_net_rpc(ctx, entry.owner, MSG_DOWNGRADE, page, entry.tseq,
                         write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                         DSM_FLAG_HAS_DATA, NULL, 0, &status, data, &data_len);
        if (rc != DSM_OK || status != DSM_ST_OK ||
            data_len != ctx->mem.page_size) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            return;
        }
    } else if (owner_local) {
        /* El dueño local se degrada antes de copiar: así ningún hilo puede
         * modificar el alias mientras se produce el contenido que se concede. */
        data = malloc(ctx->mem.page_size);
        if (!data) return;
        dsm_mem_stripe_lock(&ctx->mem, page);
        if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            return;
        }
        memcpy(data, dsm_mem_page_ptr(&ctx->mem, page),
               ctx->mem.page_size);
        ctx->mem.local[page].state = DSM_PAGE_S;
        dsm_mem_stripe_unlock(&ctx->mem, page);
        data_len = ctx->mem.page_size;
    }

    if (!data) {
        /* La página no está en la vista del home: su contenido sale del marco
         * de respaldo, de un par que todavía lo conserve o de ceros. */
        bool known = true;
        data = malloc(ctx->mem.page_size);
        if (!data) return;
        home_page_content(ctx, page, data, &known);
        if (!known) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            return;
        }
        data_len = ctx->mem.page_size;
    }

    if (!write) {
        (void)page_apply(ctx, page, DSM_PAGE_S, data, data_len);
        page_persist_home(ctx, page);

        pthread_mutex_lock(&ctx->msi.dir_mu);
        entry = ctx->msi.dir[page];
        entry.state = DSM_DIR_SHARED;
        entry.owner = DSM_SLOT_NONE;
        copyset_set(&entry, requester);
        copyset_set(&entry, ctx->memb.self_slot);
        ctx->msi.dir[page] = entry;
        pthread_mutex_unlock(&ctx->msi.dir_mu);

        msg_header_t response;
        memset(&response, 0, sizeof(response));
        response.magic = DSM_MAGIC;
        response.cluster_id = ctx->cfg.cluster_id;
        response.proto_version = DSM_PROTO_VERSION;
        response.type = MSG_PAGE_DATA;
        response.src_slot = ctx->memb.self_slot;
        response.dst_slot = requester;
        response.req_id = m->hdr.req_id;
        response.page_id = page;
        response.data_version = entry.version;
        response.payload_len = data_len;
        (void)dsm_net_send(ctx, requester, &response, data);
        free(data);
        return;
    }

    rc = invalidate_remote_copies(ctx, page, &entry, requester);
    if (rc != DSM_OK) {
        free(data);
        send_nack(ctx, m, DSM_ST_BUSY);
        return;
    }

    /* El home conserva la versión entregada: si el anillo le quita la página,
     * el nuevo home la recupera del respaldo o del nodo que la recibió. */
    (void)dsm_mem_backing_store(&ctx->mem, page, data);
    page_invalidate_local(ctx, page);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    entry.state = DSM_DIR_MODIFIED;
    entry.owner = requester;
    memset(entry.copyset, 0, sizeof(entry.copyset));
    copyset_set(&entry, requester);
    entry.tseq++;
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    msg_header_t response;
    memset(&response, 0, sizeof(response));
    response.magic = DSM_MAGIC;
    response.cluster_id = ctx->cfg.cluster_id;
    response.proto_version = DSM_PROTO_VERSION;
    response.type = MSG_GRANT;
    response.src_slot = ctx->memb.self_slot;
    response.dst_slot = requester;
    response.req_id = m->hdr.req_id;
    response.page_id = page;
    response.data_version = entry.version;
    response.flags = data_len ? DSM_FLAG_HAS_DATA : 0;
    response.payload_len = data_len;
    (void)dsm_net_send(ctx, requester, &response, data);
    free(data);
}

/* --- Mensajes de coherencia --- */

void msi_on_page_data(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    /* PAGE_DATA completa IS_PEND -> S y protege la página como solo lectura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_S,
                     m->payload, m->hdr.payload_len);
}

void msi_on_grant(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    /* GRANT completa IM_PEND/SM_PEND -> M y permite escritura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_M,
                     m->payload, m->hdr.payload_len);
}

void msi_on_invalidate(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;
    /* INVALIDATE fuerza S/M -> I antes de confirmar al home: el ACK solo se
     * envía cuando no queda ninguna copia accesible localmente. */
    page_invalidate_local(ctx, m->hdr.page_id);
    atomic_fetch_add_explicit(&ctx->ctr.invalidations, 1, memory_order_relaxed);

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic = DSM_MAGIC;
    ack.cluster_id = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type = MSG_INVALIDATE_ACK;
    ack.src_slot = ctx->memb.self_slot;
    ack.dst_slot = m->hdr.src_slot;
    ack.req_id = m->hdr.req_id;
    ack.page_id = m->hdr.page_id;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, NULL);
}

void msi_on_invalidate_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

void msi_on_downgrade(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;

    uint32_t page = m->hdr.page_id;
    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) return;
    dsm_mem_stripe_lock(&ctx->mem, page);
    /* Cerrar primero toda escritura impide que un hilo local modifique el
     * alias durante la copia que viajará al custodio. */
    if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
        dsm_mem_stripe_unlock(&ctx->mem, page);
        free(data);
        return;
    }
    memcpy(data, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
    /* DOWNGRADE transforma M -> S después de copiar la versión más reciente. */
    ctx->mem.local[page].state = DSM_PAGE_S;
    dsm_mem_stripe_unlock(&ctx->mem, page);

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic = DSM_MAGIC;
    ack.cluster_id = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type = MSG_DOWNGRADE_ACK;
    ack.src_slot = ctx->memb.self_slot;
    ack.dst_slot = m->hdr.src_slot;
    ack.req_id = m->hdr.req_id;
    ack.page_id = page;
    ack.flags = DSM_FLAG_HAS_DATA;
    ack.payload_len = ctx->mem.page_size;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, data);
    free(data);
}

void msi_on_downgrade_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    (void)ctx;
    (void)m;
}

/* WRITEBACK llega al home con el contenido de una página en M. El home lo
 * copia a su marco de respaldo y confirma con WRITEBACK_ACK. */
void msi_on_writeback(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;
    if (m->hdr.payload_len != ctx->mem.page_size || !m->payload) return;

    uint32_t page = m->hdr.page_id;
    uint16_t status = DSM_ST_OK;
    if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
        status = DSM_ST_NOT_HOME;
    } else if (dsm_mem_backing_store(&ctx->mem, page, m->payload) != DSM_OK) {
        status = DSM_ST_NO_SPACE;
    }

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic         = DSM_MAGIC;
    ack.cluster_id    = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type          = MSG_WRITEBACK_ACK;
    ack.src_slot      = ctx->memb.self_slot;
    ack.dst_slot      = m->hdr.src_slot;
    ack.req_id        = m->hdr.req_id;
    ack.page_id       = page;
    ack.status        = status;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, NULL);
}

/* Persiste en el respaldo del home el contenido de una página que este nodo
 * tiene en M. El home escribe su propio marco; cualquier otro nodo envía el
 * contenido al home por WRITEBACK y espera el acuse. */
int msi_writeback_page(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx || page >= ctx->mem.npages) return DSM_ERR_INVALID;

    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) return DSM_ERR_MEM;

    dsm_mem_stripe_lock(&ctx->mem, page);
    memcpy(data, dsm_mem_page_ptr(&ctx->mem, page), ctx->mem.page_size);
    dsm_mem_stripe_unlock(&ctx->mem, page);

    int rc;
    uint32_t home = ring_home_of(ctx, page);
    if (home == ctx->memb.self_slot) {
        rc = dsm_mem_backing_store(&ctx->mem, page, data);
    } else {
        uint16_t status = DSM_ST_OK;
        uint32_t rx_len = 0;
        rc = dsm_net_rpc(ctx, home, MSG_WRITEBACK, page,
                         ctx->mem.local[page].tseq_seen, DSM_ACCESS_WRITE,
                         DSM_FLAG_HAS_DATA, data, ctx->mem.page_size,
                         &status, NULL, &rx_len);
        if (rc == DSM_OK && status != DSM_ST_OK) {
            rc = DSM_ERR_NET;
        }
    }

    free(data);
    return rc;
}

void msi_counters(dsm_ctx_t *ctx, uint64_t *out_m, uint64_t *out_s, uint64_t *out_i)
{
    uint64_t m = 0, s = 0, i = 0;
    if (ctx && ctx->mem.local) {
        for (uint32_t p = 0; p < ctx->mem.npages; p++) {
            switch (ctx->mem.local[p].state) {
            case DSM_PAGE_M: m++; break;
            case DSM_PAGE_S: s++; break;
            default:         i++; break;
            }
        }
    }
    if (out_m) *out_m = m;
    if (out_s) *out_s = s;
    if (out_i) *out_i = i;
}
