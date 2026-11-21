#include "msi.h"
#include "dsm_internal.h"

#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

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

#if defined(DSM_TEST_HOOKS)
static pthread_mutex_t g_busy_test_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_busy_test_cv = PTHREAD_COND_INITIALIZER;
static uint32_t g_busy_test_page = DSM_SLOT_NONE;
static bool g_busy_test_entered;
static bool g_busy_test_release;
static bool g_busy_test_fail;
static bool g_busy_test_second_seen;
static uint16_t g_downgrade_test_status = DSM_ST_OK;
static bool g_downgrade_test_override;
static uint32_t g_downgrade_test_calls;

void msi_test_busy_hook_config(uint32_t page, bool fail)
{
    pthread_mutex_lock(&g_busy_test_mu);
    g_busy_test_page = page;
    g_busy_test_fail = fail;
    g_busy_test_entered = false;
    g_busy_test_release = false;
    g_busy_test_second_seen = false;
    pthread_mutex_unlock(&g_busy_test_mu);
}

void msi_test_busy_hook_wait_entered(void)
{
    pthread_mutex_lock(&g_busy_test_mu);
    while (!g_busy_test_entered) {
        pthread_cond_wait(&g_busy_test_cv, &g_busy_test_mu);
    }
    pthread_mutex_unlock(&g_busy_test_mu);
}

void msi_test_busy_hook_release(void)
{
    pthread_mutex_lock(&g_busy_test_mu);
    g_busy_test_release = true;
    pthread_cond_broadcast(&g_busy_test_cv);
    pthread_mutex_unlock(&g_busy_test_mu);
}

bool msi_test_busy_hook_second_seen(void)
{
    pthread_mutex_lock(&g_busy_test_mu);
    bool seen = g_busy_test_second_seen;
    pthread_mutex_unlock(&g_busy_test_mu);
    return seen;
}

void msi_test_downgrade_config(uint16_t status)
{
    pthread_mutex_lock(&g_busy_test_mu);
    g_downgrade_test_status = status;
    g_downgrade_test_override = true;
    g_downgrade_test_calls = 0;
    pthread_mutex_unlock(&g_busy_test_mu);
}

void msi_test_downgrade_reset(void)
{
    pthread_mutex_lock(&g_busy_test_mu);
    g_downgrade_test_override = false;
    g_downgrade_test_calls = 0;
    pthread_mutex_unlock(&g_busy_test_mu);
}

uint32_t msi_test_downgrade_calls(void)
{
    pthread_mutex_lock(&g_busy_test_mu);
    uint32_t calls = g_downgrade_test_calls;
    pthread_mutex_unlock(&g_busy_test_mu);
    return calls;
}

static bool busy_test_wait(dsm_ctx_t *ctx, uint32_t page)
{
    (void)ctx;
    pthread_mutex_lock(&g_busy_test_mu);
    if (!g_busy_test_entered && g_busy_test_page == page) {
        g_busy_test_entered = true;
        pthread_cond_broadcast(&g_busy_test_cv);
        while (!g_busy_test_release) {
            pthread_cond_wait(&g_busy_test_cv, &g_busy_test_mu);
        }
        bool fail = g_busy_test_fail;
        pthread_mutex_unlock(&g_busy_test_mu);
        return fail;
    }
    pthread_mutex_unlock(&g_busy_test_mu);
    return false;
}
#endif

/* --- Secuencia de transacciones de las respuestas de página --- */

/* Toda respuesta que transporta contenido de página avanza esta secuencia.
 * El receptor solo instala una respuesta cuyo tseq supere el ultimo que aplico,
 * de modo que una trama repetida o atrasada no reescriba la tabla de paginas. */
static uint32_t dir_tseq_next(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx || !ctx->msi.dir || page >= ctx->msi.cap) return 1u;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    uint32_t tseq = ctx->msi.dir[page].tseq + 1u;
    if (tseq == 0) tseq = 1u;
    ctx->msi.dir[page].tseq = tseq;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return tseq;
}

/* Decicion centralizada: compara y, cuando procede, publica el tseq antes de
 * que el llamador instale datos o permisos. */
static bool tseq_accept(dsm_ctx_t *ctx, uint32_t page, uint32_t tseq)
{
    if (!ctx || !ctx->mem.local || page >= ctx->mem.npages || tseq == 0) {
        return false;
    }

    bool accept;
    dsm_mem_stripe_lock(&ctx->mem, page);
    if (tseq <= ctx->mem.local[page].tseq_seen) {
        atomic_fetch_add_explicit(&ctx->ctr.tseq_discarded, 1,
                                  memory_order_relaxed);
        accept = false;
    } else {
        ctx->mem.local[page].tseq_seen = tseq;
        accept = true;
    }
    dsm_mem_stripe_unlock(&ctx->mem, page);
    return accept;
}

/* Predicado sin efectos: lo usa la capa de red para no despertar una RPC con
 * una respuesta que el worker va a descartar de todos modos. */
bool msi_tseq_stale(dsm_ctx_t *ctx, uint32_t page, uint32_t tseq)
{
    if (!ctx || !ctx->mem.local || page >= ctx->mem.npages || tseq == 0) {
        return true;
    }

    dsm_mem_stripe_lock(&ctx->mem, page);
    bool stale = tseq <= ctx->mem.local[page].tseq_seen;
    dsm_mem_stripe_unlock(&ctx->mem, page);
    return stale;
}

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

/* Persiste el contenido vigente de una página custodiada y, con RAID5 activo,
 * emite el delta de paridad contra el respaldo previo: una página nunca
 * persistida cuenta como ceros. Cuando el contenido no cambia respecto del
 * respaldo no se genera delta, que seria un XOR de ceros. */
static int home_persist_delta(dsm_ctx_t *ctx, uint32_t page, const void *data)
{
    uint8_t *old = NULL;
    if (ctx->cfg.redundancy_raid5) {
        old = malloc(ctx->mem.page_size);
        if (old && dsm_mem_backing_load(&ctx->mem, page, old) != DSM_OK) {
            memset(old, 0, ctx->mem.page_size);
        }
    }

    int rc = dsm_mem_backing_store(&ctx->mem, page, data);
    if (rc == DSM_OK && old &&
        memcmp(old, data, ctx->mem.page_size) != 0) {
        (void)raid_on_write(ctx, page, old, data);
    } else if (rc == DSM_OK && !old && ctx->cfg.redundancy_raid5) {
        DSM_LOG_WARN("Sin memoria para el delta de paridad de la pagina %u", page);
    }
    free(old);
    return rc;
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
 * confirmó que guardó el contenido. Va marcado como traspaso: el contenido ya
 * está acumulado en la paridad y un writeback ordinario generaría un delta
 * contra un respaldo vacío que la cancelaría. El tseq propio viaja en la
 * cabecera para que el nuevo home continúe la secuencia en lugar de
 * reiniciarla y que sus respuestas no se descarten por antiguas. */
static int home_transfer(dsm_ctx_t *ctx, uint32_t page, uint32_t home,
                         const void *data)
{
    if (home == DSM_SLOT_NONE || home == ctx->memb.self_slot) {
        return DSM_ERR_INVALID;
    }

    uint32_t tseq = dir_snapshot(ctx, page).tseq;
    uint16_t status = DSM_ST_OK;
    uint32_t rx_len = 0;
    int rc = dsm_net_rpc(ctx, home, MSG_WRITEBACK, page, tseq,
                         DSM_ACCESS_WRITE,
                         DSM_FLAG_HAS_DATA | DSM_FLAG_TRANSFER, data,
                         ctx->mem.page_size, &status, NULL, &rx_len);
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

/* Instala el contenido y los permisos de una respuesta de página. Con
 * require_pending, la página debe estar en el estado pendiente que la
 * solicitud dejo: la comprobacion ocurre dentro del cerrojo, de modo que
 * una invalidacion que llegue entre el chequeo previo y la aplicacion no
 * puede reabrir una página ya invalidada. */
static int page_apply(dsm_ctx_t *ctx, uint32_t page, dsm_page_state_t state,
                      const void *payload, uint32_t payload_len, uint32_t tseq,
                      bool require_pending)
{
    if (page >= ctx->mem.npages) return DSM_ERR_INVALID;
    if (payload_len != 0 && payload_len != ctx->mem.page_size) {
        return DSM_ERR_PROTO;
    }
    if (!tseq_accept(ctx, page, tseq)) return DSM_OK;

    dsm_mem_stripe_lock(&ctx->mem, page);
    if (require_pending) {
        uint8_t current = ctx->mem.local[page].state;
        bool expected = (state == DSM_PAGE_M)
                            ? (current == DSM_PAGE_IM_PEND ||
                               current == DSM_PAGE_SM_PEND)
                            : (current == DSM_PAGE_IS_PEND ||
                               current == DSM_PAGE_SM_PEND);
        if (!expected) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            return DSM_ERR_BUSY;
        }
    }
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

/* --- Espera de acuses de una transaccion --- */

static uint32_t msi_ack_timeout_ms(dsm_ctx_t *ctx)
{
    if (ctx->cfg.lock_timeout_ms) return ctx->cfg.lock_timeout_ms;
    if (ctx->cfg.request_timeout_ms) return ctx->cfg.request_timeout_ms;
    return 10000u;
}

int msi_ack_wait_begin(dsm_ctx_t *ctx, uint32_t txn,
                       const uint8_t *mask, uint32_t expected)
{
    if (!ctx || txn == 0 || expected == 0) return DSM_ERR_INVALID;

    int rc = DSM_ERR_NO_SPACE;
    pthread_mutex_lock(&ctx->msi.dir_mu);
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        msi_ack_wait_t *w = &ctx->msi.ack_waits[i];
        if (w->active) continue;
        while (sem_trywait(&w->sem) == 0) {
        }
        w->txn      = txn;
        w->expected = expected;
        w->received = 0;
        memcpy(w->ok_mask, mask, sizeof(w->ok_mask));
        w->status   = DSM_ST_OK;
        w->done     = 0;
        w->active   = true;
        rc = DSM_OK;
        break;
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return rc;
}

void msi_ack_wait_cancel(dsm_ctx_t *ctx, uint32_t txn)
{
    if (!ctx) return;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        msi_ack_wait_t *w = &ctx->msi.ack_waits[i];
        if (w->active && w->txn == txn) {
            w->active = false;
            w->done = 0;
            break;
        }
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
}

int msi_ack_wait_result(dsm_ctx_t *ctx, uint32_t txn, uint32_t timeout_ms)
{
    if (!ctx) return DSM_ERR_INVALID;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    msi_ack_wait_t *wait = NULL;
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        msi_ack_wait_t *w = &ctx->msi.ack_waits[i];
        if (w->active && w->txn == txn) {
            wait = w;
            break;
        }
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    /* Una entrada completada antes de la espera sigue activa hasta que este
     * hilo la consuma: el NOT_FOUND queda para transacciones inexistentes. */
    if (!wait) return DSM_ERR_NOT_FOUND;

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec  += (time_t)(timeout_ms / 1000u);
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }

    int wait_rc;
    do {
        wait_rc = sem_timedwait(&wait->sem, &deadline);
    } while (wait_rc != 0 && errno == EINTR);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    /* El despertar debe seguir correspondiendo a la misma transaccion: la
     * entrada solo la libera este hilo, pero no se consume estado ajeno. */
    if (!wait->active || wait->txn != txn) {
        pthread_mutex_unlock(&ctx->msi.dir_mu);
        return DSM_ERR_NOT_FOUND;
    }
    if (wait_rc != 0 && !wait->done) {
        wait->active = false;
        wait->done = 0;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
        return (errno == ETIMEDOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
    }
    /* Un acuse que llego entre el vencimiento y la cancelacion cuenta como
     * respuesta: el semaforo drenado se limpia al reutilizar la ranura. */
    uint16_t status = wait->status;
    wait->active = false;
    wait->done = 0;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return (status == DSM_ST_OK) ? DSM_OK : DSM_ERR_NET;
}

static bool ack_mask_empty(const uint8_t *mask)
{
    for (uint32_t i = 0; i < MAX_NODES / 8u; i++) {
        if (mask[i] != 0) return false;
    }
    return true;
}

/* Registra el acuse de un remitente en la espera que corresponda a su txn. */
static void ack_wait_on_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.src_slot >= MAX_NODES) return;
    uint32_t byte = m->hdr.src_slot / 8u;
    uint8_t bit = (uint8_t)(1u << (m->hdr.src_slot % 8u));

    pthread_mutex_lock(&ctx->msi.dir_mu);
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        msi_ack_wait_t *w = &ctx->msi.ack_waits[i];
        if (!w->active || w->txn != m->hdr.req_id) continue;
        /* Un duplicado o un remitente ajeno al conjunto no cuenta. */
        if ((w->ok_mask[byte] & bit) == 0) break;
        w->ok_mask[byte] = (uint8_t)(w->ok_mask[byte] & ~bit);
        w->received++;
        if (m->hdr.status != DSM_ST_OK) w->status = m->hdr.status;
        if ((w->received >= w->expected && ack_mask_empty(w->ok_mask)) ||
            w->status != DSM_ST_OK) {
            /* La entrada sigue activa para que su dueño la encuentre y
             * consuma el resultado; solo el resultado la libera. */
            w->done = 1;
            sem_post(&w->sem);
        }
        break;
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
}

/* Invalida todas las copias remotas y solo devuelve el permiso de escritura
 * cuando ha llegado un acuse de cada destinatario. */
static int invalidate_remote_copies(dsm_ctx_t *ctx, uint32_t page,
                                    const dir_entry_t *entry,
                                    uint32_t keep_slot)
{
    uint8_t mask[MAX_NODES / 8];
    memset(mask, 0, sizeof(mask));
    uint32_t expected = 0;

    for (uint32_t slot = 0; slot < MAX_NODES; slot++) {
        if (!copyset_has(entry, slot) || slot == keep_slot) continue;
        if (slot == ctx->memb.self_slot) {
            page_invalidate_local(ctx, page);
            atomic_fetch_add_explicit(&ctx->ctr.invalidations, 1,
                                      memory_order_relaxed);
            continue;
        }
        mask[slot / 8u] |= (uint8_t)(1u << (slot % 8u));
        expected++;
    }
    if (expected == 0) return DSM_OK;

    uint32_t txn = dsm_net_next_req(ctx);
    int rc = msi_ack_wait_begin(ctx, txn, mask, expected);
    if (rc != DSM_OK) return rc;

    for (uint32_t slot = 0; slot < MAX_NODES; slot++) {
        if ((mask[slot / 8u] & (1u << (slot % 8u))) == 0) continue;

        msg_header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.magic         = DSM_MAGIC;
        hdr.cluster_id    = ctx->cfg.cluster_id;
        hdr.proto_version = DSM_PROTO_VERSION;
        hdr.type          = MSG_INVALIDATE;
        hdr.src_slot      = ctx->memb.self_slot;
        hdr.dst_slot      = slot;
        hdr.req_id        = txn;
        hdr.page_id       = page;
        hdr.tseq          = entry->tseq;
        hdr.access        = DSM_ACCESS_WRITE;
        hdr.epoch         = ctx->memb.epoch;

        if (dsm_net_send(ctx, slot, &hdr, NULL) != DSM_OK) {
            msi_ack_wait_cancel(ctx, txn);
            return DSM_ERR_NET;
        }
    }

    return msi_ack_wait_result(ctx, txn, msi_ack_timeout_ms(ctx));
}

/* --- Fallo local atendido por el propio home --- */

static int home_local_fault(dsm_ctx_t *ctx, uint32_t page, bool write)
{
    bool reserved = false;
    int result = DSM_OK;
    bool recovered_owner = false;
    dir_entry_t entry = dir_snapshot(ctx, page);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    if (ctx->msi.dir[page].busy) {
        pthread_mutex_unlock(&ctx->msi.dir_mu);
        return DSM_ERR_BUSY;
    }
    ctx->msi.dir[page].busy = 1;
    reserved = true;
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    uint32_t previous_owner = entry.owner;
    uint8_t *data = malloc(ctx->mem.page_size);
    if (!data) {
        result = DSM_ERR_MEM;
        goto out;
    }

    bool remote_owner = (entry.state == DSM_DIR_MODIFIED &&
                         entry.owner != DSM_SLOT_NONE &&
                         entry.owner != ctx->memb.self_slot &&
                         owner_alive(ctx, entry.owner));
    bool local_owner = (entry.state == DSM_DIR_MODIFIED &&
                        entry.owner == ctx->memb.self_slot);

    if (remote_owner) {
        uint16_t status = DSM_ST_OK;
        uint32_t data_len = ctx->mem.page_size;
        uint32_t ack_tseq = 0;
        uint32_t down_tseq = dir_tseq_next(ctx, page);
        uint32_t txn = dsm_net_next_req(ctx);
        uint8_t mask[MAX_NODES / 8];
        memset(mask, 0, sizeof(mask));
        mask[entry.owner / 8u] |= (uint8_t)(1u << (entry.owner % 8u));
        (void)msi_ack_wait_begin(ctx, txn, mask, 1);
        int rc = dsm_net_rpc_ex(ctx, entry.owner, MSG_DOWNGRADE, page,
                                down_tseq, write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                                DSM_FLAG_HAS_DATA, NULL, 0, &status, data,
                                &data_len, txn, &ack_tseq);
        msi_ack_wait_cancel(ctx, txn);
        if (rc != DSM_OK) {
            free(data);
            result = rc;
            goto out;
        }
        if (status == DSM_ST_BUSY) {
            free(data);
            result = DSM_ERR_BUSY;
            goto out;
        }
        if (status == DSM_ST_BAD_STATE) {
            /* LIMITACIÓN: dir_entry_t no guarda la incarnation del owner.
             * La membresía solo conoce la incarnation vigente, no cuál tenía
             * el owner cuando otorgó M, por lo que no puede invalidarse un
             * reinicio con el mismo slot. */
            free(data);
            data = malloc(ctx->mem.page_size);
            if (!data) {
                result = DSM_ERR_MEM;
                goto out;
            }
            if (dsm_mem_backing_load(&ctx->mem, page, data) != DSM_OK) {
                free(data);
                data = NULL;
                result = DSM_ERR_BUSY;
                goto out;
            }
            recovered_owner = true;
        } else if (status != DSM_ST_OK ||
                   data_len != ctx->mem.page_size) {
            free(data);
            result = DSM_ERR_NET;
            goto out;
        }
    } else if (entry.state == DSM_DIR_MODIFIED &&
               entry.owner != DSM_SLOT_NONE &&
               entry.owner != ctx->memb.self_slot &&
               !owner_alive(ctx, entry.owner)) {
        /* LIMITACIÓN: owner muerto en M: el respaldo no se usa porque puede
         * contener una versión anterior a la única escritura pendiente. */
        result = DSM_ERR_BUSY;
        goto out;
    } else if (local_owner) {
        /* Antes de copiar, cerrar escritura impide que un hilo pierda el
         * valor que debe entregar al degradarse de M a S. */
        dsm_mem_stripe_lock(&ctx->mem, page);
        if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            free(data);
            result = DSM_ERR_MEM;
            goto out;
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
            result = DSM_ERR_NET;
            goto out;
        }
    }

    if (write) {
        /* Conserva primero una copia propia del contenido y acumula su
         * delta de paridad. Las copias remotas viejas se invalidan antes
         * de abrir la vista local en M. */
        if (home_persist_delta(ctx, page, data) != DSM_OK) {
            free(data);
            result = DSM_ERR_NO_SPACE;
            goto out;
        }
        int inv_rc = invalidate_remote_copies(ctx, page, &entry,
                                              ctx->memb.self_slot);
        if (inv_rc != DSM_OK) {
            free(data);
            result = inv_rc;
            goto out;
        }

        int rc = page_apply(ctx, page, DSM_PAGE_M,
                            data, ctx->mem.page_size,
                            dir_tseq_next(ctx, page), true);
        free(data);
        if (rc != DSM_OK) {
            result = rc;
            goto out;
        }
    } else {
        int rc = page_apply(ctx, page, DSM_PAGE_S,
                            data, ctx->mem.page_size,
                            dir_tseq_next(ctx, page), true);
        if (rc == DSM_OK) {
            /* En lectura la copia ya quedó aplicada y puede persistirse con
             * seguridad antes de publicar el directorio. El persist incluye
             * el delta de paridad cuando el contenido cambió respecto del
             * marco. */
            rc = home_persist_delta(ctx, page, data);
        }
        free(data);
        if (rc != DSM_OK) {
            result = rc;
            goto out;
        }
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
        if (recovered_owner) {
            memset(entry.copyset, 0, sizeof(entry.copyset));
        }
        copyset_set(&entry, ctx->memb.self_slot);
        if (!recovered_owner && previous_owner != DSM_SLOT_NONE) {
            copyset_set(&entry, previous_owner);
        }
    }
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    result = DSM_OK;

out:
    if (reserved) {
        pthread_mutex_lock(&ctx->msi.dir_mu);
        ctx->msi.dir[page].busy = 0;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    }
    return result;
}

/* --- Inicialización --- */

int msi_init(dsm_ctx_t *ctx)
{
    dsm_msi_t *msi = &ctx->msi;
    memset(msi, 0, sizeof(*msi));
    pthread_mutex_init(&msi->dir_mu, NULL);

    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        if (sem_init(&msi->ack_waits[i].sem, 0, 0) != 0) {
            for (uint32_t j = 0; j < i; j++) {
                sem_destroy(&msi->ack_waits[j].sem);
            }
            pthread_mutex_destroy(&msi->dir_mu);
            return DSM_ERR_MEM;
        }
    }

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
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        sem_destroy(&ctx->msi.ack_waits[i].sem);
    }
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

    /* Un home distinto del conocido reinicia la secuencia de transacciones:
     * sin esto, la primera respuesta del nuevo home (tseq bajo) se
     * descartaria por antigua y la página quedaria sin servicio. */
    if (previous != DSM_SLOT_NONE && previous != home) {
        dsm_mem_stripe_lock(&ctx->mem, page);
        ctx->mem.local[page].tseq_seen = 0;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    }

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
    uint32_t resp_tseq = 0;
    dsm_mem_stripe_lock(&ctx->mem, page);
    uint32_t seen_tseq = ctx->mem.local[page].tseq_seen;
    dsm_mem_stripe_unlock(&ctx->mem, page);
    int rc = dsm_net_rpc_ex(ctx, home, MSG_PAGE_REQUEST, page,
                            seen_tseq,
                            write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                            DSM_FLAG_WANT_DATA, NULL, 0, &status, rx, &rx_len,
                            0, &resp_tseq);
    if (rc == DSM_OK && status == DSM_ST_OK) {
        rc = page_apply(ctx, page, write ? DSM_PAGE_M : DSM_PAGE_S,
                        rx, rx_len, resp_tseq, true);
    } else if (rc == DSM_OK && status == DSM_ST_BUSY) {
        page_invalidate_local(ctx, page);
        rc = DSM_ERR_BUSY;
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
    response.tseq          = dir_tseq_next(ctx, page);
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
    bool reserved = false;

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

    pthread_mutex_lock(&ctx->msi.dir_mu);
    if (ctx->msi.dir[page].busy) {
        pthread_mutex_unlock(&ctx->msi.dir_mu);
#if defined(DSM_TEST_HOOKS)
        pthread_mutex_lock(&g_busy_test_mu);
        g_busy_test_second_seen = true;
        pthread_mutex_unlock(&g_busy_test_mu);
#endif
        send_nack(ctx, m, DSM_ST_BUSY);
        return;
    }
    ctx->msi.dir[page].busy = 1;
    reserved = true;
    pthread_mutex_unlock(&ctx->msi.dir_mu);

#if defined(DSM_TEST_HOOKS)
    if (busy_test_wait(ctx, page)) {
        goto out;
    }
#endif

    dir_entry_t entry = dir_snapshot(ctx, page);

    uint8_t *data = NULL;
    uint32_t data_len = 0;
    int rc = DSM_OK;
    bool phantom_owner = false;
    bool recovered_owner = false;

    /* Regla exacta: el fantasma solo existe en M y el owner coincide con
     * el solicitante. SHARED/UNCACHED no contienen ese significado aunque
     * el owner conservado no sea significativo. */
    phantom_owner = (entry.state == DSM_DIR_MODIFIED &&
                     entry.owner == requester &&
                     owner_alive(ctx, requester));

    bool owner_remote = (entry.state == DSM_DIR_MODIFIED &&
                         entry.owner != DSM_SLOT_NONE &&
                         entry.owner != ctx->memb.self_slot);
    bool owner_local = (entry.state == DSM_DIR_MODIFIED &&
                        entry.owner == ctx->memb.self_slot);

    if (phantom_owner) {
        data = malloc(ctx->mem.page_size);
        if (!data) goto out;
        if (dsm_mem_backing_load(&ctx->mem, page, data) != DSM_OK) {
            free(data);
            data = NULL;
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
        data_len = ctx->mem.page_size;
    } else if (entry.state == DSM_DIR_MODIFIED &&
               entry.owner != DSM_SLOT_NONE &&
               entry.owner != ctx->memb.self_slot &&
               !owner_alive(ctx, entry.owner)) {
        /* LIMITACIÓN: owner muerto en M. El respaldo puede ser anterior a la
         * escritura pendiente y no se usa. */
        send_nack(ctx, m, DSM_ST_BUSY);
        goto out;
    } else if (owner_remote) {
        data = malloc(ctx->mem.page_size);
        if (!data) goto out;
        data_len = ctx->mem.page_size;
        uint16_t status = DSM_ST_OK;
        uint32_t ack_tseq = 0;
        uint32_t down_tseq = dir_tseq_next(ctx, page);
        uint32_t txn = dsm_net_next_req(ctx);
        uint8_t mask[MAX_NODES / 8];
        memset(mask, 0, sizeof(mask));
        mask[entry.owner / 8u] |= (uint8_t)(1u << (entry.owner % 8u));
#if defined(DSM_TEST_HOOKS)
        pthread_mutex_lock(&g_busy_test_mu);
        if (g_downgrade_test_override) {
            g_downgrade_test_calls++;
            status = g_downgrade_test_status;
            rc = DSM_OK;
            data_len = (status == DSM_ST_OK) ? ctx->mem.page_size : 0;
        } else {
            (void)msi_ack_wait_begin(ctx, txn, mask, 1);
            rc = dsm_net_rpc_ex(ctx, entry.owner, MSG_DOWNGRADE, page, down_tseq,
                                write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                                DSM_FLAG_HAS_DATA, NULL, 0, &status, data,
                                &data_len, txn, &ack_tseq);
            msi_ack_wait_cancel(ctx, txn);
        }
        pthread_mutex_unlock(&g_busy_test_mu);
#else
        (void)msi_ack_wait_begin(ctx, txn, mask, 1);
        rc = dsm_net_rpc_ex(ctx, entry.owner, MSG_DOWNGRADE, page, down_tseq,
                            write ? DSM_ACCESS_WRITE : DSM_ACCESS_READ,
                            DSM_FLAG_HAS_DATA, NULL, 0, &status, data,
                            &data_len, txn, &ack_tseq);
        msi_ack_wait_cancel(ctx, txn);
#endif
        if (rc != DSM_OK) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
        if (status == DSM_ST_BUSY) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
        if (status == DSM_ST_BAD_STATE) {
            /* LIMITACIÓN: dir_entry_t no guarda la incarnation del owner.
             * No podemos distinguir en 2c un reinicio con el mismo slot,
             * por lo que usamos el respaldo vigente con este riesgo conocido.
             * Más adelante puede añadirse owner_incarnation al directorio. */
            free(data);
            data = malloc(ctx->mem.page_size);
            if (!data) goto out;
            if (dsm_mem_backing_load(&ctx->mem, page, data) != DSM_OK) {
                free(data);
                data = NULL;
                send_nack(ctx, m, DSM_ST_BUSY);
                goto out;
            }
            data_len = ctx->mem.page_size;
            recovered_owner = true;
        } else if (status != DSM_ST_OK ||
                   data_len != ctx->mem.page_size) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
    } else if (owner_local) {
        /* El dueño local se degrada antes de copiar: así ningún hilo puede
         * modificar el alias mientras se produce el contenido que se concede. */
        data = malloc(ctx->mem.page_size);
        if (!data) goto out;
        dsm_mem_stripe_lock(&ctx->mem, page);
        if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
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
        if (!data) goto out;
        home_page_content(ctx, page, data, &known);
        if (!known) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
        data_len = ctx->mem.page_size;
    }

    if (!write) {
        uint32_t tseq = dir_tseq_next(ctx, page);
        (void)page_apply(ctx, page, DSM_PAGE_S, data, data_len, tseq, false);
        (void)home_persist_delta(ctx, page, data);

        pthread_mutex_lock(&ctx->msi.dir_mu);
        entry = ctx->msi.dir[page];
        entry.state = DSM_DIR_SHARED;
        entry.owner = DSM_SLOT_NONE;
        if (phantom_owner || recovered_owner) {
            memset(entry.copyset, 0, sizeof(entry.copyset));
        }
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
        response.tseq = tseq;
        response.data_version = entry.version;
        response.payload_len = data_len;
        (void)dsm_net_send(ctx, requester, &response, data);
        free(data);
        goto out;
    }

    if (!phantom_owner && !recovered_owner) {
        rc = invalidate_remote_copies(ctx, page, &entry, requester);
        if (rc != DSM_OK) {
            free(data);
            send_nack(ctx, m, DSM_ST_BUSY);
            goto out;
        }
    }

    /* El home conserva la versión entregada: si el anillo le quita la página,
     * el nuevo home la recupera del respaldo o del nodo que la recibió. El
     * respaldo arrastra su delta de paridad cuando el contenido cambió. */
    (void)home_persist_delta(ctx, page, data);
    page_invalidate_local(ctx, page);

    uint32_t tseq = dir_tseq_next(ctx, page);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    entry = ctx->msi.dir[page];
    entry.state = DSM_DIR_MODIFIED;
    entry.owner = requester;
    memset(entry.copyset, 0, sizeof(entry.copyset));
    copyset_set(&entry, requester);
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
    response.tseq = tseq;
    response.data_version = entry.version;
    response.flags = data_len ? DSM_FLAG_HAS_DATA : 0;
    response.payload_len = data_len;
    (void)dsm_net_send(ctx, requester, &response, data);
    free(data);

out:
    if (reserved) {
        pthread_mutex_lock(&ctx->msi.dir_mu);
        ctx->msi.dir[page].busy = 0;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    }
}

/* --- Mensajes de coherencia --- */

static bool page_accept_pending(dsm_ctx_t *ctx, uint32_t page)
{
    if (page >= ctx->mem.npages) return false;
    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t state = ctx->mem.local[page].state;
    bool pending = state == DSM_PAGE_IS_PEND || state == DSM_PAGE_IM_PEND ||
                   state == DSM_PAGE_SM_PEND;
    dsm_mem_stripe_unlock(&ctx->mem, page);
    return pending;
}

void msi_on_page_data(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    if (!page_accept_pending(ctx, m->hdr.page_id)) return;
    /* PAGE_DATA completa IS_PEND -> S y protege la página como solo lectura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_S,
                     m->payload, m->hdr.payload_len, m->hdr.tseq, true);
}

void msi_on_grant(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    if (!page_accept_pending(ctx, m->hdr.page_id)) return;
    /* GRANT completa IM_PEND/SM_PEND -> M y permite escritura. */
    (void)page_apply(ctx, m->hdr.page_id, DSM_PAGE_M,
                     m->payload, m->hdr.payload_len, m->hdr.tseq, true);
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
    ack_wait_on_ack(ctx, m);
}

void msi_on_downgrade(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;

    uint32_t page = m->hdr.page_id;
    uint16_t status = DSM_ST_OK;
    uint8_t *data = NULL;

    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t state = ctx->mem.local[page].state;
    if (state == DSM_PAGE_IS_PEND ||
        state == DSM_PAGE_IM_PEND ||
        state == DSM_PAGE_SM_PEND) {
        status = DSM_ST_BUSY;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    } else if (state != DSM_PAGE_M) {
        status = DSM_ST_BAD_STATE;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    } else {
        /* Camino M: mprotect antes de copiar, tal como estaba aprobado. */
        data = malloc(ctx->mem.page_size);
        if (!data) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            status = DSM_ST_NO_SPACE;
        } else if (dsm_mem_set_prot(&ctx->mem, page, PROT_READ) != DSM_OK) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            free(data);
            data = NULL;
            status = DSM_ST_BAD_STATE;
        } else {
            memcpy(data, dsm_mem_page_ptr(&ctx->mem, page),
                   ctx->mem.page_size);
            ctx->mem.local[page].state = DSM_PAGE_S;
            dsm_mem_stripe_unlock(&ctx->mem, page);
        }
    }

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
    ack.status = status;
    ack.tseq = m->hdr.tseq;
    if (data) {
        ack.flags = DSM_FLAG_HAS_DATA;
        ack.payload_len = ctx->mem.page_size;
    }
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, data);
    free(data);
}

void msi_on_downgrade_ack(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m) return;
    if ((m->hdr.flags & DSM_FLAG_HAS_DATA) && m->payload) {
        (void)tseq_accept(ctx, m->hdr.page_id, m->hdr.tseq);
    }
    ack_wait_on_ack(ctx, m);
}

/* WRITEBACK llega al home con el contenido de una página en M. El home lo
 * copia a su marco de respaldo, con su delta de paridad, y confirma con
 * WRITEBACK_ACK. */
void msi_on_writeback(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;
    if (m->hdr.payload_len != ctx->mem.page_size || !m->payload) return;

    uint32_t page = m->hdr.page_id;
    uint16_t status = DSM_ST_OK;

    /* La secuencia de leer el respaldo, persistir y emitir el delta de
     * paridad no puede intercalarse con otra de la misma página: se
     * reserva con el busy del directorio como el resto de las rutas del
     * home. */
    pthread_mutex_lock(&ctx->msi.dir_mu);
    if (ctx->msi.dir[page].busy) {
        pthread_mutex_unlock(&ctx->msi.dir_mu);
        status = DSM_ST_BUSY;
    } else {
        ctx->msi.dir[page].busy = 1;
        pthread_mutex_unlock(&ctx->msi.dir_mu);

        if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
            status = DSM_ST_NOT_HOME;
        } else if (m->hdr.flags & DSM_FLAG_TRANSFER) {
            /* Traspaso de custodia: el contenido no cambia, así que se
             * persiste sin delta de paridad, y la secuencia del custodio
             * anterior se siembra para que las respuestas de este home
             * superen el tseq_seen que los demás ya registraron. */
            pthread_mutex_lock(&ctx->msi.dir_mu);
            if (m->hdr.tseq != 0 && m->hdr.tseq > ctx->msi.dir[page].tseq) {
                ctx->msi.dir[page].tseq = m->hdr.tseq;
            }
            pthread_mutex_unlock(&ctx->msi.dir_mu);
            if (dsm_mem_backing_store(&ctx->mem, page, m->payload) != DSM_OK) {
                status = DSM_ST_NO_SPACE;
            }
        } else if (home_persist_delta(ctx, page, m->payload) != DSM_OK) {
            status = DSM_ST_NO_SPACE;
        }

        pthread_mutex_lock(&ctx->msi.dir_mu);
        ctx->msi.dir[page].busy = 0;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
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

/* Instala en el nuevo custodio la copia que le entrega el nodo que custodiaba
 * la página antes de la baja. */
void msi_on_page_move(dsm_ctx_t *ctx, const dsm_msg_t *m)
{
    if (!ctx || !m || m->hdr.page_id >= ctx->mem.npages) return;
    if (m->hdr.payload_len != ctx->mem.page_size || !m->payload) return;

    uint32_t page = m->hdr.page_id;
    uint16_t status = DSM_ST_OK;

    /* El custodio anterior viaja en la cabecera: su secuencia de
     * transacciones se siembra aqui para que las respuestas de este home
     * superen el tseq_seen que los demas nodos ya registraron. */
    pthread_mutex_lock(&ctx->msi.dir_mu);
    if (m->hdr.tseq != 0 && m->hdr.tseq > ctx->msi.dir[page].tseq) {
        ctx->msi.dir[page].tseq = m->hdr.tseq;
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    uint32_t tseq = dir_tseq_next(ctx, page);

    if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
        status = DSM_ST_NOT_HOME;
    } else if (dsm_mem_backing_store(&ctx->mem, page, m->payload) != DSM_OK) {
        status = DSM_ST_NO_SPACE;
    } else {
        dsm_mem_stripe_lock(&ctx->mem, page);
        dsm_mem_install_page(&ctx->mem, page, m->payload);
        (void)dsm_mem_set_prot(&ctx->mem, page, PROT_READ);
        ctx->mem.local[page].state = DSM_PAGE_S;
        ctx->mem.local[page].tseq_seen = tseq;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    }

    pthread_mutex_lock(&ctx->msi.dir_mu);
    dir_entry_t *entry = &ctx->msi.dir[page];
    ctx->msi.home_map[page] = ctx->memb.self_slot;
    entry->version++;
    if (status == DSM_ST_OK) {
        entry->state = DSM_DIR_SHARED;
        entry->owner = DSM_SLOT_NONE;
        memset(entry->copyset, 0, sizeof(entry->copyset));
        copyset_set(entry, ctx->memb.self_slot);
        copyset_set(entry, m->hdr.src_slot);
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    msg_header_t ack;
    memset(&ack, 0, sizeof(ack));
    ack.magic         = DSM_MAGIC;
    ack.cluster_id    = ctx->cfg.cluster_id;
    ack.proto_version = DSM_PROTO_VERSION;
    ack.type          = MSG_PAGE_MOVE_ACK;
    ack.src_slot      = ctx->memb.self_slot;
    ack.dst_slot      = m->hdr.src_slot;
    ack.req_id        = m->hdr.req_id;
    ack.page_id       = page;
    ack.status        = status;
    (void)dsm_net_send(ctx, m->hdr.src_slot, &ack, NULL);
}

/* Reasigna las páginas cuyo custodio era el nodo caído. Solo se sirve una copia
 * cuando hay datos vigentes; sin copia la página queda inválida y nadie la
 * anuncia como compartida. */
void msi_on_node_down(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES || !ctx->msi.dir) return;

    uint32_t npages = ctx->mem.npages < ctx->msi.cap ? ctx->mem.npages : ctx->msi.cap;

    for (uint32_t page = 0; page < npages; page++) {
        if (home_known(ctx, page) != slot) continue;

        uint32_t new_home = ring_home_of(ctx, page);
        if (new_home == DSM_SLOT_NONE || new_home >= MAX_NODES ||
            new_home == slot) {
            continue;
        }

        dir_entry_t entry = dir_snapshot(ctx, page);
        uint8_t *data = malloc(ctx->mem.page_size);
        bool have = false;
        bool transfer_failed = false;

        if (data) {
            /* La vista local y el marco de respaldo son la primera fuente: una
             * copia cacheada no siempre figura en el copyset del directorio. */
            have = page_can_serve(ctx, page, data);
            for (uint32_t s = 0; !have && s < MAX_NODES; s++) {
                if (s == ctx->memb.self_slot || s == slot ||
                    !copyset_has(&entry, s)) {
                    continue;
                }
                uint16_t status = DSM_ST_OK;
                uint32_t rx_len = ctx->mem.page_size;
                int rc = dsm_net_rpc(ctx, s, MSG_PAGE_REQUEST, page,
                                     entry.tseq, DSM_ACCESS_READ,
                                     DSM_FLAG_PROBE | DSM_FLAG_WANT_DATA,
                                     NULL, 0, &status, data, &rx_len);
                if (rc == DSM_OK && status == DSM_ST_OK &&
                    rx_len == ctx->mem.page_size) {
                    have = true;
                }
            }

            if (have) {
                uint32_t tseq = dir_tseq_next(ctx, page);
                if (new_home == ctx->memb.self_slot) {
                    (void)dsm_mem_backing_store(&ctx->mem, page, data);
                    dsm_mem_stripe_lock(&ctx->mem, page);
                    dsm_mem_install_page(&ctx->mem, page, data);
                    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_READ);
                    ctx->mem.local[page].state = DSM_PAGE_S;
                    ctx->mem.local[page].tseq_seen = tseq;
                    dsm_mem_stripe_unlock(&ctx->mem, page);
                } else {
                    uint16_t status = DSM_ST_OK;
                    uint32_t rx_len = 0;
                    int rc = dsm_net_rpc(ctx, new_home, MSG_PAGE_MOVE, page,
                                         tseq, DSM_ACCESS_READ, DSM_FLAG_HAS_DATA,
                                         data, ctx->mem.page_size, &status,
                                         NULL, &rx_len);
                    if (rc != DSM_OK || status != DSM_ST_OK) {
                        /* El nuevo home no recibio el contenido, pero la
                         * copia de este nodo sigue valida: no se destruye. */
                        have = false;
                        transfer_failed = true;
                    }
                }
            }
            free(data);
        }

        (void)dir_tseq_next(ctx, page);
        pthread_mutex_lock(&ctx->msi.dir_mu);
        dir_entry_t *d = &ctx->msi.dir[page];
        d->version++;
        ctx->msi.home_map[page] = new_home;
        if (have) {
            d->state = DSM_DIR_SHARED;
            d->owner = DSM_SLOT_NONE;
            copyset_set(d, new_home);
            if (new_home == ctx->memb.self_slot) {
                copyset_set(d, ctx->memb.self_slot);
            }
        } else {
            d->state = DSM_DIR_UNCACHED;
            d->owner = DSM_SLOT_NONE;
            memset(d->copyset, 0, sizeof(d->copyset));
        }
        pthread_mutex_unlock(&ctx->msi.dir_mu);

        if (!have) {
            if (transfer_failed) {
                /* Un traspaso fallido no destruye la copia local: sigue
                 * siendo la unica fuente y el nuevo home la recuperara con
                 * una sonda cuando la necesite. */
                DSM_LOG_WARN("Traspaso de la página %u al slot %u fallo tras "
                             "la baja del slot %u; se conserva la copia local",
                             page, new_home, slot);
            } else {
                page_invalidate_local(ctx, page);
                (void)dsm_mem_backing_drop(&ctx->mem, page);
                DSM_LOG_WARN("Página %u sin copia válida tras la baja del slot %u; "
                             "queda inválida", page, slot);
            }
        }
    }
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
    uint32_t seen_tseq = ctx->mem.local[page].tseq_seen;
    dsm_mem_stripe_unlock(&ctx->mem, page);

    int rc;
    uint32_t home = ring_home_of(ctx, page);
    if (home == ctx->memb.self_slot) {
        /* La secuencia de respaldo y delta se reserva con el busy del
         * directorio, como en el receptor del WRITEBACK. */
        pthread_mutex_lock(&ctx->msi.dir_mu);
        if (ctx->msi.dir[page].busy) {
            pthread_mutex_unlock(&ctx->msi.dir_mu);
            free(data);
            return DSM_ERR_BUSY;
        }
        ctx->msi.dir[page].busy = 1;
        pthread_mutex_unlock(&ctx->msi.dir_mu);

        rc = home_persist_delta(ctx, page, data);

        pthread_mutex_lock(&ctx->msi.dir_mu);
        ctx->msi.dir[page].busy = 0;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    } else {
        uint16_t status = DSM_ST_OK;
        uint32_t rx_len = 0;
        rc = dsm_net_rpc(ctx, home, MSG_WRITEBACK, page,
                         seen_tseq, DSM_ACCESS_WRITE,
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
