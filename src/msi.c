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

static void copyset_clear(dir_entry_t *entry, uint32_t slot)
{
    if (slot < MAX_NODES) {
        entry->copyset[slot / 8] &= (uint8_t)~(1u << (slot % 8));
    }
}

static void page_invalidate_local(dsm_ctx_t *ctx, uint32_t page);
static void page_done_broadcast(dsm_ctx_t *ctx, uint32_t page);

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

/* Decisión centralizada: compara y, cuando procede, publica el tseq antes de
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

/* Predicado sin efectos: lo usa la capa de red para descartar respuestas
 * repetidas sin despertar la RPC ni tocar la tabla de páginas. */
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

/* Retira un slot del copyset publicado. Se usa cuando el nodo ya no puede
 * tener una copia válida: dejarlo ahí haría esperar un acuse que nunca llega. */
static void dir_copyset_clear(dsm_ctx_t *ctx, uint32_t page, uint32_t slot)
{
    if (page >= ctx->msi.cap || slot >= MAX_NODES) return;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    copyset_clear(&ctx->msi.dir[page], slot);
    pthread_mutex_unlock(&ctx->msi.dir_mu);
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
    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t state = ctx->mem.local[page].state;
    dsm_mem_stripe_unlock(&ctx->mem, page);
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

/* Persiste el contenido vigente de una página custodiada y, con redundancia
 * activa, emite el delta de paridad contra el respaldo previo: una página nunca
 * persistida cuenta como ceros. Cuando el contenido no cambia respecto del
 * respaldo no se genera delta, que seria un XOR de ceros. */
static int home_persist_delta(dsm_ctx_t *ctx, uint32_t page, const void *data)
{
    uint8_t *old = NULL;
    if (ctx->cfg.redundancy) {
        old = malloc(ctx->mem.page_size);
        if (old && dsm_mem_backing_load(&ctx->mem, page, old) != DSM_OK) {
            memset(old, 0, ctx->mem.page_size);
        }
    }

    int rc = dsm_mem_backing_store(&ctx->mem, page, data);
    if (rc == DSM_OK && old &&
        memcmp(old, data, ctx->mem.page_size) != 0) {
        (void)raid_on_write(ctx, page, old, data);
    } else if (rc == DSM_OK && !old && ctx->cfg.redundancy) {
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
    page_done_broadcast(ctx, page);

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

/* --- Cola FIFO de espera por página ---
 *
 * Un único mutex protege las colas de todas las páginas: es un cerrojo de
 * coordinación local, distinto del directorio y de la red. Cada finalización
 * incrementa la generación de la página y difunde su condvar, de modo que
 * todos los esperadores reintentan una sola vez sobre el estado ya publicado.
 * El mutex se libera antes de cualquier RPC: la cola nunca se retiene a través
 * de la red y los productores —hilos de aplicación, workers y listener— no
 * necesitan más que este cerrojo para encolar o despertar.
 */

static void page_done_broadcast(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx->msi.page_q || page >= ctx->msi.cap) return;

    pthread_mutex_lock(&ctx->msi.wait_mu);
    ctx->msi.page_q[page].generation++;
    pthread_cond_broadcast(&ctx->msi.page_q[page].cv);
    pthread_mutex_unlock(&ctx->msi.wait_mu);
}

/* Generación de finalizaciones de una página. El despachador la captura antes
 * de intentar el fallo y la entrega a msi_page_wait, de modo que una
 * finalización publicada entre el rechazo y el encolado no se pierda: la
 * generación ya cambió y la espera retorna de inmediato. */
uint64_t msi_page_generation(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx || !ctx->msi.page_q || page >= ctx->msi.cap) return 0;

    pthread_mutex_lock(&ctx->msi.wait_mu);
    uint64_t generation = ctx->msi.page_q[page].generation;
    pthread_mutex_unlock(&ctx->msi.wait_mu);
    return generation;
}

int msi_page_wait(dsm_ctx_t *ctx, uint32_t page, fault_slot_t *slot,
                  uint64_t since, uint32_t timeout_ms)
{
    if (!ctx || !slot || !ctx->msi.page_q || page >= ctx->msi.cap) {
        return DSM_ERR_INVALID;
    }

    page_wait_t *q = &ctx->msi.page_q[page];
    wait_rec_t rec = { .slot = slot, .page = page, .next = NULL };

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec  += (time_t)(timeout_ms / 1000u);
    deadline.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&ctx->msi.wait_mu);

    /* Encolar antes de comprobar la generación: si la finalización ocurrió
     * antes del enqueado, la generación observada ya es distinta y no se
     * bloquea; si ocurre después, la difusión la despierta. */
    if (q->tail) {
        q->tail->next = &rec;
    } else {
        q->head = &rec;
    }
    q->tail = &rec;
    q->count++;

    int rc = DSM_OK;

    for (;;) {
        if (!atomic_load(&ctx->running)) {
            rc = DSM_ERR_TIMEOUT; /* apagamiento: liberar la ranura sin esperar */
            break;
        }
        if (q->generation != since) {
            break; /* transición terminada: el despachador reintenta */
        }
        int wr = pthread_cond_timedwait(&q->cv, &ctx->msi.wait_mu, &deadline);
        if (wr == ETIMEDOUT) {
            rc = DSM_ERR_TIMEOUT;
            break;
        }
        if (wr != 0 && wr != EINTR) {
            rc = DSM_ERR_NET;
            break;
        }
        if (!atomic_load(&ctx->running)) {
            rc = DSM_ERR_TIMEOUT;
            break;
        }
    }

    /* Retirar la ranura de la cola, cualquiera que sea el desenlace. */
    wait_rec_t *prev = NULL;
    wait_rec_t *cur = q->head;
    while (cur && cur != &rec) {
        prev = cur;
        cur = cur->next;
    }
    if (cur == &rec) {
        if (prev) {
            prev->next = rec.next;
        } else {
            q->head = rec.next;
        }
        if (q->tail == &rec) q->tail = prev;
        q->count--;
    }

    pthread_mutex_unlock(&ctx->msi.wait_mu);
    return rc;
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
        page_done_broadcast(ctx, page);
        return DSM_ERR_PROTO;
    }
    if (!tseq_accept(ctx, page, tseq)) {
        /* Una respuesta antigua no debe dejar la página pendiente para
         * siempre: el hilo que la esperaba volvería a fallar y encontraría
         * IS_PEND/IM_PEND/SM_PEND en cada reintento, mientras el home ya la
         * considera concedida. Solo se descarta el estado pendiente; una
         * copia S/M válida con una secuencia mayor se conserva intacta. */
        dsm_mem_stripe_lock(&ctx->mem, page);
        uint8_t pending = ctx->mem.local[page].state;
        if (pending == DSM_PAGE_IS_PEND || pending == DSM_PAGE_IM_PEND ||
            pending == DSM_PAGE_SM_PEND) {
            (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
            ctx->mem.local[page].state = DSM_PAGE_I;
        }
        dsm_mem_stripe_unlock(&ctx->mem, page);
        page_done_broadcast(ctx, page);
        return DSM_OK;
    }

    dsm_mem_stripe_lock(&ctx->mem, page);
    if (require_pending) {
        uint8_t current = ctx->mem.local[page].state;
        bool pending = current == DSM_PAGE_IS_PEND ||
                       current == DSM_PAGE_IM_PEND ||
                       current == DSM_PAGE_SM_PEND;
        /* Una invalidación recibida mientras la petición estaba en vuelo deja
         * la página en I; la respuesta es posterior a esa invalidación y su
         * tseq ya superó la secuencia vista, así que debe instalarse. También
         * se instala M sobre S (la copia S es de solo lectura y no puede tener
         * escrituras más nuevas); omitirlo dejaba al home creyendo que poseía
         * M mientras su página seguía en S, y el hilo repetía el fallo para
         * siempre. Solo se omite cuando la copia local ya es igual o más
         * fuerte: M sobre M y S sobre M o S. */
        bool superseded = (current == state) ||
                          (state == DSM_PAGE_S && current == DSM_PAGE_M);
        if (!pending && current != DSM_PAGE_I && superseded) {
            dsm_mem_stripe_unlock(&ctx->mem, page);
            page_done_broadcast(ctx, page);
            return DSM_OK;
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
    page_done_broadcast(ctx, page);
    return rc;
}

static void page_invalidate_local(dsm_ctx_t *ctx, uint32_t page)
{
    dsm_mem_stripe_lock(&ctx->mem, page);
    /* INVALIDATE transforma cualquier copia local en I y la vuelve inaccesible. */
    (void)dsm_mem_set_prot(&ctx->mem, page, PROT_NONE);
    ctx->mem.local[page].state = DSM_PAGE_I;
    dsm_mem_stripe_unlock(&ctx->mem, page);
    page_done_broadcast(ctx, page);
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
 * cuando ha llegado un acuse de cada destinatario. Los slots que ya no
 * pertenecen a un nodo vivo se retiran del copyset: esperar su acuse agotaría
 * el plazo en cada concesión. */
static int invalidate_remote_copies(dsm_ctx_t *ctx, uint32_t page,
                                    dir_entry_t *entry, uint32_t keep_slot)
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
        if (!owner_alive(ctx, slot)) {
            copyset_clear(entry, slot);
            dir_copyset_clear(ctx, page, slot);
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
        hdr.epoch         = atomic_load(&ctx->memb.epoch);

        if (dsm_net_send(ctx, slot, &hdr, NULL) != DSM_OK) {
            msi_ack_wait_cancel(ctx, txn);
            return DSM_ERR_NET;
        }
    }

    return msi_ack_wait_result(ctx, txn, msi_ack_timeout_ms(ctx));
}

/* --- Turno exclusivo por página en el home ---
 *
 * La transacción de una página se serializa con dir[page].busy. Una solicitud
 * local que lo encuentra tomado se encola y aguarda en la condvar con plazo
 * LOCK_TIMEOUT_MS. Una solicitud remota no ocupa un worker mientras espera: se
 * deja diferida en la misma cola y, al liberarse el turno, se reencola para que
 * un worker la atienda con el turno ya reservado. Los números de secuencia
 * conservan el orden FIFO entre locales y remotas.
 *
 * El dueño jamás retiene dir_mu ni la franja de la página durante una llamada
 * de red; solo conserva busy. Un único cerrojo (txn_mu) cubre las colas de
 * todas las páginas y el orden de cerrojos es siempre txn_mu -> dir_mu. */

struct msi_txn_waiter {
    struct msi_txn_waiter *prev;
    struct msi_txn_waiter *next;
    uint64_t seq;
    bool granted;
};

struct msi_txn_deferred {
    struct msi_txn_deferred *next;
    uint64_t seq;
    dsm_msg_t msg;
};

/* Marca interna del mensaje reencolado que ya tiene el turno reservado: el
 * worker lo atiende sin volver a competir por busy. No viaja en el cable. */
#define DSM_FLAG_TXN_OWNER (1u << 6)

static void txn_deadline(struct timespec *deadline, uint32_t timeout_ms)
{
    clock_gettime(CLOCK_REALTIME, deadline);
    deadline->tv_sec  += (time_t)(timeout_ms / 1000u);
    deadline->tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
    if (deadline->tv_nsec >= 1000000000L) {
        deadline->tv_sec++;
        deadline->tv_nsec -= 1000000000L;
    }
}

/* Elige al siguiente de la cola respetando el orden de llegada entre locales y
 * remotas. Deja busy tomado por el elegido. Devuelve el mensaje diferido (que
 * el llamador debe reencolar) o NULL; granted_waiter indica que se concedió el
 * turno a un hilo local. Se llama con txn_mu tomado. */
static msi_txn_deferred_t *txn_select_locked(dsm_ctx_t *ctx, uint32_t page,
                                             bool *granted_waiter)
{
    msi_txn_queue_t *q = &ctx->msi.txn_q[page];
    msi_txn_deferred_t *defer = NULL;

    *granted_waiter = false;

    pthread_mutex_lock(&ctx->msi.dir_mu);
    ctx->msi.dir[page].busy = 0;

    bool pick_defer = q->defer_head != NULL &&
                      (q->wait_head == NULL ||
                       q->defer_head->seq < q->wait_head->seq);
    if (pick_defer) {
        defer = q->defer_head;
        q->defer_head = defer->next;
        if (!q->defer_head) q->defer_tail = NULL;
        defer->next = NULL;
        ctx->msi.dir[page].busy = 1;
    } else if (q->wait_head) {
        msi_txn_waiter_t *w = q->wait_head;
        q->wait_head = w->next;
        if (q->wait_head) {
            q->wait_head->prev = NULL;
        } else {
            q->wait_tail = NULL;
        }
        w->prev = w->next = NULL;
        w->granted = true;
        ctx->msi.dir[page].busy = 1;
        *granted_waiter = true;
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    return defer;
}

static void home_txn_release(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx || !ctx->msi.txn_q || page >= ctx->msi.cap) return;

    for (;;) {
        pthread_mutex_lock(&ctx->msi.txn_mu);
        bool granted = false;
        msi_txn_deferred_t *defer = txn_select_locked(ctx, page, &granted);
        if (granted) {
            pthread_cond_broadcast(&ctx->msi.txn_cv);
        }
        pthread_mutex_unlock(&ctx->msi.txn_mu);

        if (!defer) return;

        dsm_msg_t msg = defer->msg;
        free(defer);
        msg.hdr.flags |= DSM_FLAG_TXN_OWNER;

        uint8_t *owned = (uint8_t *)msg.payload;
        if (atomic_load(&ctx->running) && ctx->worker.threads &&
            worker_enqueue(ctx, &msg, owned) == DSM_OK) {
            return;
        }
        free(owned);
        /* Sin workers o sin capacidad de cola el mensaje se descarta y el
         * turno reservado pasa al siguiente; el cliente reintenta. Si el nodo
         * se está apagando, este bucle vacía los diferidos y libera busy. */
    }
}

/* Toma el turno de la página. Si defer_msg no es NULL y hay que esperar, la
 * solicitud remota se deja diferida y se devuelve DSM_ERR_AGAIN sin bloquear al
 * worker. Para solicitudes locales espera con plazo. Devuelve DSM_OK con busy
 * retenido, DSM_ERR_TIMEOUT si vence el plazo, DSM_ERR_AGAIN si quedó diferida
 * y DSM_ERR_MEM/INVALID si no hay recursos. */
static int home_txn_acquire(dsm_ctx_t *ctx, uint32_t page,
                            const dsm_msg_t *defer_msg, bool *contended)
{
    if (contended) *contended = false;
    if (!ctx || !ctx->msi.txn_q || page >= ctx->msi.cap) {
        return DSM_ERR_INVALID;
    }

    msi_txn_queue_t *q = &ctx->msi.txn_q[page];
    msi_txn_waiter_t self = { .prev = NULL, .next = NULL, .seq = 0,
                              .granted = false };

    pthread_mutex_lock(&ctx->msi.txn_mu);

    pthread_mutex_lock(&ctx->msi.dir_mu);
    if (!ctx->msi.dir[page].busy && q->wait_head == NULL &&
        q->defer_head == NULL) {
        ctx->msi.dir[page].busy = 1;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
        pthread_mutex_unlock(&ctx->msi.txn_mu);
        return DSM_OK;
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);

    if (contended) *contended = true;
    if (!atomic_load(&ctx->running)) {
        pthread_mutex_unlock(&ctx->msi.txn_mu);
        return DSM_ERR_TIMEOUT;
    }

    if (defer_msg) {
        msi_txn_deferred_t *node = malloc(sizeof(*node));
        if (!node) {
            pthread_mutex_unlock(&ctx->msi.txn_mu);
            return DSM_ERR_MEM;
        }
        node->next = NULL;
        node->seq  = q->seq_next++;
        node->msg  = *defer_msg;
        node->msg.payload = NULL;
        /* PAGE_REQUEST viaja sin carga; WRITEBACK conserva su página en una
         * copia propia para que el worker no quede retenido esperando el
         * turno ni dependa del buffer que libera la cola de trabajo. */
        if (defer_msg->payload && defer_msg->hdr.payload_len != 0) {
            uint8_t *copy = malloc(defer_msg->hdr.payload_len);
            if (!copy) {
                free(node);
                pthread_mutex_unlock(&ctx->msi.txn_mu);
                return DSM_ERR_MEM;
            }
            memcpy(copy, defer_msg->payload, defer_msg->hdr.payload_len);
            node->msg.payload = copy;
        }
        if (q->defer_tail) {
            q->defer_tail->next = node;
        } else {
            q->defer_head = node;
        }
        q->defer_tail = node;
        pthread_mutex_unlock(&ctx->msi.txn_mu);
        return DSM_ERR_AGAIN;
    }

    self.seq = q->seq_next++;
    self.prev = q->wait_tail;
    if (q->wait_tail) {
        q->wait_tail->next = &self;
    } else {
        q->wait_head = &self;
    }
    q->wait_tail = &self;

    struct timespec deadline;
    txn_deadline(&deadline, msi_ack_timeout_ms(ctx));
    int rc = DSM_OK;

    for (;;) {
        if (self.granted) break;
        if (!atomic_load(&ctx->running)) {
            rc = DSM_ERR_TIMEOUT;
            break;
        }
        int wr = pthread_cond_timedwait(&ctx->msi.txn_cv, &ctx->msi.txn_mu,
                                        &deadline);
        if (self.granted) break;
        if (wr == ETIMEDOUT) {
            rc = DSM_ERR_TIMEOUT;
            break;
        }
        if (wr != 0 && wr != EINTR) {
            rc = DSM_ERR_NET;
            break;
        }
    }

    if (!self.granted) {
        /* Retirar el nodo vencido sin alterar el orden de los restantes. */
        if (self.prev) {
            self.prev->next = self.next;
        } else {
            q->wait_head = self.next;
        }
        if (self.next) {
            self.next->prev = self.prev;
        } else {
            q->wait_tail = self.prev;
        }
        self.prev = self.next = NULL;
    }

    pthread_mutex_unlock(&ctx->msi.txn_mu);
    return rc;
}

void msi_abort_waits(dsm_ctx_t *ctx)
{
    if (!ctx || !ctx->msi.txn_q) return;

    pthread_mutex_lock(&ctx->msi.txn_mu);
    pthread_cond_broadcast(&ctx->msi.txn_cv);
    pthread_mutex_unlock(&ctx->msi.txn_mu);
}

/* --- Fallo local atendido por el propio home --- */

static int home_local_fault(dsm_ctx_t *ctx, uint32_t page, bool write)
{
    int acq = home_txn_acquire(ctx, page, NULL, NULL);
    if (acq != DSM_OK) return acq;

    int result = DSM_OK;
    bool owner_lost = false;
    dir_entry_t entry = dir_snapshot(ctx, page);

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
        if (status == DSM_ST_BUSY || status == DSM_ST_BAD_STATE) {
            /* El dueño no tiene una copia utilizable: está pendiente de otra
             * transacción (BUSY) o ya la perdió (BAD_STATE). En ambos casos no
             * puede haber escrituras nuevas sin confirmar, porque el home
             * persiste el contenido antes de conceder M; su respaldo es la
             * última versión confirmada. Usarlo rompe el ciclo en que el dueño
             * aguarda en la misma cola del home y este, a su vez, lo necesita
             * para degradarlo. */
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
            owner_lost = true;
        } else if (status != DSM_ST_OK ||
                   data_len != ctx->mem.page_size) {
            free(data);
            result = DSM_ERR_NET;
            goto out;
        }
        /* El dueño degradado no conserva copia utilizable; se retira del
         * copyset para no enviarle una invalidación inútil. Las demás copias
         * de lectura, si las hay, se conservan y se invalidarán en la
         * concesión de escritura. */
        if (owner_lost && entry.owner < MAX_NODES) {
            entry.copyset[entry.owner / 8u] &=
                (uint8_t)~(1u << (entry.owner % 8u));
        }
    } else if (entry.state == DSM_DIR_MODIFIED &&
               entry.owner != DSM_SLOT_NONE &&
               entry.owner != ctx->memb.self_slot &&
               !owner_alive(ctx, entry.owner)) {
        /* LIMITACIÓN: owner muerto en M: el respaldo no se usa porque puede
         * contener una versión anterior a la única escritura pendiente.
         *
         * Decisión: se reporta DSM_ERR_NODE_DOWN y no DSM_ERR_BUSY. La
         * condición es permanente —nadie devolverá la copia en M— así que
         * reintentar no la resuelve; el error terminal hace que el
         * despachador despierte la ranura de inmediato, sin ciclos de
         * espera, y el hilo de aplicación decide al reintentar el acceso.
         * El estado local ya quedó invalidado por el llamador. */
        result = DSM_ERR_NODE_DOWN;
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
        /* Se conserva el resto del copyset: una copia de lectura remota que
         * no se invalidó sigue siendo válida y debe poder invalidarse después. */
        copyset_set(&entry, ctx->memb.self_slot);
        if (!owner_lost && previous_owner != DSM_SLOT_NONE) {
            copyset_set(&entry, previous_owner);
        }
    }
    ctx->msi.dir[page] = entry;
    pthread_mutex_unlock(&ctx->msi.dir_mu);
    result = DSM_OK;

out:
    home_txn_release(ctx, page);
    page_done_broadcast(ctx, page);
    return result;
}

/* --- Inicialización --- */

int msi_init(dsm_ctx_t *ctx)
{
    dsm_msi_t *msi = &ctx->msi;
    memset(msi, 0, sizeof(*msi));
    if (pthread_mutex_init(&msi->dir_mu, NULL) != 0) {
        return DSM_ERR_MEM;
    }
    if (pthread_mutex_init(&msi->wait_mu, NULL) != 0) {
        pthread_mutex_destroy(&msi->dir_mu);
        return DSM_ERR_MEM;
    }
    if (pthread_mutex_init(&msi->txn_mu, NULL) != 0) {
        pthread_mutex_destroy(&msi->wait_mu);
        pthread_mutex_destroy(&msi->dir_mu);
        return DSM_ERR_MEM;
    }
    if (pthread_cond_init(&msi->txn_cv, NULL) != 0) {
        pthread_mutex_destroy(&msi->txn_mu);
        pthread_mutex_destroy(&msi->wait_mu);
        pthread_mutex_destroy(&msi->dir_mu);
        return DSM_ERR_MEM;
    }

    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        if (sem_init(&msi->ack_waits[i].sem, 0, 0) != 0) {
            for (uint32_t j = 0; j < i; j++) {
                sem_destroy(&msi->ack_waits[j].sem);
            }
            pthread_cond_destroy(&msi->txn_cv);
            pthread_mutex_destroy(&msi->txn_mu);
            pthread_mutex_destroy(&msi->wait_mu);
            pthread_mutex_destroy(&msi->dir_mu);
            return DSM_ERR_MEM;
        }
    }

    if (ctx->cfg.page_size == 0 || ctx->cfg.view_size < ctx->cfg.page_size) {
        return DSM_OK;
    }

    msi->cap = (size_t)(ctx->cfg.view_size / ctx->cfg.page_size);
    msi->dir = calloc(msi->cap, sizeof(*msi->dir));
    if (!msi->dir) {
        msi->cap = 0;
        goto fail_resources;
    }

    msi->home_map = malloc(msi->cap * sizeof(*msi->home_map));
    if (!msi->home_map) {
        free(msi->dir);
        msi->dir = NULL;
        msi->cap = 0;
        goto fail_resources;
    }

    msi->page_q = calloc(msi->cap, sizeof(*msi->page_q));
    if (!msi->page_q) {
        free(msi->home_map);
        msi->home_map = NULL;
        free(msi->dir);
        msi->dir = NULL;
        msi->cap = 0;
        goto fail_resources;
    }

    msi->txn_q = calloc(msi->cap, sizeof(*msi->txn_q));
    if (!msi->txn_q) {
        free(msi->page_q);
        msi->page_q = NULL;
        free(msi->home_map);
        msi->home_map = NULL;
        free(msi->dir);
        msi->dir = NULL;
        msi->cap = 0;
        goto fail_resources;
    }

    for (size_t i = 0; i < msi->cap; i++) {
        if (pthread_cond_init(&msi->page_q[i].cv, NULL) != 0) {
            for (size_t j = 0; j < i; j++) {
                pthread_cond_destroy(&msi->page_q[j].cv);
            }
            free(msi->txn_q);
            msi->txn_q = NULL;
            free(msi->page_q);
            msi->page_q = NULL;
            free(msi->home_map);
            msi->home_map = NULL;
            free(msi->dir);
            msi->dir = NULL;
            msi->cap = 0;
            goto fail_resources;
        }
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

fail_resources:
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        sem_destroy(&msi->ack_waits[i].sem);
    }
    pthread_cond_destroy(&msi->txn_cv);
    pthread_mutex_destroy(&msi->txn_mu);
    pthread_mutex_destroy(&msi->wait_mu);
    pthread_mutex_destroy(&msi->dir_mu);
    return DSM_ERR_MEM;
}

void msi_destroy(dsm_ctx_t *ctx)
{
    if (ctx->msi.page_q) {
        for (size_t i = 0; i < ctx->msi.cap; i++) {
            pthread_cond_destroy(&ctx->msi.page_q[i].cv);
        }
        free(ctx->msi.page_q);
        ctx->msi.page_q = NULL;
    }
    if (ctx->msi.txn_q) {
        for (size_t i = 0; i < ctx->msi.cap; i++) {
            msi_txn_deferred_t *node = ctx->msi.txn_q[i].defer_head;
            while (node) {
                msi_txn_deferred_t *next = node->next;
                free((void *)node->msg.payload);
                free(node);
                node = next;
            }
        }
    }
    free(ctx->msi.txn_q);
    ctx->msi.txn_q = NULL;
    free(ctx->msi.home_map);
    ctx->msi.home_map = NULL;
    free(ctx->msi.dir);
    ctx->msi.dir = NULL;
    ctx->msi.cap = 0;
    for (uint32_t i = 0; i < MSI_ACK_WAIT_SLOTS; i++) {
        sem_destroy(&ctx->msi.ack_waits[i].sem);
    }
    pthread_cond_destroy(&ctx->msi.txn_cv);
    pthread_mutex_destroy(&ctx->msi.txn_mu);
    pthread_mutex_destroy(&ctx->msi.wait_mu);
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
        /* El fallo demuestra que el permiso de la vista no corresponde al
         * estado publicado (por ejemplo, una transición concurrente dejó el
         * PTE anterior). Se restablece el permiso antes de devolver el acceso
         * para que el hilo no vuelva a fallar en el mismo almacén. */
        int prot = (current == DSM_PAGE_M) ? (PROT_READ | PROT_WRITE)
                                           : PROT_READ;
        int prc = dsm_mem_set_prot(&ctx->mem, page, prot);
        dsm_mem_stripe_unlock(&ctx->mem, page);
        return (prc == DSM_OK) ? DSM_OK : DSM_ERR_MEM;
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

    /* La transición a pendiente es atómica con la comprobación del estado:
     * si otro fallo de esta página ya la dejó pendiente, este hilo no
     * sobrescribe ese estado ni inicia una segunda petición. Dos fallos
     * simultáneos ya no pueden pisarse el IS_PEND/IM_PEND/SM_PEND y provocar
     * la cascada de respuestas rechazadas por page_apply. */
    dsm_mem_stripe_lock(&ctx->mem, page);
    current = ctx->mem.local[page].state;
    if ((!write && (current == DSM_PAGE_S || current == DSM_PAGE_M)) ||
        (write && current == DSM_PAGE_M)) {
        int prot = (current == DSM_PAGE_M) ? (PROT_READ | PROT_WRITE)
                                           : PROT_READ;
        int prc = dsm_mem_set_prot(&ctx->mem, page, prot);
        dsm_mem_stripe_unlock(&ctx->mem, page);
        return (prc == DSM_OK) ? DSM_OK : DSM_ERR_MEM;
    }
    if (current == DSM_PAGE_IS_PEND || current == DSM_PAGE_IM_PEND ||
        current == DSM_PAGE_SM_PEND) {
        dsm_mem_stripe_unlock(&ctx->mem, page);
        return DSM_ERR_BUSY;
    }
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
    } else if (rc == DSM_OK && status == DSM_ST_NODE_DOWN) {
        /* El home informó de un dueño remoto muerto en M: error terminal para
         * esta ranura. La página sigue inválida y el despachador no la vuelve a
         * encolar en el mismo estado. */
        page_invalidate_local(ctx, page);
        rc = DSM_ERR_NODE_DOWN;
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
    if (!ctx || !m) return;
    uint32_t page = m->hdr.page_id;
    /* Un mensaje reencolado ya trae el turno reservado por la liberación
     * anterior; no debe volver a competir por busy. */
    bool reserved = (m->hdr.flags & DSM_FLAG_TXN_OWNER) != 0;
    if (page >= ctx->mem.npages) {
        if (reserved) home_txn_release(ctx, page);
        return;
    }

    /* Las sondas llegan a nodos que ya no son home y deben contestarse con el
     * contenido que conserven. Un mensaje reencolado con el turno reservado
     * nunca es una sonda. */
    if (!reserved && (m->hdr.flags & DSM_FLAG_PROBE)) {
        home_handle_probe(ctx, m);
        return;
    }

    uint32_t requester = m->hdr.src_slot;
    bool write = m->hdr.access == DSM_ACCESS_WRITE;

    /* Un nodo que dejó de ser home rechaza la solicitud: el solicitante vuelve
     * a resolver el anillo y no quedan dos custodios sirviendo la página. */
    if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
        send_nack(ctx, m, DSM_ST_NOT_HOME);
        goto out;
    }

    if (!reserved) {
        bool contended = false;
        int acq = home_txn_acquire(ctx, page, m, &contended);
#if defined(DSM_TEST_HOOKS)
        if (contended) {
            pthread_mutex_lock(&g_busy_test_mu);
            g_busy_test_second_seen = true;
            pthread_mutex_unlock(&g_busy_test_mu);
        }
#endif
        if (acq == DSM_ERR_AGAIN) {
            /* Quedó diferida en la cola FIFO de la página; se reencolará al
             * liberarse el turno. El worker vuelve de inmediato. */
            return;
        }
        if (acq != DSM_OK) {
            /* Solo se rechaza cuando no hay recursos para encolar. */
            send_nack(ctx, m, DSM_ST_BUSY);
            return;
        }
        reserved = true;
    }

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
    bool owner_lost = false;

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
        /* Dueño muerto en M: no se sirve una copia del respaldo porque puede
         * ser anterior a la única escritura pendiente. El rechazo es terminal;
         * DSM_ST_NODE_DOWN distingue esta condición de un BUSY transitorio y
         * evita que el solicitante reintente sin avanzar. */
        send_nack(ctx, m, DSM_ST_NODE_DOWN);
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
        if (status == DSM_ST_BUSY || status == DSM_ST_BAD_STATE) {
            /* El dueño remoto está pendiente de otra transacción o perdió la
             * copia: el home conserva en su respaldo la última versión
             * confirmada, persistida antes de conceder M, así que puede
             * continuar sin esperar a que el dueño salga de su propio turno.
             * Esto corta el ciclo dueño pendiente -> home espera degradación. */
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
            owner_lost = true;
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

    /* El dueño que no pudo entregar la copia queda fuera del copyset; las
     * demás copias de lectura siguen siendo válidas y se invalidarán antes de
     * cualquier concesión de escritura. */
    if (owner_lost && entry.owner < MAX_NODES) {
        entry.copyset[entry.owner / 8u] &=
            (uint8_t)~(1u << (entry.owner % 8u));
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
        /* Las copias de lectura de otros nodos siguen siendo válidas: el
         * copyset se conserva para poder invalidarlas en la próxima
         * concesión de escritura. Borrarlo aquí las dejaba vivas pero
         * olvidadas, y sus lecturas posteriores devolvían contenido viejo. */
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

    /* El contenido recién obtenido puede ser la única copia de las últimas
     * escrituras del dueño: se persiste antes de cualquier paso que pueda
     * abortar la concesión. Si la invalidación falla, el respaldo ya quedó
     * actualizado y se descarta la copia local para no volver a servir una
     * versión anterior en el reintento. */
    (void)home_persist_delta(ctx, page, data);

    /* Antes de conceder M hay que retirar toda copia de lectura, incluso si el
     * contenido salió del respaldo porque el dueño estaba pendiente o perdido:
     * el copyset conserva a los nodos que aún la tienen y ellos deben quedar
     * invalidados para no leer una versión anterior. */
    rc = invalidate_remote_copies(ctx, page, &entry, requester);
    if (rc != DSM_OK) {
        page_invalidate_local(ctx, page);
        free(data);
        send_nack(ctx, m, DSM_ST_BUSY);
        goto out;
    }

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
        home_txn_release(ctx, page);
    }
    page_done_broadcast(ctx, page);
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
    uint32_t page = m->hdr.page_id;

    /* La invalidación pertenece a una transacción del home posterior a
     * cualquier concesión anterior. Registrar su secuencia permite descartar
     * una respuesta vieja que llegue después por otro enlace, en lugar de
     * aplicarla sobre la copia que el home ya consideraba invalidada. */
    if (m->hdr.tseq != 0) {
        dsm_mem_stripe_lock(&ctx->mem, page);
        if (m->hdr.tseq > ctx->mem.local[page].tseq_seen) {
            ctx->mem.local[page].tseq_seen = m->hdr.tseq;
        }
        dsm_mem_stripe_unlock(&ctx->mem, page);
    }

    /* INVALIDATE fuerza S/M -> I antes de confirmar al home: el ACK solo se
     * envía cuando no queda ninguna copia accesible localmente. */
    page_invalidate_local(ctx, page);
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
    /* Una degradación es una transacción del home posterior a cualquier
     * concesión anterior. Registrar su secuencia hace que una GRANT atrasada
     * de esa concesión anterior se descarte si llega después: de lo contrario
     * este nodo volvería a M después de que el home ya concedió la página a
     * otro, y una invalidación posterior perdería sus escrituras. */
    if (m->hdr.tseq != 0 && m->hdr.tseq > ctx->mem.local[page].tseq_seen) {
        ctx->mem.local[page].tseq_seen = m->hdr.tseq;
    }
    uint8_t state = ctx->mem.local[page].state;
    if (state == DSM_PAGE_IS_PEND || state == DSM_PAGE_IM_PEND) {
        status = DSM_ST_BUSY;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    } else if (state != DSM_PAGE_M && state != DSM_PAGE_S &&
               state != DSM_PAGE_SM_PEND) {
        status = DSM_ST_BAD_STATE;
        dsm_mem_stripe_unlock(&ctx->mem, page);
    } else {
        /* Se sirven M, S y SM_PEND. En SM_PEND la actualización a M todavía
         * está en curso, pero el contenido subyacente sigue siendo la copia S
         * válida: las escrituras que provocaron el fallo aún no ocurrieron.
         * Servirla rompe el ciclo en que el home necesita degradar al dueño
         * mientras ese dueño aguarda su propio turno en la misma cola. En M se
         * retira el permiso de escritura antes de copiar para que ningún hilo
         * modifique el contenido mientras se produce la respuesta. */
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
            if (state == DSM_PAGE_M) {
                ctx->mem.local[page].state = DSM_PAGE_S;
            }
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
    page_done_broadcast(ctx, page);
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
    bool reserved = (m->hdr.flags & DSM_FLAG_TXN_OWNER) != 0;

    /* La secuencia de leer el respaldo, persistir y emitir el delta de
     * paridad no puede intercalarse con otra de la misma página. Si el turno
     * está ocupado, el WRITEBACK se deja diferido con su payload y el worker
     * queda libre: dsm_sync no consume el pool esperando una página ajena. */
    if (!reserved) {
        if (ring_home_of(ctx, page) != ctx->memb.self_slot) {
            status = DSM_ST_NOT_HOME;
        } else {
            int acq = home_txn_acquire(ctx, page, m, NULL);
            if (acq == DSM_ERR_AGAIN) {
                return; /* diferido: el turno lo atenderá desde la cola */
            }
            if (acq != DSM_OK) {
                status = DSM_ST_BUSY;
            } else {
                reserved = true;
            }
        }
    }

    if (reserved) {
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

        home_txn_release(ctx, page);
    }
    page_done_broadcast(ctx, page);

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
    page_done_broadcast(ctx, page);
}

/* Reasigna las páginas cuyo custodio era el nodo caído. Solo se sirve una copia
 * cuando hay datos vigentes; sin copia la página queda inválida y nadie la
 * anuncia como compartida. */
void msi_on_node_down(dsm_ctx_t *ctx, uint32_t slot)
{
    if (!ctx || slot >= MAX_NODES || !ctx->msi.dir) return;

    uint32_t npages = ctx->mem.npages < ctx->msi.cap ? ctx->mem.npages : ctx->msi.cap;

    /* Un slot caído no puede conservar copias válidas: se retira de todos los
     * copyset, no solo de las páginas cuyo home era ese nodo. De lo contrario
     * la próxima concesión de escritura espera un acuse que nunca llegará. */
    pthread_mutex_lock(&ctx->msi.dir_mu);
    for (uint32_t page = 0; page < npages; page++) {
        copyset_clear(&ctx->msi.dir[page], slot);
    }
    pthread_mutex_unlock(&ctx->msi.dir_mu);

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
        page_done_broadcast(ctx, page);

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
        /* La secuencia de respaldo y delta toma el turno del directorio, como
         * en el receptor del WRITEBACK. Si está ocupado se espera en la cola
         * con plazo en lugar de fallar de inmediato. */
        int acq = home_txn_acquire(ctx, page, NULL, NULL);
        if (acq != DSM_OK) {
            free(data);
            return (acq == DSM_ERR_TIMEOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_BUSY;
        }

        rc = home_persist_delta(ctx, page, data);

        home_txn_release(ctx, page);
        page_done_broadcast(ctx, page);
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

int msi_release_page(dsm_ctx_t *ctx, uint32_t page)
{
    if (!ctx || page >= ctx->mem.npages || page >= ctx->msi.cap) {
        return DSM_ERR_INVALID;
    }

    dsm_mem_stripe_lock(&ctx->mem, page);
    uint8_t state = ctx->mem.local[page].state;
    dsm_mem_stripe_unlock(&ctx->mem, page);

    /* Una transición en curso puede observar un contenido a medias: el bloque
     * no se libera hasta que la página quede estable. */
    if (state == DSM_PAGE_IS_PEND || state == DSM_PAGE_IM_PEND ||
        state == DSM_PAGE_SM_PEND) {
        return DSM_ERR_BUSY;
    }

    if (state == DSM_PAGE_M) {
        int rc = msi_writeback_page(ctx, page);
        if (rc != DSM_OK) return rc;
    }

    if (ring_home_of(ctx, page) == ctx->memb.self_slot) {
        /* Antes de olvidar el copyset hay que retirar las copias remotas: si
         * quedaran vivas, una reserva posterior del mismo rango las leería
         * como contenido vigente. El intento es acotado por el plazo de
         * acuses y omite los slots muertos. */
        uint32_t tseq = dir_tseq_next(ctx, page);
        dir_entry_t entry = dir_snapshot(ctx, page);
        entry.tseq = tseq;

        int inv_rc = invalidate_remote_copies(ctx, page, &entry,
                                              ctx->memb.self_slot);
        if (inv_rc != DSM_OK) {
            /* La entrada conserva su copyset y su secuencia ya avanzó: una
             * próxima concesión de escritura volverá a intentar invalidar.
             * La página no se libera, así que page_pull no puede adoptar una
             * copia vieja como si fuera el contenido de un rango nuevo. */
            page_done_broadcast(ctx, page);
            return (inv_rc == DSM_ERR_NO_SPACE) ? inv_rc : DSM_ERR_BUSY;
        }

        /* El home es este nodo: la copia deja de existir, pero el respaldo
         * conserva el contenido para la próxima reserva. */
        pthread_mutex_lock(&ctx->msi.dir_mu);
        dir_entry_t *e = &ctx->msi.dir[page];
        e->state  = DSM_DIR_UNCACHED;
        e->owner  = DSM_SLOT_NONE;
        memset(e->copyset, 0, sizeof(e->copyset));
        e->tseq = tseq;
        pthread_mutex_unlock(&ctx->msi.dir_mu);
    }
    /* La página volvió a un estado estable: despertar a los esperadores de la
     * cola para que reintenten sobre el directorio ya publicado. */
    page_done_broadcast(ctx, page);
    return DSM_OK;
}

void msi_counters(dsm_ctx_t *ctx, uint64_t *out_m, uint64_t *out_s, uint64_t *out_i)
{
    uint64_t m = 0, s = 0, i = 0;
    if (ctx && ctx->mem.local) {
        for (uint32_t p = 0; p < ctx->mem.npages; p++) {
            /* El estado de la página se copia bajo el cerrojo de franja: es
             * una consulta de diagnóstico y no debe leer una escritura a
             * medias de otra transacción. */
            dsm_mem_stripe_lock(&ctx->mem, p);
            uint8_t state = ctx->mem.local[p].state;
            dsm_mem_stripe_unlock(&ctx->mem, p);
            switch (state) {
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
