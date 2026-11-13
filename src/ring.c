#include "ring.h"
#include "dsm_internal.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* Vnodes por mebibyte cuando la configuración no define VNODES_PER_MB */
#define RING_DEFAULT_VNODES_PER_MB 16u

/* Tope de vnodes que un nodo puede proyectar, para acotar el arreglo */
#define RING_MAX_VNODES_PER_NODE (1u << 20)

/* Serializa la construcción del anillo de cada contexto: el arreglo se
 * reemplaza en sitio mientras otros hilos lo consultan. */
static pthread_mutex_t g_ring_mu = PTHREAD_MUTEX_INITIALIZER;

uint64_t ring_vnode_pos(const uint8_t uuid[16], uint32_t idx)
{
    uint64_t u0 = 0, u1 = 0;
    memcpy(&u0, uuid, 8);
    memcpy(&u1, uuid + 8, 8);
    return fmix64(u0 ^ fmix64(u1 ^ (uint64_t)idx));
}

/* Cantidad de vnodes de un nodo a partir de las páginas que aporta. Un cliente
 * no aporta páginas y no custodia ninguna: devuelve 0 y queda fuera del anillo,
 * de modo que ring_home_of nunca lo elige. */
static uint32_t ring_node_vnodes(const dsm_membership_t *m, uint32_t slot)
{
    if (m->nodes[slot].is_client || m->nodes[slot].mem_pages == 0) {
        return 0;
    }

    uint64_t page_size = m->cfg ? m->cfg->page_size : 0;
    uint64_t per_mb    = m->cfg ? m->cfg->vnodes_per_mb : 0;
    if (page_size == 0) page_size = 4096u;
    if (per_mb == 0) per_mb = RING_DEFAULT_VNODES_PER_MB;

    uint64_t mem_bytes = (uint64_t)m->nodes[slot].mem_pages * page_size;
    uint64_t vnodes = (mem_bytes / (1024u * 1024u)) * per_mb;

    if (vnodes == 0) vnodes = 1;
    if (vnodes > RING_MAX_VNODES_PER_NODE) vnodes = RING_MAX_VNODES_PER_NODE;
    return (uint32_t)vnodes;
}

/* Ranura que participa del anillo: presente, viva y con memoria que custodiar.
 * Un cliente no entra aquí, así que su alta o baja no reconstruye el anillo ni
 * desplaza el home de ninguna página. */
static bool ring_slot_live(const dsm_membership_t *m, uint32_t slot)
{
    if (slot >= MAX_NODES) return false;
    if ((m->present[slot / 8] & (uint8_t)(1u << (slot % 8))) == 0) return false;
    if (m->nodes[slot].state == DSM_NODE_DEAD) return false;
    return !m->nodes[slot].is_client && m->nodes[slot].mem_pages > 0;
}

/* Arreglo auxiliar de pares (posición, ranura) para ordenar juntos */
typedef struct {
    uint64_t pos;
    uint32_t slot;
} ring_pair_t;

/* Orden ascendente por posición; a igual posición gana la ranura menor para
 * que el orden no dependa de la secuencia de inserción. */
static int ring_pair_cmp(const void *a, const void *b)
{
    const ring_pair_t *x = (const ring_pair_t *)a;
    const ring_pair_t *y = (const ring_pair_t *)b;
    if (x->pos < y->pos) return -1;
    if (x->pos > y->pos) return 1;
    if (x->slot < y->slot) return -1;
    if (x->slot > y->slot) return 1;
    return 0;
}

/* Firma barata de la membresía: detecta altas, bajas, cambios de estado y de
 * memoria aportada sin depender de que la época se incremente en cada cambio. */
static uint64_t ring_membership_sig(const dsm_membership_t *m)
{
    uint64_t sig = fmix64(m->epoch ^ 0x9e3779b97f4a7c15ULL);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!ring_slot_live(m, s)) continue;
        uint64_t h = fmix64(((uint64_t)s << 32) ^ m->nodes[s].mem_pages);
        h ^= fmix64((uint64_t)m->nodes[s].state + 0x100);
        for (uint32_t i = 0; i < 16; i += 8) {
            uint64_t chunk = 0;
            memcpy(&chunk, m->nodes[s].uuid + i, 8);
            h ^= fmix64(chunk);
        }
        sig ^= fmix64(h + s);
    }
    return sig;
}

int ring_build(dsm_ring_t *r, dsm_membership_t *m)
{
    if (!r || !m) return DSM_ERR_INVALID;

    uint32_t total = 0;
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (ring_slot_live(m, s)) {
            total += ring_node_vnodes(m, s);
        }
    }

    if (total == 0) {
        /* Sin ningun nodo con memoria no hay custodio posible para ninguna
         * página: un cliente solo puede operar contra un cluster con memoria. */
        r->n     = 0;
        r->built = false;
        r->epoch = m->epoch;
        r->sig   = ring_membership_sig(m);
        DSM_LOG_ERR("El anillo no tiene ningun nodo con memoria: se necesita "
                    "al menos un nodo que aporte paginas");
        return DSM_ERR_NO_SPACE;
    }

    ring_pair_t *pairs = malloc((size_t)total * sizeof(*pairs));
    if (!pairs) return DSM_ERR_MEM;

    uint32_t k = 0;
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (!ring_slot_live(m, s)) continue;
        uint32_t vnodes = ring_node_vnodes(m, s);
        for (uint32_t v = 0; v < vnodes; v++) {
            pairs[k].pos  = ring_vnode_pos(m->nodes[s].uuid, v);
            pairs[k].slot = s;
            k++;
        }
    }

    qsort(pairs, total, sizeof(*pairs), ring_pair_cmp);

    if (r->cap < total) {
        uint64_t *nvpos  = realloc(r->vpos, (size_t)total * sizeof(*nvpos));
        if (!nvpos) {
            free(pairs);
            return DSM_ERR_MEM;
        }
        r->vpos = nvpos;
        uint32_t *nvslot = realloc(r->vslot, (size_t)total * sizeof(*nvslot));
        if (!nvslot) {
            free(pairs);
            return DSM_ERR_MEM;
        }
        r->vslot = nvslot;
        r->cap   = total;
    }

    for (uint32_t i = 0; i < total; i++) {
        r->vpos[i]  = pairs[i].pos;
        r->vslot[i] = pairs[i].slot;
    }
    free(pairs);

    r->n     = total;
    r->built = true;
    r->epoch = m->epoch;
    r->sig   = ring_membership_sig(m);
    return DSM_OK;
}

void ring_free(dsm_ring_t *r)
{
    if (!r) return;
    free(r->vpos);
    free(r->vslot);
    r->vpos  = NULL;
    r->vslot = NULL;
    r->n     = 0;
    r->cap   = 0;
    r->epoch = 0;
    r->sig   = 0;
    r->built = false;
}

/* Primer vnode con posición mayor o igual a pos; con wrap-around al inicio */
static uint32_t ring_lookup(const dsm_ring_t *r, uint64_t pos)
{
    uint32_t lo = 0, hi = r->n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (r->vpos[mid] < pos) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return (lo == r->n) ? 0 : lo;
}

/* Ranura de respaldo cuando el anillo no se puede construir. Prefiere la
 * propia si aporta memoria; si no, el primer nodo vivo con memoria, para que un
 * cliente nunca quede como custodio de una página. */
static uint32_t ring_fallback_slot(const dsm_ctx_t *ctx, uint32_t self)
{
    const dsm_membership_t *m = &ctx->memb;

    if (self < MAX_NODES && !m->nodes[self].is_client && m->nodes[self].mem_pages > 0) {
        return self;
    }
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (ring_slot_live(m, s)) {
            return s;
        }
    }
    return (self != DSM_SLOT_NONE) ? self : 0;
}

uint32_t ring_home_of(dsm_ctx_t *ctx, uint32_t page_id)
{    /* Determinismo: la posición de cada vnode depende solo del UUID del nodo y
     * del índice, y la cantidad de vnodes solo de su memoria, de modo que dos
     * nodos con la misma membresía y la misma configuración obtienen el mismo
     * home para toda página. Si durante el descubrimiento un nodo tiene una
     * vista de membresía distinta, el home puede diferir hasta que las tablas
     * converjan; el diseño lo tolera porque los cambios de membresía son poco
     * frecuentes y las solicitudes resuelven con reintento. */
    dsm_membership_t *m = &ctx->memb;
    uint32_t home = DSM_SLOT_NONE;
    uint32_t self = DSM_SLOT_NONE;

    pthread_mutex_lock(&g_ring_mu);

    pthread_rwlock_rdlock(&m->rw);
    self = m->self_slot;
    uint64_t sig = ring_membership_sig(m);
    if (ctx->ring.built && ctx->ring.sig == sig) {
        home = ctx->ring.vslot[ring_lookup(&ctx->ring, fmix64(page_id))];
    }
    pthread_rwlock_unlock(&m->rw);

    if (home == DSM_SLOT_NONE) {
        /* La membresía cambió o el anillo aún no existe: reconstruirlo. La
         * construcción toma el rwlock de escritura. */
        pthread_rwlock_wrlock(&m->rw);
        int rc = ring_build(&ctx->ring, m);
        if (rc == DSM_OK && ctx->ring.built) {
            home = ctx->ring.vslot[ring_lookup(&ctx->ring, fmix64(page_id))];
        }
        pthread_rwlock_unlock(&m->rw);
    }

    pthread_mutex_unlock(&g_ring_mu);

    if (home == DSM_SLOT_NONE) {
        /* Sin anillo construible no hay custodio determinista: se elige un
         * nodo con memoria, nunca un cliente, que no puede custodiar. */
        home = ring_fallback_slot(ctx, self);
    }
    return home;
}
