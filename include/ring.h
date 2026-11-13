#ifndef DSM_RING_H
#define DSM_RING_H

#include <stdint.h>
#include <stdbool.h>
#include "dsm_types.h"
#include "discovery.h"

/* Anillo de dispersión consistente de 64 bits ponderado por memoria.
 *
 * Cada nodo vivo proyecta varios vnodes sobre el anillo, en cantidad
 * proporcional a su memoria (VNODES_PER_MB por mebibyte aportado). Los vnodes
 * se guardan ordenados por posición para resolver el home de una página con
 * búsqueda binaria. */
typedef struct {
    uint64_t *vpos;          /* Posición de cada vnode, ascendente */
    uint32_t *vslot;         /* Ranura dueña de la posición homóloga */
    uint32_t  n;             /* Vnodes actuales */
    uint32_t  cap;           /* Capacidad reservada de vpos y vslot */
    uint64_t  epoch;         /* Época de membresía con la que se construyó */
    uint64_t  sig;           /* Firma de la membresía usada al construir */
    bool      built;         /* El arreglo refleja una membresía no vacía */
} dsm_ring_t;

struct dsm_ctx;
typedef struct dsm_ctx dsm_ctx_t;

static inline uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

uint64_t ring_vnode_pos(const uint8_t uuid[16], uint32_t idx);
int      ring_build(dsm_ring_t *r, dsm_membership_t *m);
void     ring_free(dsm_ring_t *r);
uint32_t ring_home_of(dsm_ctx_t *ctx, uint32_t page_id);

#endif /* DSM_RING_H */
