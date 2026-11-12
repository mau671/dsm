#ifndef DSM_RING_H
#define DSM_RING_H

#include <stdint.h>
#include <stdbool.h>
#include "dsm_types.h"
#include "discovery.h"

typedef struct {
    uint64_t *vpos;
    uint8_t  *vslot;
    uint32_t  n;
    uint64_t  epoch;
    uint32_t  cache_home[4096];
    uint32_t  cache_epoch[4096];
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
