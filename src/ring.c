#include "ring.h"
#include "dsm_internal.h"
#include <stdlib.h>
#include <string.h>

uint64_t ring_vnode_pos(const uint8_t uuid[16], uint32_t idx)
{
    uint64_t u0 = 0, u1 = 0;
    memcpy(&u0, uuid, 8);
    memcpy(&u1, uuid + 8, 8);
    return fmix64(u0 ^ fmix64(u1 ^ (uint64_t)idx));
}

int ring_build(dsm_ring_t *r, dsm_membership_t *m)
{
    (void)r;
    (void)m;
    return DSM_OK;
}

void ring_free(dsm_ring_t *r)
{
    (void)r;
}

uint32_t ring_home_of(dsm_ctx_t *ctx, uint32_t page_id)
{
    /* Mapeo determinista modular sobre nodos vivos para el prototipo 1 */
    uint32_t live_slots[MAX_NODES];
    uint32_t count = 0;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (membership_slot_present(ctx, s) && ctx->memb.nodes[s].state != DSM_NODE_DEAD) {
            live_slots[count++] = s;
        }
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (count == 0) return ctx->memb.self_slot;
    return live_slots[page_id % count];
}
