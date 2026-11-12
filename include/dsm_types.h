#ifndef DSM_TYPES_H
#define DSM_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>

#ifndef MAX_NODES
#define MAX_NODES 256
#endif

#define DSM_SLOT_NONE       UINT32_C(0xFFFFFFFF)
#define DSM_PAGE_ID_NONE    UINT32_C(0xFFFFFFFF)
#define DSM_TXN_NONE        UINT32_C(0xFFFFFFFF)
#define DSM_WAIT_NONE       UINT32_C(0xFFFFFFFF)
#define DSM_MAX_PAYLOAD     (1024u * 1024u)
#define DSM_FAULTQ_CAP      4096u

typedef enum {
    DSM_OK            =  0,
    DSM_ERR_CONFIG    = -1,
    DSM_ERR_NOT_INIT  = -2,
    DSM_ERR_MEM       = -3,
    DSM_ERR_NET       = -4,
    DSM_ERR_TIMEOUT   = -5,
    DSM_ERR_NO_SPACE  = -6,
    DSM_ERR_INVALID   = -7,
    DSM_ERR_NODE_DOWN = -8,
    DSM_ERR_BUSY      = -9,
    DSM_ERR_PROTO     = -10,
    DSM_ERR_INTR      = -11,
    DSM_ERR_AGAIN     = -12,
    DSM_ERR_NOT_FOUND = -13
} dsm_status_t;

/* Estados MSI locales. El valor numérico viaja en mensajes: no reordenar. */
typedef enum {
    DSM_PAGE_I       = 0,
    DSM_PAGE_S       = 1,
    DSM_PAGE_M       = 2,
    DSM_PAGE_IS_PEND = 3,
    DSM_PAGE_IM_PEND = 4,
    DSM_PAGE_SM_PEND = 5
} dsm_page_state_t;

/* Estado del directorio en el home. */
typedef enum {
    DSM_DIR_UNCACHED = 0,
    DSM_DIR_SHARED   = 1,
    DSM_DIR_MODIFIED = 2
} dsm_dir_state_t;

/* Estado de membresía. */
typedef enum {
    DSM_NODE_ALIVE   = 1,
    DSM_NODE_SUSPECT = 2,
    DSM_NODE_DEAD    = 3
} dsm_node_state_t;

/* Banderas de la cabecera del mensaje. */
enum {
    DSM_FLAG_WANT_DATA = 1u << 0,
    DSM_FLAG_HAS_DATA  = 1u << 1,
    DSM_FLAG_UPGRADE   = 1u << 2,
    DSM_FLAG_RETRY     = 1u << 3,
    DSM_FLAG_PROBE     = 1u << 4
};

/* Códigos de diagnóstico y rechazo (campo status). */
typedef enum {
    DSM_ST_OK              = 0,
    DSM_ST_STALE_EPOCH     = 1,
    DSM_ST_NOT_HOME        = 2,
    DSM_ST_NOT_CLUSTER     = 3,
    DSM_ST_BAD_STATE       = 4,
    DSM_ST_BUSY            = 5,
    DSM_ST_SLOT_TAKEN      = 6,
    DSM_ST_NO_SPACE        = 7,
    DSM_ST_CONFIG_MISMATCH = 8
} dsm_status_code_t;

/* Estadísticas de diagnóstico visibles por la aplicación */
typedef struct {
    uint64_t read_faults;
    uint64_t write_faults;
    uint64_t msgs_sent;
    uint64_t msgs_recv;
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    uint64_t invalidations;
    uint64_t fault_latency_us_total;
    uint64_t pages_m;
    uint64_t pages_s;
    uint64_t pages_i;
    uint32_t pending_count;
    uint32_t wait_pool_used;
    uint32_t slots_enqueued;
    uint32_t slots_woken;
} dsm_stats_t;

#endif /* DSM_TYPES_H */
