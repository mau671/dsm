#ifndef DSM_WIRE_H
#define DSM_WIRE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "dsm_types.h"

#define DSM_MAGIC          UINT32_C(0x44534D32)   /* "DSM2" */
#define DSM_PROTO_VERSION  2u
#define DSM_HDR_LEN        56u
#define DSM_MAX_WIRE_MSG   (DSM_HDR_LEN + DSM_MAX_PAYLOAD)

/* Offsets exactos en el cable de la cabecera unificada de 56 bytes */
enum {
    OFF_MAGIC        =  0,
    OFF_CLUSTER_ID   =  4,
    OFF_PROTO_VER    =  8,
    OFF_TYPE         = 10,
    OFF_EPOCH        = 12,
    OFF_SRC_SLOT     = 20,
    OFF_DST_SLOT     = 24,
    OFF_REQ_ID       = 28,
    OFF_PAGE_ID      = 32,
    OFF_TSEQ         = 36,
    OFF_DATA_VERSION = 40,
    OFF_ACCESS       = 44,
    OFF_FLAGS        = 45,
    OFF_STATUS       = 46,
    OFF_PAYLOAD_LEN  = 48,
    OFF_CRC32        = 52
};

/* Cabecera en memoria (64 bytes por alineación natural a 8 de uint64_t epoch).
 * NUNCA se transmite cruda en el socket. */
typedef struct {
    uint32_t magic;
    uint32_t cluster_id;
    uint16_t proto_version;
    uint16_t type;
    uint64_t epoch;
    uint32_t src_slot;
    uint32_t dst_slot;
    uint32_t req_id;
    uint32_t page_id;
    uint32_t tseq;
    uint32_t data_version;
    uint8_t  access;
    uint8_t  flags;
    uint16_t status;
    uint32_t payload_len;
    uint32_t crc32;
} msg_header_t;

/* Mensaje completo en memoria */
typedef struct {
    msg_header_t hdr;
    const void  *payload;
} dsm_msg_t;

/* Tipos de mensaje del protocolo */
enum dsm_msg_type {
    MSG_HELLO            = 0x01,
    MSG_WELCOME          = 0x02,
    MSG_JOIN             = 0x03,
    MSG_JOIN_ACK         = 0x04,
    MSG_HEARTBEAT        = 0x05,
    MSG_LEAVE            = 0x06,
    MSG_NODE_DOWN        = 0x07,
    MSG_MEMBERSHIP_REQ   = 0x08,
    MSG_MEMBERSHIP_RESP  = 0x09,
    MSG_PAGE_MOVE        = 0x0A,
    MSG_PAGE_MOVE_ACK    = 0x0B,

    MSG_PAGE_REQUEST     = 0x10,
    MSG_PAGE_DATA        = 0x11,
    MSG_GRANT            = 0x12,
    MSG_INVALIDATE       = 0x13,
    MSG_INVALIDATE_ACK   = 0x14,
    MSG_DOWNGRADE        = 0x15,
    MSG_DOWNGRADE_ACK    = 0x16,
    MSG_WRITEBACK        = 0x17,
    MSG_WRITEBACK_ACK    = 0x18,

    MSG_PARITY_DELTA     = 0x20,
    MSG_PARITY_ACK       = 0x21,
    MSG_STRIPE_PUT       = 0x22,
    MSG_STRIPE_PUT_ACK   = 0x23,
    MSG_STRIPE_READ      = 0x24,
    MSG_STRIPE_DATA      = 0x25,
    MSG_STATE_REPORT     = 0x26,
    MSG_DIR_DONE         = 0x27,

    MSG_LOCK_REQ         = 0x30,
    MSG_LOCK_GRANT       = 0x31,
    MSG_LOCK_REL         = 0x32,
    MSG_BARRIER_ARRIVE   = 0x33,
    MSG_BARRIER_GO       = 0x34,

    MSG_QUERY_REQ        = 0x40,
    MSG_QUERY_RESP       = 0x41,
    MSG_NACK             = 0x7F
};

/* Cargas fijas serializadas campo por campo */

/* 40 B en memoria, 36 B exactos en el cable */
typedef struct {
    uint8_t  uuid[16];
    uint32_t ip;                 /* orden de red */
    uint16_t port;
    uint8_t  slot;
    uint8_t  state;
    uint64_t incarnation;
    uint32_t mem_pages;
} membership_wire_t;

#define DSM_MEMBERSHIP_WIRE_LEN 36u

typedef struct {                 /* 24 B */
    uint32_t alloc_id;
    uint32_t first_page;
    uint32_t npages;
    uint32_t slot;
    uint32_t pid;
    uint32_t reserved;
} alloc_rec_t;

#define DSM_ALLOC_REC_LEN 24u

typedef struct {                 /* 72 B */
    uint32_t page_id;
    uint32_t owner;
    uint32_t version;
    uint32_t tseq;
    uint32_t frame;
    uint32_t stripe_id;
    uint32_t wait_head;
    uint32_t wait_tail;
    uint32_t txn;
    uint8_t  state;
    uint8_t  busy;
    uint16_t reserved;
    uint8_t  copyset[MAX_NODES / 8];
} dir_entry_t;

#define DSM_DIR_ENTRY_LEN 72u

/* --- Consultas de diagnóstico (QUERY_REQ / QUERY_RESP) ---
 *
 * dsmctl no integra la membresía: abre una conexión TCP efímera al puerto de
 * escucha de un nodo, envía QUERY_REQ y lee la QUERY_RESP. La carga útil de
 * la petición va serializada campo por campo en 12 bytes y la de la
 * respuesta en 8 bytes seguidos de los registros del tipo consultado. */

enum dsm_query_kind {
    DSM_QUERY_NODES = 0,
    DSM_QUERY_STATS = 1,
    DSM_QUERY_PAGES = 2
};

enum dsm_query_scope {
    DSM_QUERY_LOCAL  = 0,
    DSM_QUERY_GLOBAL = 1
};

/* 12 B en el cable */
typedef struct {
    uint8_t  kind;
    uint8_t  scope;
    uint16_t reserved;
    uint32_t first;
    uint32_t max;
} query_req_t;

#define DSM_QUERY_REQ_LEN 12u

/* 8 B en el cable */
typedef struct {
    uint8_t  kind;
    uint8_t  more;
    uint16_t reserved;
    uint32_t count;
} query_resp_t;

#define DSM_QUERY_RESP_HDR_LEN 8u

/* Registro de membresía: 52 B en el cable */
typedef struct {
    uint32_t slot;
    uint8_t  uuid[16];
    uint32_t ip;                 /* orden de red */
    uint16_t port;               /* orden de host */
    uint8_t  state;              /* dsm_node_state_t */
    uint8_t  missed_hb;
    uint32_t mem_pages;
    uint32_t free_pages;
    uint64_t epoch;
    uint64_t last_hb_ms;
} node_rec_t;

#define DSM_NODE_REC_LEN 52u

/* Registro de la tabla de páginas local: 16 B en el cable */
typedef struct {
    uint32_t page_id;
    uint32_t home;
    uint32_t version;
    uint8_t  state;
    uint8_t  pend_inval;
    uint8_t  pend_down;
    uint8_t  reserved;
} page_rec_t;

#define DSM_PAGE_REC_LEN 16u

/* Carga del mensaje WELCOME */
typedef struct {
    uint64_t epoch;
    uint8_t  origin_uuid[16];
    uint32_t count;
    membership_wire_t members[MAX_NODES];
    uint32_t alloc_count;
    alloc_rec_t allocs[64];
} welcome_t;

/* Verificaciones estáticas de estructura en memoria */
_Static_assert(sizeof(msg_header_t)  == 64, "msg_header_t debe ser 64 B");
_Static_assert(_Alignof(msg_header_t) == 8,  "msg_header_t debe alinear a 8");
_Static_assert(sizeof(membership_wire_t) == 40, "membership_wire_t en memoria debe ser 40 B");
_Static_assert(sizeof(alloc_rec_t)  == 24, "alloc_rec_t debe ser 24 B");
_Static_assert(sizeof(dir_entry_t)  == 72, "dir_entry_t debe ser 72 B");

/* --- Funciones de serialización y validación --- */

uint64_t dsm_htonll(uint64_t v);
uint64_t dsm_ntohll(uint64_t v);

/* CRC32 IEEE 802.3 */
uint32_t dsm_crc32(uint32_t seed, const void *buf, size_t len);

/* Empaquetado y desempaquetado de la cabecera */
void dsm_wire_pack(const msg_header_t *hdr, uint8_t out[DSM_HDR_LEN]);
int  dsm_wire_unpack(const uint8_t in[DSM_HDR_LEN], msg_header_t *out);

/* Finaliza el empaquetado calculando y escribiendo el CRC32 */
uint32_t dsm_wire_finish(msg_header_t *hdr, uint8_t out[DSM_HDR_LEN],
                         const void *payload, uint32_t payload_len);

/* Valida cabecera completa, cluster_id y CRC32 */
int dsm_wire_validate(const msg_header_t *hdr, const uint8_t raw[DSM_HDR_LEN],
                      const void *payload, uint32_t cluster_id);

/* Valida longitud esperada de carga según el tipo de mensaje */
bool wire_payload_ok(uint16_t type, uint8_t flags, uint32_t len, uint32_t page_size);

/* Empaquetado de cargas fijas */
size_t dsm_pack_membership(const membership_wire_t *m, uint8_t out[DSM_MEMBERSHIP_WIRE_LEN]);
void   dsm_unpack_membership(const uint8_t in[DSM_MEMBERSHIP_WIRE_LEN], membership_wire_t *m);

size_t dsm_pack_alloc_rec(const alloc_rec_t *a, uint8_t out[DSM_ALLOC_REC_LEN]);
void   dsm_unpack_alloc_rec(const uint8_t in[DSM_ALLOC_REC_LEN], alloc_rec_t *a);

size_t dsm_pack_dir_entry(const dir_entry_t *d, uint8_t out[DSM_DIR_ENTRY_LEN]);
void   dsm_unpack_dir_entry(const uint8_t in[DSM_DIR_ENTRY_LEN], dir_entry_t *d);

size_t dsm_pack_query_req(const query_req_t *q, uint8_t out[DSM_QUERY_REQ_LEN]);
void   dsm_unpack_query_req(const uint8_t in[DSM_QUERY_REQ_LEN], query_req_t *q);

size_t dsm_pack_query_resp(const query_resp_t *r, uint8_t out[DSM_QUERY_RESP_HDR_LEN]);
void   dsm_unpack_query_resp(const uint8_t in[DSM_QUERY_RESP_HDR_LEN], query_resp_t *r);

size_t dsm_pack_node_rec(const node_rec_t *n, uint8_t out[DSM_NODE_REC_LEN]);
void   dsm_unpack_node_rec(const uint8_t in[DSM_NODE_REC_LEN], node_rec_t *n);

size_t dsm_pack_page_rec(const page_rec_t *p, uint8_t out[DSM_PAGE_REC_LEN]);
void   dsm_unpack_page_rec(const uint8_t in[DSM_PAGE_REC_LEN], page_rec_t *p);

/* Empaquetado de WELCOME */
size_t dsm_pack_welcome(const welcome_t *w, uint8_t *out, size_t max_out);
int    dsm_unpack_welcome(const uint8_t *in, size_t len, welcome_t *w);

const char *dsm_msg_type_name(uint16_t type);
bool dsm_msg_is_response(uint16_t type);
bool dsm_msg_is_membership(uint16_t type);

#endif /* DSM_WIRE_H */
