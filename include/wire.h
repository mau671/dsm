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
    MSG_LOCK_MIGRATE     = 0x36,
    MSG_LOCK_STATE       = 0x37,

    MSG_QUERY_REQ        = 0x40,
    MSG_QUERY_RESP       = 0x41,

    MSG_DIST_RUN         = 0x50,
    MSG_DIST_DONE        = 0x51,

    MSG_ALLOC_REQ        = 0x60,
    MSG_ALLOC_RESP       = 0x61,
    MSG_FREE_REQ         = 0x62,
    MSG_FREE_RESP        = 0x63,
    /** Sincronización del mapa global al asumir la coordinación. */
    MSG_ALLOC_SYNC       = 0x64,
    MSG_ALLOC_STATE      = 0x65,

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

/** Tipo de registro de la instantánea del mapa global en WELCOME. */
enum alloc_rec_kind {
    /** Bloque vivo transferido con su rango exacto. */
    DSM_ALLOC_REC_LIVE      = 0u,
    /** Hueco libre que el coordinador puede reutilizar. */
    DSM_ALLOC_REC_HOLE      = 1u,
    /** Marca de agua: first_page es el cursor de asignación. */
    DSM_ALLOC_REC_WATERMARK = 2u
};

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

/* --- Ejecución distribuida (DIST_RUN / DIST_DONE) ---
 *
 * El coordinador envía DIST_RUN con el rango de direcciones y el plazo de la
 * partición. Cada nodo responde DIST_DONE con el resultado del trabajo.
 * Los campos se serializan en el orden indicado por los desplazamientos de
 * dsm_pack_dist_run y dsm_pack_dist_done, nunca con memcpy de la estructura. */

/* 48 B exactos en el cable */
typedef struct {
    uint64_t src_va;
    uint64_t dst_va;
    uint64_t nbytes;
    uint32_t job;
    uint32_t kernel;
    uint32_t part_id;
    uint32_t nparts;
    uint32_t deadline_ms;
    uint32_t launch_seq;
} dist_run_wire_t;

#define DSM_DIST_RUN_WIRE_LEN 48u

/* 44 B exactos en el cable */
typedef struct {
    uint32_t job;
    uint32_t part_id;
    uint32_t status;
    uint32_t items;
    uint64_t crc;
    uint64_t us;
    uint32_t launch_seq;
    uint32_t faults_r;
    uint32_t faults_w;
} dist_done_wire_t;

#define DSM_DIST_DONE_WIRE_LEN 44u

/* --- Paridad y migración de páginas --- */

/* 76 B exactos en el cable: identificador, K, custodio de paridad y las
 * listas de páginas y custodios de hasta ocho miembros de datos. */
typedef struct {
    uint32_t stripe_id;
    uint8_t  k;
    uint8_t  reserved[3];
    uint32_t parity_slot;
    uint32_t data_page[8];
    uint32_t data_slot[8];
} stripe_wire_t;

#define DSM_STRIPE_WIRE_LEN      76u
#define DSM_STRIPE_DATA_SLOTS    8u

/* Cabecera de PARITY_DELTA; el vector XOR ocupa page_size bytes más */
typedef struct {
    uint32_t stripe_id;
} parity_delta_wire_t;

#define DSM_PARITY_DELTA_HDR_LEN 4u
#define DSM_PARITY_DELTA_WIRE_LEN(ps) (DSM_PARITY_DELTA_HDR_LEN + (uint32_t)(ps))

/* Palabra de 4 B compartida por PARITY_ACK, STRIPE_PUT_ACK, STATE_REPORT
 * y DIR_DONE. En el ACK transporta el identificador de la franja. */
typedef struct {
    uint32_t value;
} wire_word_t;

#define DSM_WIRE_WORD_LEN        4u

/* 8 B exactos en el cable */
typedef struct {
    uint32_t stripe_id;
    uint8_t  kind;
    uint8_t  reserved[3];
} stripe_read_wire_t;

#define DSM_STRIPE_READ_WIRE_LEN 8u

/* --- Coordinación del espacio global de la vista (ALLOC/FREE) ---
 *
 * El coordinador del clúster —el nodo vivo de menor ranura— mantiene el mapa
 * global de reservas sobre el mismo rango de páginas que todos los nodos
 * mapean en BASE_ADDR. Cliente y coordinador intercambian esta carga útil
 * idéntica en petición y respuesta; el campo status viaja en la carga y no en
 * la cabecera para no mezclar rechazos de transporte con rechazos del mapa. */
typedef struct {
    uint32_t npages;      /**< Páginas solicitadas o concedidas. */
    uint32_t first_page;  /**< Primera página concedida, o DSM_PAGE_ID_NONE. */
    uint32_t status;      /**< dsm_status_code_t del mapa global. */
    uint32_t pid;         /**< Proceso solicitante, para trazabilidad. */
} alloc_msg_t;

#define DSM_ALLOC_MSG_LEN 16u

/* --- Consultas de diagnóstico (QUERY_REQ / QUERY_RESP) ---
 *
 * dsmctl no integra la membresía: abre una conexión TCP efímera al puerto de
 * escucha de un nodo, envía QUERY_REQ y lee la QUERY_RESP. La carga útil de
 * la petición va serializada campo por campo en 12 bytes y la de la
 * respuesta en 8 bytes seguidos de los registros del tipo consultado. */

enum dsm_query_kind {
    DSM_QUERY_NODES = 0,
    DSM_QUERY_STATS = 1,
    DSM_QUERY_PAGES = 2,
    /* Tabla de directorio global en texto, generada con dsm_pages_global. */
    DSM_QUERY_DIR   = 3,
    /* Bloques reservados del mapa global. */
    DSM_QUERY_ALLOC = 4,
    /* Genera los informes HTML en el nodo consultado. La ruta viaja como
     * carga adicional después de query_req_t. */
    DSM_QUERY_HTML  = 5
};

/* Ruta máxima que acepta una petición DSM_QUERY_HTML. */
#define DSM_QUERY_HTML_PATH_MAX 256u

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

/* Registro de la tabla de páginas local: 24 B en el cable */
typedef struct {
    uint32_t page_id;
    uint32_t home;
    uint32_t version;
    uint8_t  state;
    uint8_t  pend_inval;
    uint8_t  pend_down;
    uint8_t  reserved;
    uint32_t pid;     /**< Proceso que reservó la página, 0 si no hay bloque. */
    uint32_t order;   /**< Índice de la página dentro de su bloque reservado. */
} page_rec_t;

#define DSM_PAGE_REC_LEN 24u

/* --- Migración de cerrojos (LOCK_MIGRATE / LOCK_STATE) ---
 *
 * Al cerrarse una ranura, quien detecta la baja difunde LOCK_MIGRATE con la
 * época de sincronización vigente. Cada nodo responde con LOCK_STATE a los
 * custodios que corresponda, exponiendo los cerrojos que sostiene y las
 * peticiones que tiene en vuelo. El nuevo custodio reconstruye así la tabla
 * sin depender del estado volátil del nodo caído. Todos los campos viajan
 * serializados por separado, nunca con memcpy de la estructura. */

/* 12 B en el cable */
typedef struct {
    uint64_t epoch;       /* Época de sincronización, no la de membresía */
    uint32_t dead_slot;
} lock_migrate_wire_t;

#define DSM_LOCK_MIGRATE_WIRE_LEN 12u

/* 12 B en el cable por registro */
typedef struct {
    uint32_t lock_id;
    uint32_t req_id;
    uint8_t  owner;       /* 1 si el emisor es el dueño, 0 si está esperando */
    uint8_t  reserved[3];
} lock_state_rec_t;

#define DSM_LOCK_STATE_REC_LEN 12u

/* 20 B en el cable más los registros */
typedef struct {
    uint64_t epoch;
    uint32_t dead_slot;
    uint32_t slot;        /* Ranura que informa */
    uint32_t count;       /* Cantidad de registros incluidos */
} lock_state_wire_t;

#define DSM_LOCK_STATE_HDR_LEN 20u
#define DSM_LOCK_STATE_MAX_RECS 512u

/* 96 B en el cable: doce contadores de 64 bits para dsmctl stats */
typedef struct {
    uint64_t read_faults;
    uint64_t write_faults;
    uint64_t msgs_sent;
    uint64_t msgs_recv;
    uint64_t bytes_sent;
    uint64_t bytes_recv;
    uint64_t dist_tasks_run;
    uint64_t dist_tasks_failed;
    uint64_t parity_deltas;
    uint64_t stripe_rebuilds;
    uint64_t tseq_discarded;
    uint64_t hb_missed;
} stats_wire_t;

#define DSM_STATS_WIRE_LEN 96u

/* Carga del mensaje WELCOME */
typedef struct {
    uint64_t epoch;
    uint8_t  origin_uuid[16];
    uint32_t count;
    membership_wire_t members[MAX_NODES];
    uint32_t alloc_count;
    alloc_rec_t allocs[64];
} welcome_t;

/* Tamano maximo del WELCOME en el cable: cabecera, todas las membresias y
 * todas las reservas que caben en welcome_t. Dimensiona el bufer del
 * respondedor para que ningun clúster legible quede sin respuesta. */
#define DSM_WELCOME_WIRE_MAX \
    (28u + (size_t)MAX_NODES * DSM_MEMBERSHIP_WIRE_LEN + 4u + \
     64u * DSM_ALLOC_REC_LEN)

/* Verificaciones estáticas de estructura en memoria */
_Static_assert(sizeof(msg_header_t)  == 64, "msg_header_t debe ser 64 B");
_Static_assert(_Alignof(msg_header_t) == 8,  "msg_header_t debe alinear a 8");
_Static_assert(sizeof(membership_wire_t) == 40, "membership_wire_t en memoria debe ser 40 B");
_Static_assert(sizeof(alloc_rec_t)  == 24, "alloc_rec_t debe ser 24 B");
_Static_assert(sizeof(dir_entry_t)  == 72, "dir_entry_t debe ser 72 B");
_Static_assert(DSM_DIST_RUN_WIRE_LEN == 48u, "DIST_RUN debe ser 48 B en el cable");
_Static_assert(sizeof(dist_run_wire_t) == DSM_DIST_RUN_WIRE_LEN, "dist_run_wire_t debe ser 48 B");
_Static_assert(DSM_DIST_DONE_WIRE_LEN == 44u, "DIST_DONE debe ser 44 B en el cable");
/* En memoria la estructura acolchada a 8 bytes ocupa 48: el cable usa los
 * desplazamientos exactos de dsm_pack_dist_done, nunca sizeof. */
_Static_assert(sizeof(dist_done_wire_t) >= DSM_DIST_DONE_WIRE_LEN,
               "dist_done_wire_t debe cubrir la carga de DIST_DONE");
_Static_assert(DSM_STRIPE_WIRE_LEN == 76u, "STRIPE_PUT debe ser 76 B en el cable");
_Static_assert(sizeof(stripe_wire_t) == DSM_STRIPE_WIRE_LEN, "stripe_wire_t debe ser 76 B");
_Static_assert(DSM_PARITY_DELTA_HDR_LEN == 4u, "la cabecera de PARITY_DELTA debe ser 4 B");
_Static_assert(sizeof(parity_delta_wire_t) == DSM_PARITY_DELTA_HDR_LEN, "parity_delta_wire_t debe ser 4 B");
_Static_assert(DSM_WIRE_WORD_LEN == 4u, "la palabra de estado debe ser 4 B");
_Static_assert(sizeof(wire_word_t) == DSM_WIRE_WORD_LEN, "wire_word_t debe ser 4 B");
_Static_assert(DSM_STRIPE_READ_WIRE_LEN == 8u, "STRIPE_READ debe ser 8 B en el cable");
_Static_assert(sizeof(stripe_read_wire_t) == DSM_STRIPE_READ_WIRE_LEN, "stripe_read_wire_t debe ser 8 B");
_Static_assert(DSM_ALLOC_MSG_LEN == 16u, "ALLOC/FREE debe ser 16 B en el cable");
_Static_assert(sizeof(alloc_msg_t) == DSM_ALLOC_MSG_LEN, "alloc_msg_t debe ser 16 B");
_Static_assert(DSM_PAGE_REC_LEN == 24u, "page_rec_t debe ser 24 B en el cable");
_Static_assert(sizeof(page_rec_t) == DSM_PAGE_REC_LEN, "page_rec_t debe ser 24 B");
_Static_assert(DSM_LOCK_MIGRATE_WIRE_LEN == 12u, "LOCK_MIGRATE debe ser 12 B en el cable");
_Static_assert(DSM_LOCK_STATE_HDR_LEN == 20u, "LOCK_STATE debe tener 20 B de cabecera");
_Static_assert(DSM_LOCK_STATE_REC_LEN == 12u, "cada registro de LOCK_STATE debe ser 12 B");
_Static_assert(DSM_STATS_WIRE_LEN == 96u, "las estadísticas deben ser 96 B en el cable");
_Static_assert(sizeof(stats_wire_t) == DSM_STATS_WIRE_LEN, "stats_wire_t debe ser 96 B");

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

size_t dsm_pack_stats(const stats_wire_t *s, uint8_t out[DSM_STATS_WIRE_LEN]);
void   dsm_unpack_stats(const uint8_t in[DSM_STATS_WIRE_LEN], stats_wire_t *s);

size_t dsm_pack_lock_migrate(const lock_migrate_wire_t *m,
                             uint8_t out[DSM_LOCK_MIGRATE_WIRE_LEN]);
void   dsm_unpack_lock_migrate(const uint8_t in[DSM_LOCK_MIGRATE_WIRE_LEN],
                               lock_migrate_wire_t *m);
size_t dsm_pack_lock_state(const lock_state_wire_t *s,
                           const lock_state_rec_t *recs, uint8_t *out);
int    dsm_unpack_lock_state(const uint8_t *in, size_t len,
                             lock_state_wire_t *s, lock_state_rec_t *recs);

size_t dsm_pack_node_rec(const node_rec_t *n, uint8_t out[DSM_NODE_REC_LEN]);
void   dsm_unpack_node_rec(const uint8_t in[DSM_NODE_REC_LEN], node_rec_t *n);

size_t dsm_pack_page_rec(const page_rec_t *p, uint8_t out[DSM_PAGE_REC_LEN]);
void   dsm_unpack_page_rec(const uint8_t in[DSM_PAGE_REC_LEN], page_rec_t *p);

/* Empaquetado de ejecución distribuida */
size_t dsm_pack_dist_run(const dist_run_wire_t *d, uint8_t out[DSM_DIST_RUN_WIRE_LEN]);
void   dsm_unpack_dist_run(const uint8_t in[DSM_DIST_RUN_WIRE_LEN], dist_run_wire_t *d);
size_t dsm_pack_dist_done(const dist_done_wire_t *d, uint8_t out[DSM_DIST_DONE_WIRE_LEN]);
void   dsm_unpack_dist_done(const uint8_t in[DSM_DIST_DONE_WIRE_LEN], dist_done_wire_t *d);

/* Empaquetado de paridad y migración de páginas */
size_t dsm_pack_stripe(const stripe_wire_t *s, uint8_t out[DSM_STRIPE_WIRE_LEN]);
void   dsm_unpack_stripe(const uint8_t in[DSM_STRIPE_WIRE_LEN], stripe_wire_t *s);
size_t dsm_pack_parity_delta(const parity_delta_wire_t *d, const uint8_t *delta,
                             uint32_t page_size, uint8_t *out);
void   dsm_unpack_parity_delta(const uint8_t *in, uint32_t page_size,
                               parity_delta_wire_t *d, uint8_t *delta);
size_t dsm_pack_word(const wire_word_t *w, uint8_t out[DSM_WIRE_WORD_LEN]);
void   dsm_unpack_word(const uint8_t in[DSM_WIRE_WORD_LEN], wire_word_t *w);
size_t dsm_pack_stripe_read(const stripe_read_wire_t *r, uint8_t out[DSM_STRIPE_READ_WIRE_LEN]);
void   dsm_unpack_stripe_read(const uint8_t in[DSM_STRIPE_READ_WIRE_LEN], stripe_read_wire_t *r);

/* Empaquetado de la coordinación de espacio global */
size_t dsm_pack_alloc_msg(const alloc_msg_t *a, uint8_t out[DSM_ALLOC_MSG_LEN]);
void   dsm_unpack_alloc_msg(const uint8_t in[DSM_ALLOC_MSG_LEN], alloc_msg_t *a);
size_t dsm_pack_page_payload(const uint8_t *data, uint8_t *out, uint32_t len);
void   dsm_unpack_page_payload(const uint8_t *in, uint8_t *data, uint32_t len);
size_t dsm_pack_stripe_data(const uint8_t *data, uint8_t *out, uint32_t len);
void   dsm_unpack_stripe_data(const uint8_t *in, uint8_t *data, uint32_t len);
size_t dsm_pack_page_move(const uint8_t *data, uint8_t *out, uint32_t len);

/* Empaquetado de WELCOME */
size_t dsm_pack_welcome(const welcome_t *w, uint8_t *out, size_t max_out);
int    dsm_unpack_welcome(const uint8_t *in, size_t len, welcome_t *w);

const char *dsm_msg_type_name(uint16_t type);
bool dsm_msg_is_response(uint16_t type);
bool dsm_msg_is_membership(uint16_t type);

#endif /* DSM_WIRE_H */
