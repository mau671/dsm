#ifndef DSM_TYPES_H
#define DSM_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>

/**
 * @file dsm_types.h
 * @brief Tipos públicos, constantes y códigos de estado de la biblioteca DSM.
 *
 * Este encabezado no declara funciones: reúne las definiciones que el API
 * pública (dsm.h) expone a las aplicaciones y que las aplicaciones usan para
 * interpretar los resultados de las llamadas.
 */

/** @brief Cantidad máxima de nodos que admite el cluster (ranuras de membresía). */
#ifndef MAX_NODES
#define MAX_NODES 256
#endif

/** @brief Valor centinela para "sin ranura asignada" (campo de slot). */
#define DSM_SLOT_NONE       UINT32_C(0xFFFFFFFF)
/** @brief Valor centinela para "sin página válida" (campo de página). */
#define DSM_PAGE_ID_NONE    UINT32_C(0xFFFFFFFF)
/** @brief Valor centinela para "sin transacción asociada". */
#define DSM_TXN_NONE        UINT32_C(0xFFFFFFFF)
/** @brief Valor centinela para "sin entrada en la cola de espera". */
#define DSM_WAIT_NONE       UINT32_C(0xFFFFFFFF)
/** @brief Tamaño máximo, en bytes, del payload de un mensaje del protocolo (1 MiB). */
#define DSM_MAX_PAYLOAD     (1024u * 1024u)
/** @brief Capacidad de la cola interna de fallos de página pendientes de despacho. */
#define DSM_FAULTQ_CAP      4096u

/**
 * @brief Códigos de estado devueltos por las funciones del API DSM.
 *
 * Todos los valores negativos indican error; DSM_OK (cero) indica éxito.
 * Cada función documenta en dsm.h el subconjunto de códigos que puede
 * devolver realmente.
 */
typedef enum {
    /** Operación completada con éxito. */
    DSM_OK            =  0,
    /**
     * Error de configuración: el archivo dsm.conf no se puede abrir, no es un
     * archivo regular, supera 1 MiB, tiene una línea o clave inválida, un valor
     * fuera de rango, o el par rechazó la unión por discrepancia de la
     * configuración compartida (shared_hash distinto).
     */
    DSM_ERR_CONFIG    = -1,
    /** El subsistema no está inicializado: no existe contexto activo en este proceso. */
    DSM_ERR_NOT_INIT  = -2,
    /**
     * Error de memoria o de recursos locales: fallo de asignación (calloc,
     * malloc, mmap, memfd_create, ftruncate), fallo al crear hilos o
     * semáforos, o fallo al instalar el manejador de SIGSEGV.
     */
    DSM_ERR_MEM       = -3,
    /**
     * Error de red: no hay puerto libre en el rango configurado, fallo de
     * listen, conexión rechazada o caída del socket durante el envío o la
     * recepción.
     */
    DSM_ERR_NET       = -4,
    /** Tiempo de espera agotado al esperar una respuesta de un par (request_timeout_ms). */
    DSM_ERR_TIMEOUT   = -5,
    /** Sin espacio: la vista compartida no tiene páginas libres o no quedan ranuras de nodo. */
    DSM_ERR_NO_SPACE  = -6,
    /** Parámetro o puntero inválido: puntero de salida NULL, tamaño cero o índice fuera de rango. */
    DSM_ERR_INVALID   = -7,
    /** Nodo inaccesible: la ranura destino no pertenece a la membresía activa. */
    DSM_ERR_NODE_DOWN = -8,
    /** Recurso transitoriamente ocupado: la página ya tiene un fallo de página en curso. */
    DSM_ERR_BUSY      = -9,
    /**
     * Error de protocolo: cabecera inválida, versión incompatible, tipo de
     * mensaje inesperado o fallo de validación CRC32.
     */
    DSM_ERR_PROTO     = -10,
    /**
     * Llamada interrumpida por una señal. Definido en el enum; ninguna función
     * del API pública lo devuelve.
     */
    DSM_ERR_INTR      = -11,
    /**
     * Recurso no disponible de inmediato; la operación puede reintentarse.
     * Definido en el enum; ninguna función del API pública lo devuelve.
     */
    DSM_ERR_AGAIN     = -12,
    /**
     * No se encontraron pares en el cluster durante el descubrimiento. Lo
     * consume dsm_init internamente para asumir el rol de primer nodo, por lo
     * que no llega a la aplicación.
     */
    DSM_ERR_NOT_FOUND = -13
} dsm_status_t;

/**
 * @brief Estados MSI de una página en la copia local del nodo.
 *
 * El valor numérico viaja en los mensajes del protocolo: no reordenar.
 */
typedef enum {
    /** Invalid: la página no está presente o no es válida localmente; el acceso genera fallo. */
    DSM_PAGE_I       = 0,
    /** Shared: la página está presente en modo lectura y puede compartirse con otros nodos. */
    DSM_PAGE_S       = 1,
    /** Modified: la única copia válida es la local y está modificada (sucia). */
    DSM_PAGE_M       = 2,
    /** Invalid-Shared pendiente: se solicitó una copia de lectura y se espera la respuesta del home. */
    DSM_PAGE_IS_PEND = 3,
    /** Invalid-Modified pendiente: se solicitó permiso de escritura desde el estado I y se espera la respuesta. */
    DSM_PAGE_IM_PEND = 4,
    /** Shared-Modified pendiente: se solicitó permiso de escritura desde el estado S y se espera la invalidación de las demás copias. */
    DSM_PAGE_SM_PEND = 5
} dsm_page_state_t;

/**
 * @brief Estado de una página en el directorio del nodo custodio (home).
 */
typedef enum {
    /** Ningún nodo conserva copia de la página; el home es la única fuente. */
    DSM_DIR_UNCACHED = 0,
    /** Uno o más nodos conservan copias de lectura; el home conserva el contenido válido. */
    DSM_DIR_SHARED   = 1,
    /** Un nodo concreto posee la copia modificada; el home no tiene el contenido actualizado. */
    DSM_DIR_MODIFIED = 2
} dsm_dir_state_t;

/**
 * @brief Estado de una ranura en la tabla de membresía del cluster.
 */
typedef enum {
    /** El nodo responde a los heartbeats y participa del cluster. */
    DSM_NODE_ALIVE   = 1,
    /** El nodo dejó de responder pero aún no se le declara muerto. */
    DSM_NODE_SUSPECT = 2,
    /** El nodo se retiró o fue declarado muerto; su ranura queda libre. */
    DSM_NODE_DEAD    = 3
} dsm_node_state_t;

/**
 * @brief Banderas de la cabecera de un mensaje del protocolo DSM.
 */
enum {
    /** El emisor solicita el contenido de la página junto con la respuesta. */
    DSM_FLAG_WANT_DATA = 1u << 0,
    /** El mensaje transporta el contenido de una página en su payload. */
    DSM_FLAG_HAS_DATA  = 1u << 1,
    /** La solicitud pide permiso de escritura (ascenso de permiso). */
    DSM_FLAG_UPGRADE   = 1u << 2,
    /** El mensaje es un reintento de una solicitud anterior. */
    DSM_FLAG_RETRY     = 1u << 3,
    /** El mensaje es una sonda de descubrimiento de pares. */
    DSM_FLAG_PROBE     = 1u << 4,
    /** WRITEBACK de traspaso de home: persiste el respaldo sin emitir el
     *  delta de paridad, porque el contenido no cambia, solo su custodio. */
    DSM_FLAG_TRANSFER  = 1u << 5
};

/**
 * @brief Códigos de diagnóstico y de rechazo que viajan en el campo status
 * de la cabecera de los mensajes. No son valores de retorno del API pública.
 */
typedef enum {
    /** Sin condición de rechazo. */
    DSM_ST_OK              = 0,
    /** El mensaje corresponde a una época (epoch) anterior a la vigente. */
    DSM_ST_STALE_EPOCH     = 1,
    /** El receptor no es el nodo custodio (home) de la página solicitada. */
    DSM_ST_NOT_HOME        = 2,
    /** El emisor no pertenece al cluster indicado. */
    DSM_ST_NOT_CLUSTER     = 3,
    /** La página no admite la transición de estado solicitada. */
    DSM_ST_BAD_STATE       = 4,
    /** El recurso está ocupado por otra operación en curso. */
    DSM_ST_BUSY            = 5,
    /** La ranura solicitada ya está asignada a otro nodo. */
    DSM_ST_SLOT_TAKEN      = 6,
    /** No quedan páginas o ranuras disponibles. */
    DSM_ST_NO_SPACE        = 7,
    /** La configuración compartida del par no coincide (shared_hash distinto). */
    DSM_ST_CONFIG_MISMATCH = 8,
    /**
     * El dueño de la copia modificada murió antes de devolverla. El estado es
     * terminal: el directorio no puede servirse hasta que la recuperación de
     * baja reasigne la página. El solicitante lo traduce a DSM_ERR_NODE_DOWN.
     */
    DSM_ST_NODE_DOWN       = 9,
    /** Un participante anunció un parámetro distinto del fijado por el custodio. */
    DSM_ST_INVALID         = 10
} dsm_status_code_t;

/**
 * @brief Instantánea de las estadísticas operativas del nodo local.
 *
 * Se obtiene con dsm_stats(), que rellena todos los campos y no conserva
 * valores previos. Los contadores son acumulados desde dsm_init().
 */
typedef struct {
    /** Fallos de página de lectura atendidos por el manejador de SIGSEGV. */
    uint64_t read_faults;
    /** Fallos de página de escritura atendidos por el manejador de SIGSEGV. */
    uint64_t write_faults;
    /** Mensajes enviados a otros nodos. */
    uint64_t msgs_sent;
    /** Mensajes recibidos de otros nodos. */
    uint64_t msgs_recv;
    /** Bytes de mensajes enviados, incluyendo cabecera y payload. */
    uint64_t bytes_sent;
    /** Bytes de mensajes recibidos, incluyendo cabecera y payload. */
    uint64_t bytes_recv;
    /** Invalidaciones aplicadas sobre copias locales, por orden remota o del
     *  home local. */
    uint64_t invalidations;
    /** Suma acumulada de la latencia de resolución de fallos, en microsegundos. */
    uint64_t fault_latency_us_total;
    /** Fallos de página atendidos por el despachador, incluidos los que
     *  terminan en error. */
    uint64_t faults_serviced;
    /** Latencia media por fallo resuelto, en microsegundos (total/serviced). */
    uint64_t fault_latency_us_avg;
    /** Páginas de la vista local en estado Modified. */
    uint64_t pages_m;
    /** Páginas de la vista local en estado Shared. */
    uint64_t pages_s;
    /** Páginas de la vista local en estado Invalid. */
    uint64_t pages_i;
    /** Respuestas descartadas por un número de secuencia de transacción repetido o antiguo. */
    uint64_t tseq_discarded;
    /** Deltas de paridad enviados a los pares del cluster. */
    uint64_t parity_deltas;
    /** Bytes de paridad transmitidos a los pares del cluster. */
    uint64_t parity_bytes;
    /** Franjas de paridad reconstruidas a partir de los datos supervivientes. */
    uint64_t stripe_rebuilds;
    /** Suma acumulada de la latencia de paridad, en microsegundos. */
    uint64_t parity_latency_us;
    /** Particiones de ejecución distribuida ejecutadas correctamente. */
    uint64_t dist_tasks_run;
    /** Particiones de ejecución distribuida que terminaron en fallo. */
    uint64_t dist_tasks_failed;
    /** Transiciones a SUSPECT y DEAD acumuladas de los pares del clúster. */
    uint64_t hb_missed;
    /** Cantidad de operaciones de página pendientes de respuesta. */
    uint32_t pending_count;
    /** Entradas ocupadas del conjunto de espera de peticiones. */
    uint32_t wait_pool_used;
    /** Fallos de página encolados para el despachador. */
    uint32_t slots_enqueued;
    /** Fallos de página resueltos y despertados por el despachador. */
    uint32_t slots_woken;
} dsm_stats_t;

#endif /* DSM_TYPES_H */
