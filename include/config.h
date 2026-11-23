#ifndef DSM_CONFIG_H
#define DSM_CONFIG_H

#include <stdint.h>
#include <stdbool.h>
#include "dsm_types.h"

#define DSM_MAX_PEER_HOSTS 16
#define DSM_MAX_HOST_LEN   256

/* Tope de intentos de una RPC de pagina. Acota el peor caso de una falla de
 * pagina: (intentos - 1) esperas de backoff mas un plazo de espera cada una. */
#define DSM_MAX_REQUEST_RETRIES 10u

/* Bloque de puertos asignado al grupo de trabajo (100 puertos).
 * El sistema solo puede escuchar/sondear dentro de este bloque: el
 * laboratorio comparte maquinas entre equipos y el rango es exclusivo. */
#ifndef DSM_PORT_BLOCK_MIN
#define DSM_PORT_BLOCK_MIN 27600u
#endif
#ifndef DSM_PORT_BLOCK_MAX
#define DSM_PORT_BLOCK_MAX 27699u
#endif

typedef struct dsm_config {
    uint32_t cluster_id;
    uint32_t page_size;
    uint64_t base_addr;
    uint64_t base_addr_tsan;
    uint64_t view_size;
    uint64_t node_mem;
    uint32_t max_runtime_nodes;
    uint32_t vnodes_per_mb;
    bool     redundancy_raid5;
    uint32_t raid_k;
    uint32_t chunk_pages;

    uint16_t port_min;
    uint16_t port_max;
    char     peer_hosts[DSM_MAX_PEER_HOSTS][DSM_MAX_HOST_LEN];
    uint32_t peer_host_count;

    uint32_t connect_timeout_ms;
    uint32_t discovery_timeout_ms;
    uint32_t send_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t request_retries;
    uint32_t reconnect_min_ms;
    uint32_t reconnect_max_ms;

    uint32_t hb_interval_ms;
    uint32_t hb_suspect;
    uint32_t hb_dead;

    uint32_t worker_threads;
    uint32_t dispatcher_threads;
    uint32_t page_lock_stripes;
    uint32_t home_slack_permille;   /* 1250 = 1.25 */
    uint32_t lock_timeout_ms;
    uint32_t worker_queue_cap;
    uint32_t wait_pool_entries;
    uint32_t dist_timeout_ms;

    char     log_level[16];
    char     output_dir[256];

    /* Propiedad de este proceso, no del archivo compartido: se toma de la
     * variable de entorno DSM_NODE_MODE. Un cliente usa la memoria del cluster
     * sin aportar memoria ni custodiar páginas. */
    bool     client_mode;

    /* Hash CRC32 del archivo original para validar consistencia entre nodos */
    uint32_t raw_hash;
} dsm_config_t;

/**
 * Carga un archivo de configuración dsm.conf, calcula su raw_hash y valida campos.
 * @param cfg Estructura de configuración a rellenar.
 * @param path Ruta al archivo (NULL para "dsm.conf").
 * @return DSM_OK en éxito, DSM_ERR_CONFIG o DSM_ERR_MEM en fallo.
 */
int config_load(dsm_config_t *cfg, const char *path);

/**
 * Valida las restricciones cruzadas de configuración.
 * @return DSM_OK si es válida, DSM_ERR_CONFIG si viola restricciones.
 */
int config_validate(dsm_config_t *cfg);

/**
 * No hay recursos dinámicos asociados a la configuración. La función existe
 * por simetría con config_load y no realiza ninguna operación.
 */
void config_free(dsm_config_t *cfg);

/**
 * Vuelca la configuración actual en el log a nivel INFO.
 */
void config_dump(const dsm_config_t *cfg);

#endif /* DSM_CONFIG_H */
