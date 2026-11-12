#include "config.h"
#include "wire.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

/* Recorta espacios en blanco al inicio y al final de una cadena en sitio */
static char *trim_whitespace(char *str)
{
    while (isspace((unsigned char)*str)) str++;
    if (*str == 0) return str;

    char *end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}

/* Parsea un entero de 64 bits verificando formato estricto y rango.
 * Rechaza signo negativo (strtoull lo aceptaria y desbordaria silenciosamente),
 * cadenas vacias y cualquier caracter de cola que no sea espacio. */
static int parse_u64(const char *val_str, uint64_t *out)
{
    if (!val_str || *val_str == '\0' || *val_str == '-') {
        return -1;
    }
    char *endptr = NULL;
    errno = 0;
    unsigned long long v = strtoull(val_str, &endptr, 0);
    if (errno != 0 || endptr == val_str) {
        return -1;
    }
    while (*endptr) {
        if (!isspace((unsigned char)*endptr)) return -1;
        endptr++;
    }
    *out = (uint64_t)v;
    return 0;
}

/* Parsea un entero de 32 bits */
static int parse_u32(const char *val_str, uint32_t *out)
{
    uint64_t v = 0;
    if (parse_u64(val_str, &v) != 0 || v > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

/* Parsea un entero de 16 bits */
static int parse_u16(const char *val_str, uint16_t *out)
{
    uint64_t v = 0;
    if (parse_u64(val_str, &v) != 0 || v > UINT16_MAX) {
        return -1;
    }
    *out = (uint16_t)v;
    return 0;
}

/* Parsea flotante con hasta 3 decimales significativos y lo convierte a
 * milesimas enteras. Se hace analisis lexico (no strtod) para que la
 * conversion sea exacta y determinista: "1.25" -> 1250, "1.2" -> 1200,
 * "1" -> 1000. Se rechazan exponentes, signos, valores < 1.0 y cualquier
 * digito distinto de cero mas alla del tercer decimal. */
static int parse_slack_permille(const char *s, uint32_t *out)
{
    if (!s || *s == '\0' || *s == '-' || *s == '+') {
        return -1;
    }

    const char *p = s;
    uint64_t whole = 0;
    int whole_digits = 0;

    while (isdigit((unsigned char)*p)) {
        if (whole > 4000000000ULL) return -1;   /* desbordamiento temprano */
        whole = whole * 10u + (uint64_t)(*p - '0');
        whole_digits++;
        p++;
    }
    if (whole_digits == 0) {
        return -1;
    }

    uint32_t frac = 0;
    int frac_digits = 0;

    if (*p == '.') {
        p++;
        if (!isdigit((unsigned char)*p)) {
            return -1;                          /* "1." no es valido */
        }
        while (isdigit((unsigned char)*p)) {
            if (frac_digits < 3) {
                frac = frac * 10u + (uint32_t)(*p - '0');
            } else if (*p != '0') {
                return -1;                      /* mas de 3 decimales reales */
            }
            frac_digits++;
            p++;
        }
    }

    while (*p) {
        if (!isspace((unsigned char)*p)) return -1;
        p++;
    }

    while (frac_digits < 3) {
        frac *= 10u;
        frac_digits++;
    }

    uint64_t total = whole * 1000ULL + (uint64_t)frac;
    if (total > UINT32_MAX) {
        return -1;
    }
    if (total < 1000ULL) {
        return -1;                              /* HOME_SLACK >= 1.0 */
    }

    *out = (uint32_t)total;
    return 0;
}

/* Parsea lista de hosts separados por coma */
static int parse_hosts(char *val_str, dsm_config_t *cfg)
{
    cfg->peer_host_count = 0;
    char *token = strtok(val_str, ",");
    while (token) {
        char *h = trim_whitespace(token);
        if (*h != '\0') {
            if (cfg->peer_host_count >= DSM_MAX_PEER_HOSTS) {
                return -1;
            }
            strncpy(cfg->peer_hosts[cfg->peer_host_count], h, DSM_MAX_HOST_LEN - 1);
            cfg->peer_hosts[cfg->peer_host_count][DSM_MAX_HOST_LEN - 1] = '\0';
            cfg->peer_host_count++;
        }
        token = strtok(NULL, ",");
    }
    return (cfg->peer_host_count > 0) ? 0 : -1;
}

/* Máscara de bits para detectar claves duplicadas */
#define KEY_CLUSTER_ID          (1ULL << 0)
#define KEY_PAGE_SIZE           (1ULL << 1)
#define KEY_BASE_ADDR           (1ULL << 2)
#define KEY_BASE_ADDR_TSAN      (1ULL << 3)
#define KEY_VIEW_SIZE           (1ULL << 4)
#define KEY_NODE_MEM            (1ULL << 5)
#define KEY_MAX_RUNTIME_NODES   (1ULL << 6)
#define KEY_VNODES_PER_MB       (1ULL << 7)
#define KEY_CHUNK_PAGES         (1ULL << 8)
#define KEY_REDUNDANCY          (1ULL << 9)
#define KEY_RAID_K              (1ULL << 10)
#define KEY_PORT_MIN            (1ULL << 11)
#define KEY_PORT_MAX            (1ULL << 12)
#define KEY_PEER_HOSTS          (1ULL << 13)
#define KEY_CONNECT_TIMEOUT_MS  (1ULL << 14)
#define KEY_DISCOVERY_TIMEOUT_MS (1ULL << 15)
#define KEY_SEND_TIMEOUT_MS     (1ULL << 16)
#define KEY_REQUEST_TIMEOUT_MS  (1ULL << 17)
#define KEY_REQUEST_RETRIES     (1ULL << 18)
#define KEY_RECONNECT_MIN_MS    (1ULL << 19)
#define KEY_RECONNECT_MAX_MS    (1ULL << 20)
#define KEY_HB_INTERVAL_MS      (1ULL << 21)
#define KEY_HB_SUSPECT          (1ULL << 22)
#define KEY_HB_DEAD             (1ULL << 23)
#define KEY_WORKER_THREADS      (1ULL << 24)
#define KEY_DISPATCHER_THREADS  (1ULL << 25)
#define KEY_PAGE_LOCK_STRIPES   (1ULL << 26)
#define KEY_HOME_SLACK          (1ULL << 27)
#define KEY_WAIT_POOL_ENTRIES   (1ULL << 28)
#define KEY_LOCK_TIMEOUT_MS     (1ULL << 29)
#define KEY_WORKER_QUEUE_CAP    (1ULL << 30)
#define KEY_OUTPUT_DIR          (1ULL << 31)
#define KEY_SYNC_THREADS        (1ULL << 32)
#define KEY_LOG_LEVEL           (1ULL << 33)

int config_load(dsm_config_t *cfg, const char *path)
{
    if (!cfg) return DSM_ERR_INVALID;
    if (!path) path = "dsm.conf";

    memset(cfg, 0, sizeof(*cfg));

    /* Valores predeterminados recomendados */
    cfg->cluster_id           = 0x534F3031;
    cfg->page_size            = 4096;
    cfg->base_addr            = 0x200000000000ULL;
    cfg->base_addr_tsan       = 0x4000000000ULL;
    cfg->view_size            = 4294967296ULL;
    cfg->node_mem             = 268435456ULL;
    cfg->max_runtime_nodes    = 256;
    cfg->vnodes_per_mb        = 16;
    cfg->chunk_pages          = 256;
    cfg->redundancy_raid5     = false;
    cfg->raid_k               = 2;
    cfg->port_min             = 27600;
    cfg->port_max             = 27699;
    cfg->connect_timeout_ms   = 200;
    cfg->discovery_timeout_ms = 300;
    cfg->send_timeout_ms      = 2000;
    cfg->request_timeout_ms   = 3000;
    cfg->request_retries      = 3;
    cfg->reconnect_min_ms     = 100;
    cfg->reconnect_max_ms     = 2000;
    cfg->hb_interval_ms       = 500;
    cfg->hb_suspect           = 3;
    cfg->hb_dead              = 6;
    cfg->worker_threads       = 4;
    cfg->dispatcher_threads   = 2;
    cfg->page_lock_stripes    = 256;
    cfg->home_slack_permille   = 1250;
    cfg->wait_pool_entries    = 4096;
    cfg->lock_timeout_ms      = 10000;
    cfg->worker_queue_cap     = 4096;
    cfg->sync_threads         = 4;
    strncpy(cfg->output_dir, "./reportes", sizeof(cfg->output_dir) - 1);
    strncpy(cfg->log_level, "INFO", sizeof(cfg->log_level) - 1);

    FILE *f = fopen(path, "rb");
    if (!f) {
        DSM_LOG_ERR("No se pudo abrir el archivo de configuracion '%s': %s", path, strerror(errno));
        return DSM_ERR_CONFIG;
    }

    /* stat en lugar de fseek/ftell: una ruta que apunta a un directorio o a un
     * dispositivo no es un archivo de configuracion valido y ftell no la
     * distingue de un archivo vacio. */
    struct stat st;
    if (fstat(fileno(f), &st) != 0 || !S_ISREG(st.st_mode)) {
        fclose(f);
        DSM_LOG_ERR("La ruta de configuracion '%s' no es un archivo regular", path);
        return DSM_ERR_CONFIG;
    }

    if (st.st_size < 0 || (uint64_t)st.st_size > 1024ULL * 1024ULL) {
        fclose(f);
        DSM_LOG_ERR("Archivo de configuracion invalido o demasiado grande");
        return DSM_ERR_CONFIG;
    }

    size_t fsize = (size_t)st.st_size;
    uint8_t *raw_buf = malloc(fsize + 1);
    if (!raw_buf) {
        fclose(f);
        return DSM_ERR_MEM;
    }

    if (fread(raw_buf, 1, fsize, f) != fsize) {
        free(raw_buf);
        fclose(f);
        return DSM_ERR_CONFIG;
    }
    raw_buf[fsize] = '\0';
    fclose(f);

    /* Calcular CRC32 sobre el contenido exacto */
    cfg->raw_hash = dsm_crc32(0, raw_buf, (size_t)fsize);

    /* Procesar linea por linea */
    uint64_t seen_keys = 0;
    char *cursor = (char *)raw_buf;
    int line_num = 0;

    while (cursor && *cursor) {
        line_num++;
        char *line_end = strchr(cursor, '\n');
        if (line_end) {
            *line_end = '\0';
        }

        /* Descartar comentarios en la linea (#) */
        char *comment = strchr(cursor, '#');
        if (comment) {
            *comment = '\0';
        }

        char *trimmed = trim_whitespace(cursor);
        if (*trimmed == '\0') {
            cursor = line_end ? line_end + 1 : NULL;
            continue;
        }

        char *eq = strchr(trimmed, '=');
        if (!eq) {
            DSM_LOG_ERR("Error en %s:%d: linea sin separador '=': '%s'", path, line_num, trimmed);
            free(raw_buf);
            return DSM_ERR_CONFIG;
        }

        *eq = '\0';
        char *key = trim_whitespace(trimmed);
        char *val = trim_whitespace(eq + 1);

        uint64_t mask = 0;
        int parse_err = 0;

        if (strcasecmp(key, "CLUSTER_ID") == 0) {
            mask = KEY_CLUSTER_ID;
            parse_err = parse_u32(val, &cfg->cluster_id);
        } else if (strcasecmp(key, "PAGE_SIZE") == 0) {
            mask = KEY_PAGE_SIZE;
            parse_err = parse_u32(val, &cfg->page_size);
        } else if (strcasecmp(key, "BASE_ADDR") == 0) {
            mask = KEY_BASE_ADDR;
            parse_err = parse_u64(val, &cfg->base_addr);
        } else if (strcasecmp(key, "BASE_ADDR_TSAN") == 0) {
            mask = KEY_BASE_ADDR_TSAN;
            parse_err = parse_u64(val, &cfg->base_addr_tsan);
        } else if (strcasecmp(key, "VIEW_SIZE") == 0) {
            mask = KEY_VIEW_SIZE;
            parse_err = parse_u64(val, &cfg->view_size);
        } else if (strcasecmp(key, "NODE_MEM") == 0) {
            mask = KEY_NODE_MEM;
            parse_err = parse_u64(val, &cfg->node_mem);
        } else if (strcasecmp(key, "MAX_RUNTIME_NODES") == 0) {
            mask = KEY_MAX_RUNTIME_NODES;
            parse_err = parse_u32(val, &cfg->max_runtime_nodes);
        } else if (strcasecmp(key, "VNODES_PER_MB") == 0) {
            mask = KEY_VNODES_PER_MB;
            parse_err = parse_u32(val, &cfg->vnodes_per_mb);
        } else if (strcasecmp(key, "CHUNK_PAGES") == 0) {
            mask = KEY_CHUNK_PAGES;
            parse_err = parse_u32(val, &cfg->chunk_pages);
        } else if (strcasecmp(key, "REDUNDANCY") == 0) {
            mask = KEY_REDUNDANCY;
            if (strcasecmp(val, "NONE") == 0) {
                cfg->redundancy_raid5 = false;
            } else if (strcasecmp(val, "RAID5") == 0) {
                cfg->redundancy_raid5 = true;
            } else {
                parse_err = -1;
            }
        } else if (strcasecmp(key, "RAID_K") == 0) {
            mask = KEY_RAID_K;
            parse_err = parse_u32(val, &cfg->raid_k);
        } else if (strcasecmp(key, "PORT_MIN") == 0) {
            mask = KEY_PORT_MIN;
            parse_err = parse_u16(val, &cfg->port_min);
        } else if (strcasecmp(key, "PORT_MAX") == 0) {
            mask = KEY_PORT_MAX;
            parse_err = parse_u16(val, &cfg->port_max);
        } else if (strcasecmp(key, "PEER_HOSTS") == 0) {
            mask = KEY_PEER_HOSTS;
            parse_err = parse_hosts(val, cfg);
        } else if (strcasecmp(key, "CONNECT_TIMEOUT_MS") == 0) {
            mask = KEY_CONNECT_TIMEOUT_MS;
            parse_err = parse_u32(val, &cfg->connect_timeout_ms);
        } else if (strcasecmp(key, "DISCOVERY_TIMEOUT_MS") == 0) {
            mask = KEY_DISCOVERY_TIMEOUT_MS;
            parse_err = parse_u32(val, &cfg->discovery_timeout_ms);
        } else if (strcasecmp(key, "SEND_TIMEOUT_MS") == 0) {
            mask = KEY_SEND_TIMEOUT_MS;
            parse_err = parse_u32(val, &cfg->send_timeout_ms);
        } else if (strcasecmp(key, "REQUEST_TIMEOUT_MS") == 0) {
            mask = KEY_REQUEST_TIMEOUT_MS;
            parse_err = parse_u32(val, &cfg->request_timeout_ms);
        } else if (strcasecmp(key, "REQUEST_RETRIES") == 0) {
            mask = KEY_REQUEST_RETRIES;
            parse_err = parse_u32(val, &cfg->request_retries);
        } else if (strcasecmp(key, "RECONNECT_MIN_MS") == 0) {
            mask = KEY_RECONNECT_MIN_MS;
            parse_err = parse_u32(val, &cfg->reconnect_min_ms);
        } else if (strcasecmp(key, "RECONNECT_MAX_MS") == 0) {
            mask = KEY_RECONNECT_MAX_MS;
            parse_err = parse_u32(val, &cfg->reconnect_max_ms);
        } else if (strcasecmp(key, "HB_INTERVAL_MS") == 0) {
            mask = KEY_HB_INTERVAL_MS;
            parse_err = parse_u32(val, &cfg->hb_interval_ms);
        } else if (strcasecmp(key, "HB_SUSPECT") == 0) {
            mask = KEY_HB_SUSPECT;
            parse_err = parse_u32(val, &cfg->hb_suspect);
        } else if (strcasecmp(key, "HB_DEAD") == 0) {
            mask = KEY_HB_DEAD;
            parse_err = parse_u32(val, &cfg->hb_dead);
        } else if (strcasecmp(key, "WORKER_THREADS") == 0) {
            mask = KEY_WORKER_THREADS;
            parse_err = parse_u32(val, &cfg->worker_threads);
        } else if (strcasecmp(key, "DISPATCHER_THREADS") == 0) {
            mask = KEY_DISPATCHER_THREADS;
            parse_err = parse_u32(val, &cfg->dispatcher_threads);
        } else if (strcasecmp(key, "PAGE_LOCK_STRIPES") == 0) {
            mask = KEY_PAGE_LOCK_STRIPES;
            parse_err = parse_u32(val, &cfg->page_lock_stripes);
        } else if (strcasecmp(key, "HOME_SLACK") == 0) {
            mask = KEY_HOME_SLACK;
            parse_err = parse_slack_permille(val, &cfg->home_slack_permille);
        } else if (strcasecmp(key, "WAIT_POOL_ENTRIES") == 0) {
            mask = KEY_WAIT_POOL_ENTRIES;
            parse_err = parse_u32(val, &cfg->wait_pool_entries);
        } else if (strcasecmp(key, "LOCK_TIMEOUT_MS") == 0) {
            mask = KEY_LOCK_TIMEOUT_MS;
            parse_err = parse_u32(val, &cfg->lock_timeout_ms);
        } else if (strcasecmp(key, "WORKER_QUEUE_CAP") == 0) {
            mask = KEY_WORKER_QUEUE_CAP;
            parse_err = parse_u32(val, &cfg->worker_queue_cap);
        } else if (strcasecmp(key, "SYNC_THREADS") == 0) {
            mask = KEY_SYNC_THREADS;
            parse_err = parse_u32(val, &cfg->sync_threads);
        } else if (strcasecmp(key, "OUTPUT_DIR") == 0) {
            mask = KEY_OUTPUT_DIR;
            strncpy(cfg->output_dir, val, sizeof(cfg->output_dir) - 1);
            cfg->output_dir[sizeof(cfg->output_dir) - 1] = '\0';
        } else if (strcasecmp(key, "DSM_LOG_LEVEL") == 0 || strcasecmp(key, "LOG_LEVEL") == 0) {
            mask = KEY_LOG_LEVEL;
            strncpy(cfg->log_level, val, sizeof(cfg->log_level) - 1);
            dsm_log_set_level(dsm_log_level_from_str(val));
        } else {
            DSM_LOG_ERR("Clave de configuracion desconocida en %s:%d: '%s'", path, line_num, key);
            free(raw_buf);
            return DSM_ERR_CONFIG;
        }

        if (parse_err != 0) {
            DSM_LOG_ERR("Valor invalido para clave '%s' en %s:%d: '%s'", key, path, line_num, val);
            free(raw_buf);
            return DSM_ERR_CONFIG;
        }

        if (mask != 0) {
            if (seen_keys & mask) {
                DSM_LOG_ERR("Clave duplicada '%s' en %s:%d", key, path, line_num);
                free(raw_buf);
                return DSM_ERR_CONFIG;
            }
            seen_keys |= mask;
        }

        cursor = line_end ? line_end + 1 : NULL;
    }

    free(raw_buf);
    return config_validate(cfg);
}

int config_validate(dsm_config_t *cfg)
{
    if (!cfg) return DSM_ERR_INVALID;

    if (cfg->max_runtime_nodes > MAX_NODES) {
        DSM_LOG_ERR("MAX_RUNTIME_NODES no puede superar %d; recompile con -DMAX_NODES", MAX_NODES);
        return DSM_ERR_CONFIG;
    }

    /* Cero nodos en runtime deja membership_next_free_slot() sin ningun slot
     * posible: el nodo no puede unirse ni solo. */
    if (cfg->max_runtime_nodes == 0) {
        DSM_LOG_ERR("MAX_RUNTIME_NODES debe ser al menos 1");
        return DSM_ERR_CONFIG;
    }

    /* PAGE_SIZE = 0 pasaria los chequeos de modulo y potencia de dos y luego
     * provocaria una division por cero al validar VIEW_SIZE. */
    if (cfg->page_size == 0) {
        DSM_LOG_ERR("PAGE_SIZE debe ser mayor que cero");
        return DSM_ERR_CONFIG;
    }

    long sc_page = sysconf(_SC_PAGESIZE);
    if (sc_page <= 0) sc_page = 4096;
    if (cfg->page_size % (uint32_t)sc_page != 0 || (cfg->page_size & (cfg->page_size - 1)) != 0) {
        DSM_LOG_ERR("PAGE_SIZE debe ser multiplo de %ld y potencia de dos", sc_page);
        return DSM_ERR_CONFIG;
    }

    /* VIEW_SIZE = 0 es multiplo de cualquier PAGE_SIZE, pero no describe
     * ninguna vista mapeable. */
    if (cfg->view_size == 0) {
        DSM_LOG_ERR("VIEW_SIZE debe ser mayor que cero");
        return DSM_ERR_CONFIG;
    }

    if (cfg->view_size % cfg->page_size != 0) {
        DSM_LOG_ERR("VIEW_SIZE debe ser multiplo de PAGE_SIZE");
        return DSM_ERR_CONFIG;
    }

    if (cfg->view_size > (64ULL << 30)) {
        DSM_LOG_ERR("VIEW_SIZE no puede exceder 64 GiB");
        return DSM_ERR_CONFIG;
    }

    if (cfg->base_addr % (2 * 1024 * 1024) != 0) {
        DSM_LOG_ERR("BASE_ADDR debe estar alineado a 2 MiB");
        return DSM_ERR_CONFIG;
    }

    /* La vista se mapea en [BASE_ADDR, BASE_ADDR + VIEW_SIZE). Si esa suma
     * desborda uint64, el mmap recibe un rango que cruza el final del espacio
     * de direcciones y falla con EINVAL/ENOMEM ya con hilos creados. */
    if (cfg->base_addr > UINT64_MAX - cfg->view_size) {
        DSM_LOG_ERR("BASE_ADDR + VIEW_SIZE desborda el espacio de direcciones");
        return DSM_ERR_CONFIG;
    }

    /* BASE_ADDR_TSAN se usa como direccion de mmap bajo -DDSM_TSAN. Dejarlo
     * desalineado convierte un error de configuracion en un EINVAL opaco
     * dentro de dsm_mem_init, despues de haber arrancado hilos. */
    if (cfg->base_addr_tsan != 0 &&
        cfg->base_addr_tsan % (2 * 1024 * 1024) != 0) {
        DSM_LOG_ERR("BASE_ADDR_TSAN debe estar alineado a 2 MiB");
        return DSM_ERR_CONFIG;
    }

    if (cfg->port_min > cfg->port_max || (cfg->port_max - cfg->port_min + 1) < 1) {
        DSM_LOG_ERR("Rango de puertos invalido: %u - %u", cfg->port_min, cfg->port_max);
        return DSM_ERR_CONFIG;
    }

    /* El rango debe quedar contenido en el bloque asignado al grupo. Un
     * PORT_MIN/PORT_MAX fuera de [DSM_PORT_BLOCK_MIN, DSM_PORT_BLOCK_MAX]
     * chocaria con los puertos de otro equipo en las mismas maquinas. */
    if (cfg->port_min < DSM_PORT_BLOCK_MIN || cfg->port_max > DSM_PORT_BLOCK_MAX) {
        DSM_LOG_ERR("Rango de puertos [%u, %u] fuera del bloque asignado [%u, %u]",
                    cfg->port_min, cfg->port_max,
                    DSM_PORT_BLOCK_MIN, DSM_PORT_BLOCK_MAX);
        return DSM_ERR_CONFIG;
    }

    if (cfg->hb_suspect >= cfg->hb_dead) {
        DSM_LOG_ERR("HB_SUSPECT debe ser menor que HB_DEAD");
        return DSM_ERR_CONFIG;
    }

    /* membership_evaluate() calcula 'elapsed / hb_interval_ms': un intervalo
     * de cero no es una configuracion lenta, es una division por cero en el
     * hilo de latidos. */
    if (cfg->hb_interval_ms == 0) {
        DSM_LOG_ERR("HB_INTERVAL_MS debe ser mayor que cero");
        return DSM_ERR_CONFIG;
    }

    if (cfg->worker_threads < 1 || cfg->worker_threads > 64 ||
        cfg->dispatcher_threads < 1 || cfg->dispatcher_threads > 64) {
        DSM_LOG_ERR("Numero de hilos worker/dispatcher fuera de rango [1, 64]");
        return DSM_ERR_CONFIG;
    }

    if ((cfg->page_lock_stripes & (cfg->page_lock_stripes - 1)) != 0 || cfg->page_lock_stripes < 1) {
        DSM_LOG_ERR("PAGE_LOCK_STRIPES debe ser potencia de dos");
        return DSM_ERR_CONFIG;
    }

    if (cfg->node_mem % cfg->page_size != 0 || cfg->node_mem < cfg->page_size) {
        DSM_LOG_ERR("NODE_MEM debe ser multiplo de PAGE_SIZE y >= PAGE_SIZE");
        return DSM_ERR_CONFIG;
    }

    if (cfg->redundancy_raid5) {
        DSM_LOG_ERR("RAID5 no esta soportado en el prototipo 1 (desactive con REDUNDANCY = NONE)");
        return DSM_ERR_CONFIG;
    }

    if (cfg->home_slack_permille < 1000) {
        DSM_LOG_ERR("HOME_SLACK no puede ser menor que 1.0 (1000 por mil)");
        return DSM_ERR_CONFIG;
    }

    if (cfg->request_retries < 1) {
        DSM_LOG_ERR("REQUEST_RETRIES debe ser >= 1");
        return DSM_ERR_CONFIG;
    }

    return DSM_OK;
}

void config_free(dsm_config_t *cfg)
{
    (void)cfg;
}

void config_dump(const dsm_config_t *cfg)
{
    DSM_LOG_INFO("Configuracion cargada:");
    DSM_LOG_INFO("  CLUSTER_ID        = 0x%08X", cfg->cluster_id);
    DSM_LOG_INFO("  PAGE_SIZE         = %u", cfg->page_size);
    DSM_LOG_INFO("  BASE_ADDR         = 0x%lX", (unsigned long)cfg->base_addr);
    DSM_LOG_INFO("  VIEW_SIZE         = %lu (%lu MB)", (unsigned long)cfg->view_size, (unsigned long)(cfg->view_size >> 20));
    DSM_LOG_INFO("  NODE_MEM          = %lu (%lu MB)", (unsigned long)cfg->node_mem, (unsigned long)(cfg->node_mem >> 20));
    DSM_LOG_INFO("  PORT_RANGE        = %u - %u", cfg->port_min, cfg->port_max);
    DSM_LOG_INFO("  PEER_HOSTS_COUNT  = %u", cfg->peer_host_count);
    for (uint32_t i = 0; i < cfg->peer_host_count; i++) {
        DSM_LOG_INFO("    Host[%u]         = %s", i, cfg->peer_hosts[i]);
    }
    DSM_LOG_INFO("  HEARTBEAT         = %u ms (suspect=%u, dead=%u)",
                 cfg->hb_interval_ms, cfg->hb_suspect, cfg->hb_dead);
    DSM_LOG_INFO("  THREADS           = workers:%u, dispatchers:%u",
                 cfg->worker_threads, cfg->dispatcher_threads);
    DSM_LOG_INFO("  HOME_SLACK        = %u.%03u",
                 cfg->home_slack_permille / 1000, cfg->home_slack_permille % 1000);
    DSM_LOG_INFO("  RAW_HASH          = 0x%08X", cfg->raw_hash);
}
