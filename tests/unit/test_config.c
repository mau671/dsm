/* ==============================================================================
 * test_config.c — Suite de pruebas unitarias y de estres para el modulo de
 *                 configuracion (src/config.c).
 *
 * Cobertura:
 *   1. Carga del dsm.conf canonico del repositorio y defaults documentados.
 *   2. Archivos malformados reales: linea sin '=', clave desconocida, valor
 *      vacio, valor basura, EOF sin newline, archivo vacio, archivo binario.
 *   3. Overflow numerico en cada anchura (u16/u32/u64), negativos, hex, octal
 *      implicito, sufijos basura y limites exactos.
 *   4. Regla de puertos asignados (bloque 27600..27699 del grupo).
 *   5. shared_hash = CRC32 de las claves compartidas; NODE_MEM, rutas, hilos
 *      y temporizaciones locales no lo alteran.
 *   6. HOME_SLACK: conversion exacta a milesimas enteras y rechazo de valores
 *      con mas de tres decimales significativos o no representables.
 *   7. Validaciones cruzadas (page_size, view_size, alineacion, heartbeats,
 *      hilos, stripes, node_mem, paridad, retries).
 *   8. Duplicados, recarga sobre estructura sucia, y ausencia de fugas.
 *   9. Rigor adicional: estabilidad de shared_hash, limites de sistema uno por
 *      uno con VIEW_SIZE/NODE_MEM coherentes, archivo de 1 MiB exacto y
 *      1 MiB + 1, rutas no regulares, y config_validate() mutada campo a campo.
 *  10. REQUEST_RETRIES: un par que descarta la primera peticion y responde la
 *      segunda demuestra el reenvio real de dsm_net_rpc() con req_id nuevo y
 *      espera de backoff, y con REQUEST_RETRIES = 1 no hay segundo intento.
 * ============================================================================== */

#include "test.h"
#include "config.h"
#include "wire.h"
#include "net.h"
#include "util.h"
#include "dsm_internal.h"

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>

/* ----------------------------------------------------------------------------
 * Utilidades de fixtures
 * ------------------------------------------------------------------------- */

#define FIXTURE_DIR "/tmp/dsm_test_config"

static void fixture_setup(void)
{
    mkdir(FIXTURE_DIR, 0700);
}

/* Ruta estable por nombre de fixture (sin colisiones con otras suites) */
static const char *fixture_path(const char *name, char out[256])
{
    snprintf(out, 256, FIXTURE_DIR "/%s", name);
    return out;
}

/* Escribe 'content' en el fixture y devuelve la ruta (valida mientras viva buf) */
static const char *write_fixture(const char *name, const char *content, char buf[256])
{
    const char *path = fixture_path(name, buf);
    FILE *f = fopen(path, "wb");
    if (!f) return NULL;
    fwrite(content, 1, strlen(content), f);
    fclose(f);
    return path;
}

/* Escribe exactamente 'len' bytes arbitrarios (no asume '\0') */
static const char *write_fixture_raw(const char *name, const void *data, size_t len,
                                     char buf[256])
{
    const char *path = fixture_path(name, buf);
    FILE *f = fopen(path, "wb");
    if (!f) return NULL;
    fwrite(data, 1, len, f);
    fclose(f);
    return path;
}

/* Configuracion valida minima: todo lo que config_validate exige explicito.
 * Es un literal (no una variable) para permitir concatenacion en las macros. */
#define VALID_MIN \
    "CLUSTER_ID         = 0x534F3031\n" \
    "PAGE_SIZE          = 4096\n" \
    "BASE_ADDR          = 0x200000000000\n" \
    "VIEW_SIZE          = 4294967296\n" \
    "NODE_MEM           = 268435456\n" \
    "PORT_MIN           = 27600\n" \
    "PORT_MAX           = 27699\n" \

/* Carga un fixture compuesto y devuelve el codigo de config_load */
static int load_fixture(const char *name, const char *content, dsm_config_t *cfg)
{
    char path[256];
    if (!write_fixture(name, content, path)) return DSM_ERR_INVALID;
    return config_load(cfg, path);
}

/* ----------------------------------------------------------------------------
 * 1. Carga del dsm.conf real del repositorio
 * ------------------------------------------------------------------------- */

static int test_load_repo_config(void)
{
    dsm_config_t cfg;
    int rc = config_load(&cfg, "dsm.conf");
    REQUIRE(rc == DSM_OK);

    /* Identidad y estructura */
    CHECK(cfg.cluster_id == 0x534F3031);
    CHECK(cfg.page_size == 4096);
    CHECK(cfg.base_addr == 0x200000000000ULL);
    CHECK(cfg.base_addr_tsan == 0x4000000000ULL);
    CHECK(cfg.view_size == 4294967296ULL);
    CHECK(cfg.node_mem == 268435456ULL);
    CHECK(cfg.max_runtime_nodes == 256);
    CHECK(cfg.vnodes_per_mb == 16);
    CHECK(cfg.redundancy == false);

    /* Red y descubrimiento */
    CHECK(cfg.port_min == 27600);
    CHECK(cfg.port_max == 27699);
    CHECK(cfg.discovery_group == 0xEFFF2A63u);
    CHECK(cfg.discovery_port == 27699);

    /* Temporizaciones */
    CHECK(cfg.connect_timeout_ms == 200);
    CHECK(cfg.discovery_timeout_ms == 300);
    CHECK(cfg.send_timeout_ms == 2000);
    CHECK(cfg.request_timeout_ms == 3000);
    CHECK(cfg.request_retries == 3);
    CHECK(cfg.reconnect_min_ms == 100);
    CHECK(cfg.reconnect_max_ms == 2000);
    CHECK(cfg.hb_interval_ms == 500);
    CHECK(cfg.hb_suspect == 3);
    CHECK(cfg.hb_dead == 6);

    /* Hilos y limites */
    CHECK(cfg.worker_threads == 4);
    CHECK(cfg.dispatcher_threads == 2);
    CHECK(cfg.page_lock_stripes == 256);
    CHECK(cfg.home_slack_permille == 1250);       /* 1.25 -> 1250 */
    CHECK(cfg.lock_timeout_ms == 10000);
    CHECK(cfg.worker_queue_cap == 4096);

    /* Diagnostico */
    CHECK(strcmp(cfg.log_level, "INFO") == 0);

    /* El hash se calcula sobre las claves compartidas, no sobre los bytes
     * del archivo. Dos lecturas del mismo archivo coinciden. */
    CHECK(cfg.shared_hash != 0);

    dsm_config_t again;
    REQUIRE(config_load(&again, "dsm.conf") == DSM_OK);
    CHECK(again.shared_hash == cfg.shared_hash);
    CHECK(again.discovery_group == cfg.discovery_group);
    CHECK(again.discovery_port == cfg.discovery_port);

    config_free(&again);
    config_free(&cfg);
    return 0;
}

/* ----------------------------------------------------------------------------
 * 2. Archivos malformados reales
 * ------------------------------------------------------------------------- */

static int test_malformed_syntax_variants(void)
{
    dsm_config_t cfg;

    /* 2.1 Linea sin '=' */
    CHECK(load_fixture("bad_noeq.conf", "PAGE_SIZE 4096\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.2 Clave desconocida (typo) */
    CHECK(load_fixture("bad_unknown.conf", "PAGE_SIZ = 4096\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.3 Clave presente, valor vacio */
    CHECK(load_fixture("bad_emptyval.conf", "PAGE_SIZE =\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.4 Valor basura no numerico */
    CHECK(load_fixture("bad_garbage.conf", "PAGE_SIZE = cuatro mil\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.5 Valor numerico con sufijo basura */
    CHECK(load_fixture("bad_suffix.conf", "PAGE_SIZE = 4096x\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.6 Separador sin clave */
    CHECK(load_fixture("bad_nokey.conf", "= 4096\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.7 Doble '=': el segundo queda dentro del valor y rompe el parseo */
    CHECK(load_fixture("bad_doubleeq.conf", "PAGE_SIZE = 4096 = 8192\n", &cfg) == DSM_ERR_CONFIG);

    /* 2.8 REDUNDANCY con token invalido */
    CHECK(load_fixture("bad_redundancy.conf", VALID_MIN "REDUNDANCY = RAID6\n", &cfg)
          == DSM_ERR_CONFIG);

    /* 2.9 Archivo inexistente */
    CHECK(config_load(&cfg, FIXTURE_DIR "/no_existe_jamas.conf") == DSM_ERR_CONFIG);

    return 0;
}

static int test_malformed_edge_inputs(void)
{
    dsm_config_t cfg;
    char path[256];

    /* Archivo vacio: todos los defaults y hash de las claves compartidas */
    const char *p = write_fixture("empty.conf", "", path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.shared_hash != 0);
    CHECK(cfg.page_size == 4096);                /* default aplicado */
    CHECK(cfg.port_min == 27600);

    /* Solo comentarios y lineas en blanco */
    p = write_fixture("comments.conf",
                      "# comentario\n\n   \n\t\n# otro\n", path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.page_size == 4096);

    /* Sin salto de linea final: la ultima clave debe procesarse */
    p = write_fixture("nonewline.conf", VALID_MIN, path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.port_min == 27600);

    /* CRLF: '\r' debe recortarse como espacio y no contaminar el valor */
    p = write_fixture("crlf.conf",
                      "CLUSTER_ID = 0x534F3031\r\n"
                      "PAGE_SIZE = 4096\r\n"
                      "PORT_MIN = 27600\r\n"
                      "PORT_MAX = 27699\r\n", path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.port_min == 27600);
    CHECK(cfg.page_size == 4096);

    /* Comentario al final del valor (estilo del dsm.conf real) */
    p = write_fixture("inline_comment.conf",
                      "PAGE_SIZE = 4096  # pagina de 4 KiB\n"
                      "PORT_MIN = 27600  # inicio\n"
                      "PORT_MAX = 27699  # fin\n", path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.page_size == 4096);
    CHECK(cfg.port_min == 27600);

    /* Contenido binario: NUL embebido corta el cursor, pero no debe crashear */
    const uint8_t bin[] = { 'P','A','G','E','_','S','I','Z','E','=','4','0','9','6',
                            0x00, 0x01, 0x02, 0xFF };
    p = write_fixture_raw("binary.conf", bin, sizeof(bin), path);
    REQUIRE(p != NULL);
    int rc = config_load(&cfg, p);
    CHECK(rc == DSM_OK || rc == DSM_ERR_CONFIG); /* nunca crash ni OK silencioso roto */

    return 0;
}

static int test_duplicate_key_detection(void)
{
    dsm_config_t cfg;

    /* Mismo valor repetido sigue siendo error (ambiguedad de configuracion) */
    CHECK(load_fixture("dup_same.conf",
                       "PORT_MIN = 27600\nPORT_MIN = 27600\n", &cfg) == DSM_ERR_CONFIG);

    /* Distinto valor repetido */
    CHECK(load_fixture("dup_diff.conf",
                       "PORT_MIN = 27600\nPORT_MIN = 27605\n", &cfg) == DSM_ERR_CONFIG);

    /* Duplicado con distinto case: la clave es case-insensitive */
    CHECK(load_fixture("dup_case.conf",
                       "PORT_MIN = 27600\nport_min = 27605\n", &cfg) == DSM_ERR_CONFIG);

    /* Duplicado separado por lineas en blanco y comentarios */
    CHECK(load_fixture("dup_far.conf",
                       "PAGE_SIZE = 4096\n\n# ruido\n\nPAGE_SIZE = 8192\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Dos alias de log level tambien cuentan como duplicado */
    CHECK(load_fixture("dup_logalias.conf",
                       "DSM_LOG_LEVEL = INFO\nLOG_LEVEL = DEBUG\n", &cfg) == DSM_ERR_CONFIG);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 3. Overflow numerico y limites
 * ------------------------------------------------------------------------- */

static int test_numeric_overflow(void)
{
    dsm_config_t cfg;

    /* u32: UINT32_MAX pasa, UINT32_MAX+1 falla */
    CHECK(load_fixture("u32_max.conf", "CLUSTER_ID = 4294967295\n", &cfg) == DSM_OK);
    CHECK(cfg.cluster_id == UINT32_MAX);
    CHECK(load_fixture("u32_over.conf", "CLUSTER_ID = 4294967296\n", &cfg) == DSM_ERR_CONFIG);
    CHECK(load_fixture("u32_huge.conf",
                       "CLUSTER_ID = 18446744073709551616\n", &cfg) == DSM_ERR_CONFIG);

    /* u16: 65536 desborda el tipo; 65535 parsea pero queda fuera del bloque */
    CHECK(load_fixture("u16_max.conf",
                       "PORT_MIN = 65535\nPORT_MAX = 65535\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("u16_over.conf", "PORT_MIN = 65536\n", &cfg) == DSM_ERR_CONFIG);

    /* u64: valor maximo alineado a 2 MiB. VIEW_SIZE se reduce para que
     * BASE_ADDR + VIEW_SIZE siga cabiendo en el espacio de direcciones. */
    CHECK(load_fixture("u64_max.conf",
                       "BASE_ADDR = 18446744073707454464\nVIEW_SIZE = 4096\n", &cfg)
          == DSM_OK);
    CHECK(cfg.base_addr == 18446744073707454464ULL);
    CHECK(load_fixture("u64_over.conf",
                       "BASE_ADDR = 18446744073709551616\n", &cfg) == DSM_ERR_CONFIG);

    /* Negativos: no representables en tipos sin signo, deben rechazarse */
    CHECK(load_fixture("neg_page.conf", "PAGE_SIZE = -4096\n", &cfg) == DSM_ERR_CONFIG);
    CHECK(load_fixture("neg_ports.conf", "PORT_MIN = -1\n", &cfg) == DSM_ERR_CONFIG);
    CHECK(load_fixture("neg_base.conf", "BASE_ADDR = -0x200000\n", &cfg) == DSM_ERR_CONFIG);

    /* Hex y octal implicito (base 0) */
    CHECK(load_fixture("hex.conf", "CLUSTER_ID = 0xDEADBEEF\n", &cfg) == DSM_OK);
    CHECK(cfg.cluster_id == 0xDEADBEEF);
    CHECK(load_fixture("octal.conf", "WORKER_QUEUE_CAP = 010\n", &cfg) == DSM_OK);
    CHECK(cfg.worker_queue_cap == 8);             /* 010 octal == 8 */
    CHECK(load_fixture("hex_over.conf", "CLUSTER_ID = 0x1FFFFFFFF\n", &cfg) == DSM_ERR_CONFIG);

    /* +N explicito: strtoull lo acepta; debe seguir siendo un valor valido */
    CHECK(load_fixture("plus.conf", "WORKER_QUEUE_CAP = +512\n", &cfg) == DSM_OK);
    CHECK(cfg.worker_queue_cap == 512);

    /* Numero flotante donde se espera entero */
    CHECK(load_fixture("float_int.conf", "PAGE_SIZE = 4096.0\n", &cfg) == DSM_ERR_CONFIG);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 4. Regla del bloque de puertos asignado (27600..27699)
 * ------------------------------------------------------------------------- */

static int test_port_block_rule(void)
{
    dsm_config_t cfg;

    /* El default documentado es exactamente el bloque del grupo */
    dsm_config_t defaults;
    char path[256];
    const char *p = write_fixture("ports_default.conf", "", path);
    REQUIRE(p != NULL);
    REQUIRE(config_load(&defaults, p) == DSM_OK);
    CHECK(defaults.port_min == 27600);
    CHECK(defaults.port_max == 27699);
    CHECK((defaults.port_max - defaults.port_min + 1) == 100);

    /* Limites exactos del bloque aceptados */
    CHECK(load_fixture("ports_exact.conf",
                       "PORT_MIN = 27600\nPORT_MAX = 27699\n", &cfg) == DSM_OK);

    /* Un unico puerto (min == max) es un rango valido de tamano 1 */
    CHECK(load_fixture("ports_single.conf",
                       "PORT_MIN = 27650\nPORT_MAX = 27650\n", &cfg) == DSM_OK);
    CHECK(cfg.port_min == 27650 && cfg.port_max == 27650);

    /* Invertido: min > max debe rechazarse */
    CHECK(load_fixture("ports_inv.conf",
                       "PORT_MIN = 27699\nPORT_MAX = 27600\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Fuera del bloque asignado: no debe aceptarse silenciosamente en el
     * default del grupo. El rango declarado por el grupo es 27600..27699. */
    CHECK(load_fixture("ports_below.conf",
                       "PORT_MIN = 27500\nPORT_MAX = 27599\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_above.conf",
                       "PORT_MIN = 27700\nPORT_MAX = 27799\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Rango que solapa parcialmente el bloque pero lo excede */
    CHECK(load_fixture("ports_overlap_lo.conf",
                       "PORT_MIN = 27590\nPORT_MAX = 27699\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_overlap_hi.conf",
                       "PORT_MIN = 27600\nPORT_MAX = 27710\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Subbloque dentro del asignado: aceptado */
    CHECK(load_fixture("ports_sub.conf",
                       "PORT_MIN = 27610\nPORT_MAX = 27619\n", &cfg) == DSM_OK);

    /* Puerto 0 y puertos privilegiados fuera de rango */
    CHECK(load_fixture("ports_zero.conf",
                       "PORT_MIN = 0\nPORT_MAX = 0\n", &cfg) == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_priv.conf",
                       "PORT_MIN = 80\nPORT_MAX = 90\n", &cfg) == DSM_ERR_CONFIG);

    return 0;
}

/* ---------------------------------------------------------------------------
 * 5. shared_hash: solo las claves que deben coincidir entre nodos
 * ------------------------------------------------------------------------- */

#define SHARED_BASE \
    "CLUSTER_ID         = 0x534F3031\n" \
    "PAGE_SIZE          = 4096\n" \
    "BASE_ADDR          = 0x200000000000\n" \
    "VIEW_SIZE          = 4294967296\n" \
    "PORT_MIN           = 27600\n" \
    "PORT_MAX           = 27699\n" \
    "REDUNDANCY         = false\n"

static int test_shared_hash_subset(void)
{
    dsm_config_t a;
    dsm_config_t b;
    dsm_config_t c;
    char path[256];

    const char *pa = write_fixture("shared_a.conf",
        SHARED_BASE
        "NODE_MEM            = 268435456\n"
        "OUTPUT_DIR          = reports\n"
        "DSM_LOG_LEVEL       = INFO\n"
        "WORKER_THREADS      = 4\n", path);
    REQUIRE(pa != NULL);
    REQUIRE(config_load(&a, pa) == DSM_OK);

    const char *pb = write_fixture("shared_b.conf",
        SHARED_BASE
        "NODE_MEM            = 67108864\n"
        "OUTPUT_DIR          = /tmp/reportes_b\n"
        "DSM_LOG_LEVEL       = DEBUG\n"
        "WORKER_THREADS      = 2\n"
        "CONNECT_TIMEOUT_MS  = 900\n", path);
    REQUIRE(pb != NULL);
    REQUIRE(config_load(&b, pb) == DSM_OK);

    CHECK(a.node_mem != b.node_mem);
    CHECK(a.shared_hash == b.shared_hash);

    const char *pc = write_fixture("shared_c.conf",
        "CLUSTER_ID         = 0x534F3032\n"
        "PAGE_SIZE          = 4096\n"
        "BASE_ADDR          = 0x200000000000\n"
        "VIEW_SIZE          = 4294967296\n"
        "PORT_MIN           = 27600\n"
        "PORT_MAX           = 27699\n"
        "REDUNDANCY         = false\n", path);
    REQUIRE(pc != NULL);
    REQUIRE(config_load(&c, pc) == DSM_OK);
    CHECK(c.shared_hash != a.shared_hash);

    pc = write_fixture("shared_d.conf",
        "CLUSTER_ID         = 0x534F3031\n"
        "PAGE_SIZE          = 4096\n"
        "BASE_ADDR          = 0x200000000000\n"
        "VIEW_SIZE          = 4294967296\n"
        "PORT_MIN           = 27600\n"
        "PORT_MAX           = 27699\n"
        "REDUNDANCY         = true\n", path);
    REQUIRE(pc != NULL);
    REQUIRE(config_load(&c, pc) == DSM_OK);
    CHECK(c.shared_hash != a.shared_hash);

    pc = write_fixture("shared_e.conf",
        SHARED_BASE
        "DISCOVERY_PORT      = 27651\n", path);
    REQUIRE(pc != NULL);
    REQUIRE(config_load(&c, pc) == DSM_OK);
    CHECK(c.shared_hash != a.shared_hash);

    return 0;
}

static int test_shared_hash_ignores_local(void)
{
    dsm_config_t a;
    dsm_config_t b;
    char path[256];

    const char *pa = write_fixture("shared_local_a.conf",
        SHARED_BASE
        "NODE_MEM            = 268435456\n"
        "BASE_ADDR_TSAN      = 0x4000000000\n"
        "OUTPUT_DIR          = reports\n"
        "DSM_LOG_LEVEL       = INFO\n"
        "WORKER_THREADS      = 4\n", path);
    REQUIRE(pa != NULL);
    REQUIRE(config_load(&a, pa) == DSM_OK);

    const char *pb = write_fixture("shared_local_b.conf",
        "# comentario distinto\n"
        "CLUSTER_ID = 0x534F3031\n"
        "PAGE_SIZE = 4096\n"
        "BASE_ADDR = 0x200000000000\n"
        "VIEW_SIZE = 4294967296\n"
        "PORT_MIN = 27600\n"
        "PORT_MAX = 27699\n"
        "DISCOVERY_GROUP = 239.255.42.1\n"
        "REDUNDANCY = false\n"
        "NODE_MEM = 1073741824\n"
        "BASE_ADDR_TSAN = 0x4000000000\n"
        "OUTPUT_DIR = /tmp/otro\n"
        "DSM_LOG_LEVEL = ERROR\n"
        "WORKER_THREADS = 1\n"
        "HB_INTERVAL_MS = 900\n"
        "REQUEST_TIMEOUT_MS = 8000\n", path);
    REQUIRE(pb != NULL);
    REQUIRE(config_load(&b, pb) == DSM_OK);

    CHECK(a.node_mem != b.node_mem);
    CHECK(strcmp(a.output_dir, b.output_dir) != 0);
    CHECK(a.worker_threads != b.worker_threads);
    CHECK(a.shared_hash == b.shared_hash);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 6. HOME_SLACK -> milesimas enteras
 * ------------------------------------------------------------------------- */

static int test_home_slack_conversion(void)
{
    struct { const char *literal; uint32_t expected; } ok_cases[] = {
        { "1.25",  1250 },
        { "1.0",   1000 },
        { "1",     1000 },
        { "1.2",   1200 },
        { "1.5",   1500 },
        { "2.0",   2000 },
        { "2.75",  2750 },
        { "1.001", 1001 },
        { "0.999", 0 },  /* marcador: debe rechazarse, ver abajo */
    };

    dsm_config_t cfg;
    char conf[512];

    /* Casos validos */
    for (size_t i = 0; i < sizeof(ok_cases)/sizeof(ok_cases[0]); i++) {
        if (ok_cases[i].expected == 0) continue; /* se prueba como rechazo abajo */

        snprintf(conf, sizeof(conf), "HOME_SLACK = %s\n",
                 ok_cases[i].literal);
        int rc = load_fixture("slack_ok.conf", conf, &cfg);
        CHECK(rc == DSM_OK);
        if (rc == DSM_OK) {
            CHECK(cfg.home_slack_permille == ok_cases[i].expected);
        }
    }

    /* Prefijo alternativo: HOME_SLACK con espacios internos alrededor del '=' */
    REQUIRE(load_fixture("slack_spaces.conf",
                         "HOME_SLACK   =    1.25\n", &cfg) == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);

    /* Decimales con ceros de relleno: 1.250 es exactamente 1250 */
    REQUIRE(load_fixture("slack_pad.conf", "HOME_SLACK = 1.250\n", &cfg)
            == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);
    REQUIRE(load_fixture("slack_pad2.conf", "HOME_SLACK = 1.2500\n", &cfg)
            == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);

    /* Mas de tres decimales SIGNIFICATIVOS: rechazo */
    CHECK(load_fixture("slack_4dec.conf", "HOME_SLACK = 1.2501\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_4dec_b.conf", "HOME_SLACK = 1.2345\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_4dec_c.conf", "HOME_SLACK = 1.0001\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Menor que 1.0: rechazo */
    CHECK(load_fixture("slack_below.conf", "HOME_SLACK = 0.999\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_zero.conf", "HOME_SLACK = 0\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_neg.conf", "HOME_SLACK = -1.25\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Sintaxis no soportada: exponente, coma decimal, signo, punto suelto */
    CHECK(load_fixture("slack_exp.conf", "HOME_SLACK = 1e0\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_exp2.conf", "HOME_SLACK = 1.25e2\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_comma.conf", "HOME_SLACK = 1,25\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_plus.conf", "HOME_SLACK = +1.25\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_dot.conf", "HOME_SLACK = 1.\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_dotlead.conf", "HOME_SLACK = .25\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_text.conf", "HOME_SLACK = mucho\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_empty.conf", "HOME_SLACK =\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Desbordamiento: entero gigantesco */
    CHECK(load_fixture("slack_overflow.conf",
                       "HOME_SLACK = 4294967.296\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_overflow2.conf",
                       "HOME_SLACK = 99999999999\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Conversion determinista: la misma cadena siempre da el mismo entero,
     * lo cual es el requisito del calculo de capacidad distribuido. */
    for (int i = 0; i < 5; i++) {
        REQUIRE(load_fixture("slack_det.conf", "HOME_SLACK = 1.333\n",
                             &cfg) == DSM_OK);
        CHECK(cfg.home_slack_permille == 1333);
    }

    return 0;
}

/* ----------------------------------------------------------------------------
 * 7. Validaciones cruzadas
 * ------------------------------------------------------------------------- */

static int test_cross_validation_rules(void)
{
    dsm_config_t cfg;

    /* Cada archivo parte de los defaults (validos) y aplica solo la clave
     * bajo prueba, de modo que cualquier fallo es atribuible a esa clave. */
#define EXPECT_OK(extra) \
    CHECK(load_fixture("cv_ok.conf", extra, &cfg) == DSM_OK)
#define EXPECT_BAD(name, extra) \
    CHECK(load_fixture(name, extra, &cfg) == DSM_ERR_CONFIG)

    /* PAGE_SIZE: potencia de dos y multiplo del tamano de pagina del SO */
    EXPECT_BAD("cv_page_zero.conf", "PAGE_SIZE = 0\n");
    EXPECT_BAD("cv_page_odd.conf", "PAGE_SIZE = 5000\n");
    EXPECT_BAD("cv_page_3k.conf", "PAGE_SIZE = 3000\n");
    EXPECT_OK("PAGE_SIZE = 8192\n");
    EXPECT_OK("PAGE_SIZE = 4096\n");

    /* VIEW_SIZE: multiplo de PAGE_SIZE y <= 64 GiB */
    EXPECT_BAD("cv_view_unaligned.conf", "VIEW_SIZE = 4294967297\n");
    EXPECT_BAD("cv_view_zero.conf", "VIEW_SIZE = 0\n");
    EXPECT_OK("VIEW_SIZE = 68719476736\n");                        /* 64 GiB exactos */
    EXPECT_BAD("cv_view_over.conf", "VIEW_SIZE = 137438953472\n");  /* 128 GiB */

    /* BASE_ADDR: alineado a 2 MiB */
    EXPECT_BAD("cv_base_unaligned.conf", "BASE_ADDR = 0x200000000001\n");
    EXPECT_BAD("cv_base_1mib.conf", "BASE_ADDR = 0x100000\n");
    EXPECT_OK("BASE_ADDR = 0x40000000\n");

    /* Heartbeats: suspect < dead estricto */
    EXPECT_BAD("cv_hb_equal.conf", "HB_SUSPECT = 6\nHB_DEAD = 6\n");
    EXPECT_BAD("cv_hb_inv.conf", "HB_SUSPECT = 7\nHB_DEAD = 6\n");
    EXPECT_OK("HB_SUSPECT = 2\nHB_DEAD = 5\n");

    /* Hilos en [1, 64] */
    EXPECT_BAD("cv_workers_zero.conf", "WORKER_THREADS = 0\n");
    EXPECT_BAD("cv_workers_65.conf", "WORKER_THREADS = 65\n");
    EXPECT_BAD("cv_disp_zero.conf", "DISPATCHER_THREADS = 0\n");
    EXPECT_BAD("cv_disp_65.conf", "DISPATCHER_THREADS = 65\n");
    EXPECT_OK("WORKER_THREADS = 64\nDISPATCHER_THREADS = 64\n");
    EXPECT_OK("WORKER_THREADS = 1\nDISPATCHER_THREADS = 1\n");

    /* PAGE_LOCK_STRIPES: potencia de dos >= 1 */
    EXPECT_BAD("cv_stripes_zero.conf", "PAGE_LOCK_STRIPES = 0\n");
    EXPECT_BAD("cv_stripes_3.conf", "PAGE_LOCK_STRIPES = 3\n");
    EXPECT_BAD("cv_stripes_100.conf", "PAGE_LOCK_STRIPES = 100\n");
    EXPECT_OK("PAGE_LOCK_STRIPES = 1\n");
    EXPECT_OK("PAGE_LOCK_STRIPES = 1024\n");

    /* NODE_MEM: multiplo de PAGE_SIZE y >= PAGE_SIZE */
    EXPECT_BAD("cv_mem_small.conf", "NODE_MEM = 2048\n");
    EXPECT_BAD("cv_mem_unaligned.conf", "NODE_MEM = 268435457\n");
    EXPECT_BAD("cv_mem_zero.conf", "NODE_MEM = 0\n");
    EXPECT_OK("NODE_MEM = 4096\n");

    /* MAX_RUNTIME_NODES <= MAX_NODES (256 compilado) */
    EXPECT_BAD("cv_nodes_over.conf", "MAX_RUNTIME_NODES = 257\n");
    EXPECT_OK("MAX_RUNTIME_NODES = 1\n");
    EXPECT_OK("MAX_RUNTIME_NODES = 256\n");

    /* REDUNDANCY solo admite booleanos */
    EXPECT_OK("REDUNDANCY = true\n");
    EXPECT_OK("REDUNDANCY = false\n");
    EXPECT_OK("REDUNDANCY = 1\n");
    EXPECT_OK("REDUNDANCY = 0\n");
    EXPECT_BAD("cv_red_texto.conf", "REDUNDANCY = paridad\n");

    /* REQUEST_RETRIES >= 1 y acotado por DSM_MAX_REQUEST_RETRIES */
    EXPECT_BAD("cv_retries_zero.conf", "REQUEST_RETRIES = 0\n");
    EXPECT_OK("REQUEST_RETRIES = 1\n");
    EXPECT_BAD("cv_retries_over.conf", "REQUEST_RETRIES = 11\n");
    EXPECT_OK("REQUEST_RETRIES = 10\n");

    /* El backoff exponencial exige min <= max */
    EXPECT_BAD("cv_backoff_inv.conf", "RECONNECT_MIN_MS = 3000\nRECONNECT_MAX_MS = 2000\n");
    EXPECT_OK("RECONNECT_MIN_MS = 500\nRECONNECT_MAX_MS = 500\n");

    /* HOME_SLACK < 1.0 mediante el entero ya convertido */
    EXPECT_BAD("cv_slack_small.conf", "HOME_SLACK = 0.5\n");

    /* DISCOVERY_GROUP debe ser multicast IPv4 y DISCOVERY_PORT > 0 */
    EXPECT_BAD("cv_disc_unicast.conf", "DISCOVERY_GROUP = 192.168.1.1\n");
    EXPECT_BAD("cv_disc_zero.conf", "DISCOVERY_GROUP = 0.0.0.0\n");
    EXPECT_BAD("cv_disc_broadcast.conf", "DISCOVERY_GROUP = 255.255.255.255\n");
    EXPECT_BAD("cv_disc_badip.conf", "DISCOVERY_GROUP = 239.255.x.1\n");
    EXPECT_BAD("cv_disc_port_zero.conf", "DISCOVERY_PORT = 0\n");
    EXPECT_OK("DISCOVERY_GROUP = 239.255.42.99\nDISCOVERY_PORT = 27699\n");
    CHECK(cfg.discovery_group == 0xEFFF2A63u);
    CHECK(cfg.discovery_port == 27699);

#undef EXPECT_OK
#undef EXPECT_BAD
    return 0;
}

/* ----------------------------------------------------------------------------
 * 8. Robustez: recarga sobre estructura sucia y entrada nula
 * ------------------------------------------------------------------------- */

static int test_reload_and_null_safety(void)
{
    dsm_config_t cfg;

    /* config_load(NULL) -> DSM_ERR_INVALID sin crash */
    CHECK(config_load(NULL, "dsm.conf") == DSM_ERR_INVALID);

    /* path NULL usa "dsm.conf" del cwd (el Makefile ejecuta desde la raiz) */
    if (access("dsm.conf", R_OK) == 0) {
        CHECK(config_load(&cfg, NULL) == DSM_OK);
        CHECK(cfg.page_size == 4096);
    }

    /* Ensuciar la estructura y recargar: todo debe quedar en defaults+archivo,
     * sin residuos de la carga anterior. */
    memset(&cfg, 0xA5, sizeof(cfg));
    cfg.shared_hash = 0xDEADBEEF;
    REQUIRE(config_load(&cfg, "dsm.conf") == DSM_OK);
    CHECK(cfg.discovery_group == 0xEFFF2A63u);
    dsm_config_t ref;
    REQUIRE(config_load(&ref, "dsm.conf") == DSM_OK);
    CHECK(cfg.shared_hash == ref.shared_hash);
    CHECK(cfg.page_size == 4096);

    /* config_free es idempotente y tolera NULL */
    config_free(&cfg);
    config_free(&cfg);
    config_free(NULL);

    /* config_validate(NULL) no debe usarse; en cambio validar una copia
     * valida recien cargada debe dar DSM_OK. */
    REQUIRE(config_load(&cfg, "dsm.conf") == DSM_OK);
    CHECK(config_validate(&cfg) == DSM_OK);

    /* Validacion directa de una estructura construida a mano con campo invalido */
    dsm_config_t bad = cfg;
    bad.page_size = 0;
    CHECK(config_validate(&bad) == DSM_ERR_CONFIG);
    bad = cfg;
    bad.view_size = 3;              /* no multiplo de 4096 */
    CHECK(config_validate(&bad) == DSM_ERR_CONFIG);
    bad = cfg;
    bad.port_min = 27699;
    bad.port_max = 27600;
    CHECK(config_validate(&bad) == DSM_ERR_CONFIG);
    bad = cfg;
    bad.hb_suspect = bad.hb_dead;
    CHECK(config_validate(&bad) == DSM_ERR_CONFIG);
    bad = cfg;
    bad.redundancy = true;
    CHECK(config_validate(&bad) == DSM_OK);
    bad.redundancy = false;
    CHECK(config_validate(&bad) == DSM_OK);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 9. Estres: muchos archivos y valores aleatorios deterministas
 * ------------------------------------------------------------------------- */

static int test_stress_many_configs(void)
{
    dsm_config_t cfg;
    char conf[256];
    char name[64];
    unsigned ok = 0, bad = 0;

    /* Barrido determinista de PAGE_SIZE alrededor de valores validos e
     * invalidos; el parser nunca debe caer en comportamiento indefinido. */
    for (uint32_t ps = 1; ps <= 65536; ps <<= 1) {
        snprintf(conf, sizeof(conf),
                 "PAGE_SIZE = %u\n"
                 "VIEW_SIZE = 4294967296\n"
                 "NODE_MEM = 268435456\n"
                 "", ps);
        snprintf(name, sizeof(name), "stress_ps_%u.conf", ps);
        int rc = load_fixture(name, conf, &cfg);
        CHECK(rc == DSM_OK || rc == DSM_ERR_CONFIG);
        if (rc == DSM_OK) {
            ok++;
            CHECK(cfg.page_size == ps);
            CHECK((ps & (ps - 1)) == 0);
        } else {
            bad++;
        }
    }
    CHECK(ok > 0);
    CHECK(ok + bad == 17);      /* 1,2,4,...,65536 */

    /* Barrido de HOME_SLACK en pasos de 1/1000 con digitos controlados */
    for (uint32_t millis = 900; millis <= 2100; millis += 37) {
        snprintf(conf, sizeof(conf), "HOME_SLACK = %u.%03u\n",
                 millis / 1000, millis % 1000);
        snprintf(name, sizeof(name), "stress_slack_%u.conf", millis);
        int rc = load_fixture(name, conf, &cfg);
        if (millis < 1000) {
            CHECK(rc == DSM_ERR_CONFIG);
        } else {
            CHECK(rc == DSM_OK);
            CHECK(cfg.home_slack_permille == millis);
        }
    }

    /* 200 archivos con distinto CLUSTER_ID: todos hashes distintos */
    {
        uint32_t hashes[200];
        for (int i = 0; i < 200; i++) {
            static char body[512];
            size_t blen = (size_t)snprintf(body, sizeof(body),
                                           "CLUSTER_ID = %d\n"
                                           "PAGE_SIZE = 4096\n"
                                           "", i);
            char path[256];
            const char *p = write_fixture_raw("stress_hash.conf", body, blen, path);
            REQUIRE(p != NULL);
            REQUIRE(config_load(&cfg, p) == DSM_OK);
            hashes[i] = cfg.shared_hash;
        }
        for (int i = 0; i < 200; i++) {
            for (int j = i + 1; j < 200; j++) {
                if (hashes[i] == hashes[j]) {
                    printf("   colision de CRC32 entre fixtures %d y %d\n", i, j);
                    g_fails++;
                }
            }
        }
    }

    return 0;
}

/* ----------------------------------------------------------------------------
 * 10. shared_hash: estabilidad y separacion de claves compartidas
 * ------------------------------------------------------------------------- */

static int test_rig_shared_hash_integridad(void)
{
    dsm_config_t a;
    dsm_config_t b;
    char path[256];

    const char *pa = write_fixture("rig_shared_a.conf",
        SHARED_BASE
        "NODE_MEM = 268435456\n", path);
    REQUIRE(pa != NULL);
    REQUIRE(config_load(&a, pa) == DSM_OK);

    /* Dos lecturas consecutivas producen el mismo hash. */
    REQUIRE(config_load(&b, pa) == DSM_OK);
    CHECK(b.shared_hash == a.shared_hash);

    /* Un comentario distinto o un cambio de NODE_MEM no altera el hash. */
    const char *pb = write_fixture("rig_shared_b.conf",
        "# otra firma\n"
        "CLUSTER_ID = 0x534F3031\n"
        "PAGE_SIZE = 4096\n"
        "BASE_ADDR = 0x200000000000\n"
        "VIEW_SIZE = 4294967296\n"
        "PORT_MIN = 27600\n"
        "PORT_MAX = 27699\n"
        "REDUNDANCY = false\n"
        "NODE_MEM = 134217728\n", path);
    REQUIRE(pb != NULL);
    REQUIRE(config_load(&b, pb) == DSM_OK);
    CHECK(b.shared_hash == a.shared_hash);

    /* Un cambio en HOME_SLACK si altera el hash. */
    const char *pc = write_fixture("rig_shared_c.conf",
        "CLUSTER_ID = 0x534F3031\n"
        "PAGE_SIZE = 4096\n"
        "BASE_ADDR = 0x200000000000\n"
        "VIEW_SIZE = 4294967296\n"
        "PORT_MIN = 27600\n"
        "PORT_MAX = 27699\n"
        "REDUNDANCY = false\n"
        "HOME_SLACK = 1.50\n", path);
    REQUIRE(pc != NULL);
    REQUIRE(config_load(&b, pc) == DSM_OK);
    CHECK(b.shared_hash != a.shared_hash);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 11. Limites numericos y restricciones de sistema, clave por clave
 * ------------------------------------------------------------------------- */

static int test_rig_limites_sistema(void)
{
    dsm_config_t cfg;
    char body[256];

    long sc = sysconf(_SC_PAGESIZE);
    if (sc <= 0) sc = 4096;

#define RIG_BAD(name, text) \
    CHECK(load_fixture(name, text, &cfg) == DSM_ERR_CONFIG)
#define RIG_OK(name, text) \
    CHECK(load_fixture(name, text, &cfg) == DSM_OK)

    /* --- Puertos: rango invertido, fuera de u16 y fuera del bloque --- */
    RIG_BAD("rig_port_inv.conf", "PORT_MIN = 27690\nPORT_MAX = 27610\n");
    RIG_BAD("rig_port_70000_max.conf", "PORT_MIN = 27600\nPORT_MAX = 70000\n");
    RIG_BAD("rig_port_70000_min.conf", "PORT_MIN = 70000\nPORT_MAX = 27699\n");
    RIG_BAD("rig_port_u16_gap.conf", "PORT_MIN = 27600\nPORT_MAX = 65535\n");
    RIG_OK("rig_port_full.conf", "PORT_MIN = 27600\nPORT_MAX = 27699\n");
    RIG_OK("rig_port_size1.conf", "PORT_MIN = 27699\nPORT_MAX = 27699\n");

    /* --- PAGE_SIZE: potencia de dos Y multiplo de _SC_PAGESIZE --- */
    RIG_BAD("rig_page_4097.conf", "PAGE_SIZE = 4097\n");
    snprintf(body, sizeof(body), "PAGE_SIZE = %ld\n", sc * 3);   /* 3*sc: no potencia de 2 */
    RIG_BAD("rig_page_sc3.conf", body);
    snprintf(body, sizeof(body), "PAGE_SIZE = %ld\n", sc / 2);   /* menor que la pagina del SO */
    RIG_BAD("rig_page_half.conf", body);
    snprintf(body, sizeof(body), "PAGE_SIZE = %ld\n", sc * 2);
    RIG_OK("rig_page_double.conf", body);
    /* 12288 = 3 * 4096: multiplo del tamano de pagina del SO pero NO potencia
     * de dos. Con VIEW_SIZE y NODE_MEM coherentes, el unico rechazo posible es
     * la regla de potencia de dos. */
    RIG_BAD("rig_page_12288_coherente.conf",
            "PAGE_SIZE = 12288\n"
            "VIEW_SIZE = 12582912\n"          /* 12288 * 1024 */
            "NODE_MEM = 122880\n");           /* 12288 * 10 */
    RIG_BAD("rig_page_12288_defaults.conf", "PAGE_SIZE = 12288\n");
    /* VIEW_SIZE debe ser multiplo del PAGE_SIZE elegido */
    RIG_BAD("rig_page8192_view4096.conf", "PAGE_SIZE = 8192\nVIEW_SIZE = 4096\n");

    /* --- VIEW_SIZE: multiplo de page_size y cota 64 GiB --- */
    RIG_BAD("rig_view_4097.conf", "VIEW_SIZE = 4097\n");
    RIG_OK("rig_view_64gib.conf", "VIEW_SIZE = 68719476736\n");
    snprintf(body, sizeof(body), "VIEW_SIZE = %llu\n", (64ULL << 30) + 4096ULL);
    RIG_BAD("rig_view_64gib_plus.conf", body);
    RIG_BAD("rig_view_zero2.conf", "VIEW_SIZE = 0\n");

    /* --- BASE_ADDR: alineacion estricta a 2 MiB --- */
    RIG_BAD("rig_base_plus1.conf", "BASE_ADDR = 0x200000000001\n");
    RIG_BAD("rig_base_plus4k.conf", "BASE_ADDR = 0x200000001000\n");
    RIG_BAD("rig_base_2mib_minus1.conf", "BASE_ADDR = 0x1FFFFF\n");
    RIG_OK("rig_base_2mib.conf", "BASE_ADDR = 0x200000\n");
    RIG_OK("rig_base_max_aligned.conf", "BASE_ADDR = 0xFFFFFFFEFFE00000\n");
    /* El mayor BASE_ADDR alineado desborda al sumarle VIEW_SIZE. */
    RIG_BAD("rig_base_view_overflow.conf", "BASE_ADDR = 0xFFFFFFFFFFE00000\n");
    /* BASE_ADDR_TSAN tambien debe estar alineado a 2 MiB. */
    RIG_BAD("rig_base_tsan_plus1.conf", "BASE_ADDR_TSAN = 0x4000000001\n");
    RIG_OK("rig_base_tsan_aligned.conf", "BASE_ADDR_TSAN = 0x40000000\n");
    RIG_OK("rig_base_tsan_zero.conf", "BASE_ADDR_TSAN = 0\n");   /* desactivado */

    /* --- NODE_MEM: multiplo de page_size y >= page_size --- */
    RIG_BAD("rig_mem_4097.conf", "NODE_MEM = 4097\n");
    RIG_BAD("rig_mem_1.conf", "NODE_MEM = 1\n");
    RIG_BAD("rig_mem_below_page.conf", "NODE_MEM = 2048\n");
    RIG_OK("rig_mem_one_page.conf", "NODE_MEM = 4096\n");
    RIG_OK("rig_mem_3pages.conf", "NODE_MEM = 12288\n");   /* no hace falta potencia de 2 */

    /* --- HOME_SLACK: >= 1.0 y como maximo 3 decimales significativos --- */
    RIG_BAD("rig_slack_095.conf", "HOME_SLACK = 0.95\n");
    RIG_BAD("rig_slack_12505.conf", "HOME_SLACK = 1.2505\n");
    RIG_BAD("rig_slack_10005.conf", "HOME_SLACK = 1.0005\n");
    RIG_OK("rig_slack_125.conf", "HOME_SLACK = 1.25\n");
    CHECK(cfg.home_slack_permille == 1250);      /* 1.25 -> 1250 exacto */
    RIG_OK("rig_slack_1000.conf", "HOME_SLACK = 1.000\n");
    CHECK(cfg.home_slack_permille == 1000);

    /* --- Hilos worker/dispatcher en [1, 64] --- */
    RIG_BAD("rig_workers_0.conf", "WORKER_THREADS = 0\n");
    RIG_BAD("rig_workers_65.conf", "WORKER_THREADS = 65\n");
    RIG_BAD("rig_disp_0.conf", "DISPATCHER_THREADS = 0\n");
    RIG_BAD("rig_disp_65.conf", "DISPATCHER_THREADS = 65\n");
    RIG_OK("rig_threads_min.conf", "WORKER_THREADS = 1\nDISPATCHER_THREADS = 1\n");
    RIG_OK("rig_threads_max.conf", "WORKER_THREADS = 64\nDISPATCHER_THREADS = 64\n");

    /* --- PAGE_LOCK_STRIPES: potencia de dos --- */
    RIG_BAD("rig_stripes_255.conf", "PAGE_LOCK_STRIPES = 255\n");
    RIG_BAD("rig_stripes_4095.conf", "PAGE_LOCK_STRIPES = 4095\n");
    RIG_BAD("rig_stripes_0.conf", "PAGE_LOCK_STRIPES = 0\n");
    RIG_OK("rig_stripes_1.conf", "PAGE_LOCK_STRIPES = 1\n");
    RIG_OK("rig_stripes_4096.conf", "PAGE_LOCK_STRIPES = 4096\n");

    /* --- Heartbeats: suspect < dead estricto (con enteros crudos) --- */
    RIG_BAD("rig_hb_eq.conf", "HB_SUSPECT = 6\nHB_DEAD = 6\n");
    RIG_BAD("rig_hb_gt.conf", "HB_SUSPECT = 8\nHB_DEAD = 7\n");
    RIG_OK("rig_hb_ok.conf", "HB_SUSPECT = 0\nHB_DEAD = 1\n");
    /* Un intervalo de 0 divide en membership_evaluate(): no es configurable. */
    RIG_BAD("rig_hb_interval_zero.conf", "HB_INTERVAL_MS = 0\n");
    RIG_OK("rig_hb_interval_1.conf", "HB_INTERVAL_MS = 1\n");

    /* --- MAX_RUNTIME_NODES: al menos un slot utilizable --- */
    RIG_BAD("rig_nodes_zero.conf", "MAX_RUNTIME_NODES = 0\n");
    RIG_OK("rig_nodes_one.conf", "MAX_RUNTIME_NODES = 1\n");
    RIG_BAD("rig_nodes_over.conf", "MAX_RUNTIME_NODES = 257\n");

    /* --- REQUEST_RETRIES >= 1 --- */
    RIG_BAD("rig_retries_0.conf", "REQUEST_RETRIES = 0\n");

    /* --- Duplicados y claves desconocidas --- */
    RIG_BAD("rig_dup_slack.conf",
            "HOME_SLACK = 1.25\nHOME_SLACK = 1.30\n");
    RIG_BAD("rig_unknown_port.conf", "PORT_M1N = 27600\n");
    RIG_BAD("rig_unknown_foo.conf", "FOO_BAR = 1\n");
    RIG_BAD("rig_unknown_prefix.conf",
            "PAGE_SIZE_EXTRA = 4096\n");

#undef RIG_BAD
#undef RIG_OK
    return 0;
}

/* ----------------------------------------------------------------------------
 * 12. Archivos de 1 MiB exacto, 1 MiB + 1 y rutas no regulares
 * ------------------------------------------------------------------------- */

static int test_rig_archivo_enorme_y_no_regular(void)
{
    dsm_config_t cfg;
    char path[256];
    const size_t exact = 1024u * 1024u;

    /* 1 MiB exacto de comentarios: el limite superior se acepta (los
     * defaults son validos y el contenido no declara claves). */
    uint8_t *buf = malloc(exact);
    REQUIRE(buf != NULL);
    memset(buf, '#', exact);

    const char *p = write_fixture_raw("rig_1mib.conf", buf, exact, path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.page_size == 4096);

    /* Un byte mas supera el limite: rechazo por tamano, antes de parsear. */
    uint8_t *big = malloc(exact + 1);
    REQUIRE(big != NULL);
    memset(big, '#', exact + 1);
    p = write_fixture_raw("rig_1mib_plus1.conf", big, exact + 1, path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);

    /* Ruta inexistente. */
    CHECK(config_load(&cfg, FIXTURE_DIR "/rig_no_existe.conf") == DSM_ERR_CONFIG);

    /* Directorio: fstat dice que no es un archivo regular. */
    CHECK(config_load(&cfg, FIXTURE_DIR) == DSM_ERR_CONFIG);

    /* Dispositivo de caracteres: tampoco es un dsm.conf valido. */
    CHECK(config_load(&cfg, "/dev/null") == DSM_ERR_CONFIG);

    free(buf);
    free(big);
    return 0;
}

/* ----------------------------------------------------------------------------
 * 13. config_validate() directa: una mutacion por campo y cotas aceptadas
 * ------------------------------------------------------------------------- */

static int test_rig_validacion_directa(void)
{
    dsm_config_t base;
    REQUIRE(config_load(&base, "dsm.conf") == DSM_OK);

#define RIG_MUT(field, value) do {                  \
        dsm_config_t m = base;                      \
        m.field = (value);                          \
        CHECK(config_validate(&m) == DSM_ERR_CONFIG); \
    } while (0)
#define RIG_MUT_OK(field, value) do {               \
        dsm_config_t m = base;                      \
        m.field = (value);                          \
        CHECK(config_validate(&m) == DSM_OK);       \
    } while (0)

    /* Puertos: invertidos y fuera del bloque asignado. */
    RIG_MUT(port_min, (uint16_t)27700);
    RIG_MUT(port_max, (uint16_t)27700);
    RIG_MUT(port_min, (uint16_t)0);
    RIG_MUT(port_max, (uint16_t)0);

    /* Paginas y vista. */
    RIG_MUT(page_size, 4097u);
    RIG_MUT(page_size, 12288u);                    /* multiplo de 4096, no potencia de 2 */
    RIG_MUT(view_size, 4097ULL);
    RIG_MUT(view_size, 0ULL);
    RIG_MUT(view_size, (64ULL << 30) + 4096ULL);
    RIG_MUT_OK(view_size, 64ULL << 30);

    /* Direccion base. */
    RIG_MUT(base_addr, base.base_addr + 1ULL);
    RIG_MUT(base_addr, base.base_addr + 4096ULL);
    RIG_MUT(base_addr, 0x1FFFFFULL);
    RIG_MUT_OK(base_addr, 0x40000000ULL);
    RIG_MUT_OK(base_addr, 0xFFFFFFFEFFE00000ULL);   /* ultimo alineado con vista de 4 GiB */
    RIG_MUT(base_addr, 0xFFFFFFFFFFE00000ULL);      /* alineado pero BASE+VIEW desborda */
    RIG_MUT(base_addr_tsan, 0x40000000ULL + 1ULL);
    RIG_MUT_OK(base_addr_tsan, 0ULL);

    /* Memoria local. */
    RIG_MUT(node_mem, 4097ULL);
    RIG_MUT(node_mem, 0ULL);
    RIG_MUT(node_mem, 2048ULL);                    /* menor que page_size */
    RIG_MUT_OK(node_mem, 4096ULL);
    RIG_MUT_OK(node_mem, 12288ULL);

    /* Hilos y stripes. */
    RIG_MUT(worker_threads, 0u);
    RIG_MUT(worker_threads, 65u);
    RIG_MUT(dispatcher_threads, 0u);
    RIG_MUT(dispatcher_threads, 65u);
    RIG_MUT_OK(worker_threads, 1u);
    RIG_MUT_OK(dispatcher_threads, 64u);
    RIG_MUT(page_lock_stripes, 255u);
    RIG_MUT(page_lock_stripes, 0u);
    RIG_MUT_OK(page_lock_stripes, 1u);
    RIG_MUT_OK(page_lock_stripes, 4096u);

    /* Heartbeats. */
    RIG_MUT(hb_suspect, base.hb_dead);
    RIG_MUT(hb_suspect, base.hb_dead + 1u);
    RIG_MUT(hb_dead, 0u);
    RIG_MUT_OK(hb_suspect, base.hb_dead - 1u);
    RIG_MUT(hb_interval_ms, 0u);

    /* Resto de reglas cruzadas. */
    RIG_MUT(home_slack_permille, 999u);
    RIG_MUT(request_retries, 0u);
    RIG_MUT(request_retries, DSM_MAX_REQUEST_RETRIES + 1u);
    RIG_MUT_OK(request_retries, DSM_MAX_REQUEST_RETRIES);
    RIG_MUT(reconnect_min_ms, base.reconnect_max_ms + 1u);
    RIG_MUT_OK(reconnect_min_ms, base.reconnect_max_ms);
    RIG_MUT(max_runtime_nodes, (uint32_t)MAX_NODES + 1u);
    RIG_MUT_OK(redundancy, true);

    /* Los defaults del archivo canonico son validos y no se mutan al validar. */
    dsm_config_t copia = base;
    CHECK(config_validate(&base) == DSM_OK);
    CHECK(memcmp(&base, &copia, sizeof(base)) == 0);

#undef RIG_MUT
#undef RIG_MUT_OK
    return 0;
}

/* ----------------------------------------------------------------------------
 * 14. shared_hash solo existe para configuraciones validas
 *
 * Un archivo que no se puede parsear no publica un hash parcial: el campo
 * queda en cero y el error de configuracion es el unico resultado.
 * ------------------------------------------------------------------------- */

static int test_rig_hash_sobrevive_al_rechazo(void)
{
    dsm_config_t cfg;
    char path[256];

    /* Un parseo rechazado no publica un hash parcial. */
    const char *bad_unknown = "CLAVE_INVENTADA = 7\n";
    const char *p = write_fixture("hash_reject_unknown.conf", bad_unknown, path);
    REQUIRE(p != NULL);
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);
    CHECK(cfg.shared_hash == 0);

    const char *bad_syntax = "PORT_MIN 27600\n";
    p = write_fixture("hash_reject_syntax.conf", bad_syntax, path);
    REQUIRE(p != NULL);
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);
    CHECK(cfg.shared_hash == 0);

    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, FIXTURE_DIR "/hash_reject_missing.conf") == DSM_ERR_CONFIG);
    CHECK(cfg.shared_hash == 0);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 15. REQUEST_RETRIES gobierna reintentos reales de dsm_net_rpc()
 *
 * El par de prueba acepta conexiones en el bloque del grupo. La primera
 * peticion se cierra sin respuesta (fallo de transporte) y la segunda se
 * responde con MSG_NACK, de modo que el exito solo es posible si el emisor
 * reenvio la solicitud. El listener real del nodo completa la peticion
 * pendiente cuando la respuesta llega por TCP.
 * ------------------------------------------------------------------------- */

typedef struct {
    int      listen_fd;
    int      nconn;         /* conexiones que atiende antes de salir */
    bool     reply_last;    /* responde la ultima conexion */
    uint16_t reply_port;    /* puerto de escucha del nodo emisor */
    uint16_t reply_status;
    uint32_t seen_req[4];
    int      nseen;
} rpc_peer_t;

/* Acepta una conexion con plazo, para que un cliente que no reintenta no
 * deje el hilo del par bloqueado. */
static int peer_accept(int listen_fd)
{
    struct pollfd pfd = { .fd = listen_fd, .events = POLLIN, .revents = 0 };
    int rc;
    do {
        rc = poll(&pfd, 1, 5000);
    } while (rc < 0 && errno == EINTR);
    if (rc <= 0) return -1;
    return accept(listen_fd, NULL, NULL);
}

/* Lee cabecera y carga de una trama con la misma ruta que usa el daemon. */
static int peer_read_msg(int fd, msg_header_t *hdr)
{
    uint8_t raw[DSM_HDR_LEN];
    if (dsm_read_full_deadline(fd, raw, DSM_HDR_LEN, 2000) != DSM_OK) return -1;
    if (dsm_wire_unpack(raw, hdr) != DSM_OK) return -1;

    if (hdr->payload_len > 0) {
        uint8_t *payload = malloc(hdr->payload_len);
        if (!payload) return -1;
        int rc = dsm_read_full_deadline(fd, payload, hdr->payload_len, 2000);
        free(payload);
        if (rc != DSM_OK) return -1;
    }
    return 0;
}

static void *rpc_peer_main(void *arg)
{
    rpc_peer_t *p = (rpc_peer_t *)arg;

    for (int i = 0; i < p->nconn; i++) {
        int cfd = peer_accept(p->listen_fd);
        if (cfd < 0) break;

        msg_header_t hdr;
        if (peer_read_msg(cfd, &hdr) != 0) {
            close(cfd);
            continue;
        }
        if (p->nseen < 4) {
            p->seen_req[p->nseen++] = hdr.req_id;
        }

        if (!p->reply_last || i != p->nconn - 1) {
            close(cfd);
            continue;
        }

        /* El protocolo responde por una conexion nueva hacia el puerto de
         * escucha del solicitante, no por el socket de la peticion. */
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(p->reply_port);

        int out_fd = dsm_connect_timeout(&addr, 500);
        if (out_fd >= 0) {
            msg_header_t resp;
            memset(&resp, 0, sizeof(resp));
            resp.magic         = DSM_MAGIC;
            resp.cluster_id    = hdr.cluster_id;
            resp.proto_version = DSM_PROTO_VERSION;
            resp.type          = MSG_NACK;
            resp.src_slot      = hdr.dst_slot;
            resp.dst_slot      = hdr.src_slot;
            resp.req_id        = hdr.req_id;
            resp.status        = p->reply_status;
            resp.epoch         = hdr.epoch;

            uint8_t raw[DSM_HDR_LEN];
            dsm_wire_finish(&resp, raw, NULL, 0);
            dsm_send_full(out_fd, raw, DSM_HDR_LEN);
            shutdown(out_fd, SHUT_RDWR);
            close(out_fd);
        }
        close(cfd);
    }
    return NULL;
}

/* Reserva un puerto libre del bloque asignado y lo deja escuchando. */
static int peer_listen(uint16_t *port_out)
{
    for (uint16_t port = DSM_PORT_BLOCK_MIN; port <= DSM_PORT_BLOCK_MAX; port++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port);

        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
            listen(fd, 8) == 0) {
            *port_out = port;
            return fd;
        }
        close(fd);
    }
    return -1;
}

/* Prepara un contexto de prueba con un par alcanzable en 'peer_port'. */
static dsm_ctx_t *rpc_ctx_create(uint16_t peer_port, uint32_t retries,
                                 uint32_t request_timeout_ms)
{
    dsm_ctx_t *ctx = dsm_ctx_create_test();
    if (!ctx) return NULL;

    ctx->cfg.cluster_id         = 0x534F3031u;
    ctx->cfg.page_size          = 4096;
    ctx->cfg.request_timeout_ms = request_timeout_ms;
    ctx->cfg.request_retries    = retries;
    ctx->cfg.reconnect_min_ms   = 10;
    ctx->cfg.reconnect_max_ms   = 40;
    ctx->cfg.connect_timeout_ms = 200;
    ctx->cfg.send_timeout_ms    = 500;
    ctx->memb.self_slot         = 0;

    uint16_t listen_port = 0;
    ctx->net.listen_fd = peer_listen(&listen_port);
    if (ctx->net.listen_fd < 0) {
        net_destroy(ctx);
        sync_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return NULL;
    }
    ctx->net.listen_port = listen_port;

    pthread_rwlock_wrlock(&ctx->memb.rw);
    node_info_t *ni = &ctx->memb.nodes[1];
    memset(ni, 0, sizeof(*ni));
    ni->slot  = 1;
    ni->ip    = htonl(INADDR_LOOPBACK);
    ni->port  = peer_port;
    ni->state = DSM_NODE_ALIVE;
    ctx->memb.present[1 / 8] |= (uint8_t)(1u << (1 % 8));
    pthread_rwlock_unlock(&ctx->memb.rw);

    atomic_store(&ctx->running, true);
    if (net_start_listener(ctx) != DSM_OK) {
        close(ctx->net.listen_fd);
        ctx->net.listen_fd = -1;
        net_destroy(ctx);
        sync_destroy(ctx);
        membership_destroy(ctx);
        free(ctx);
        return NULL;
    }
    return ctx;
}

static void rpc_ctx_destroy(dsm_ctx_t *ctx)
{
    atomic_store(&ctx->running, false);
    net_stop_listener(ctx);
    net_destroy(ctx);
    sync_destroy(ctx);
    membership_destroy(ctx);
    ring_free(&ctx->ring);
    free(ctx);
}

static int test_request_retries_reintenta(void)
{
    uint16_t peer_port = 0;
    int peer_fd = peer_listen(&peer_port);
    REQUIRE(peer_fd >= 0);

    rpc_peer_t peer;
    memset(&peer, 0, sizeof(peer));
    peer.listen_fd   = peer_fd;
    peer.nconn       = 2;        /* primera sin respuesta, segunda con NACK */
    peer.reply_last  = true;
    peer.reply_status = DSM_ST_OK;

    dsm_ctx_t *ctx = rpc_ctx_create(peer_port, 2, 200);
    REQUIRE(ctx != NULL);
    peer.reply_port = ctx->net.listen_port;

    pthread_t tid;
    REQUIRE(pthread_create(&tid, NULL, rpc_peer_main, &peer) == 0);

    uint16_t status = 0xFFFF;
    uint32_t rx_len = 0;
    uint64_t t0 = mono_ms();
    int rc = dsm_net_rpc(ctx, 1, MSG_PAGE_REQUEST, 0, 0, 0, 0,
                         NULL, 0, &status, NULL, &rx_len);
    uint64_t elapsed = mono_ms() - t0;

    pthread_join(tid, NULL);

    CHECK(rc == DSM_OK);
    CHECK(status == DSM_ST_OK);
    CHECK(peer.nseen == 2);
    /* El reintento espera al menos RECONNECT_MIN_MS antes de reenviar. */
    CHECK(elapsed >= ctx->cfg.reconnect_min_ms);
    if (peer.nseen == 2) {
        /* Cada reintento usa un req_id nuevo: el pendiente anterior ya fue
         * retirado y reutilizar su identificador confundiria la correlacion. */
        CHECK(peer.seen_req[0] != peer.seen_req[1]);
    }

    rpc_ctx_destroy(ctx);
    close(peer_fd);
    return 0;
}

static int test_request_retries_un_solo_intento(void)
{
    uint16_t peer_port = 0;
    int peer_fd = peer_listen(&peer_port);
    REQUIRE(peer_fd >= 0);

    rpc_peer_t peer;
    memset(&peer, 0, sizeof(peer));
    peer.listen_fd  = peer_fd;
    peer.nconn      = 1;         /* solo una conexion: REQUEST_RETRIES = 1 */
    peer.reply_last = false;

    dsm_ctx_t *ctx = rpc_ctx_create(peer_port, 1, 200);
    REQUIRE(ctx != NULL);

    pthread_t tid;
    REQUIRE(pthread_create(&tid, NULL, rpc_peer_main, &peer) == 0);

    uint16_t status = 0xFFFF;
    uint32_t rx_len = 0;
    int rc = dsm_net_rpc(ctx, 1, MSG_PAGE_REQUEST, 0, 0, 0, 0,
                         NULL, 0, &status, NULL, &rx_len);

    pthread_join(tid, NULL);

    CHECK(rc == DSM_ERR_TIMEOUT);
    CHECK(peer.nseen == 1);

    rpc_ctx_destroy(ctx);
    close(peer_fd);
    return 0;
}

/* ------------------------------------------------------------------------- */

int main(void)
{
    printf("=== Test Suite: Config ===\n");
    fixture_setup();

    RUN_TEST(test_load_repo_config);
    RUN_TEST(test_malformed_syntax_variants);
    RUN_TEST(test_malformed_edge_inputs);
    RUN_TEST(test_duplicate_key_detection);
    RUN_TEST(test_numeric_overflow);
    RUN_TEST(test_port_block_rule);
    RUN_TEST(test_shared_hash_subset);
    RUN_TEST(test_shared_hash_ignores_local);
    RUN_TEST(test_home_slack_conversion);
    RUN_TEST(test_cross_validation_rules);
    RUN_TEST(test_reload_and_null_safety);
    RUN_TEST(test_stress_many_configs);
    RUN_TEST(test_rig_shared_hash_integridad);
    RUN_TEST(test_rig_limites_sistema);
    RUN_TEST(test_rig_archivo_enorme_y_no_regular);
    RUN_TEST(test_rig_validacion_directa);
    RUN_TEST(test_rig_hash_sobrevive_al_rechazo);
    RUN_TEST(test_request_retries_reintenta);
    RUN_TEST(test_request_retries_un_solo_intento);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
