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
 *   5. raw_hash = CRC32 completo del archivo, con vectores conocidos y
 *      deteccion de diferencias de un solo byte en cualquier posicion.
 *   6. HOME_SLACK: conversion exacta a milesimas enteras y rechazo de valores
 *      con mas de tres decimales significativos o no representables.
 *   7. Validaciones cruzadas (page_size, view_size, alineacion, heartbeats,
 *      hilos, stripes, node_mem, RAID5, retries).
 *   8. Duplicados, recarga sobre estructura sucia, y ausencia de fugas.
 *   9. Rigor adicional: integridad de raw_hash byte a byte (un byte cambiado,
 *      un espacio final, dos lecturas identicas), limites de sistema uno por
 *      uno con VIEW_SIZE/NODE_MEM coherentes, archivo de 1 MiB exacto y
 *      1 MiB + 1, rutas no regulares, y config_validate() mutada campo a campo.
 * ============================================================================== */

#include "test.h"
#include "config.h"
#include "wire.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
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
    "PEER_HOSTS         = 127.0.0.1,lab-h01\n"

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
    CHECK(cfg.chunk_pages == 256);
    CHECK(cfg.redundancy_raid5 == false);
    CHECK(cfg.raid_k == 2);

    /* Red */
    CHECK(cfg.port_min == 27600);
    CHECK(cfg.port_max == 27699);
    CHECK(cfg.peer_host_count == 5);
    CHECK(strcmp(cfg.peer_hosts[0], "127.0.0.1") == 0);
    CHECK(strcmp(cfg.peer_hosts[1], "lab-h01") == 0);
    CHECK(strcmp(cfg.peer_hosts[4], "lab-h04") == 0);

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
    CHECK(cfg.wait_pool_entries == 4096);
    CHECK(cfg.lock_timeout_ms == 10000);
    CHECK(cfg.worker_queue_cap == 4096);
    CHECK(cfg.sync_threads == 4);                 /* clave que antes no existia */

    /* Diagnostico */
    CHECK(strcmp(cfg.output_dir, "./reportes") == 0);
    CHECK(strcmp(cfg.log_level, "INFO") == 0);

    /* raw_hash == CRC32 exacto del archivo, no un valor cualquiera */
    FILE *f = fopen("dsm.conf", "rb");
    REQUIRE(f != NULL);
    static uint8_t buf[1 << 20];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    CHECK(n == 1933);
    CHECK(cfg.raw_hash == dsm_crc32(0, buf, n));
    CHECK(cfg.raw_hash == 0xBB8AFAE5u);

    /* Dos lecturas identicas del mismo archivo producen el mismo hash exacto.
     * Esto es la base del handshake HELLO: si dos nodos leen el mismo byte
     * stream deben coincidir bit a bit, no solo en los campos parseados. */
    dsm_config_t again;
    REQUIRE(config_load(&again, "dsm.conf") == DSM_OK);
    CHECK(again.raw_hash == cfg.raw_hash);
    CHECK(memcmp(&again.peer_hosts, &cfg.peer_hosts, sizeof(cfg.peer_hosts)) == 0);
    CHECK(again.peer_host_count == cfg.peer_host_count);

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

    /* Archivo vacio: todos los defaults, debe validar y hashear el vacio */
    const char *p = write_fixture("empty.conf", "", path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.raw_hash == 0x00000000u);          /* CRC32 de cero bytes */
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
                       "PORT_MIN = 65535\nPORT_MAX = 65535\nPEER_HOSTS = a\n", &cfg)
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
    CHECK(load_fixture("octal.conf", "CHUNK_PAGES = 010\n", &cfg) == DSM_OK);
    CHECK(cfg.chunk_pages == 8);                  /* 010 octal == 8 */
    CHECK(load_fixture("hex_over.conf", "CLUSTER_ID = 0x1FFFFFFFF\n", &cfg) == DSM_ERR_CONFIG);

    /* +N explicito: strtoull lo acepta; debe seguir siendo un valor valido */
    CHECK(load_fixture("plus.conf", "CHUNK_PAGES = +512\n", &cfg) == DSM_OK);
    CHECK(cfg.chunk_pages == 512);

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
    const char *p = write_fixture("ports_default.conf", "PEER_HOSTS = a\n", path);
    REQUIRE(p != NULL);
    REQUIRE(config_load(&defaults, p) == DSM_OK);
    CHECK(defaults.port_min == 27600);
    CHECK(defaults.port_max == 27699);
    CHECK((defaults.port_max - defaults.port_min + 1) == 100);

    /* Limites exactos del bloque aceptados */
    CHECK(load_fixture("ports_exact.conf",
                       "PORT_MIN = 27600\nPORT_MAX = 27699\nPEER_HOSTS = a\n", &cfg) == DSM_OK);

    /* Un unico puerto (min == max) es un rango valido de tamano 1 */
    CHECK(load_fixture("ports_single.conf",
                       "PORT_MIN = 27650\nPORT_MAX = 27650\nPEER_HOSTS = a\n", &cfg) == DSM_OK);
    CHECK(cfg.port_min == 27650 && cfg.port_max == 27650);

    /* Invertido: min > max debe rechazarse */
    CHECK(load_fixture("ports_inv.conf",
                       "PORT_MIN = 27699\nPORT_MAX = 27600\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Fuera del bloque asignado: no debe aceptarse silenciosamente en el
     * default del grupo. El rango declarado por el grupo es 27600..27699. */
    CHECK(load_fixture("ports_below.conf",
                       "PORT_MIN = 27500\nPORT_MAX = 27599\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_above.conf",
                       "PORT_MIN = 27700\nPORT_MAX = 27799\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Rango que solapa parcialmente el bloque pero lo excede */
    CHECK(load_fixture("ports_overlap_lo.conf",
                       "PORT_MIN = 27590\nPORT_MAX = 27699\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_overlap_hi.conf",
                       "PORT_MIN = 27600\nPORT_MAX = 27710\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Subbloque dentro del asignado: aceptado */
    CHECK(load_fixture("ports_sub.conf",
                       "PORT_MIN = 27610\nPORT_MAX = 27619\nPEER_HOSTS = a\n", &cfg) == DSM_OK);

    /* Puerto 0 y puertos privilegiados fuera de rango */
    CHECK(load_fixture("ports_zero.conf",
                       "PORT_MIN = 0\nPORT_MAX = 0\nPEER_HOSTS = a\n", &cfg) == DSM_ERR_CONFIG);
    CHECK(load_fixture("ports_priv.conf",
                       "PORT_MIN = 80\nPORT_MAX = 90\nPEER_HOSTS = a\n", &cfg) == DSM_ERR_CONFIG);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 5. raw_hash: CRC32 completo del archivo
 * ------------------------------------------------------------------------- */

static int test_raw_hash_vectors(void)
{
    dsm_config_t cfg;

    /* Vector canonico CRC32 ("123456789" -> 0xCBF43926) via dsm_crc32 */
    CHECK(dsm_crc32(0, "123456789", 9) == 0xCBF43926u);
    CHECK(dsm_crc32(0, "", 0) == 0x00000000u);

    /* El hash debe ser el CRC del CONTENIDO EXACTO, no del parseo */
    char path[256];
    const char *content = VALID_MIN;
    const char *p = write_fixture("hash_base.conf", content, path);
    REQUIRE(p != NULL);
    REQUIRE(config_load(&cfg, p) == DSM_OK);
    CHECK(cfg.raw_hash == dsm_crc32(0, content, strlen(content)));

    /* Dos archivos semanticamente equivalentes pero con distinto espaciado
     * DEBEN tener hashes distintos: el hash es sobre bytes, no sobre
     * significado. Esto es lo que exige el handshake HELLO. */
    dsm_config_t cfg_b;
    char path2[256];
    char path3[256];
    const char *p2 = write_fixture("hash_space.conf",
                                   "CLUSTER_ID=0x534F3031\nPAGE_SIZE=4096\n", path2);
    REQUIRE(p2 != NULL);
    const char *p3 = write_fixture("hash_space2.conf",
                                   "CLUSTER_ID = 0x534F3031\nPAGE_SIZE = 4096\n", path3);
    REQUIRE(p3 != NULL);
    REQUIRE(config_load(&cfg, p2) == DSM_OK);
    REQUIRE(config_load(&cfg_b, p3) == DSM_OK);
    CHECK(cfg.raw_hash != cfg_b.raw_hash);

    return 0;
}

static int test_raw_hash_single_byte_sensitivity(void)
{
    /* Construir un archivo valido y verificar que cambiar UN solo byte en
     * cualquier posicion cambia el raw_hash resultante. */
    static const char base[] = VALID_MIN;
    size_t len = strlen(base);

    dsm_config_t ref;
    char path[256];
    const char *p = write_fixture("hash_sens_base.conf", base, path);
    REQUIRE(p != NULL);
    REQUIRE(config_load(&ref, p) == DSM_OK);

    /* Cambios en el cuerpo del archivo: cada posicion debe alterar el hash.
     * Se excluyen las posiciones cuyo byte alterado haria el archivo invalido
     * (eso tambien cambia el comportamiento, pero queremos el hash del mismo
     * tamano). Solo comprobamos que cuando sigue siendo cargable, el hash
     * cambia; y que si deja de ser cargable, no devuelve OK con el hash viejo. */
    uint8_t mut[512];
    REQUIRE(len < sizeof(mut));

    unsigned changed_loadable = 0;
    unsigned rejected = 0;

    for (size_t i = 0; i < len; i++) {
        memcpy(mut, base, len);

        /* Alterar el byte: si es un digito, cambiar por otro digito;
         * si es letra, cambiar por otra letra; si es espacio, por otro espacio.
         * Asi el archivo suele seguir siendo sintacticamente valido. */
        unsigned char c = mut[i];
        if (c >= '0' && c <= '9') {
            mut[i] = (uint8_t)((c == '9') ? '8' : '9');
        } else if (c >= 'a' && c <= 'z') {
            mut[i] = (uint8_t)((c == 'z') ? 'y' : 'z');
        } else if (c >= 'A' && c <= 'Z') {
            mut[i] = (uint8_t)((c == 'Z') ? 'Y' : 'Z');
        } else if (c == ' ') {
            mut[i] = '\t';
        } else {
            mut[i] = (uint8_t)((c == '=') ? ':' : '=');
        }

        if (mut[i] == (uint8_t)c) {
            continue; /* sin cambio efectivo */
        }

        char mpath[256];
        const char *mp = write_fixture_raw("hash_sens_mut.conf", mut, len, mpath);
        REQUIRE(mp != NULL);

        dsm_config_t m;
        int rc = config_load(&m, mp);
        if (rc == DSM_OK) {
            changed_loadable++;
            CHECK(m.raw_hash != ref.raw_hash);
        } else {
            rejected++;
            /* Rechazo explicito: siginifica que el byte alterado rompio el
             * parseo, lo cual tambien es una diferencia detectable. */
            CHECK(rc == DSM_ERR_CONFIG);
        }
    }

    /* Debe haber mutaciones que sigan siendo cargables y cambien el hash:
     * de lo contrario no estamos probando sensibilidad real. */
    CHECK(changed_loadable > 0);
    CHECK(changed_loadable + rejected == len);

    /* Byte final agregado: mismo contenido logico, hash distinto */
    char extra[600];
    size_t elen = len;
    memcpy(extra, base, len);
    extra[elen++] = '\n';
    char epath[256];
    const char *ep = write_fixture_raw("hash_sens_extra.conf", extra, elen, epath);
    REQUIRE(ep != NULL);
    dsm_config_t e;
    REQUIRE(config_load(&e, ep) == DSM_OK);
    CHECK(e.raw_hash != ref.raw_hash);

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

        snprintf(conf, sizeof(conf), "HOME_SLACK = %s\nPEER_HOSTS = a\n",
                 ok_cases[i].literal);
        int rc = load_fixture("slack_ok.conf", conf, &cfg);
        CHECK(rc == DSM_OK);
        if (rc == DSM_OK) {
            CHECK(cfg.home_slack_permille == ok_cases[i].expected);
        }
    }

    /* Prefijo alternativo: HOME_SLACK con espacios internos alrededor del '=' */
    REQUIRE(load_fixture("slack_spaces.conf",
                         "HOME_SLACK   =    1.25\nPEER_HOSTS = a\n", &cfg) == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);

    /* Decimales con ceros de relleno: 1.250 es exactamente 1250 */
    REQUIRE(load_fixture("slack_pad.conf", "HOME_SLACK = 1.250\nPEER_HOSTS = a\n", &cfg)
            == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);
    REQUIRE(load_fixture("slack_pad2.conf", "HOME_SLACK = 1.2500\nPEER_HOSTS = a\n", &cfg)
            == DSM_OK);
    CHECK(cfg.home_slack_permille == 1250);

    /* Mas de tres decimales SIGNIFICATIVOS: rechazo */
    CHECK(load_fixture("slack_4dec.conf", "HOME_SLACK = 1.2501\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_4dec_b.conf", "HOME_SLACK = 1.2345\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_4dec_c.conf", "HOME_SLACK = 1.0001\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Menor que 1.0: rechazo */
    CHECK(load_fixture("slack_below.conf", "HOME_SLACK = 0.999\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_zero.conf", "HOME_SLACK = 0\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_neg.conf", "HOME_SLACK = -1.25\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Sintaxis no soportada: exponente, coma decimal, signo, punto suelto */
    CHECK(load_fixture("slack_exp.conf", "HOME_SLACK = 1e0\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_exp2.conf", "HOME_SLACK = 1.25e2\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_comma.conf", "HOME_SLACK = 1,25\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_plus.conf", "HOME_SLACK = +1.25\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_dot.conf", "HOME_SLACK = 1.\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_dotlead.conf", "HOME_SLACK = .25\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_text.conf", "HOME_SLACK = mucho\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_empty.conf", "HOME_SLACK =\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Desbordamiento: entero gigantesco */
    CHECK(load_fixture("slack_overflow.conf",
                       "HOME_SLACK = 4294967.296\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);
    CHECK(load_fixture("slack_overflow2.conf",
                       "HOME_SLACK = 99999999999\nPEER_HOSTS = a\n", &cfg)
          == DSM_ERR_CONFIG);

    /* Conversion determinista: la misma cadena siempre da el mismo entero,
     * lo cual es el requisito del calculo de capacidad distribuido. */
    for (int i = 0; i < 5; i++) {
        REQUIRE(load_fixture("slack_det.conf", "HOME_SLACK = 1.333\nPEER_HOSTS = a\n",
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

    /* RAID5 no soportado en P1 */
    EXPECT_BAD("cv_raid5.conf", "REDUNDANCY = RAID5\n");
    EXPECT_OK("REDUNDANCY = NONE\n");
    EXPECT_OK("REDUNDANCY = none\n");       /* case-insensitive */

    /* REQUEST_RETRIES >= 1 */
    EXPECT_BAD("cv_retries_zero.conf", "REQUEST_RETRIES = 0\n");
    EXPECT_OK("REQUEST_RETRIES = 1\n");

    /* HOME_SLACK < 1.0 mediante el entero ya convertido */
    EXPECT_BAD("cv_slack_small.conf", "HOME_SLACK = 0.5\n");

    /* PEER_HOSTS: al menos un host, lista con huecos se normaliza */
    EXPECT_BAD("cv_hosts_empty.conf", "PEER_HOSTS =\n");
    EXPECT_BAD("cv_hosts_commas.conf", "PEER_HOSTS = ,,,\n");
    EXPECT_OK("PEER_HOSTS = a,,b\n");
    CHECK(cfg.peer_host_count == 2);
    CHECK(strcmp(cfg.peer_hosts[0], "a") == 0);
    CHECK(strcmp(cfg.peer_hosts[1], "b") == 0);

    /* Demasiados hosts: > DSM_MAX_PEER_HOSTS (16) */
    {
        char many[1024];
        int n = snprintf(many, sizeof(many), "PEER_HOSTS = ");
        for (int i = 0; i < 17; i++) {
            n += snprintf(many + n, sizeof(many) - (size_t)n, "h%02d%s",
                          i, (i < 16) ? "," : "\n");
        }
        CHECK(load_fixture("cv_hosts_17.conf", many, &cfg) == DSM_ERR_CONFIG);

        /* Exactamente 16 si debe aceptarse */
        char sixteen[1024];
        n = snprintf(sixteen, sizeof(sixteen), "PEER_HOSTS = ");
        for (int i = 0; i < 16; i++) {
            n += snprintf(sixteen + n, sizeof(sixteen) - (size_t)n, "h%02d%s",
                          i, (i < 15) ? "," : "\n");
        }
        CHECK(load_fixture("cv_hosts_16.conf", sixteen, &cfg) == DSM_OK);
        CHECK(cfg.peer_host_count == 16);
        CHECK(strcmp(cfg.peer_hosts[15], "h15") == 0);
    }

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
    cfg.peer_host_count = 99;
    cfg.raw_hash = 0xDEADBEEF;
    REQUIRE(config_load(&cfg, "dsm.conf") == DSM_OK);
    CHECK(cfg.peer_host_count == 5);
    CHECK(cfg.raw_hash == 0xBB8AFAE5u);
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
    bad.redundancy_raid5 = true;
    CHECK(config_validate(&bad) == DSM_ERR_CONFIG);

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
                 "PEER_HOSTS = a\n", ps);
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
        snprintf(conf, sizeof(conf), "HOME_SLACK = %u.%03u\nPEER_HOSTS = a\n",
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

    /* 200 archivos con un byte final distinto: todos hashes distintos */
    {
        uint32_t hashes[200];
        for (int i = 0; i < 200; i++) {
            static char body[512];
            size_t blen = (size_t)snprintf(body, sizeof(body),
                                           "PAGE_SIZE = 4096\n"
                                           "PEER_HOSTS = a\n"
                                           "# relleno %03d\n", i);
            char path[256];
            const char *p = write_fixture_raw("stress_hash.conf", body, blen, path);
            REQUIRE(p != NULL);
            REQUIRE(config_load(&cfg, p) == DSM_OK);
            hashes[i] = cfg.raw_hash;
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
 * 10. raw_hash: integridad del archivo real byte a byte
 * ------------------------------------------------------------------------- */

static int test_rig_raw_hash_integridad(void)
{
    dsm_config_t a, b;
    char path[256];
    static const char base[] = VALID_MIN;
    size_t len = strlen(base);

    const char *p = write_fixture_raw("rig_hash_base.conf", base, len, path);
    REQUIRE(p != NULL);
    REQUIRE(config_load(&a, p) == DSM_OK);

    /* Dos lecturas consecutivas sin tocar el archivo: hash identico y
     * reproducible contra el CRC de los bytes exactos. */
    REQUIRE(config_load(&b, p) == DSM_OK);
    CHECK(b.raw_hash == a.raw_hash);
    CHECK(a.raw_hash == dsm_crc32(0, base, len));

    /* Cambiar EXACTAMENTE un byte dentro del valor de PEER_HOSTS mantiene el
     * archivo sintacticamente valido y del mismo tamano, pero cambia el hash:
     * 127.0.0.1 -> 127.0.0.2. */
    static uint8_t mut[512];
    REQUIRE(len < sizeof(mut));
    memcpy(mut, base, len);
    mut[len] = '\0';
    char *host = strstr((char *)mut, "127.0.0.1");
    REQUIRE(host != NULL);
    size_t off = (size_t)(host - (char *)mut);
    mut[off + 8] = '2';

    size_t diffs = 0;
    for (size_t i = 0; i < len; i++) {
        if (mut[i] != (uint8_t)base[i]) diffs++;
    }
    CHECK(diffs == 1);

    const char *mp = write_fixture_raw("rig_hash_1byte.conf", mut, len, path);
    REQUIRE(mp != NULL);
    REQUIRE(config_load(&b, mp) == DSM_OK);
    CHECK(strcmp(b.peer_hosts[0], "127.0.0.2") == 0);   /* el cambio se aplico */
    CHECK(b.raw_hash != a.raw_hash);
    CHECK(b.raw_hash == dsm_crc32(0, mut, len));

    /* Un espacio anadido al final no altera ninguna clave, pero si el hash:
     * el handshake HELLO compara bytes de archivo, no configuracion resuelta. */
    static uint8_t extra[512];
    memcpy(extra, base, len);
    extra[len] = ' ';
    const char *ep = write_fixture_raw("rig_hash_space.conf", extra, len + 1, path);
    REQUIRE(ep != NULL);
    REQUIRE(config_load(&b, ep) == DSM_OK);
    CHECK(b.raw_hash != a.raw_hash);
    CHECK(b.raw_hash == dsm_crc32(0, extra, len + 1));

    /* Cambiar un byte de un comentario (que el parser descarta) tambien
     * cambia el hash: cubre el archivo completo, no solo las claves. */
    static const char con_comentario[] = VALID_MIN "# firma A\n";
    static const char otro_comentario[] = VALID_MIN "# firma B\n";
    char path2[256];
    const char *cp = write_fixture_raw("rig_hash_coment_a.conf",
                                       con_comentario, strlen(con_comentario), path2);
    REQUIRE(cp != NULL);
    REQUIRE(config_load(&a, cp) == DSM_OK);
    cp = write_fixture_raw("rig_hash_coment_b.conf",
                           otro_comentario, strlen(otro_comentario), path2);
    REQUIRE(cp != NULL);
    REQUIRE(config_load(&b, cp) == DSM_OK);
    CHECK(a.raw_hash != b.raw_hash);
    CHECK(a.peer_host_count == b.peer_host_count);  /* misma configuracion util */

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
            "HOME_SLACK = 1.25\nHOME_SLACK = 1.30\nPEER_HOSTS = a\n");
    RIG_BAD("rig_dup_hosts.conf",
            "PEER_HOSTS = a\nPEER_HOSTS = b\n");
    RIG_BAD("rig_unknown_port.conf", "PORT_M1N = 27600\nPEER_HOSTS = a\n");
    RIG_BAD("rig_unknown_foo.conf", "FOO_BAR = 1\nPEER_HOSTS = a\n");
    RIG_BAD("rig_unknown_prefix.conf",
            "PAGE_SIZE_EXTRA = 4096\nPEER_HOSTS = a\n");

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
    CHECK(cfg.raw_hash == dsm_crc32(0, buf, exact));

    /* Un byte mas supera el limite: rechazo antes de parsear. */
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
    RIG_MUT(max_runtime_nodes, (uint32_t)MAX_NODES + 1u);
    RIG_MUT(redundancy_raid5, true);

    /* Los defaults del archivo canonico son validos y no se mutan al validar. */
    dsm_config_t copia = base;
    CHECK(config_validate(&base) == DSM_OK);
    CHECK(memcmp(&base, &copia, sizeof(base)) == 0);

#undef RIG_MUT
#undef RIG_MUT_OK
    return 0;
}

/* ----------------------------------------------------------------------------
 * 14. raw_hash disponible incluso cuando el parseo rechaza el archivo
 *
 * Un nodo que recibe un dsm.conf con una clave desconocida debe poder
 * reportar contra que hash fallo: el hash es del byte stream, no del
 * resultado del parseo, asi que se calcula antes de procesar las lineas.
 * ------------------------------------------------------------------------- */

static int test_rig_hash_sobrevive_al_rechazo(void)
{
    dsm_config_t cfg;
    char path[256];

    /* Clave desconocida en la linea 3: parseo rechazado, hash presente. */
    const char *bad_unknown =
        "PAGE_SIZE = 4096\n"
        "\n"
        "CLAVE_INVENTADA = 7\n";
    const char *p = write_fixture("hash_reject_unknown.conf", bad_unknown, path);
    REQUIRE(p != NULL);
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);
    CHECK(cfg.raw_hash == dsm_crc32(0, bad_unknown, strlen(bad_unknown)));

    /* Linea sin '=': mismo contrato. */
    const char *bad_syntax = "PORT_MIN 27600\n";
    p = write_fixture("hash_reject_syntax.conf", bad_syntax, path);
    REQUIRE(p != NULL);
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);
    CHECK(cfg.raw_hash == dsm_crc32(0, bad_syntax, strlen(bad_syntax)));

    /* Valor invalido: idem. */
    const char *bad_value = "PAGE_SIZE = cuatro mil\n";
    p = write_fixture("hash_reject_value.conf", bad_value, path);
    REQUIRE(p != NULL);
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, p) == DSM_ERR_CONFIG);
    CHECK(cfg.raw_hash == dsm_crc32(0, bad_value, strlen(bad_value)));

    /* Dos archivos distintos que fallan en la misma linea tienen hashes
     * distintos: el hash identifica el contenido, no el tipo de error. */
    const char *bad_a = "PAGE_SIZE = 4096\nFOO = 1\n";
    const char *bad_b = "PAGE_SIZE = 4096\nBAR = 1\n";
    dsm_config_t ca;
    dsm_config_t cb;
    memset(&ca, 0, sizeof(ca));
    memset(&cb, 0, sizeof(cb));
    p = write_fixture("hash_reject_a.conf", bad_a, path);
    REQUIRE(p != NULL);
    CHECK(config_load(&ca, p) == DSM_ERR_CONFIG);
    p = write_fixture("hash_reject_b.conf", bad_b, path);
    REQUIRE(p != NULL);
    CHECK(config_load(&cb, p) == DSM_ERR_CONFIG);
    CHECK(ca.raw_hash == dsm_crc32(0, bad_a, strlen(bad_a)));
    CHECK(cb.raw_hash == dsm_crc32(0, bad_b, strlen(bad_b)));
    CHECK(ca.raw_hash != cb.raw_hash);

    /* Un archivo inexistente no tiene bytes: no hay hash que reportar. */
    memset(&cfg, 0, sizeof(cfg));
    CHECK(config_load(&cfg, FIXTURE_DIR "/hash_reject_missing.conf") == DSM_ERR_CONFIG);
    CHECK(cfg.raw_hash == 0);

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
    RUN_TEST(test_raw_hash_vectors);
    RUN_TEST(test_raw_hash_single_byte_sensitivity);
    RUN_TEST(test_home_slack_conversion);
    RUN_TEST(test_cross_validation_rules);
    RUN_TEST(test_reload_and_null_safety);
    RUN_TEST(test_stress_many_configs);
    RUN_TEST(test_rig_raw_hash_integridad);
    RUN_TEST(test_rig_limites_sistema);
    RUN_TEST(test_rig_archivo_enorme_y_no_regular);
    RUN_TEST(test_rig_validacion_directa);
    RUN_TEST(test_rig_hash_sobrevive_al_rechazo);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
