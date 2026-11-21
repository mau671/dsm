#include "test.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "dist.h"
#include "mem.h"
#include "ring.h"
#include "wire.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PAGE_BYTES 4096u
#define VIEW_PAGES 16u
#define KERNEL_XOR 7u

typedef struct {
    uint8_t key;
    uint8_t fill;
} kernel_args_t;

static kernel_args_t g_kernel_args;

/* Kernel de prueba: copia el rango de entrada a la salida aplicando XOR. */
static void kernel_xor(void *arg, const dsm_dist_part_t *part,
                       dsm_dist_result_t *out)
{
    kernel_args_t *ka = (kernel_args_t *)arg;
    uint8_t *dst = (uint8_t *)(uintptr_t)part->dst_va;
    const uint8_t *src = (const uint8_t *)(uintptr_t)part->src_va;

    for (uint64_t i = 0; i < part->nbytes; i++) {
        dst[i] = (uint8_t)(src[i] ^ ka->key);
    }
    out->items = part->nbytes;
}

/* Kernel de reemplazo: escribe un patron fijo para distinguirlo del anterior. */
static void kernel_fill(void *arg, const dsm_dist_part_t *part,
                        dsm_dist_result_t *out)
{
    kernel_args_t *ka = (kernel_args_t *)arg;
    uint8_t *dst = (uint8_t *)(uintptr_t)part->dst_va;

    memset(dst, ka->fill, (size_t)part->nbytes);
    out->items = part->nbytes;
}

static dsm_ctx_t *ctx_create(void)
{
    dsm_ctx_t *ctx = dsm_ctx_create_test();
    if (!ctx) return NULL;

    ctx->cfg.page_size = PAGE_BYTES;
    ctx->cfg.view_size = (uint64_t)VIEW_PAGES * PAGE_BYTES;
    ctx->cfg.node_mem = (uint64_t)8 * PAGE_BYTES;
    ctx->cfg.home_slack_permille = 1250;
    ctx->cfg.base_addr = 0x200000000000ULL;
    ctx->cfg.base_addr_tsan = 0x4000000000ULL;
    ctx->cfg.page_lock_stripes = 8;
    ctx->cfg.cluster_id = 0x534F3031u;
    ctx->cfg.request_timeout_ms = 100;
    ctx->cfg.request_retries = 1;
    ctx->cfg.chunk_pages = 4;
    ctx->cfg.wait_pool_entries = 64;
    ctx->cfg.dist_timeout_ms = 2000;

    if (dsm_mem_init(&ctx->mem, &ctx->cfg) != DSM_OK) {
        dist_destroy(ctx);
        membership_destroy(ctx);
        net_destroy(ctx);
        free(ctx);
        return NULL;
    }

    pthread_rwlock_wrlock(&ctx->memb.rw);
    ctx->memb.self_slot = 0;
    pthread_rwlock_unlock(&ctx->memb.rw);
    membership_add_self(ctx);
    if (ring_build(&ctx->ring, &ctx->memb) != DSM_OK) {
        dsm_mem_destroy(&ctx->mem);
        dist_destroy(ctx);
        membership_destroy(ctx);
        net_destroy(ctx);
        free(ctx);
        return NULL;
    }

    dsm_ctx_install_test(ctx);
    return ctx;
}

static void ctx_destroy(dsm_ctx_t *ctx)
{
    dsm_ctx_reset();
    ring_free(&ctx->ring);
    dsm_mem_destroy(&ctx->mem);
    dist_destroy(ctx);
    membership_destroy(ctx);
    net_destroy(ctx);
    free(ctx);
}

static void parts_init(dsm_dist_part_t *parts, uint32_t n, uint32_t len)
{
    uint8_t *base = NULL;
    dsm_ctx_t *ctx = dsm_ctx_get();
    base = ctx->mem.alias;

    for (uint32_t i = 0; i < n; i++) {
        parts[i].id     = i;
        parts[i].src_va = (uint64_t)(uintptr_t)(base + (size_t)i * len);
        parts[i].dst_va = (uint64_t)(uintptr_t)(base + PAGE_BYTES +
                                                (size_t)i * len);
        parts[i].nbytes = len;

        for (uint32_t j = 0; j < len; j++) {
            base[(size_t)i * len + j] = (uint8_t)(i * 7u + j * 3u + 1u);
        }
        memset(base + PAGE_BYTES + (size_t)i * len, 0, len);
    }
}

static int test_dist_launch_collect_verify(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    g_kernel_args.key = 0x5Au;
    REQUIRE(dsm_dist_register(KERNEL_XOR, kernel_xor, &g_kernel_args) == DSM_OK);

    dsm_dist_part_t parts[4];
    parts_init(parts, 4, 64);

    uint32_t workers[1] = { 0 };
    dsm_status_t rc = dsm_dist_launch(100, KERNEL_XOR, parts, 4,
                                      workers, 1, 1000);
    CHECK(rc == DSM_OK);

    dsm_dist_result_t results[8];
    uint32_t n = 8;
    rc = dsm_dist_collect(100, results, &n, 1000);
    CHECK(rc == DSM_OK);
    CHECK(n == 4);

    uint64_t expected = 0;
    for (uint32_t i = 0; i < n; i++) {
        CHECK(results[i].part_id == i);
        CHECK(results[i].slot == 0);
        CHECK(results[i].status == 0);
        CHECK(results[i].items == 64);
        CHECK(results[i].crc32 != 0);

        uint8_t *dst = ctx->mem.alias + PAGE_BYTES + (size_t)i * 64;
        for (uint32_t j = 0; j < 64; j++) {
            uint8_t want = (uint8_t)((i * 7u + j * 3u + 1u) ^ 0x5Au);
            CHECK(dst[j] == want);
        }
        expected ^= results[i].crc32;
    }

    bool ok = false;
    rc = dsm_dist_verify(100, expected, &ok);
    CHECK(rc == DSM_OK);
    CHECK(ok == true);

    ok = true;
    rc = dsm_dist_verify(100, expected ^ 0xFFFFFFFFu, &ok);
    CHECK(rc == DSM_OK);
    CHECK(ok == false);

    /* Una segunda recogida no bloqueante ya no entrega resultados. */
    uint32_t rest = 4;
    rc = dsm_dist_collect(100, results, &rest, 0);
    CHECK(rc == DSM_OK);
    CHECK(rest == 0);

    /* Con plazo, la espera vence de verdad: el reloj del condvar debe ser el
     * mismo que usa pthread_cond_timedwait. */
    rest = 4;
    uint64_t t0 = mono_ms();
    rc = dsm_dist_collect(100, results, &rest, 200);
    uint64_t elapsed = mono_ms() - t0;
    CHECK(rc == DSM_ERR_TIMEOUT);
    CHECK(elapsed >= 150);

    ctx_destroy(ctx);
    return 0;
}

static int test_dist_register_replace_and_full(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    CHECK(dsm_dist_register(KERNEL_XOR, kernel_xor, &g_kernel_args) == DSM_OK);
    CHECK(dsm_dist_register(KERNEL_XOR, NULL, NULL) == DSM_ERR_INVALID);

    g_kernel_args.fill = 0xA5u;
    CHECK(dsm_dist_register(KERNEL_XOR, kernel_fill, &g_kernel_args) == DSM_OK);

    dsm_dist_part_t part;
    memset(&part, 0, sizeof(part));
    part.id     = 0;
    part.dst_va = (uint64_t)(uintptr_t)(ctx->mem.alias + 2 * PAGE_BYTES);
    part.nbytes = 32;

    uint32_t workers[1] = { 0 };
    CHECK(dsm_dist_launch(101, KERNEL_XOR, &part, 1, workers, 1, 500) == DSM_OK);

    dsm_dist_result_t res;
    uint32_t n = 1;
    CHECK(dsm_dist_collect(101, &res, &n, 500) == DSM_OK);
    CHECK(n == 1);
    CHECK(res.status == 0);

    for (uint32_t i = 0; i < 32; i++) {
        CHECK(ctx->mem.alias[2 * PAGE_BYTES + i] == 0xA5u);
    }

    /* Ocupar las entradas libres restantes y comprobar el rechazo. */
    uint32_t id = 1000;
    while (dsm_dist_register(id, kernel_fill, &g_kernel_args) == DSM_OK) {
        id++;
        if (id > 1100) break;
    }
    CHECK(dsm_dist_register(2000, kernel_fill, &g_kernel_args) ==
          DSM_ERR_NO_SPACE);

    ctx_destroy(ctx);
    return 0;
}

static int test_dist_launch_rejects_invalid(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);
    REQUIRE(dsm_dist_register(KERNEL_XOR, kernel_xor, &g_kernel_args) == DSM_OK);

    dsm_dist_part_t parts[4];
    parts_init(parts, 4, 32);
    uint32_t workers[1] = { 0 };
    uint32_t bad_worker[1] = { 5 };

    CHECK(dsm_dist_launch(200, KERNEL_XOR, NULL, 4, workers, 1, 100) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, KERNEL_XOR, parts, 0, workers, 1, 100) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, KERNEL_XOR, parts, 4, NULL, 1, 100) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, KERNEL_XOR, parts, 4, workers, 0, 100) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, KERNEL_XOR, parts, 4, workers, 1, 0) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, 999, parts, 4, workers, 1, 100) ==
          DSM_ERR_INVALID);
    CHECK(dsm_dist_launch(200, KERNEL_XOR, parts, 4, bad_worker, 1, 100) ==
          DSM_ERR_INVALID);

    /* Primero un lanzamiento valido y luego el mismo job_id. */
    REQUIRE(dsm_dist_launch(201, KERNEL_XOR, parts, 4, workers, 1, 500) == DSM_OK);
    CHECK(dsm_dist_launch(201, KERNEL_XOR, parts, 4, workers, 1, 500) ==
          DSM_ERR_INVALID);

    /* La capacidad de la tabla de resultados acota las particiones. */
    ctx->cfg.wait_pool_entries = 2;
    CHECK(dsm_dist_launch(202, KERNEL_XOR, parts, 4, workers, 1, 500) ==
          DSM_ERR_NO_SPACE);
    ctx->cfg.wait_pool_entries = 64;

    /* Identificadores de particion repetidos o fuera de rango. */
    parts[1].id = 0;
    CHECK(dsm_dist_launch(203, KERNEL_XOR, parts, 4, workers, 1, 500) ==
          DSM_ERR_INVALID);
    parts[1].id = 9;
    CHECK(dsm_dist_launch(203, KERNEL_XOR, parts, 4, workers, 1, 500) ==
          DSM_ERR_INVALID);

    dsm_dist_result_t out[4];
    uint32_t n = 4;
    CHECK(dsm_dist_collect(9999, out, &n, 0) == DSM_ERR_INVALID);
    n = 0;
    CHECK(dsm_dist_collect(201, out, &n, 0) == DSM_OK);
    CHECK(n == 0);

    ctx_destroy(ctx);
    return 0;
}

static int test_dist_unknown_kernel_fails(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    dsm_stats_t before;
    dsm_stats_t after;
    REQUIRE(dsm_stats(&before) == DSM_OK);

    dist_run_wire_t rw;
    memset(&rw, 0, sizeof(rw));
    rw.job     = 777;
    rw.kernel  = 4242;
    rw.part_id = 0;
    rw.nparts  = 1;

    uint8_t payload[DSM_DIST_RUN_WIRE_LEN];
    dsm_pack_dist_run(&rw, payload);

    dsm_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.magic         = DSM_MAGIC;
    msg.hdr.cluster_id    = ctx->cfg.cluster_id;
    msg.hdr.proto_version = DSM_PROTO_VERSION;
    msg.hdr.type          = MSG_DIST_RUN;
    msg.hdr.src_slot      = 0;
    msg.hdr.dst_slot      = 0;
    msg.hdr.req_id        = 1;
    msg.hdr.payload_len   = DSM_DIST_RUN_WIRE_LEN;
    msg.payload           = payload;

    dist_on_run(ctx, &msg);

    REQUIRE(dsm_stats(&after) == DSM_OK);
    CHECK(after.dist_tasks_failed == before.dist_tasks_failed + 1);
    CHECK(after.dist_tasks_run == before.dist_tasks_run);

    ctx_destroy(ctx);
    return 0;
}

static int test_pages_global_text(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    ctx->mem.local[0].state     = DSM_PAGE_S;
    ctx->mem.local[0].version   = 3;
    ctx->mem.local[1].state     = DSM_PAGE_M;
    ctx->mem.local[1].version   = 4;

    char out[4096];
    dsm_status_t rc = dsm_pages_global(0, 4, out, sizeof(out));
    CHECK(rc == DSM_OK);
    CHECK(strstr(out, "Pagina") != NULL);
    CHECK(strstr(out, "Propietario") != NULL);
    CHECK(strstr(out, "Franja") != NULL);
    CHECK(strstr(out, "{0}") != NULL);
    CHECK(strstr(out, "\n0 ") != NULL);

    char tiny[16];
    CHECK(dsm_pages_global(0, 4, tiny, sizeof(tiny)) == DSM_ERR_NO_SPACE);
    CHECK(dsm_pages_global(0, 4, NULL, 32) == DSM_ERR_INVALID);
    CHECK(dsm_pages_global(0, 4, out, 0) == DSM_ERR_INVALID);

    char empty[256];
    CHECK(dsm_pages_global(VIEW_PAGES, 4, empty, sizeof(empty)) == DSM_OK);
    CHECK(strstr(empty, "\n0 ") == NULL);

    ctx_destroy(ctx);
    return 0;
}

static int read_file(const char *path, char *buf, size_t cap)
{
    FILE *fp = fopen(path, "r");
    if (!fp) return -1;

    size_t got = fread(buf, 1, cap - 1, fp);
    buf[got] = '\0';
    fclose(fp);
    return 0;
}

static int test_html_report(void)
{
    dsm_ctx_t *ctx = ctx_create();
    REQUIRE(ctx != NULL);

    ctx->mem.local[0].state = DSM_PAGE_S;
    ctx->mem.local[1].state = DSM_PAGE_M;

    /* Un cliente usa la vista pero no aporta memoria: el informe lo lista con
     * su rol y no lo cuenta como nodo. */
    pthread_rwlock_wrlock(&ctx->memb.rw);
    ctx->memb.present[1 / 8] |= (uint8_t)(1u << (1 % 8));
    ctx->memb.nodes[1].slot      = 1;
    ctx->memb.nodes[1].state     = DSM_NODE_ALIVE;
    ctx->memb.nodes[1].mem_pages = 0;
    ctx->memb.nodes[1].is_client = 1;
    memset(ctx->memb.nodes[1].uuid, 0x33, 16);
    pthread_rwlock_unlock(&ctx->memb.rw);

    char dir1[] = "/tmp/dsm-report-XXXXXX";
    REQUIRE(mkdtemp(dir1) != NULL);

    CHECK(dsm_html_report(dir1) == DSM_OK);

    char path[600];
    char content[65536];

    snprintf(path, sizeof(path), "%s/global.html", dir1);
    CHECK(read_file(path, content, sizeof(content)) == 0);
    CHECK(strstr(content, "<!DOCTYPE html>") != NULL);
    CHECK(strstr(content, "</html>") != NULL);
    CHECK(strstr(content, "Tabla global de paginas") != NULL);
    CHECK(strstr(content, "Clientes:</strong> 1") != NULL);
    CHECK(strstr(content, ">cliente<") != NULL);
    CHECK(strstr(content, "nodo-0.html") != NULL);
    CHECK(strstr(content, "{0}") != NULL);

    snprintf(path, sizeof(path), "%s/nodo-0.html", dir1);
    CHECK(read_file(path, content, sizeof(content)) == 0);
    CHECK(strstr(content, "<!DOCTYPE html>") != NULL);
    CHECK(strstr(content, "Rol:</strong> nodo") != NULL);
    CHECK(strstr(content, "Tabla de paginas") != NULL);
    CHECK(strstr(content, ">S<") != NULL);
    CHECK(strstr(content, ">M<") != NULL);

    /* OUTPUT_DIR de la configuracion es el valor por defecto. */
    char dir2[] = "/tmp/dsm-report-XXXXXX";
    REQUIRE(mkdtemp(dir2) != NULL);
    snprintf(ctx->cfg.output_dir, sizeof(ctx->cfg.output_dir), "%s", dir2);
    CHECK(dsm_html_report(NULL) == DSM_OK);
    snprintf(path, sizeof(path), "%s/global.html", dir2);
    CHECK(access(path, R_OK) == 0);
    CHECK(dsm_html_report("") == DSM_OK);

    snprintf(path, sizeof(path), "%s/global.html", dir1);
    unlink(path);
    snprintf(path, sizeof(path), "%s/nodo-0.html", dir1);
    unlink(path);
    rmdir(dir1);

    snprintf(path, sizeof(path), "%s/global.html", dir2);
    unlink(path);
    snprintf(path, sizeof(path), "%s/nodo-0.html", dir2);
    unlink(path);
    rmdir(dir2);

    ctx_destroy(ctx);
    return 0;
}

static int test_api_requires_init(void)
{
    dsm_dist_result_t out;
    uint32_t n = 1;
    char buf[64];
    bool ok = false;

    CHECK(dsm_dist_register(1, kernel_xor, NULL) == DSM_ERR_NOT_INIT);
    CHECK(dsm_dist_collect(1, &out, &n, 0) == DSM_ERR_NOT_INIT);
    CHECK(dsm_dist_verify(1, 0, &ok) == DSM_ERR_NOT_INIT);
    CHECK(dsm_pages_global(0, 1, buf, sizeof(buf)) == DSM_ERR_NOT_INIT);
    CHECK(dsm_html_report("/tmp/opencode") == DSM_ERR_NOT_INIT);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Ejecucion distribuida y consultas globales ===\n");
    RUN_TEST(test_dist_launch_collect_verify);
    RUN_TEST(test_dist_register_replace_and_full);
    RUN_TEST(test_dist_launch_rejects_invalid);
    RUN_TEST(test_dist_unknown_kernel_fails);
    RUN_TEST(test_pages_global_text);
    RUN_TEST(test_html_report);
    RUN_TEST(test_api_requires_init);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
