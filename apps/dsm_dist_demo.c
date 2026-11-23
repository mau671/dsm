#include "dsm.h"
#include "wire.h"
#include "util.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_BYTES      4096u
#define BLOCK_PAGES     4u
#define PARTS           8u
#define PART_BYTES      1024u
#define KERNEL_DIST     0x44495354u

/*
 * Demostración de ejecución distribuida. El coordinador reserva un bloque de
 * la vista DSM, lo llena con un patrón, lanza particiones entre su propia
 * ranura y una ranura remota, recoge los resultados y comprueba el CRC32 y el
 * contenido. El trabajador registra el mismo kernel y espera las peticiones
 * DIST_RUN que llegan por la red.
 *
 * El kernel aplica una XOR con una clave fija para que el contenido de salida
 * dependa del código registrado en cada proceso y no solo de una copia.
 */

static const uint8_t g_key = 0x5Au;

static void kernel_xor(void *arg, const dsm_dist_part_t *part,
                       dsm_dist_result_t *out)
{
    uint8_t key = (arg != NULL) ? *(const uint8_t *)arg : g_key;
    uint8_t *dst = (uint8_t *)(uintptr_t)part->dst_va;
    const uint8_t *src = (const uint8_t *)(uintptr_t)part->src_va;

    for (uint64_t i = 0; i < part->nbytes; i++) {
        dst[i] = (uint8_t)(src[i] ^ key);
    }
    out->items = part->nbytes;
}

static void fill_source(uint8_t *src, uint32_t bytes)
{
    for (uint32_t i = 0; i < bytes; i++) {
        src[i] = (uint8_t)((i * 7u + 3u) & 0xFFu);
    }
}

static int verify_output(const uint8_t *dst, uint32_t bytes)
{
    for (uint32_t i = 0; i < bytes; i++) {
        uint8_t want = (uint8_t)(((i * 7u + 3u) & 0xFFu) ^ g_key);
        if (dst[i] != want) return 0;
    }
    return 1;
}

static int run_job(uint32_t job_id, uint32_t worker_slot, void *base)
{
    uint32_t self = 0;
    if (dsm_node_id(&self) != DSM_OK) return 0;
    if (worker_slot == self) {
        printf("DIST_FAIL worker_slot coincide con el coordinador\n");
        return 0;
    }

    uint8_t *src = (uint8_t *)base;
    uint8_t *dst = src + BLOCK_PAGES * PAGE_BYTES / 2u;
    uint32_t bytes = PARTS * PART_BYTES;

    fill_source(src, bytes);
    memset(dst, 0, bytes);

    dsm_dist_part_t parts[PARTS];
    for (uint32_t i = 0; i < PARTS; i++) {
        parts[i].id     = i;
        parts[i].src_va = (uint64_t)(uintptr_t)(src + (size_t)i * PART_BYTES);
        parts[i].dst_va = (uint64_t)(uintptr_t)(dst + (size_t)i * PART_BYTES);
        parts[i].nbytes = PART_BYTES;
    }

    uint32_t workers[2] = { self, worker_slot };
    dsm_status_t rc = dsm_dist_launch(job_id, KERNEL_DIST, parts, PARTS,
                                      workers, 2u, 5000u);
    if (rc != DSM_OK) {
        printf("DIST_FAIL launch=%s\n", dsm_strerror(rc));
        return 0;
    }

    dsm_dist_result_t results[PARTS];
    uint32_t got = 0;
    uint32_t remote = 0;
    int complete = 1;

    while (got < PARTS) {
        uint32_t want = PARTS - got;
        dsm_dist_result_t batch[PARTS];
        memset(batch, 0, sizeof(batch));
        rc = dsm_dist_collect(job_id, batch, &want, 5000u);
        if (rc != DSM_OK) {
            printf("DIST_FAIL collect=%s\n", dsm_strerror(rc));
            complete = 0;
            break;
        }
        if (want == 0) {
            printf("DIST_FAIL collect sin resultados\n");
            complete = 0;
            break;
        }
        for (uint32_t i = 0; i < want && got < PARTS; i++) {
            results[got++] = batch[i];
            if (batch[i].status != 0) complete = 0;
            if (batch[i].slot == worker_slot) remote++;
        }
    }

    uint64_t expected = 0;
    for (uint32_t i = 0; i < PARTS; i++) {
        expected ^= dsm_crc32(0, dst + (size_t)i * PART_BYTES, PART_BYTES);
    }

    bool verified = false;
    rc = dsm_dist_verify(job_id, expected, &verified);
    int read_ok = verify_output(dst, bytes);

    printf("DIST_SLOTS local=%u remote=%u\n", PARTS - remote, remote);
    for (uint32_t i = 0; i < got; i++) {
        char where[16];
        if (results[i].slot == self) {
            snprintf(where, sizeof(where), "local");
        } else {
            snprintf(where, sizeof(where), "remoto");
        }
        printf("DIST_PART id=%u slot=%u origen=%s items=%llu us=%llu\n",
               results[i].part_id, results[i].slot, where,
               (unsigned long long)results[i].items,
               (unsigned long long)results[i].us);
    }

    int ok = complete && rc == DSM_OK && verified && read_ok && remote > 0;
    printf("DIST %s remote=%u total=%u verify=%s lectura=%s\n",
           ok ? "PASS" : "FAIL", remote, got,
           verified ? "ok" : "fallo", read_ok ? "ok" : "fallo");
    return ok;
}

static int run_coordinator(const char *conf)
{
    void *base = NULL;
    uint32_t self = 0;
    uint32_t job_id = 1;

    if (dsm_init(conf, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_alloc(BLOCK_PAGES * PAGE_BYTES, &base) != DSM_OK ||
        dsm_node_id(&self) != DSM_OK ||
        dsm_dist_register(KERNEL_DIST, kernel_xor, (void *)&g_key) != DSM_OK) {
        printf("COORD_FAIL inicialización\n");
        return 1;
    }

    printf("COORD_READY SLOT=%u VA=%p BYTES=%u\n", self, base,
           BLOCK_PAGES * PAGE_BYTES);
    fflush(stdout);

    char line[128];
    int ok = 1;
    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\r\n")] = '\0';
        unsigned slot = 0;
        if (sscanf(line, "LAUNCH %u", &slot) == 1) {
            ok = run_job(job_id++, slot, base) && ok;
        } else if (strcmp(line, "QUIT") == 0) {
            break;
        }
    }

    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

static int run_worker(const char *conf)
{
    uint32_t self = 0;

    if (dsm_init(conf, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_node_id(&self) != DSM_OK ||
        dsm_dist_register(KERNEL_DIST, kernel_xor, (void *)&g_key) != DSM_OK) {
        printf("WORKER_FAIL inicialización\n");
        return 1;
    }

    printf("WORKER_READY SLOT=%u\n", self);
    fflush(stdout);

    char line[128];
    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strcmp(line, "QUIT") == 0) break;
    }

    dsm_thread_detach();
    dsm_finalize();
    return 0;
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc != 3) {
        fprintf(stderr, "Uso: %s coord <config>\n"
                        "     %s worker <config>\n", argv[0], argv[0]);
        return 2;
    }

    if (strcmp(argv[1], "coord") == 0) {
        setenv("DSM_NODE_TAG", "dist_coord", 0);
        return run_coordinator(argv[2]);
    }
    if (strcmp(argv[1], "worker") == 0) {
        setenv("DSM_NODE_TAG", "dist_worker", 0);
        return run_worker(argv[2]);
    }

    fprintf(stderr, "Rol desconocido: %s\n", argv[1]);
    return 2;
}
