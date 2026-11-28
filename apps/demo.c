#include "dsm.h"
#include "util.h"
#include "wire.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PAGE_BYTES      4096u

/* --- Ejecución distribuida --- */

#define BLOCK_PAGES     4u
#define PARTS           8u
#define PART_BYTES      1024u
#define KERNEL_DIST     0x44495354u

/*
 * El coordinador reserva un bloque de la vista DSM, lo llena con un patrón,
 * reparte particiones entre su propia ranura y una ranura remota, recoge los
 * resultados y comprueba el CRC32 y el contenido. El trabajador registra el
 * mismo kernel y espera las peticiones DIST_RUN que llegan por la red.
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

static void dist_fill_source(uint8_t *src, uint32_t bytes)
{
    for (uint32_t i = 0; i < bytes; i++) {
        src[i] = (uint8_t)((i * 7u + 3u) & 0xFFu);
    }
}

static int dist_verify_output(const uint8_t *dst, uint32_t bytes)
{
    for (uint32_t i = 0; i < bytes; i++) {
        uint8_t want = (uint8_t)(((i * 7u + 3u) & 0xFFu) ^ g_key);
        if (dst[i] != want) return 0;
    }
    return 1;
}

static int dist_run_job(uint32_t job_id, uint32_t worker_slot, void *base)
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

    dist_fill_source(src, bytes);
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
    int read_ok = dist_verify_output(dst, bytes);

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

static int dist_run_coordinator(const char *conf)
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
            ok = dist_run_job(job_id++, slot, base) && ok;
        } else if (strcmp(line, "QUIT") == 0) {
            break;
        }
    }

    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

static int dist_run_worker(const char *conf)
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

/* --- Redundancia con paridad --- */

/*
 * El productor se ejecuta como cliente del clúster, reserva un bloque de la
 * vista y escribe un patrón conocido. Tras dsm_sync el contenido queda en los
 * custodios y la paridad en sus franjas. El cliente termina, de modo que
 * ninguna copia local sobrevive. Los procesos hold representan los nodos que
 * aportan memoria y pueden leer el bloque con el mismo desplazamiento de la
 * vista.
 */

static uint8_t raid_pattern_byte(uint32_t page, uint32_t offset)
{
    return (uint8_t)(((page * 131u) + (offset * 17u) + 0x5Au) & 0xFFu);
}

static void raid_fill_page(uint8_t *page, uint32_t index)
{
    for (uint32_t i = 0; i < PAGE_BYTES; i++) {
        page[i] = raid_pattern_byte(index, i);
    }
}

static int raid_check_page(const uint8_t *page, uint32_t index)
{
    for (uint32_t i = 0; i < PAGE_BYTES; i++) {
        if (page[i] != raid_pattern_byte(index, i)) return 0;
    }
    return 1;
}

static int parse_count(const char *text, uint32_t *out)
{
    char *end = NULL;
    unsigned long v = strtoul(text, &end, 0);
    if (!text || end == text || *end != '\0' || v == 0 || v > 4096) {
        return 0;
    }
    *out = (uint32_t)v;
    return 1;
}

static int raid_run_hold(const char *conf)
{
    uint32_t self = 0;

    if (dsm_init(conf, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_node_id(&self) != DSM_OK) {
        printf("HOLD_FAIL inicialización\n");
        return 1;
    }

    printf("HOLD_READY SLOT=%u\n", self);
    fflush(stdout);

    char line[160];
    int ok = 1;
    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\r\n")] = '\0';

        unsigned long long va = 0;
        unsigned pages = 0;
        unsigned index = 0;
        if (sscanf(line, "READPAGE 0x%llx %u", &va, &index) == 2) {
            uint8_t *base = (uint8_t *)(uintptr_t)va;
            if (raid_check_page(base + (size_t)index * PAGE_BYTES, index)) {
                printf("READPAGE PASS index=%u\n", index);
            } else {
                printf("READPAGE FAIL index=%u\n", index);
                ok = 0;
            }
        } else if (sscanf(line, "READ 0x%llx %u", &va, &pages) == 2 && pages > 0) {
            uint8_t *base = (uint8_t *)(uintptr_t)va;
            int read_ok = 1;
            for (uint32_t p = 0; p < pages; p++) {
                if (!raid_check_page(base + (size_t)p * PAGE_BYTES, p)) {
                    printf("READ_FAIL página=%u\n", p);
                    read_ok = 0;
                    ok = 0;
                    break;
                }
            }
            if (read_ok) {
                printf("READ PASS pages=%u\n", pages);
            }
        } else if (strcmp(line, "QUIT") == 0) {
            break;
        }
    }

    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

static int raid_run_producer(const char *conf, uint32_t pages, uint32_t members)
{
    void *base = NULL;
    uint32_t self = 0;
    uint64_t bytes = (uint64_t)pages * PAGE_BYTES;

    if (dsm_init(conf, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_node_id(&self) != DSM_OK) {
        printf("PRODUCE_FAIL inicialización\n");
        return 1;
    }

    dsm_status_t wait_rc = dsm_wait_members(members, 30000u);
    if (wait_rc != DSM_OK) {
        printf("PRODUCE_FAIL no se reunieron %u nodos: %s\n", members,
               dsm_strerror(wait_rc));
        dsm_thread_detach();
        dsm_finalize();
        return 1;
    }

    if (dsm_alloc(bytes, &base) != DSM_OK) {
        printf("PRODUCE_FAIL dsm_alloc\n");
        dsm_thread_detach();
        dsm_finalize();
        return 1;
    }

    for (uint32_t p = 0; p < pages; p++) {
        raid_fill_page((uint8_t *)base + (size_t)p * PAGE_BYTES, p);
    }

    dsm_status_t rc = dsm_sync();
    if (rc != DSM_OK) {
        printf("PRODUCE_FAIL dsm_sync=%s\n", dsm_strerror(rc));
        dsm_thread_detach();
        dsm_finalize();
        return 1;
    }

    int read_ok = 1;
    for (uint32_t p = 0; p < pages && read_ok; p++) {
        read_ok = raid_check_page((uint8_t *)base + (size_t)p * PAGE_BYTES, p);
    }

    dsm_stats_t st;
    memset(&st, 0, sizeof(st));
    (void)dsm_stats(&st);

    printf("RAID_READY VA=%p PAGES=%u SLOT=%u SYNC=ok LECTURA=%s "
           "DELTAS_LOCALES=%llu\n",
           base, pages, self, read_ok ? "ok" : "fallo",
           (unsigned long long)st.parity_deltas);
    fflush(stdout);

    dsm_thread_detach();
    dsm_finalize();
    return read_ok ? 0 : 1;
}

/* --- Coherencia MSI entre dos procesos --- */

#define WORD_COUNT (PAGE_BYTES / sizeof(uint32_t))

static void msi_print_stats(const char *label)
{
    dsm_stats_t stats;
    if (dsm_stats(&stats) != DSM_OK) {
        printf("%s stats=FAIL\n", label);
        fflush(stdout);
        return;
    }

    printf("%s stats read_faults=%lu write_faults=%lu "
           "pages_m=%lu pages_s=%lu pages_i=%lu "
           "msgs_sent=%lu msgs_recv=%lu invalidations=%lu "
           "faults_serviced=%lu fault_latency_us_total=%lu "
           "fault_latency_us_avg=%lu\n",
           label,
           (unsigned long)stats.read_faults,
           (unsigned long)stats.write_faults,
           (unsigned long)stats.pages_m,
           (unsigned long)stats.pages_s,
           (unsigned long)stats.pages_i,
           (unsigned long)stats.msgs_sent,
           (unsigned long)stats.msgs_recv,
           (unsigned long)stats.invalidations,
           (unsigned long)stats.faults_serviced,
           (unsigned long)stats.fault_latency_us_total,
           (unsigned long)stats.fault_latency_us_avg);
    fflush(stdout);
}

static void msi_fill_pattern(uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < WORD_COUNT; i++) {
        page[i] = base + i;
    }
}

static int msi_check_pattern(const uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < WORD_COUNT; i++) {
        if (page[i] != base + i) return 0;
    }
    return 1;
}

static int msi_wait_for_command(const char *expected)
{
    char command[32];
    if (!fgets(command, sizeof(command), stdin)) return 0;
    command[strcspn(command, "\r\n")] = '\0';
    return strcmp(command, expected) == 0;
}

static int msi_run_a(const char *conf_path)
{
    void *base = NULL;
    int ok = 1;

    if (dsm_init(conf_path, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("A INIT/ALLOC FAIL\n");
        return 1;
    }

    msi_fill_pattern((uint32_t *)base, 0xA0000000u);
    msi_print_stats("A_AFTER_WRITE");
    printf("A_READY PASS %p\n", base);
    fflush(stdout);

    if (!msi_wait_for_command("READ")) {
        ok = 0;
    } else {
        ok = msi_check_pattern((const uint32_t *)base, 0xB0000000u);
        printf("A_AFTER_B_WRITE %s\n", ok ? "PASS" : "FAIL");
        msi_print_stats("A_AFTER_READ");
    }

    if (!msi_wait_for_command("QUIT")) ok = 0;
    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

static int msi_run_b(const char *conf_path, const char *va_text)
{
    void *own = NULL;
    int ok = 1;

    char *end = NULL;
    unsigned long long raw = strtoull(va_text, &end, 0);

    if (end == va_text || raw == 0 ||
        raw % (unsigned long long)PAGE_BYTES != 0) {
        printf("B INIT/ADDR FAIL\n");
        return 1;
    }
    void *base = (void *)(uintptr_t)raw;

    if (dsm_init(conf_path, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_alloc(PAGE_BYTES, &own) != DSM_OK) {
        printf("B INIT/ALLOC FAIL\n");
        return 1;
    }

    msi_print_stats("B_BEFORE_READ");
    ok = msi_check_pattern((const uint32_t *)base, 0xA0000000u);
    printf("B_READ %s\n", ok ? "PASS" : "FAIL");
    msi_print_stats("B_AFTER_READ");

    msi_fill_pattern((uint32_t *)base, 0xB0000000u);
    printf("B_AFTER_WRITE PASS\n");
    msi_print_stats("B_AFTER_WRITE");
    fflush(stdout);

    if (!msi_wait_for_command("QUIT")) ok = 0;
    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

/* --- Entrada --- */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Uso: %s dist coord <config>\n"
            "     %s dist worker <config>\n"
            "     %s raid hold <config>\n"
            "     %s raid produce <config> <páginas> <nodos>\n"
            "     %s msi a <config>\n"
            "     %s msi b <config> <dirección-de-a>\n",
            prog, prog, prog, prog, prog, prog);
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc >= 4 && strcmp(argv[1], "dist") == 0) {
        if (strcmp(argv[2], "coord") == 0 && argc == 4) {
            setenv("DSM_NODE_TAG", "dist_coord", 0);
            return dist_run_coordinator(argv[3]);
        }
        if (strcmp(argv[2], "worker") == 0 && argc == 4) {
            setenv("DSM_NODE_TAG", "dist_worker", 0);
            return dist_run_worker(argv[3]);
        }
    }

    if (argc >= 4 && strcmp(argv[1], "raid") == 0) {
        if (strcmp(argv[2], "hold") == 0 && argc == 4) {
            char tag[64];
            snprintf(tag, sizeof(tag), "raid_hold_%ld", (long)getpid());
            setenv("DSM_NODE_TAG", tag, 0);
            return raid_run_hold(argv[3]);
        }
        if (strcmp(argv[2], "produce") == 0 && argc == 6) {
            uint32_t pages = 0;
            uint32_t members = 0;
            if (!parse_count(argv[4], &pages) ||
                !parse_count(argv[5], &members)) {
                usage(argv[0]);
                return 2;
            }
            char tag[64];
            snprintf(tag, sizeof(tag), "raid_producer_%ld", (long)getpid());
            setenv("DSM_NODE_TAG", tag, 0);
            return raid_run_producer(argv[3], pages, members);
        }
    }

    if (argc >= 4 && strcmp(argv[1], "msi") == 0) {
        char tag[64];
        if (strcmp(argv[2], "a") == 0 && argc == 4) {
            snprintf(tag, sizeof(tag), "msi_a_%ld", (long)getpid());
            setenv("DSM_NODE_TAG", tag, 1);
            return msi_run_a(argv[3]);
        }
        if (strcmp(argv[2], "b") == 0 && argc == 5) {
            snprintf(tag, sizeof(tag), "msi_b_%ld", (long)getpid());
            setenv("DSM_NODE_TAG", tag, 1);
            return msi_run_b(argv[3], argv[4]);
        }
    }

    usage(argv[0]);
    return 2;
}
