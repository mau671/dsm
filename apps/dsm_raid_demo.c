#include "dsm.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PAGE_BYTES 4096u

/*
 * Demostración de la redundancia con paridad. El productor se ejecuta como cliente
 * del clúster, reserva un bloque de la vista y escribe un patrón conocido. Tras
 * dsm_sync el contenido queda en los custodios y la paridad en sus franjas. El
 * cliente termina, de modo que ninguna copia local sobrevive. Los procesos
 * "hold" representan los nodos que aportan memoria y pueden leer el bloque con
 * el mismo desplazamiento de la vista.
 */

static uint8_t pattern_byte(uint32_t page, uint32_t offset)
{
    return (uint8_t)(((page * 131u) + (offset * 17u) + 0x5Au) & 0xFFu);
}

static void fill_page(uint8_t *page, uint32_t index)
{
    for (uint32_t i = 0; i < PAGE_BYTES; i++) {
        page[i] = pattern_byte(index, i);
    }
}

static int check_page(const uint8_t *page, uint32_t index)
{
    for (uint32_t i = 0; i < PAGE_BYTES; i++) {
        if (page[i] != pattern_byte(index, i)) return 0;
    }
    return 1;
}

static int parse_pages(const char *text, uint32_t *out)
{
    char *end = NULL;
    unsigned long v = strtoul(text, &end, 0);
    if (!text || end == text || *end != '\0' || v == 0 || v > 4096) {
        return 0;
    }
    *out = (uint32_t)v;
    return 1;
}

static int run_hold(const char *conf)
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
            if (check_page(base + (size_t)index * PAGE_BYTES, index)) {
                printf("READPAGE PASS index=%u\n", index);
            } else {
                printf("READPAGE FAIL index=%u\n", index);
                ok = 0;
            }
        } else if (sscanf(line, "READ 0x%llx %u", &va, &pages) == 2 && pages > 0) {
            uint8_t *base = (uint8_t *)(uintptr_t)va;
            int read_ok = 1;
            for (uint32_t p = 0; p < pages; p++) {
                if (!check_page(base + (size_t)p * PAGE_BYTES, p)) {
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

static int run_producer(const char *conf, uint32_t pages, uint32_t members)
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
        fill_page((uint8_t *)base + (size_t)p * PAGE_BYTES, p);
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
        read_ok = check_page((uint8_t *)base + (size_t)p * PAGE_BYTES, p);
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

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc >= 3 && strcmp(argv[1], "hold") == 0) {
        char tag[64];
        snprintf(tag, sizeof(tag), "raid_hold_%ld", (long)getpid());
        setenv("DSM_NODE_TAG", tag, 0);
        return run_hold(argv[2]);
    }

    if (argc == 5 && strcmp(argv[1], "produce") == 0) {
        uint32_t pages = 0;
        uint32_t members = 0;
        if (!parse_pages(argv[3], &pages) || !parse_pages(argv[4], &members)) {
            fprintf(stderr, "Uso: %s produce <config> <páginas> <nodos>\n",
                    argv[0]);
            return 2;
        }
        char tag[64];
        snprintf(tag, sizeof(tag), "raid_producer_%ld", (long)getpid());
        setenv("DSM_NODE_TAG", tag, 0);
        return run_producer(argv[2], pages, members);
    }

    fprintf(stderr, "Uso: %s hold <config>\n"
                    "     %s produce <config> <páginas> <nodos>\n",
            argv[0], argv[0]);
    return 2;
}
