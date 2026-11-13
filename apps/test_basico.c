#include "dsm.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_BYTES 4096u
#define WORD_COUNT (PAGE_BYTES / sizeof(uint32_t))

static void print_stats(const char *label)
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

static void fill_pattern(uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < WORD_COUNT; i++) {
        page[i] = base + i;
    }
}

static int check_pattern(const uint32_t *page, uint32_t base)
{
    for (uint32_t i = 0; i < WORD_COUNT; i++) {
        if (page[i] != base + i) return 0;
    }
    return 1;
}

static int wait_for_command(const char *expected)
{
    char command[32];
    if (!fgets(command, sizeof(command), stdin)) return 0;
    command[strcspn(command, "\r\n")] = '\0';
    return strcmp(command, expected) == 0;
}

static int run_node_a(const char *conf_path)
{
    void *base = NULL;
    int ok = 1;

    if (dsm_init(conf_path, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("A INIT/ALLOC FAIL\n");
        return 1;
    }

    fill_pattern((uint32_t *)base, 0xA0000000u);
    print_stats("A_AFTER_WRITE");
    printf("A_READY PASS\n");
    fflush(stdout);

    if (!wait_for_command("READ")) {
        ok = 0;
    } else {
        ok = check_pattern((const uint32_t *)base, 0xB0000000u);
        printf("A_AFTER_B_WRITE %s\n", ok ? "PASS" : "FAIL");
        print_stats("A_AFTER_READ");
    }

    if (!wait_for_command("QUIT")) ok = 0;
    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

static int run_node_b(const char *conf_path)
{
    void *base = NULL;
    int ok = 1;

    if (dsm_init(conf_path, 0) != DSM_OK ||
        dsm_thread_attach() != DSM_OK ||
        dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("B INIT/ALLOC FAIL\n");
        return 1;
    }

    print_stats("B_BEFORE_READ");
    ok = check_pattern((const uint32_t *)base, 0xA0000000u);
    printf("B_READ %s\n", ok ? "PASS" : "FAIL");
    print_stats("B_AFTER_READ");

    fill_pattern((uint32_t *)base, 0xB0000000u);
    printf("B_AFTER_WRITE PASS\n");
    print_stats("B_AFTER_WRITE");
    fflush(stdout);

    if (!wait_for_command("QUIT")) ok = 0;
    dsm_thread_detach();
    dsm_finalize();
    return ok ? 0 : 1;
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc != 3 || (strcmp(argv[1], "A") != 0 && strcmp(argv[1], "B") != 0)) {
        fprintf(stderr, "Uso: %s <A|B> <config>\n", argv[0]);
        return 2;
    }

    setenv("DSM_NODE_TAG",
           strcmp(argv[1], "A") == 0 ? "integration_a" : "integration_b", 1);
    return (strcmp(argv[1], "A") == 0)
               ? run_node_a(argv[2])
               : run_node_b(argv[2]);
}
