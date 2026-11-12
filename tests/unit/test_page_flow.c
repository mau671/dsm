#include "test.h"
#include "dsm.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define CONF_A "/tmp/dsm_page_flow_a.conf"
#define CONF_B "/tmp/dsm_page_flow_b.conf"

static void write_conf(const char *path, uint16_t port_min)
{
    FILE *f = fopen(path, "w");
    if (!f) return;
    fprintf(f,
            "CLUSTER_ID=0x534F3031\n"
            "PAGE_SIZE=4096\n"
            "BASE_ADDR=0x200000000000\n"
            "VIEW_SIZE=4294967296\n"
            "MAX_RUNTIME_NODES=256\n"
            "VNODES_PER_MB=16\n"
            "CHUNK_PAGES=256\n"
            "REDUNDANCY=NONE\n"
            "RAID_K=2\n"
            "NODE_MEM=67108864\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=27699\n"
            "PEER_HOSTS=127.0.0.1\n"
            "CONNECT_TIMEOUT_MS=300\n"
            "DISCOVERY_TIMEOUT_MS=300\n"
            "SEND_TIMEOUT_MS=2000\n"
            "REQUEST_TIMEOUT_MS=3000\n"
            "REQUEST_RETRIES=3\n"
            "RECONNECT_MIN_MS=100\n"
            "RECONNECT_MAX_MS=1500\n"
            "HB_INTERVAL_MS=200\n"
            "HB_SUSPECT=3\n"
            "HB_DEAD=6\n"
            "WORKER_THREADS=2\n"
            "DISPATCHER_THREADS=1\n"
            "PAGE_LOCK_STRIPES=64\n"
            "HOME_SLACK=1.25\n"
            "WAIT_POOL_ENTRIES=1024\n"
            "LOCK_TIMEOUT_MS=5000\n"
            "WORKER_QUEUE_CAP=1024\n"
            "SYNC_THREADS=2\n"
            "OUTPUT_DIR=/tmp\n"
            "DSM_LOG_LEVEL=ERROR\n",
            port_min);
    fclose(f);
}

static int test_remote_page_fault_and_write_invalidation(void)
{
    write_conf(CONF_A, 27650);
    write_conf(CONF_B, 27650);

    int ready[2];
    int command[2];
    REQUIRE(pipe(ready) == 0);
    REQUIRE(pipe(command) == 0);

    pid_t child = fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        close(ready[0]);
        close(command[1]);
        setenv("DSM_NODE_TAG", "page_flow_b", 1);
        if (dsm_init(CONF_B, 0) != DSM_OK ||
            dsm_thread_attach() != DSM_OK) {
            _exit(2);
        }

        void *base = NULL;
        if (dsm_alloc(4096, &base) != DSM_OK) _exit(3);
        volatile uint8_t *page = (volatile uint8_t *)base;
        uint8_t initial = page[0];
        page[0] = 0x5Au;
        uint8_t result[2] = { initial, page[0] };
        (void)write(ready[1], result, sizeof(result));

        char stop = 0;
        (void)read(command[0], &stop, 1);
        dsm_thread_detach();
        dsm_finalize();
        _exit(0);
    }

    close(ready[1]);
    close(command[0]);
    setenv("DSM_NODE_TAG", "page_flow_a", 1);
    REQUIRE(dsm_init(CONF_A, 0) == DSM_OK);
    REQUIRE(dsm_thread_attach() == DSM_OK);

    uint8_t child_values[2] = { 0xFF, 0xFF };
    REQUIRE(read(ready[0], child_values, sizeof(child_values)) ==
            (ssize_t)sizeof(child_values));
    CHECK(child_values[0] == 0);
    CHECK(child_values[1] == 0x5A);

    void *base = NULL;
    REQUIRE(dsm_alloc(4096, &base) == DSM_OK);
    volatile uint8_t *page = (volatile uint8_t *)base;
    CHECK(page[0] == 0x5A);

    char stop = 'X';
    CHECK(write(command[1], &stop, 1) == 1);
    close(command[1]);
    close(ready[0]);

    int status = 0;
    CHECK(waitpid(child, &status, 0) == child);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);

    dsm_thread_detach();
    CHECK(dsm_finalize() == DSM_OK);
    unlink(CONF_A);
    unlink(CONF_B);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Flujo minimo de fallo de pagina MSI ===\n");
    RUN_TEST(test_remote_page_fault_and_write_invalidation);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
