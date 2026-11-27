#include "e2e_harness.h"

#define PRODUCER_APP "./build/bin/debug/dsm_dist_demo"

#define PORT_BASE   27600u
#define PORT_SPAN   32u
#define PORT_NEED   8u

#define START_TIMEOUT_MS 90000u
#define JOIN_TIMEOUT_MS  90000u
#define JOB_TIMEOUT_MS   90000u

static void write_config(uint16_t port_min, uint16_t port_max)
{
    FILE *file = fopen(e2e_state.conf_path, "w");
    if (!file) {
        printf("FAIL: no se pudo escribir la configuración\n");
        return;
    }

    fprintf(file,
            "CLUSTER_ID=0x%08X\n"
            "PAGE_SIZE=4096\n"
            "BASE_ADDR=0x200000000000\n"
            "VIEW_SIZE=268435456\n"
            "MAX_RUNTIME_NODES=256\n"
            "VNODES_PER_MB=16\n"
            "REDUNDANCY=false\n"
            "NODE_MEM=33554432\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=%u\n"
            "DISCOVERY_GROUP=239.255.42.102\n"
            "DISCOVERY_PORT=27702\n"
            "CONNECT_TIMEOUT_MS=1000\n"
            "DISCOVERY_TIMEOUT_MS=1000\n"
            "SEND_TIMEOUT_MS=5000\n"
            "REQUEST_TIMEOUT_MS=5000\n"
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
            "LOCK_TIMEOUT_MS=10000\n"
            "WORKER_QUEUE_CAP=1024\n"
            "WAIT_POOL_ENTRIES=64\n"
            "DIST_TIMEOUT_MS=30000\n"
            "DSM_LOG_LEVEL=ERROR\n",
            (unsigned)e2e_cluster_id(), (unsigned)port_min,
            (unsigned)port_max);
    fclose(file);
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    e2e_prepare("dist");

    char tag_coord[64];
    char tag_worker[64];
    snprintf(tag_coord, sizeof(tag_coord), "e2e_dist_coord_%ld", (long)getpid());
    snprintf(tag_worker, sizeof(tag_worker), "e2e_dist_worker_%ld",
             (long)getpid());

    printf("=== Integración DSM: ejecución distribuida entre dos procesos ===\n");

    uint16_t port_min = 0;
    uint16_t port_max = 0;
    if (!e2e_pick_ports(PORT_BASE, PORT_SPAN, PORT_NEED, &port_min, &port_max)) {
        printf("FAIL: no hay un tramo de puertos libre en [%u, %u]\n",
               (unsigned)PORT_BASE, (unsigned)(PORT_BASE + PORT_SPAN));
        return 1;
    }
    write_config(port_min, port_max);
    printf("  Puertos: %u-%u\n", (unsigned)port_min, (unsigned)port_max);

    char *coord_argv[] = { PRODUCER_APP, "coord", e2e_state.conf_path, NULL };
    char *worker_argv[] = { PRODUCER_APP, "worker", e2e_state.conf_path, NULL };

    bool pass = true;
    e2e_child_t *coord = e2e_start_child("dist_coord", tag_coord, NULL,
                                         coord_argv);
    if (!coord || coord->pid <= 0) {
        printf("FAIL: no se pudo iniciar el coordinador\n");
        e2e_stop_child(coord);
        return 1;
    }

    char line[512] = "";
    unsigned coord_slot = 0;
    if (!e2e_wait_line(coord, "COORD_READY", line, sizeof(line),
                       e2e_now_ms() + START_TIMEOUT_MS) ||
        !e2e_parse_uint_after(line, "SLOT=", &coord_slot)) {
        printf("FAIL: el coordinador no publicó su ranura\n");
        pass = false;
    }

    e2e_child_t *worker = NULL;
    unsigned worker_slot = 0;
    if (pass) {
        worker = e2e_start_child("dist_worker", tag_worker, NULL, worker_argv);
        if (!worker || worker->pid <= 0) {
            printf("FAIL: no se pudo iniciar el trabajador\n");
            pass = false;
        } else if (!e2e_wait_line(worker, "WORKER_READY", line, sizeof(line),
                                  e2e_now_ms() + JOIN_TIMEOUT_MS) ||
                   !e2e_parse_uint_after(line, "SLOT=", &worker_slot) ||
                   worker_slot == coord_slot) {
            printf("FAIL: el trabajador no publicó una ranura distinta\n");
            pass = false;
        }
    }

    if (pass) {
        char command[64];
        snprintf(command, sizeof(command), "LAUNCH %u", worker_slot);
        e2e_send(coord, command);

        static const char *const markers[] = { "DIST ", "DIST_FAIL",
                                               "COORD_FAIL" };
        if (!e2e_wait_any(coord, markers, 3, line, sizeof(line),
                          e2e_now_ms() + JOB_TIMEOUT_MS)) {
            pass = false;
        } else if (!strstr(line, "DIST ") || !strstr(line, "PASS")) {
            pass = false;
        } else if (!strstr(line, "verify=ok") || !strstr(line, "lectura=ok")) {
            printf("  FAIL: la verificación o la lectura no coinciden\n");
            pass = false;
        } else {
            unsigned remote = 0;
            if (!e2e_parse_uint_after(line, "remote=", &remote) || remote == 0) {
                printf("  FAIL: ninguna partición se ejecutó en el trabajador\n");
                pass = false;
            }
        }
    }

    e2e_stop_child(coord);
    e2e_stop_child(worker);

    printf("%s\n", pass ? "PASS: ejecución distribuida con dos procesos reales" :
                         "FAIL: ejecución distribuida con dos procesos reales");
    return pass ? 0 : 1;
}
