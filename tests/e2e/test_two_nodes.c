#include "e2e_harness.h"

#define TEST_APP "./build/bin/debug/demo"

#define PORT_BASE 27664u
#define PORT_SPAN 32u
#define PORT_NEED 8u

#define START_TIMEOUT_MS 90000u
#define JOIN_TIMEOUT_MS  90000u
#define STEP_TIMEOUT_MS  60000u

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
            "VIEW_SIZE=4294967296\n"
            "MAX_RUNTIME_NODES=256\n"
            "VNODES_PER_MB=16\n"
            "REDUNDANCY=false\n"
            "NODE_MEM=67108864\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=%u\n"
            "DISCOVERY_GROUP=239.255.42.101\n"
            "DISCOVERY_PORT=27701\n"
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
            "DSM_LOG_LEVEL=ERROR\n",
            (unsigned)e2e_cluster_id(), (unsigned)port_min,
            (unsigned)port_max);
    fclose(file);
}

/* Exige que cada clave aparezca en la línea de estadísticas con un valor
 * mayor que cero. El formato lo produce msi_print_stats() de apps/demo.c. */
static bool stats_positive(const char *line, const char *const *keys,
                           size_t nkeys)
{
    bool ok = true;
    for (size_t i = 0; i < nkeys; i++) {
        char pattern[64];
        snprintf(pattern, sizeof(pattern), "%s=", keys[i]);
        const char *pos = strstr(line, pattern);
        if (!pos) {
            printf("  FAIL: la línea de stats no incluye '%s'\n", keys[i]);
            ok = false;
            continue;
        }
        unsigned long long value = strtoull(pos + strlen(pattern), NULL, 10);
        if (value == 0) {
            printf("  FAIL: %s vale 0 y se esperaba un valor mayor\n", keys[i]);
            ok = false;
        }
    }
    return ok;
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    e2e_prepare("two_nodes");

    printf("=== Integración DSM: dos nodos y flujo MSI ===\n");

    uint16_t port_min = 0;
    uint16_t port_max = 0;
    if (!e2e_pick_ports(PORT_BASE, PORT_SPAN, PORT_NEED, &port_min, &port_max)) {
        printf("FAIL: no hay un tramo de puertos libre en [%u, %u]\n",
               (unsigned)PORT_BASE, (unsigned)(PORT_BASE + PORT_SPAN));
        return 1;
    }
    write_config(port_min, port_max);
    printf("  Puertos: %u-%u\n", (unsigned)port_min, (unsigned)port_max);

    bool pass = true;
    char line[512] = "";
    char va[64] = "";

    char *a_argv[] = { TEST_APP, "msi", "a", e2e_state.conf_path, NULL };
    e2e_child_t *node_a = e2e_start_child("two_nodes_a", NULL, NULL, a_argv);
    if (!node_a || node_a->pid <= 0) {
        printf("FAIL: no se pudo iniciar nodo A\n");
        e2e_stop_child(node_a);
        return 1;
    }

    if (!e2e_wait_line(node_a, "A_AFTER_WRITE stats", line, sizeof(line),
                       e2e_now_ms() + START_TIMEOUT_MS)) {
        printf("FAIL: el nodo A no completó la escritura inicial\n");
        pass = false;
    }

    if (pass) {
        if (!e2e_wait_line(node_a, "A_READY PASS ", line, sizeof(line),
                           e2e_now_ms() + START_TIMEOUT_MS)) {
            printf("FAIL: el nodo A no publicó su dirección\n");
            pass = false;
        } else {
            const char *pos = strstr(line, "A_READY PASS ");
            pos += strlen("A_READY PASS ");
            size_t n = strcspn(pos, "\r\n");
            if (n == 0 || n >= sizeof(va)) {
                printf("FAIL: la dirección publicada por A no es válida\n");
                pass = false;
            } else {
                memcpy(va, pos, n);
                va[n] = '\0';
            }
        }
    }

    e2e_child_t *node_b = NULL;
    if (pass) {
        char *b_argv[] = { TEST_APP, "msi", "b", e2e_state.conf_path, va,
                           NULL };
        node_b = e2e_start_child("two_nodes_b", NULL, NULL, b_argv);
        if (!node_b || node_b->pid <= 0) {
            printf("FAIL: no se pudo iniciar nodo B\n");
            pass = false;
        }
    }

    if (pass) {
        pass = e2e_wait_line(node_b, "B_BEFORE_READ stats", line, sizeof(line),
                             e2e_now_ms() + JOIN_TIMEOUT_MS) && pass;
        pass = e2e_wait_line(node_b, "B_READ PASS", line, sizeof(line),
                             e2e_now_ms() + STEP_TIMEOUT_MS) && pass;
        pass = e2e_wait_line(node_b, "B_AFTER_READ stats", line, sizeof(line),
                             e2e_now_ms() + STEP_TIMEOUT_MS) && pass;
        pass = e2e_wait_line(node_b, "B_AFTER_WRITE PASS", line, sizeof(line),
                             e2e_now_ms() + STEP_TIMEOUT_MS) && pass;
        pass = e2e_wait_line(node_b, "B_AFTER_WRITE stats", line, sizeof(line),
                             e2e_now_ms() + STEP_TIMEOUT_MS) && pass;
    }

    if (pass) {
        e2e_send(node_a, "READ");
        pass = e2e_wait_line(node_a, "A_AFTER_B_WRITE PASS", line, sizeof(line),
                             e2e_now_ms() + STEP_TIMEOUT_MS) && pass;

        /* El home de la página puede pasar a B cuando B se une al anillo: en
         * ese caso A entrega su copia y la suelta al responder la sonda del
         * nuevo home, sin mensaje de invalidación que contar. La garantía
         * observable es que A deba volver a fallar para leer el valor escrito
         * por B, que es lo que comprueban estas claves. */
        static const char *const a_read_keys[] = {
            "read_faults", "faults_serviced"
        };
        if (pass &&
            e2e_wait_line(node_a, "A_AFTER_READ stats", line, sizeof(line),
                          e2e_now_ms() + STEP_TIMEOUT_MS)) {
            pass = stats_positive(line, a_read_keys,
                                  sizeof(a_read_keys) / sizeof(a_read_keys[0]));
        } else {
            pass = false;
        }
    }

    e2e_stop_child(node_a);
    e2e_stop_child(node_b);

    printf("%s\n", pass ? "PASS: flujo de página entre dos nodos" :
                         "FAIL: flujo de página entre dos nodos");
    return pass ? 0 : 1;
}
