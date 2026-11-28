#include "e2e_harness.h"
#include "dsm_types.h"

#define RAID_APP   "./build/bin/debug/demo"
#define DSMCTL_APP "./build/bin/debug/dsmctl"

#define PORT_BASE   27632u
#define PORT_SPAN   32u
#define PORT_NEED   8u

#define NODE_COUNT  3u
#define TEST_PAGES  24u
#define STRIPE_K    2u

#define START_TIMEOUT_MS   90000u
#define JOIN_TIMEOUT_MS    90000u
#define TOOL_TIMEOUT_MS    15000u
#define PRODUCE_TIMEOUT_MS 60000u
#define REBUILD_TIMEOUT_MS 120000u
#define READ_TIMEOUT_MS    60000u
#define DIR_TIMEOUT_MS     30000u

typedef struct {
    uint32_t victim;
    uint32_t pages[4096];
    uint32_t npages;
} victim_plan_t;

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
            "REDUNDANCY=true\n"
            "CHUNK_PAGES=4\n"
            "NODE_MEM=16777216\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=%u\n"
            "DISCOVERY_GROUP=239.255.42.103\n"
            "DISCOVERY_PORT=27703\n"
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
            "OUTPUT_DIR=%s\n"
            "DSM_LOG_LEVEL=ERROR\n",
            (unsigned)e2e_cluster_id(), (unsigned)port_min,
            (unsigned)port_max, e2e_state.report_dir);
    fclose(file);
}

static bool run_ctl(uint16_t port, char *const args[], char *out, size_t capacity)
{
    return e2e_run_tool(DSMCTL_APP, e2e_state.conf_path, port, args, out,
                        capacity, e2e_now_ms() + TOOL_TIMEOUT_MS) == 0;
}

/* Una consulta a cualquier puerto del tramo devuelve la tabla de membresía
 * completa; se prueba cada puerto hasta dar con el de un nodo vivo. */
static bool query_nodes(uint16_t port_min, uint16_t port_max,
                        uint16_t ports[MAX_NODES])
{
    char *args[] = { "nodes", NULL };
    char *out = malloc(65536);
    if (!out) return false;

    for (uint16_t port = port_min; port <= port_max; port++) {
        memset(ports, 0, sizeof(uint16_t) * MAX_NODES);
        if (!run_ctl(port, args, out, 65536)) continue;

        bool any = false;
        char *save = NULL;
        for (char *line = strtok_r(out, "\n", &save); line;
             line = strtok_r(NULL, "\n", &save)) {
            unsigned slot = 0;
            unsigned listed_port = 0;
            char uuid[40], ip[20], role[12], state[20];
            if (sscanf(line, "%u %39s %19s %u %11s %19s",
                       &slot, uuid, ip, &listed_port, role, state) != 6) {
                continue;
            }
            if (slot < MAX_NODES && listed_port > 0 && listed_port <= 65535) {
                ports[slot] = (uint16_t)listed_port;
                any = true;
            }
        }
        if (any) {
            free(out);
            return true;
        }
    }

    free(out);
    return false;
}

static bool fetch_ports(uint16_t port_min, uint16_t port_max,
                        const uint32_t slots[NODE_COUNT],
                        uint16_t ports[MAX_NODES], uint64_t deadline)
{
    do {
        if (query_nodes(port_min, port_max, ports)) {
            bool complete = true;
            for (uint32_t i = 0; i < NODE_COUNT; i++) {
                if (slots[i] >= MAX_NODES || ports[slots[i]] == 0) {
                    complete = false;
                    break;
                }
            }
            if (complete) return true;
        }
        e2e_sleep_ms(200);
    } while (e2e_now_ms() < deadline);

    return false;
}

static bool parse_stat(const char *text, const char *label,
                       unsigned long long *value)
{
    const char *pos = strstr(text, label);
    if (!pos) return false;
    pos += strlen(label);
    while (*pos == ' ' || *pos == ':' || *pos == '\t') pos++;
    char *end = NULL;
    unsigned long long parsed = strtoull(pos, &end, 10);
    if (end == pos) return false;
    *value = parsed;
    return true;
}

static bool query_stats(uint16_t port, unsigned long long *deltas,
                        unsigned long long *rebuilds)
{
    char *args[] = { "stats", NULL };
    char *out = malloc(65536);
    if (!out) return false;

    bool ok = run_ctl(port, args, out, 65536) &&
              parse_stat(out, "Deltas de paridad", deltas) &&
              parse_stat(out, "Franjas reconstruidas", rebuilds);
    free(out);
    return ok;
}

/* Elige una víctima y las páginas cuya franja puede reconstruirse. Una franja
 * con dos datos en el mismo nodo pierde dos incógnitas y no se recupera; esas
 * páginas no se leen. Las páginas elegidas son datos del nodo caído con su
 * compañera de franja viva, de modo que la lectura ejerce la reconstrucción. */
static bool choose_victim(uint16_t port, uint32_t pages,
                          const uint32_t slots[NODE_COUNT],
                          uint32_t home_count[MAX_NODES], victim_plan_t *plan)
{
    memset(plan, 0, sizeof(*plan));
    memset(home_count, 0, sizeof(uint32_t) * MAX_NODES);

    char first[32];
    snprintf(first, sizeof(first), "0");
    char limit[32];
    snprintf(limit, sizeof(limit), "%u", pages);

    char *args[] = { "dir", first, limit, NULL };
    char *out = malloc(65536);
    if (!out) return false;

    bool queried = false;
    uint64_t deadline = e2e_now_ms() + DIR_TIMEOUT_MS;
    do {
        if (run_ctl(port, args, out, 65536)) {
            queried = true;
            break;
        }
        e2e_sleep_ms(200);
    } while (e2e_now_ms() < deadline);
    if (!queried) {
        free(out);
        return false;
    }

    uint32_t home_of_page[4096];
    for (uint32_t i = 0; i < 4096; i++) home_of_page[i] = MAX_NODES;

    char *save = NULL;
    for (char *line = strtok_r(out, "\n", &save); line;
         line = strtok_r(NULL, "\n", &save)) {
        unsigned page = 0;
        unsigned home = 0;
        if (sscanf(line, "%u %u", &page, &home) != 2) continue;
        if (page >= pages || page >= 4096) continue;
        home_of_page[page] = home;
        if (home < MAX_NODES) home_count[home]++;
    }
    free(out);

    for (uint32_t idx = 0; idx < NODE_COUNT; idx++) {
        uint32_t slot = slots[idx];
        uint32_t candidates[4096];
        uint32_t ncand = 0;

        for (uint32_t p = 0; p < pages; p++) {
            if (home_of_page[p] != slot) continue;
            uint32_t stripe = p / STRIPE_K;
            uint32_t first_page = stripe * STRIPE_K;
            uint32_t last_page = first_page + STRIPE_K;
            if (last_page > pages) last_page = pages;
            uint32_t count = 0;
            for (uint32_t q = first_page; q < last_page; q++) {
                if (home_of_page[q] == slot) count++;
            }
            if (count == 1) candidates[ncand++] = p;
        }

        if (ncand > plan->npages) {
            plan->victim = slot;
            plan->npages = ncand < 4096u ? ncand : 4096u;
            memcpy(plan->pages, candidates, plan->npages * sizeof(uint32_t));
        }
    }

    return plan->npages > 0;
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    e2e_prepare("raid");

    printf("=== Integración DSM: paridad, caída de un nodo y reconstrucción ===\n");

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
    uint32_t slots[NODE_COUNT] = { 0 };
    e2e_child_t *nodes[NODE_COUNT] = { NULL };
    uint16_t ports[MAX_NODES] = { 0 };

    for (uint32_t i = 0; i < NODE_COUNT && pass; i++) {
        char name[32];
        char tag[64];
        snprintf(name, sizeof(name), "raid_hold_%u", i);
        snprintf(tag, sizeof(tag), "e2e_raid_hold_%ld_%u", (long)getpid(), i);

        char *argv[] = { RAID_APP, "raid", "hold", e2e_state.conf_path, NULL };
        nodes[i] = e2e_start_child(name, tag, NULL, argv);
        if (!nodes[i] || nodes[i]->pid <= 0) {
            printf("FAIL: no se pudo iniciar el nodo %u\n", i);
            pass = false;
        }
    }

    /* Los tres nodos arrancan a la vez: con redundancia activa cada uno espera
     * a que la membresía reúna el mínimo antes de publicar su ranura. */
    for (uint32_t i = 0; i < NODE_COUNT && pass; i++) {
        char line[512];
        if (!e2e_wait_line(nodes[i], "HOLD_READY", line, sizeof(line),
                           e2e_now_ms() + START_TIMEOUT_MS) ||
            !e2e_parse_uint_after(line, "SLOT=", &slots[i])) {
            printf("FAIL: el nodo %u no publicó su ranura\n", i);
            pass = false;
        }
        for (uint32_t j = 0; j < i; j++) {
            if (slots[i] == slots[j]) {
                printf("FAIL: los nodos %u y %u comparten la ranura %u\n",
                       j, i, slots[i]);
                pass = false;
            }
        }
    }

    /* Cada proceso tomó el primer puerto libre del rango; la ranura la fija
     * el reparto, no el orden de arranque. La tabla de membresía da el par
     * ranura-puerto vigente. */
    if (pass &&
        !fetch_ports(port_min, port_max, slots, ports,
                     e2e_now_ms() + JOIN_TIMEOUT_MS)) {
        printf("FAIL: la tabla de membresía no publicó los puertos\n");
        pass = false;
    }
    if (pass) {
        printf("  Puertos por ranura:");
        for (uint32_t i = 0; i < NODE_COUNT; i++) {
            printf(" %u=%u", slots[i], ports[slots[i]]);
        }
        printf("\n");
    }

    uint16_t live_port = 0;
    if (pass) {
        for (uint32_t i = 0; i < NODE_COUNT; i++) {
            if (ports[slots[i]] != 0) {
                live_port = ports[slots[i]];
                break;
            }
        }
    }

    e2e_child_t *producer = NULL;
    unsigned long long va = 0;
    unsigned pages = 0;
    if (pass) {
        char pages_text[16];
        char nodes_text[16];
        snprintf(pages_text, sizeof(pages_text), "%u", TEST_PAGES);
        snprintf(nodes_text, sizeof(nodes_text), "%u", NODE_COUNT);
        char tag[64];
        snprintf(tag, sizeof(tag), "e2e_raid_producer_%ld", (long)getpid());

        char *argv[] = { RAID_APP, "raid", "produce", e2e_state.conf_path,
                         pages_text, nodes_text, NULL };
        producer = e2e_start_child("raid_producer", tag, "client", argv);
        if (!producer || producer->pid <= 0) {
            printf("FAIL: no se pudo iniciar el productor\n");
            pass = false;
        } else {
            char ready[512] = "";
            if (!e2e_wait_line(producer, "RAID_READY", ready, sizeof(ready),
                               e2e_now_ms() + PRODUCE_TIMEOUT_MS)) {
                printf("FAIL: el productor no completó la sincronización\n");
                pass = false;
            } else {
                char *end = NULL;
                const char *vpos = strstr(ready, "VA=");
                if (!vpos) {
                    printf("FAIL: el productor no publicó la vista\n");
                    pass = false;
                } else {
                    va = strtoull(vpos + 3, &end, 0);
                    if (end == vpos + 3 || va == 0 ||
                        !e2e_parse_uint_after(ready, "PAGES=", &pages)) {
                        printf("FAIL: el productor publicó datos inválidos\n");
                        pass = false;
                    }
                }
            }
        }
    }

    /* El productor imprime RAID_READY tras sincronizar y verificar; su salida
     * limpia confirma que el bloque quedó en los custodios. */
    if (pass) {
        int status = 0;
        if (!e2e_wait_exit(producer, e2e_now_ms() + PRODUCE_TIMEOUT_MS,
                           &status)) {
            printf("FAIL: el productor no terminó su cierre\n");
            e2e_report_child(producer);
            pass = false;
        } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            printf("FAIL: el productor terminó con error\n");
            e2e_report_child(producer);
            pass = false;
        }
    }
    e2e_stop_child(producer);

    uint32_t home_count[MAX_NODES] = { 0 };
    victim_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    if (pass) {
        if (!choose_victim(live_port, pages, slots, home_count, &plan)) {
            printf("FAIL: ninguna franja permite reconstruir un dato tras la baja\n");
            pass = false;
        } else {
            printf("  Custodios:");
            for (uint32_t i = 0; i < NODE_COUNT; i++) {
                printf(" %u=%u", slots[i], home_count[slots[i]]);
            }
            printf("; víctima=%u; páginas reconstruibles=%u\n",
                   plan.victim, plan.npages);
        }
    }

    /* Estadísticas previas a la caída: el delta de paridad se emitió durante
     * la persistencia del productor. */
    if (pass) {
        unsigned long long deltas = 0;
        unsigned long long rebuilds = 0;
        uint64_t deadline = e2e_now_ms() + DIR_TIMEOUT_MS;
        bool observed = false;
        do {
            if (query_stats(live_port, &deltas, &rebuilds) && deltas > 0) {
                observed = true;
                break;
            }
            e2e_sleep_ms(200);
        } while (e2e_now_ms() < deadline);

        if (!observed) {
            printf("FAIL: no se observaron deltas de paridad\n");
            pass = false;
        } else {
            printf("  Deltas de paridad antes de la caída: %llu\n", deltas);
        }
    }

    e2e_child_t *victim_child = NULL;
    if (pass) {
        for (uint32_t i = 0; i < NODE_COUNT; i++) {
            if (slots[i] == plan.victim) {
                victim_child = nodes[i];
                break;
            }
        }
        if (!victim_child || victim_child->pid <= 0) {
            printf("FAIL: no se encontró el proceso de la víctima\n");
            pass = false;
        } else {
            printf("  Se derriba el nodo %u\n", plan.victim);
            kill(victim_child->pid, SIGKILL);
            int status = 0;
            waitpid(victim_child->pid, &status, 0);
            victim_child->pid = -1;
            victim_child->exit_status = status;
            victim_child->exited = true;
        }
    }

    /* Espera la detección de la baja y la reconstrucción de las franjas. La
     * cuenta debe dejar de crecer antes de leer: un fallo de página que llega
     * mientras la franja aún se reconstruye es terminal para el hilo. */
    e2e_child_t *reader_child = NULL;
    uint16_t reader_port = 0;
    unsigned long long rebuilds = 0;
    if (pass) {
        for (uint32_t i = 0; i < NODE_COUNT; i++) {
            if (slots[i] != plan.victim && nodes[i]->pid > 0) {
                reader_child = nodes[i];
                reader_port = ports[slots[i]];
                break;
            }
        }
        if (!reader_child || reader_port == 0) {
            printf("FAIL: no quedó ningún nodo lector vivo\n");
            pass = false;
        }
    }

    if (pass) {
        unsigned long long previous = (unsigned long long)-1;
        unsigned stable = 0;
        unsigned long long deltas = 0;
        uint64_t deadline = e2e_now_ms() + REBUILD_TIMEOUT_MS;
        do {
            e2e_sleep_ms(500);
            if (!query_stats(reader_port, &deltas, &rebuilds)) continue;
            if (rebuilds == previous) {
                stable++;
                if (stable >= 4 && rebuilds > 0) break;
            } else {
                stable = 0;
                previous = rebuilds;
            }
        } while (e2e_now_ms() < deadline);

        printf("  Franjas reconstruidas: %llu\n", rebuilds);
        if (rebuilds == 0) {
            printf("FAIL: no se observó ninguna reconstrucción\n");
            pass = false;
        }
    }

    if (pass) {
        /* Se leen solo páginas cuyo dato custodiaba la víctima y cuya franja
         * conserva la compañera y la paridad. La primera lectura de cada
         * página ejerce la reconstrucción. */
        uint32_t to_read = plan.npages < 8u ? plan.npages : 8u;
        uint32_t read_ok = 0;
        for (uint32_t i = 0; i < to_read && pass; i++) {
            char command[160];
            snprintf(command, sizeof(command), "READPAGE 0x%llx %u",
                     va, plan.pages[i]);
            e2e_send(reader_child, command);
            char response[512] = "";
            if (!e2e_wait_line(reader_child, "READPAGE ", response,
                               sizeof(response),
                               e2e_now_ms() + READ_TIMEOUT_MS) ||
                !strstr(response, "PASS")) {
                pass = false;
                break;
            }
            read_ok++;
        }
        if (pass) {
            printf("  Páginas reconstruidas y verificadas: %u\n", read_ok);
        } else {
            printf("FAIL: la lectura verificada tras la caída no coincide\n");
        }
    }

    for (uint32_t i = 0; i < NODE_COUNT; i++) {
        e2e_stop_child(nodes[i]);
    }

    printf("%s\n", pass ? "PASS: paridad conserva los datos tras la caída de un nodo" :
                         "FAIL: paridad no conservó los datos tras la caída");
    return pass ? 0 : 1;
}
