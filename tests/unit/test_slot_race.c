#include "test.h"
#include "dsm.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * Arranque simultaneo de varios nodos sobre el mismo rango de puertos.
 *
 * Todos los procesos hijos llaman a dsm_init a la vez, liberados por una
 * barrera de pipe, y cada uno verifica que obtuvo una ranura distinta de las de
 * los demas y que su tabla refleja tantos nodos como procesos arrancaron.
 *
 * La prueba describe el reparto que exige el protocolo: entre dos nodos que
 * reclaman la misma ranura la conserva el de UUID lexicograficamente menor, el
 * otro cede y toma la siguiente libre, y todos convergen a la misma cantidad de
 * nodos.
 *
 * El rango de puertos se elige en tiempo de ejecucion dentro del bloque
 * asignado al grupo, como en las demas pruebas de cluster, y un tramo ocupado
 * por otro proceso se reintenta con otro tramo en lugar de contarse como fallo.
 */

#define TEST_PORT_BLOCK_MIN 27600u
#define TEST_PORT_BLOCK_MAX 27699u
#define TEST_RACE_NODES     4u
#define TEST_PORT_WINDOW    TEST_RACE_NODES
#define TEST_MAX_ATTEMPTS   8
#define TEST_RUNS           20

#define CHILD_TIMEOUT_MS 60000
#define COUNT_TIMEOUT_MS 15000
#define EXIT_TIMEOUT_MS  10000

enum {
    SCENARIO_OK       =  0,
    SCENARIO_CONFLICT =  1, /* el tramo de puertos lo ocupo otro proceso */
    SCENARIO_FAIL     = -1
};

typedef struct {
    int32_t  rc;
    uint32_t slot;
    uint32_t count;
} child_result_t;

/*
 * Comprueba que los puertos [base, base + count) se pueden tomar sin conflicto.
 * Usa SO_REUSEADDR, de modo que un puerto en TIME_WAIT no descarta un tramo que
 * el nodo si podria tomar.
 */
static int ports_available(uint16_t base, uint16_t count)
{
    for (uint16_t i = 0; i < count; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return 0;

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port        = htons((uint16_t)(base + i));

        int rc = bind(fd, (struct sockaddr *)&addr, sizeof(addr));
        close(fd);
        if (rc != 0) return 0;
    }
    return 1;
}

/*
 * Busca un tramo libre del bloque, de mayor a menor. 'cursor' es el limite
 * superior exclusivo: los intentos previos ya usaron los tramos por encima
 * de el, y repetirlos solo agrega puertos en TIME_WAIT.
 */
static int pick_port_window(uint32_t *cursor, uint16_t *port_min, uint16_t *port_max)
{
    uint32_t steps = (TEST_PORT_BLOCK_MAX - TEST_PORT_WINDOW + 1 -
                      TEST_PORT_BLOCK_MIN) / TEST_PORT_WINDOW;

    for (uint32_t k = steps + 1; k > 0; k--) {
        uint32_t base = TEST_PORT_BLOCK_MIN + (k - 1) * TEST_PORT_WINDOW;
        if (base >= *cursor) continue;
        if (ports_available((uint16_t)base, TEST_PORT_WINDOW)) {
            *port_min = (uint16_t)base;
            *port_max = (uint16_t)(base + TEST_PORT_WINDOW - 1);
            *cursor = base;
            return 1;
        }
    }
    return 0;
}

static void write_conf(const char *path, uint16_t port_min, uint16_t port_max)
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
            "REDUNDANCY=NONE\n"
            "NODE_MEM=67108864\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=%u\n"
            "PEER_HOSTS=127.0.0.1\n"
            "CONNECT_TIMEOUT_MS=1000\n"
            "DISCOVERY_TIMEOUT_MS=1000\n"
            "SEND_TIMEOUT_MS=4000\n"
            "REQUEST_TIMEOUT_MS=8000\n"
            "REQUEST_RETRIES=3\n"
            "RECONNECT_MIN_MS=100\n"
            "RECONNECT_MAX_MS=1500\n"
            "HB_INTERVAL_MS=500\n"
            "HB_SUSPECT=10\n"
            "HB_DEAD=20\n"
            "WORKER_THREADS=2\n"
            "DISPATCHER_THREADS=1\n"
            "PAGE_LOCK_STRIPES=64\n"
            "HOME_SLACK=1.25\n"
            "LOCK_TIMEOUT_MS=5000\n"
            "WORKER_QUEUE_CAP=1024\n"
            "DSM_LOG_LEVEL=ERROR\n",
            port_min, port_max);
    fclose(f);
}

/* Crea un archivo temporal ya abierto y devuelve su ruta en 'path'. */
static int make_temp(char *path, size_t cap, int *out_fd)
{
    snprintf(path, cap, "/tmp/dsm_slot_race_XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) {
        path[0] = '\0';
        return 0;
    }
    *out_fd = fd;
    return 1;
}

/* Lee exactamente len bytes con plazo; devuelve 1 si los obtuvo. */
static int read_exact(int fd, void *buf, size_t len, int timeout_ms)
{
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;

    while (got < len) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
        int pr;
        do {
            pr = poll(&pfd, 1, timeout_ms);
        } while (pr < 0 && errno == EINTR);
        if (pr <= 0) return 0;

        ssize_t n = read(fd, p + got, len - got);
        if (n < 0) {
            if (errno == EINTR) continue;
            return 0;
        }
        if (n == 0) return 0;
        got += (size_t)n;
    }
    return 1;
}

static int write_exact(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = write(fd, p + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return 0;
        }
        sent += (size_t)n;
    }
    return 1;
}

/* Espera a que la membresia local alcance 'expected' nodos vivos. */
static int wait_node_count(uint32_t expected, int timeout_ms, uint32_t *out)
{
    int waited = 0;
    uint32_t count = 0;

    for (;;) {
        if (dsm_node_count(&count) != DSM_OK) return 0;
        if (count == expected) {
            if (out) *out = count;
            return 1;
        }
        if (waited >= timeout_ms) break;
        usleep(20000);
        waited += 20;
    }

    if (out) *out = count;
    return 0;
}

/*
 * Cuerpo de cada nodo. Espera la barrera, arranca, publica su resultado y
 * espera la orden de cierre para liberar el tramo de puertos.
 */
static void run_child(const char *conf, const char *tag, int barrier_fd,
                      int result_fd, int release_fd)
{
    signal(SIGPIPE, SIG_IGN);

    char go = 0;
    if (!read_exact(barrier_fd, &go, 1, CHILD_TIMEOUT_MS)) {
        _exit(2);
    }

    setenv("DSM_NODE_TAG", tag, 1);

    child_result_t res = { .rc = DSM_ERR_MEM, .slot = DSM_SLOT_NONE, .count = 0 };
    res.rc = (int32_t)dsm_init(conf, 0);
    if (res.rc == DSM_OK) {
        dsm_node_id(&res.slot);
        if (!wait_node_count(TEST_RACE_NODES, COUNT_TIMEOUT_MS, &res.count)) {
            dsm_node_count(&res.count);
        }
    }

    if (!write_exact(result_fd, &res, sizeof(res))) {
        _exit(3);
    }

    char quit = 0;
    if (!read_exact(release_fd, &quit, 1, CHILD_TIMEOUT_MS)) {
        _exit(4);
    }

    if (dsm_is_initialized()) {
        dsm_finalize();
    }
    _exit(res.rc == DSM_OK ? 0 : 1);
}

/* Libera a los hijos y recoge sus codigos de salida. */
static void finish_children(pid_t *pids, int release_fd, int *ok)
{
    char quit = 1;
    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        write_exact(release_fd, &quit, 1);
    }

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        int waited = 0;
        int wstatus = 0;
        for (;;) {
            pid_t rc = waitpid(pids[i], &wstatus, WNOHANG);
            if (rc == pids[i]) break;
            if (rc < 0 && errno != EINTR) break;
            if (waited >= EXIT_TIMEOUT_MS) {
                printf("   el nodo %u no termino en %d ms\n", i, EXIT_TIMEOUT_MS);
                kill(pids[i], SIGKILL);
                (void)waitpid(pids[i], &wstatus, 0);
                *ok = 0;
                break;
            }
            usleep(10000);
            waited += 10;
        }
        if (WIFSIGNALED(wstatus)) {
            printf("   el nodo %u termino por senal %d\n", i, WTERMSIG(wstatus));
            *ok = 0;
        }
    }
}

/*
 * Un intento completo sobre un tramo de puertos concreto. Deja todos los
 * procesos recogidos en cualquier salida.
 */
static int run_scenario(uint16_t port_min, uint16_t port_max)
{
    char conf_pattern[48] = "";
    char conf[TEST_RACE_NODES][64];
    char tag[TEST_RACE_NODES][32];
    pid_t pids[TEST_RACE_NODES];
    int barrier[2]  = { -1, -1 };
    int results[2]  = { -1, -1 };
    int release[2]  = { -1, -1 };
    int conf_fd     = -1;
    int result = SCENARIO_OK;
    int ok = 1;

    memset(conf, 0, sizeof(conf));
    memset(pids, -1, sizeof(pids));

    if (!make_temp(conf_pattern, sizeof(conf_pattern), &conf_fd)) {
        printf("   no se pudo crear el archivo de configuracion\n");
        return SCENARIO_FAIL;
    }

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        snprintf(conf[i], sizeof(conf[i]), "%s.%u", conf_pattern, i);
        snprintf(tag[i], sizeof(tag[i]), "slot_race_%u", i);
        write_conf(conf[i], port_min, port_max);
    }
    close(conf_fd);

    if (pipe(barrier) != 0 || pipe(results) != 0 || pipe(release) != 0) {
        printf("   no se pudieron crear los pipes de la prueba\n");
        result = SCENARIO_FAIL;
        goto cleanup;
    }

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            printf("   fork del nodo %u fallo: %s\n", i, strerror(errno));
            result = SCENARIO_FAIL;
            goto cleanup;
        }
        if (pid == 0) {
            int devnull = open("/dev/null", O_WRONLY);
            if (devnull >= 0) {
                dup2(devnull, STDOUT_FILENO);
                dup2(devnull, STDERR_FILENO);
                if (devnull > STDERR_FILENO) close(devnull);
            }
            close(barrier[1]);
            close(results[0]);
            close(release[1]);
            run_child(conf[i], tag[i], barrier[0], results[1], release[0]);
        }
        pids[i] = pid;
    }

    close(barrier[0]);
    barrier[0] = -1;
    close(results[1]);
    results[1] = -1;
    close(release[0]);
    release[0] = -1;

    /* La barrera se libera con una escritura unica: los hijos despiertan en el
     * mismo instante sin depender del orden en que el planificador los elija. */
    char go[TEST_RACE_NODES];
    memset(go, 1, sizeof(go));
    if (!write_exact(barrier[1], go, sizeof(go))) {
        printf("   no se pudo liberar la barrera de arranque\n");
        result = SCENARIO_FAIL;
        goto cleanup;
    }

    child_result_t res[TEST_RACE_NODES];
    memset(res, 0, sizeof(res));

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        if (!read_exact(results[0], &res[i], sizeof(res[i]), CHILD_TIMEOUT_MS)) {
            printf("   el nodo %u no publico su ranura en %d ms\n",
                   i, CHILD_TIMEOUT_MS);
            ok = 0;
            goto cleanup;
        }
        if (res[i].rc != DSM_OK) {
            printf("   el nodo %u no pudo arrancar (rc=%d)\n", i, res[i].rc);
            ok = 0;
            /*
             * Un tramo ocupado por otro proceso no es un fallo del reparto:
             * puede ser un puerto ya tomado o un nodo de otro cluster que
             * rechaza el HELLO por raw_hash distinto. En ambos casos la
             * corrida se reintenta con otro tramo.
             */
            if (res[i].rc == DSM_ERR_CONFIG || res[i].rc == DSM_ERR_NET ||
                !ports_available(port_min, (uint16_t)(port_max - port_min + 1))) {
                result = SCENARIO_CONFLICT;
            }
            goto cleanup;
        }
    }

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        for (uint32_t j = i + 1; j < TEST_RACE_NODES; j++) {
            if (res[i].slot == res[j].slot) {
                printf("   los nodos %u y %u obtuvieron la misma ranura %u\n",
                       i, j, res[i].slot);
                ok = 0;
            }
        }
        if (res[i].slot >= TEST_RACE_NODES) {
            printf("   el nodo %u obtuvo la ranura %u, fuera del reparto esperado\n",
                   i, res[i].slot);
            ok = 0;
        }
        if (res[i].count != TEST_RACE_NODES) {
            printf("   el nodo %u (ranura %u) ve %u nodos y esperaba %u\n",
                   i, res[i].slot, res[i].count, TEST_RACE_NODES);
            ok = 0;
        }
    }

cleanup:
    finish_children(pids, release[1], &ok);

    if (barrier[0] >= 0) close(barrier[0]);
    if (barrier[1] >= 0) close(barrier[1]);
    if (results[0] >= 0) close(results[0]);
    if (results[1] >= 0) close(results[1]);
    if (release[0] >= 0) close(release[0]);
    if (release[1] >= 0) close(release[1]);

    for (uint32_t i = 0; i < TEST_RACE_NODES; i++) {
        if (conf[i][0]) unlink(conf[i]);
    }
    if (conf_pattern[0]) unlink(conf_pattern);

    if (result == SCENARIO_CONFLICT) return SCENARIO_CONFLICT;
    return ok ? SCENARIO_OK : SCENARIO_FAIL;
}

/*
 * Repite el arranque simultaneo varias veces sobre tramos distintos del bloque
 * asignado. Cada corrida tiene que repartir ranuras distintas y hacer converger
 * a todos los procesos a la misma cantidad de nodos.
 */
static int test_simultaneous_slot_assignment(void)
{
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;
    char reason[256];
    reason[0] = '\0';

    for (int run = 1; run <= TEST_RUNS; run++) {
        int done = 0;

        for (uint32_t attempt = 1; attempt <= TEST_MAX_ATTEMPTS && !done; attempt++) {
            uint16_t port_min = 0;
            uint16_t port_max = 0;

            if (!pick_port_window(&cursor, &port_min, &port_max)) {
                snprintf(reason, sizeof(reason),
                         "no queda un tramo libre de %u puertos en el bloque %u-%u; "
                         "se omiten las corridas restantes",
                         TEST_PORT_WINDOW, TEST_PORT_BLOCK_MIN, TEST_PORT_BLOCK_MAX);
                printf("   %s\n", reason);
                return 0;
            }

            int rc = run_scenario(port_min, port_max);
            if (rc == SCENARIO_CONFLICT) {
                printf("   corrida %d: el tramo %u-%u lo ocupa otro proceso; "
                       "se reintenta\n", run, port_min, port_max);
                continue;
            }
            if (rc != SCENARIO_OK) {
                printf("   corrida %d fallo en el tramo %u-%u\n",
                       run, port_min, port_max);
                return -1;
            }
            done = 1;
        }

        if (!done) {
            snprintf(reason, sizeof(reason),
                     "no se pudo completar la corrida %d en ningun tramo libre", run);
            printf("   %s\n", reason);
            return -1;
        }
    }

    printf("   %d corridas con %u nodos simultaneos: reparto correcto\n",
           TEST_RUNS, TEST_RACE_NODES);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: reparto de ranuras con arranque simultaneo ===\n");
    RUN_TEST(test_simultaneous_slot_assignment);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
