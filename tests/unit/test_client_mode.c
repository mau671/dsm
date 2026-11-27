#include "test.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "ring.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
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
 * Modo cliente del nucleo DSM.
 *
 * El hijo corre como nodo completo y aporta la memoria del cluster; el padre se
 * une despues con DSM_NODE_MODE=client y el mismo archivo de configuracion. El
 * rol es una propiedad del proceso, no del archivo, de modo que el cliente
 * ignora el NODE_MEM configurado: no reserva respaldo, no publica paginas y no
 * proyecta vnodes.
 *
 * El cliente arranca despues de que el nodo confirme su union porque la
 * adhesion de un cliente exige al menos un miembro con memoria: sin custodio
 * posible, discovery_join falla con DSM_ERR_NO_SPACE.
 *
 * Las comprobaciones cubren las cuatro consecuencias del rol:
 *  - el cliente cuenta un solo nodo y ve solo la memoria del nodo real,
 *  - el nodo real no cuenta al cliente entre sus nodos,
 *  - el cliente accede a la memoria a traves de fallos de pagina normales,
 *  - ninguna pagina tiene al cliente como home.
 *
 * El tramo de puertos se elige en tiempo de ejecucion dentro del bloque del
 * grupo y se reintenta con otro si otro proceso lo ocupa; ese caso no es un
 * fallo de la prueba.
 */

#define PAGE_BYTES 4096u
#define PATTERN    0x3Cu
#define NODE_MEM   (16ULL * 1024ULL * 1024ULL)

#define STEP_TIMEOUT_MS 30000
#define EXIT_TIMEOUT_MS 10000
#define HOME_SAMPLES    5000u

#define TEST_PORT_BLOCK_MIN 27600u
#define TEST_PORT_BLOCK_MAX 27699u
#define TEST_PORT_WINDOW    4u
#define TEST_MAX_ATTEMPTS   3

#define CMD_START 'S'
#define CMD_QUIT  'Q'

/* Codigos de resultado que el hijo publica por el pipe de estado. */
enum {
    ST_STEP_OK       = 0,
    ST_INIT_FAIL     = 1,
    ST_ATTACH_FAIL   = 2,
    ST_COUNT_FAIL    = 3,
    ST_SIZE_FAIL     = 4,
    ST_FINALIZE_FAIL = 5,
    ST_COMMAND_FAIL  = 6
};

enum {
    SCENARIO_OK       =  0,
    SCENARIO_CONFLICT =  1, /* el tramo de puertos lo ocupo otro proceso */
    SCENARIO_FAIL     = -1
};

/* Informe de un paso del hijo: estado y las dos consultas que el padre no
 * puede hacer en su nombre. */
typedef struct {
    int32_t  code;
    uint32_t nodes;
    uint64_t global_size;
} report_t;

static const char *r_reason(int code)
{
    switch (code) {
    case ST_STEP_OK:       return "paso completado";
    case ST_INIT_FAIL:     return "dsm_init fallo en el nodo";
    case ST_ATTACH_FAIL:   return "dsm_thread_attach fallo en el nodo";
    case ST_COUNT_FAIL:    return "dsm_node_count devuelve un valor inesperado en el nodo";
    case ST_SIZE_FAIL:     return "dsm_global_size devuelve un valor inesperado en el nodo";
    case ST_FINALIZE_FAIL: return "dsm_finalize fallo en el nodo";
    case ST_COMMAND_FAIL:  return "sin orden del padre";
    default:               return "codigo desconocido";
    }
}

/* Comprueba que los puertos [base, base + count) se pueden tomar. */
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

/* Busca un tramo libre del bloque, de mayor a menor. 'cursor' es el limite
 * superior exclusivo de los tramos ya descartados. */
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
            "REDUNDANCY=false\n"
            "NODE_MEM=%llu\n"
            "PORT_MIN=%u\n"
            "PORT_MAX=%u\n"
            "CONNECT_TIMEOUT_MS=1000\n"
            "DISCOVERY_TIMEOUT_MS=1000\n"
            "SEND_TIMEOUT_MS=4000\n"
            "REQUEST_TIMEOUT_MS=8000\n"
            "REQUEST_RETRIES=3\n"
            "RECONNECT_MIN_MS=100\n"
            "RECONNECT_MAX_MS=1500\n"
            "HB_INTERVAL_MS=500\n"
            "HB_SUSPECT=6\n"
            "HB_DEAD=12\n"
            "WORKER_THREADS=2\n"
            "DISPATCHER_THREADS=1\n"
            "PAGE_LOCK_STRIPES=64\n"
            "HOME_SLACK=1.25\n"
            "LOCK_TIMEOUT_MS=5000\n"
            "WORKER_QUEUE_CAP=1024\n"
            "DSM_LOG_LEVEL=ERROR\n",
            (unsigned long long)NODE_MEM, port_min, port_max);
    fclose(f);
}

static int make_temp(char *path, size_t cap, int *out_fd)
{
    snprintf(path, cap, "/tmp/dsm_client_mode_XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) {
        path[0] = '\0';
        return 0;
    }
    *out_fd = fd;
    return 1;
}

static int wait_readable(int fd, int timeout_ms)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
    int rc;
    do {
        rc = poll(&pfd, 1, timeout_ms);
    } while (rc < 0 && errno == EINTR);
    if (rc == 0) return 0;
    if (rc < 0) return -1;
    return 1;
}

static int read_exact(int fd, void *buf, size_t len, int timeout_ms)
{
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;

    while (got < len) {
        int rd = wait_readable(fd, timeout_ms);
        if (rd <= 0) return 0;
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

static int send_command(int fd, char command)
{
    return write(fd, &command, 1) == 1;
}

static void close_fd(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static int child_report(int status_fd, int code, uint32_t nodes, uint64_t size)
{
    report_t rep;
    memset(&rep, 0, sizeof(rep));
    rep.code        = code;
    rep.nodes       = nodes;
    rep.global_size = size;
    return write(status_fd, &rep, sizeof(rep)) == (ssize_t)sizeof(rep);
}

/* Lee el informe del hijo. Devuelve 1 con codigo 0 y 0 ante timeout, EOF o
 * error, dejando el motivo en 'reason'. */
static int read_report(int status_fd, const char *step, report_t *out,
                       char *reason, size_t reason_cap)
{
    report_t rep;

    if (!read_exact(status_fd, &rep, sizeof(rep), STEP_TIMEOUT_MS)) {
        snprintf(reason, reason_cap, "sin respuesta del nodo en '%s' (%d ms)",
                 step, STEP_TIMEOUT_MS);
        return 0;
    }
    if (rep.code != ST_STEP_OK) {
        snprintf(reason, reason_cap, "%s: %s", step, r_reason(rep.code));
        return 0;
    }
    if (out) *out = rep;
    return 1;
}

static void page_fill(volatile uint8_t *page, uint8_t value)
{
    for (size_t i = 0; i < PAGE_BYTES; i++) {
        page[i] = value;
    }
}

static int page_matches(const volatile uint8_t *page, uint8_t expected)
{
    for (size_t i = 0; i < PAGE_BYTES; i++) {
        if (page[i] != expected) return 0;
    }
    return 1;
}

static int wait_child_exit(pid_t pid, int *status, int timeout_ms)
{
    int waited = 0;
    for (;;) {
        pid_t rc = waitpid(pid, status, WNOHANG);
        if (rc == pid) return 1;
        if (rc < 0 && errno != EINTR) return 0;
        if (waited >= timeout_ms) return 0;
        usleep(10000);
        waited += 10;
    }
}

/* Vuelca el log del hijo para diagnosticar un fallo del nodo. */
static void dump_child_log(const char *path)
{
    if (!path || !path[0]) return;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return;

    char buf[4096];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return;

    buf[n] = '\0';
    printf("   --- log del nodo ---\n%s   --- fin del log ---\n", buf);
}

/*
 * Cuerpo del nodo real. Confirma su union, informa lo que ve desde su lado y
 * espera la orden de cierre. Nunca retorna.
 */
static void run_child(const char *conf, int status_fd, int cmd_fd)
{
    signal(SIGPIPE, SIG_IGN);

    char command = 0;
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_START) {
        child_report(status_fd, ST_COMMAND_FAIL, 0, 0);
        _exit(ST_COMMAND_FAIL);
    }

    setenv("DSM_NODE_MODE", "node", 1);
    setenv("DSM_NODE_TAG", "client_mode_node", 1);

    if (dsm_init(conf, 0) != DSM_OK) {
        child_report(status_fd, ST_INIT_FAIL, 0, 0);
        _exit(ST_INIT_FAIL);
    }
    if (dsm_thread_attach() != DSM_OK) {
        child_report(status_fd, ST_ATTACH_FAIL, 0, 0);
        _exit(ST_ATTACH_FAIL);
    }

    uint32_t nodes = 0;
    uint64_t size = 0;
    if (dsm_node_count(&nodes) != DSM_OK || nodes != 1) {
        child_report(status_fd, ST_COUNT_FAIL, nodes, 0);
        _exit(ST_COUNT_FAIL);
    }
    if (dsm_global_size(&size) != DSM_OK || size != NODE_MEM) {
        child_report(status_fd, ST_SIZE_FAIL, nodes, size);
        _exit(ST_SIZE_FAIL);
    }
    child_report(status_fd, ST_STEP_OK, nodes, size);

    /* El cliente se une mientras este proceso espera: los latidos y el listener
     * siguen activos, que es lo que el cliente necesita para resolver sus
     * fallos de pagina. */
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_QUIT) {
        child_report(status_fd, ST_COMMAND_FAIL, 0, 0);
        _exit(ST_COMMAND_FAIL);
    }

    /* El cliente nunca aporta memoria: el nodo sigue contando un solo nodo y
     * la memoria global no cambia. */
    if (dsm_node_count(&nodes) != DSM_OK || nodes != 1) {
        child_report(status_fd, ST_COUNT_FAIL, nodes, 0);
        _exit(ST_COUNT_FAIL);
    }
    if (dsm_global_size(&size) != DSM_OK || size != NODE_MEM) {
        child_report(status_fd, ST_SIZE_FAIL, nodes, size);
        _exit(ST_SIZE_FAIL);
    }

    dsm_thread_detach();
    if (dsm_finalize() != DSM_OK) {
        child_report(status_fd, ST_FINALIZE_FAIL, nodes, size);
        _exit(ST_FINALIZE_FAIL);
    }
    child_report(status_fd, ST_STEP_OK, nodes, size);
    _exit(0);
}

/* Un tramo se considera ajeno cuando, tras un fallo de arranque, alguno de sus
 * puertos no se puede tomar. */
static int window_taken_by_other(uint16_t port_min, uint16_t port_max)
{
    return !ports_available(port_min, (uint16_t)(port_max - port_min + 1));
}

static int file_contains(const char *path, const char *needle)
{
    if (!path || !path[0]) return 0;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;

    char buf[8192];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return 0;

    buf[n] = '\0';
    return strstr(buf, needle) != NULL;
}

/*
 * Comprueba el estado del cliente desde su propio contexto: no aporta paginas,
 * el nodo si, y el anillo nunca lo elige como custodio.
 */
static int check_client_view(uint32_t *out_self, char *reason, size_t reason_cap)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) {
        snprintf(reason, reason_cap, "dsm_ctx_get devolvio NULL");
        return 0;
    }

    uint32_t self = DSM_SLOT_NONE;
    if (dsm_node_id(&self) != DSM_OK || self >= MAX_NODES) {
        snprintf(reason, reason_cap, "el cliente no obtuvo una ranura valida");
        return 0;
    }
    if (!ctx->cfg.client_mode) {
        snprintf(reason, reason_cap, "el cliente no arranco en modo cliente");
        return 0;
    }

    /* Copia de la vista de membresia bajo el candado; el anillo se consulta
     * despues porque ring_home_of toma ese mismo candado. */
    node_info_t own;
    node_info_t node;
    memset(&own, 0, sizeof(own));
    memset(&node, 0, sizeof(node));
    uint32_t node_slot = DSM_SLOT_NONE;

    pthread_rwlock_rdlock(&ctx->memb.rw);
    own = ctx->memb.nodes[self];
    for (uint32_t s = 0; s < MAX_NODES; s++) {
        if (s == self) continue;
        if (!membership_slot_present(ctx, s)) continue;
        if (ctx->memb.nodes[s].state == DSM_NODE_DEAD) continue;
        node_slot = s;
        break;
    }
    if (node_slot != DSM_SLOT_NONE) {
        node = ctx->memb.nodes[node_slot];
    }
    pthread_rwlock_unlock(&ctx->memb.rw);

    if (!own.is_client || own.mem_pages != 0 || own.free_pages != 0) {
        snprintf(reason, reason_cap,
                 "la fila propia del cliente publica %u paginas (is_client=%u)",
                 own.mem_pages, (unsigned)own.is_client);
        return 0;
    }

    if (node_slot == DSM_SLOT_NONE) {
        snprintf(reason, reason_cap, "el cliente no ve ningun nodo con memoria");
        return 0;
    }
    if (node.is_client || node.mem_pages == 0) {
        snprintf(reason, reason_cap,
                 "el nodo real se ve como cliente (is_client=%u, paginas=%u)",
                 (unsigned)node.is_client, node.mem_pages);
        return 0;
    }

    /* El anillo solo debe contener al nodo real. */
    for (uint32_t p = 0; p < HOME_SAMPLES; p++) {
        uint32_t home = ring_home_of(ctx, p);
        if (home == self) {
            snprintf(reason, reason_cap,
                     "la pagina %u tiene como home al cliente (slot %u)", p, self);
            return 0;
        }
        if (home >= MAX_NODES || home == node_slot) {
            continue;
        }
        snprintf(reason, reason_cap,
                 "la pagina %u tiene un home inesperado (slot %u)", p, home);
        return 0;
    }

    /* El nodo real no aparece como cliente ni en el conteo del anillo. */
    if (ctx->ring.n == 0) {
        snprintf(reason, reason_cap, "el anillo quedo vacio en el cliente");
        return 0;
    }

    if (out_self) *out_self = self;
    return 1;
}

/*
 * Un intento completo sobre un tramo de puertos concreto. Deja el DSM del padre
 * finalizado y el hijo recogido en todas las salidas.
 */
static int run_scenario(uint16_t port_min, uint16_t port_max)
{
    char conf[64] = "";
    char log_child[64] = "";
    int conf_fd = -1;
    int log_fd = -1;
    int command[2] = { -1, -1 }; /* padre -> hijo: concesion de cada paso */
    int status[2]  = { -1, -1 }; /* hijo -> padre: informe de cada paso */
    pid_t child = -1;
    int result = SCENARIO_FAIL;
    int ok = 1;
    char reason[256];

    if (!make_temp(conf, sizeof(conf), &conf_fd) ||
        !make_temp(log_child, sizeof(log_child), &log_fd)) {
        printf("   no se pudieron crear los archivos temporales\n");
        goto cleanup;
    }
    close(conf_fd);
    close(log_fd);
    conf_fd = -1;
    log_fd = -1;

    write_conf(conf, port_min, port_max);

    if (pipe(command) != 0 || pipe(status) != 0) {
        printf("   no se pudieron crear los pipes de la prueba\n");
        goto cleanup;
    }

    child = fork();
    if (child < 0) {
        printf("   fork fallo: %s\n", strerror(errno));
        goto cleanup;
    }
    if (child == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            if (devnull > STDOUT_FILENO) close(devnull);
        }
        dup2(log_fd, STDERR_FILENO);

        close(log_fd);
        close(command[1]);
        close(status[0]);
        run_child(conf, status[1], command[0]);
    }

    close(log_fd);
    log_fd = -1;
    close(command[0]);
    command[0] = -1;
    close(status[1]);
    status[1] = -1;

    /* El nodo arranca solo y confirma que cuenta un solo miembro. */
    report_t node_rep;
    memset(&node_rep, 0, sizeof(node_rep));
    if (!send_command(command[1], CMD_START) ||
        !read_report(status[0], "arranque del nodo", &node_rep, reason, sizeof(reason))) {
        if (window_taken_by_other(port_min, port_max) ||
            file_contains(log_child, "discrepancia en dsm.conf") ||
            file_contains(log_child, "No hay puertos disponibles")) {
            result = SCENARIO_CONFLICT;
        } else {
            printf("   el nodo real no arranco: %s\n", reason);
            ok = 0;
        }
        goto cleanup;
    }
    if (node_rep.nodes != 1 || node_rep.global_size != NODE_MEM) {
        printf("   el nodo real informa %u nodo(s) y %lu bytes; esperaba 1 y %llu\n",
               node_rep.nodes, (unsigned long)node_rep.global_size,
               (unsigned long long)NODE_MEM);
        ok = 0;
        goto cleanup;
    }

    /* Ahora el mismo proceso, con el mismo archivo, se une como cliente. */
    setenv("DSM_NODE_MODE", "client", 1);
    setenv("DSM_NODE_TAG", "client_mode_client", 1);

    if (dsm_init(conf, 0) != DSM_OK) {
        if (window_taken_by_other(port_min, port_max)) {
            result = SCENARIO_CONFLICT;
        } else {
            printf("   el cliente no pudo unirse al cluster\n");
            ok = 0;
        }
        goto cleanup;
    }
    if (dsm_thread_attach() != DSM_OK) {
        printf("   dsm_thread_attach fallo en el cliente\n");
        ok = 0;
        goto cleanup;
    }

    /* Solo cuenta al nodo real; el cliente no es un nodo del cluster. */
    uint32_t count = 0;
    if (dsm_node_count(&count) != DSM_OK || count != 1) {
        printf("   el cliente cuenta %u nodo(s); esperaba 1\n", count);
        ok = 0;
        goto cleanup;
    }

    /* La memoria global es solo la del nodo real, aunque el NODE_MEM del
     * archivo sea el mismo: el cliente no aporta ninguna pagina. */
    uint64_t global = 0;
    if (dsm_global_size(&global) != DSM_OK || global != NODE_MEM) {
        printf("   dsm_global_size en el cliente = %lu bytes; esperaba %llu\n",
               (unsigned long)global, (unsigned long long)NODE_MEM);
        ok = 0;
        goto cleanup;
    }

    /* El cliente accede a la memoria como cualquier otro proceso: reserva y
     * escribe provoca un fallo de pagina que resuelve el nodo custodio. */
    void *base = NULL;
    int alloc_rc = dsm_alloc(PAGE_BYTES, &base);
    if (alloc_rc != DSM_OK || !base) {
        printf("   dsm_alloc fallo en el cliente: %s (%d)\n",
               dsm_strerror((dsm_status_t)alloc_rc), alloc_rc);
        ok = 0;
        goto cleanup;
    }
    volatile uint8_t *page = (volatile uint8_t *)base;
    page_fill(page, PATTERN);
    if (!page_matches(page, PATTERN)) {
        printf("   el cliente no pudo releer el patron que escribio\n");
        ok = 0;
        goto cleanup;
    }

    uint32_t self = DSM_SLOT_NONE;
    if (!check_client_view(&self, reason, sizeof(reason))) {
        printf("   %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    /* El nodo real debe revalidar su vista despues de la union del cliente. */
    if (!send_command(command[1], CMD_QUIT) ||
        !read_report(status[0], "cierre del nodo", &node_rep, reason, sizeof(reason))) {
        printf("   %s\n", reason);
        ok = 0;
        goto cleanup;
    }
    if (node_rep.nodes != 1 || node_rep.global_size != NODE_MEM) {
        printf("   tras unirse el cliente, el nodo real cuenta %u nodo(s) y %lu bytes\n",
               node_rep.nodes, (unsigned long)node_rep.global_size);
        ok = 0;
    }

cleanup:
    close_fd(&command[1]);
    close_fd(&status[0]);

    if (dsm_is_initialized()) {
        dsm_thread_detach();
        CHECK(dsm_finalize() == DSM_OK);
    }

    if (child > 0) {
        int wstatus = 0;
        if (!wait_child_exit(child, &wstatus, EXIT_TIMEOUT_MS)) {
            printf("   el hijo no termino en %d ms\n", EXIT_TIMEOUT_MS);
            ok = 0;
            kill(child, SIGKILL);
            (void)waitpid(child, &wstatus, 0);
        } else if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
            if (WIFSIGNALED(wstatus)) {
                printf("   el hijo termino por senal %d\n", WTERMSIG(wstatus));
            } else {
                printf("   el hijo salio con codigo %d\n", WEXITSTATUS(wstatus));
            }
            ok = 0;
        }
    }

    if (result != SCENARIO_CONFLICT && !ok) dump_child_log(log_child);

    close_fd(&status[1]);
    close_fd(&log_fd);
    if (conf[0]) unlink(conf);
    if (log_child[0]) unlink(log_child);

    unsetenv("DSM_NODE_MODE");

    if (result == SCENARIO_CONFLICT) return SCENARIO_CONFLICT;
    return ok ? SCENARIO_OK : SCENARIO_FAIL;
}

static int test_client_mode(void)
{
    char reason[256];
    reason[0] = '\0';
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;

    for (uint32_t attempt = 1; attempt <= TEST_MAX_ATTEMPTS; attempt++) {
        uint16_t port_min = 0;
        uint16_t port_max = 0;

        if (!pick_port_window(&cursor, &port_min, &port_max)) {
            snprintf(reason, sizeof(reason),
                     "no queda un tramo libre de %u puertos en el bloque %u-%u",
                     TEST_PORT_WINDOW, TEST_PORT_BLOCK_MIN, TEST_PORT_BLOCK_MAX);
            break;
        }

        int rc = run_scenario(port_min, port_max);
        if (rc == SCENARIO_CONFLICT) {
            printf("   el tramo %u-%u lo ocupa otro proceso; se reintenta\n",
                   port_min, port_max);
            continue;
        }
        if (rc == SCENARIO_OK) return 0;

        snprintf(reason, sizeof(reason),
                 "el modo cliente fallo en el tramo %u-%u", port_min, port_max);
        break;
    }

    printf("   %s\n", reason[0] ? reason : "sin tramo de puertos disponible");
    return -1;
}

int main(void)
{
    printf("=== Test Suite: Modo cliente del nucleo DSM ===\n");
    RUN_TEST(test_client_mode);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
