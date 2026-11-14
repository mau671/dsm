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
 * Traspaso de una página cuando el anillo de dispersión cambia de home.
 *
 * El padre actúa como nodo A y el hijo como nodo B. A arranca solo y escribe un
 * patrón distinto en cada página del bloque reservado: con un único nodo el home
 * de todas ellas es A. Después B se une al clúster; el anillo se reconstruye y
 * una parte de las páginas pasa a tener home B. B lee el bloque completo y debe
 * ver el patrón de A, incluidos los ceros de las páginas que A nunca escribió.
 *
 * Sobre esa misma membresía B escribe sus propios patrones, A los relee y B
 * abandona el clúster. Con el home repartido de vuelta en A, la última relectura
 * de A debe seguir viendo lo que escribió B.
 *
 * El tramo de puertos sale del bloque asignado al grupo y se elige en tiempo de
 * ejecución; un tramo ocupado por otro proceso se reintenta con otro y no cuenta
 * como fallo. B redirige stdout a /dev/null y stderr a un archivo temporal, y
 * publica el resultado de cada paso por el pipe de estado.
 */

#define PAGE_BYTES   4096u
#define BLOCK_PAGES  8u
#define PAGE_WORDS   (PAGE_BYTES / sizeof(uint32_t))

#define STEP_TIMEOUT_MS       40000
#define EXIT_TIMEOUT_MS       15000
#define MEMBERSHIP_TIMEOUT_MS 10000
#define STATUS_LINE_MAX       256

#define TEST_PORT_BLOCK_MIN 27600u
#define TEST_PORT_BLOCK_MAX 27699u
#define TEST_PORT_WINDOW    4u
#define TEST_MAX_ATTEMPTS   3

#define CMD_START 'S'
#define CMD_READ  'R'
#define CMD_WRITE 'W'
#define CMD_QUIT  'Q'

/* Patrón que A escribe en la página i antes de que B exista. */
#define PATTERN_A(i) (UINT32_C(0xA0000000) + (uint32_t)(i) * UINT32_C(0x1000))
/* Patrón que B escribe en la página i cuando ya es miembro. */
#define PATTERN_B(i) (UINT32_C(0xB0000000) + (uint32_t)(i) * UINT32_C(0x2000))

enum {
    B_OK              = 0,
    B_INIT_FAIL       = 1,
    B_ATTACH_FAIL     = 2,
    B_ALLOC_FAIL      = 3,
    B_SLOT_MISMATCH   = 4,
    B_READ_MISMATCH   = 5,
    B_WRITE_MISMATCH  = 6,
    B_COMMAND_FAIL    = 7,
    B_FINALIZE_FAIL   = 8,
    B_HOME_STILL_SELF = 9
};

enum {
    SCENARIO_OK       =  0,
    SCENARIO_CONFLICT =  1, /* el tramo de puertos lo ocupó otro proceso */
    SCENARIO_FAIL     = -1
};

static const char *b_reason(int code)
{
    switch (code) {
    case B_OK:              return "paso completado";
    case B_INIT_FAIL:       return "dsm_init fallo";
    case B_ATTACH_FAIL:     return "dsm_thread_attach fallo";
    case B_ALLOC_FAIL:      return "dsm_alloc fallo";
    case B_SLOT_MISMATCH:   return "el nodo B no obtuvo el slot esperado";
    case B_READ_MISMATCH:   return "B no recibio el patron escrito por A";
    case B_WRITE_MISMATCH:  return "la relectura no devolvio el patron escrito";
    case B_COMMAND_FAIL:    return "sin orden del padre";
    case B_FINALIZE_FAIL:   return "dsm_finalize fallo";
    case B_HOME_STILL_SELF: return "B sigue viendo a A como home tras la union";
    default:                return "codigo desconocido";
    }
}

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
            "HB_SUSPECT=6\n"
            "HB_DEAD=12\n"
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

static int make_temp(char *path, size_t cap, int *out_fd)
{
    snprintf(path, cap, "/tmp/dsm_home_migration_XXXXXX");
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
    ssize_t n = write(fd, &command, 1);
    return n == 1;
}

static void close_fd(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static int read_status(int status_fd, const char *step, int *out_code,
                       char *reason, size_t reason_cap)
{
    char line[STATUS_LINE_MAX];
    size_t used = 0;

    if (out_code) *out_code = B_OK;

    for (;;) {
        int rd = wait_readable(status_fd, STEP_TIMEOUT_MS);
        if (rd == 0) {
            snprintf(reason, reason_cap, "sin respuesta del hijo en '%s' (%d ms)",
                     step, STEP_TIMEOUT_MS);
            return 0;
        }
        if (rd < 0) {
            snprintf(reason, reason_cap, "error de poll en el pipe de estado");
            return 0;
        }

        ssize_t n = read(status_fd, line + used, sizeof(line) - 1 - used);
        if (n < 0) {
            if (errno == EINTR) continue;
            snprintf(reason, reason_cap, "error de lectura del pipe de estado");
            return 0;
        }
        if (n == 0) {
            snprintf(reason, reason_cap,
                     "el hijo cerro el pipe de estado durante '%s'", step);
            return 0;
        }

        used += (size_t)n;
        line[used] = '\0';

        char *nl = memchr(line, '\n', used);
        if (!nl) {
            if (used >= sizeof(line) - 1) {
                snprintf(reason, reason_cap, "linea de estado desbordada");
                return 0;
            }
            continue;
        }

        *nl = '\0';
        int code = -1;
        char detail[STATUS_LINE_MAX];
        detail[0] = '\0';
        if (sscanf(line, "B %d %255[^\n]", &code, detail) != 2) {
            snprintf(reason, reason_cap, "linea de estado invalida: '%s'", line);
            return 0;
        }
        if (out_code) *out_code = code;
        if (code != B_OK) {
            snprintf(reason, reason_cap, "%s: %s", step, detail);
            return 0;
        }
        return 1;
    }
}

static int grant_step(int cmd_fd, int status_fd, char command, const char *step,
                      int *out_code, char *reason, size_t reason_cap)
{
    if (!send_command(cmd_fd, command)) {
        snprintf(reason, reason_cap, "no se pudo enviar '%s' al hijo", step);
        return 0;
    }
    return read_status(status_fd, step, out_code, reason, reason_cap);
}

static void child_report(int status_fd, int code, const char *detail)
{
    char line[STATUS_LINE_MAX];
    int len = snprintf(line, sizeof(line), "B %d %s\n", code,
                       detail ? detail : b_reason(code));
    if (len > 0) {
        (void)write(status_fd, line, (size_t)len);
    }
}

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
    printf("   --- log del nodo B ---\n%s   --- fin del log ---\n", buf);
}

static void block_fill(uint8_t *block, uint32_t page, uint32_t base)
{
    uint32_t *words = (uint32_t *)(block + (size_t)page * PAGE_BYTES);
    for (uint32_t i = 0; i < PAGE_WORDS; i++) {
        words[i] = base + i;
    }
}

static int block_page_matches(const uint8_t *block, uint32_t page, uint32_t base)
{
    const uint32_t *words = (const uint32_t *)(block + (size_t)page * PAGE_BYTES);
    for (uint32_t i = 0; i < PAGE_WORDS; i++) {
        if (words[i] != base + i) return 0;
    }
    return 1;
}

/* Verifica el bloque completo contra el patrón del emisor indicado: 0 para el
 * de A y 1 para el de B. */
static int block_matches(const uint8_t *block, int from_b)
{
    for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
        uint32_t base = from_b ? PATTERN_B(p) : PATTERN_A(p);
        if (!block_page_matches(block, p, base)) return 0;
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
 * Cuerpo del nodo B. Nunca retorna. Se une al clúster que ya levantó A, lee el
 * bloque completo, escribe sus propios patrones y abandona el clúster.
 */
static void run_child(const char *conf_b, int status_fd, int cmd_fd)
{
    signal(SIGPIPE, SIG_IGN);

    char command = 0;
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_START) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de arranque");
        _exit(B_COMMAND_FAIL);
    }

    setenv("DSM_NODE_TAG", "home_migration_b", 1);
    if (dsm_init(conf_b, 0) != DSM_OK) {
        child_report(status_fd, B_INIT_FAIL, b_reason(B_INIT_FAIL));
        _exit(B_INIT_FAIL);
    }
    if (dsm_thread_attach() != DSM_OK) {
        child_report(status_fd, B_ATTACH_FAIL, b_reason(B_ATTACH_FAIL));
        _exit(B_ATTACH_FAIL);
    }

    void *base = NULL;
    if (dsm_alloc((uint64_t)BLOCK_PAGES * PAGE_BYTES, &base) != DSM_OK) {
        child_report(status_fd, B_ALLOC_FAIL, b_reason(B_ALLOC_FAIL));
        _exit(B_ALLOC_FAIL);
    }
    uint8_t *block = (uint8_t *)base;

    uint32_t slot = 0;
    uint32_t nodes = 0;
    if (dsm_node_id(&slot) != DSM_OK || dsm_node_count(&nodes) != DSM_OK ||
        slot == 0 || nodes != 2) {
        char detail[STATUS_LINE_MAX];
        snprintf(detail, sizeof(detail),
                 "B esperaba un slot distinto de 0 de 2 nodos; obtuvo slot %u de %u",
                 slot, nodes);
        child_report(status_fd, B_SLOT_MISMATCH, detail);
        _exit(B_SLOT_MISMATCH);
    }

    /* El bloque debe tener al menos una página cuyo home sea B, o la prueba no
     * ejercitaría el traspaso. */
    dsm_ctx_t *ctx = dsm_ctx_get();
    uint32_t own_home = 0;
    for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
        if (ctx && ring_home_of(ctx, p) == ctx->memb.self_slot) own_home++;
    }
    if (own_home == 0) {
        child_report(status_fd, B_HOME_STILL_SELF, b_reason(B_HOME_STILL_SELF));
        _exit(B_HOME_STILL_SELF);
    }

    child_report(status_fd, B_OK, "joined");
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_READ) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de lectura");
        _exit(B_COMMAND_FAIL);
    }
    /* Cada página cuyo home es B se sirve desde la copia que A dejó al
     * transferirla: el patrón debe coincidir con el que escribió A. */
    if (!block_matches(block, 0)) {
        char detail[STATUS_LINE_MAX];
        uint32_t bad = BLOCK_PAGES;
        for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
            if (!block_page_matches(block, p, PATTERN_A(p))) {
                bad = p;
                break;
            }
        }
        snprintf(detail, sizeof(detail),
                 "%s: primera pagina distinta %u (page[0]=0x%08X, esperaba 0x%08X)",
                 b_reason(B_READ_MISMATCH), bad,
                 bad < BLOCK_PAGES
                     ? ((const uint32_t *)(block + (size_t)bad * PAGE_BYTES))[0]
                     : 0u,
                 bad < BLOCK_PAGES ? PATTERN_A(bad) : 0u);
        child_report(status_fd, B_READ_MISMATCH, detail);
        _exit(B_READ_MISMATCH);
    }
    child_report(status_fd, B_OK, "read");

    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_WRITE) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de escritura");
        _exit(B_COMMAND_FAIL);
    }
    for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
        block_fill(block, p, PATTERN_B(p));
    }
    if (!block_matches(block, 1)) {
        child_report(status_fd, B_WRITE_MISMATCH, b_reason(B_WRITE_MISMATCH));
        _exit(B_WRITE_MISMATCH);
    }
    child_report(status_fd, B_OK, "write");

    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_QUIT) {
        child_report(status_fd, B_COMMAND_FAIL, b_reason(B_COMMAND_FAIL));
        _exit(B_COMMAND_FAIL);
    }

    dsm_thread_detach();
    if (dsm_finalize() != DSM_OK) {
        child_report(status_fd, B_FINALIZE_FAIL, b_reason(B_FINALIZE_FAIL));
        _exit(B_FINALIZE_FAIL);
    }
    child_report(status_fd, B_OK, "quit");
    _exit(0);
}

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

static int log_shows_window_conflict(const char *log_path)
{
    return file_contains(log_path, "discrepancia en dsm.conf") ||
           file_contains(log_path, "No hay puertos disponibles");
}

static int run_scenario(uint16_t port_min, uint16_t port_max)
{
    char conf_a[64] = "";
    char conf_b[64] = "";
    char log_b[64] = "";
    int conf_a_fd = -1;
    int conf_b_fd = -1;
    int log_b_fd = -1;
    int command[2] = { -1, -1 };
    int status[2]  = { -1, -1 };
    pid_t child = -1;
    int result = SCENARIO_FAIL;
    int ok = 1;
    int b_code = B_OK;
    char reason[STATUS_LINE_MAX];
    reason[0] = '\0';

    if (!make_temp(conf_a, sizeof(conf_a), &conf_a_fd) ||
        !make_temp(conf_b, sizeof(conf_b), &conf_b_fd) ||
        !make_temp(log_b, sizeof(log_b), &log_b_fd)) {
        printf("   no se pudieron crear los archivos temporales\n");
        goto cleanup;
    }
    close(conf_a_fd);
    close(conf_b_fd);
    conf_a_fd = -1;
    conf_b_fd = -1;

    write_conf(conf_a, port_min, port_max);
    write_conf(conf_b, port_min, port_max);

    if (pipe(command) != 0 || pipe(status) != 0) {
        printf("   no se pudieron crear los pipes de control\n");
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
        dup2(log_b_fd, STDERR_FILENO);

        close(log_b_fd);
        close(command[1]);
        close(status[0]);
        run_child(conf_b, status[1], command[0]);
    }

    close(log_b_fd);
    log_b_fd = -1;
    close(command[0]);
    command[0] = -1;
    close(status[1]);
    status[1] = -1;

    /* A arranca solo y escribe el bloque completo: todas las páginas quedan con
     * home A. */
    setenv("DSM_NODE_TAG", "home_migration_a", 1);
    if (dsm_init(conf_a, 0) != DSM_OK) {
        /* Un nodo ajeno dentro del tramo aborta el descubrimiento con
         * DSM_ERR_CONFIG o deja el puerto ocupado: se reintenta con otro tramo. */
        printf("   el nodo A no pudo arrancar en el tramo %u-%u\n",
               port_min, port_max);
        result = SCENARIO_CONFLICT;
        goto cleanup;
    }

    if (dsm_thread_attach() != DSM_OK) {
        printf("   dsm_thread_attach fallo en el nodo A\n");
        ok = 0;
        goto cleanup;
    }

    void *base = NULL;
    if (dsm_alloc((uint64_t)BLOCK_PAGES * PAGE_BYTES, &base) != DSM_OK) {
        printf("   dsm_alloc fallo en el nodo A\n");
        ok = 0;
        goto cleanup;
    }
    uint8_t *block = (uint8_t *)base;

    uint32_t a_slot = 0;
    uint32_t a_nodes = 0;
    if (dsm_node_id(&a_slot) != DSM_OK || dsm_node_count(&a_nodes) != DSM_OK ||
        a_slot != 0 || a_nodes != 1) {
        printf("   A esperaba ser el slot 0 de 1 nodo; obtuvo slot %u de %u\n",
               a_slot, a_nodes);
        ok = 0;
        goto cleanup;
    }

    for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
        block_fill(block, p, PATTERN_A(p));
    }
    if (!block_matches(block, 0)) {
        printf("   A no pudo escribir el bloque inicial\n");
        ok = 0;
        goto cleanup;
    }

    /* B se une y confirma su ranura. */
    if (!grant_step(command[1], status[0], CMD_START, "union de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no se unio al cluster: %s\n", reason);
        ok = 0;
        if (b_code == B_INIT_FAIL &&
            (window_taken_by_other(port_min, port_max) ||
             log_shows_window_conflict(log_b))) {
            result = SCENARIO_CONFLICT;
        }
        goto cleanup;
    }

    if (!wait_node_count(2, MEMBERSHIP_TIMEOUT_MS, &a_nodes)) {
        printf("   B confirmo su union, pero A sigue viendo %u nodo(s)\n", a_nodes);
        ok = 0;
        goto cleanup;
    }

    /* Con la membresía completa, parte del bloque debe tener home B. */
    dsm_ctx_t *ctx = dsm_ctx_get();
    uint32_t moved = 0;
    for (uint32_t p = 0; p < BLOCK_PAGES; p++) {
        if (ctx && ring_home_of(ctx, p) != 0) moved++;
    }
    if (moved == 0) {
        printf("   el anillo no cambio el home de ninguna pagina (%u paginas)\n",
               BLOCK_PAGES);
        ok = 0;
    }

    /* B lee el bloque: las páginas cuyo home pasó a B deben conservar el patrón
     * que escribió A antes de la union. */
    if (!grant_step(command[1], status[0], CMD_READ, "lectura de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no completo la lectura: %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    /* B escribe sus patrones con el home ya repartido. */
    if (!grant_step(command[1], status[0], CMD_WRITE, "escritura de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no completo la escritura: %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    /* A relee el bloque completo: la lectura debe traer la versión de B. */
    if (!block_matches(block, 1)) {
        printf("   A no ve el patron de B en todo el bloque\n");
        ok = 0;
    }

    /* B abandona el clúster y A queda como único custodio de todas las páginas. */
    if (!send_command(command[1], CMD_QUIT)) {
        printf("   no se pudo enviar la orden de cierre al hijo\n");
        ok = 0;
        goto cleanup;
    }
    close_fd(&command[1]);

    if (!read_status(status[0], "cierre de B", &b_code, reason, sizeof(reason))) {
        printf("   %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    if (!wait_node_count(1, MEMBERSHIP_TIMEOUT_MS, &a_nodes)) {
        printf("   A sigue viendo %u nodo(s) tras la salida de B\n", a_nodes);
        ok = 0;
        goto cleanup;
    }

    /* La relectura posterior a la salida debe seguir viendo lo que escribió B. */
    if (!block_matches(block, 1)) {
        printf("   A perdio el patron de B despues de su salida\n");
        ok = 0;
    }

cleanup:
    close_fd(&command[1]);
    close_fd(&status[0]);

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

    if (dsm_is_initialized()) {
        dsm_thread_detach();
        CHECK(dsm_finalize() == DSM_OK);
    }

    if (result != SCENARIO_CONFLICT && !ok) dump_child_log(log_b);

    close_fd(&status[1]);
    close_fd(&log_b_fd);
    if (conf_a[0]) unlink(conf_a);
    if (conf_b[0]) unlink(conf_b);
    if (log_b[0]) unlink(log_b);

    if (result == SCENARIO_CONFLICT) return SCENARIO_CONFLICT;
    return ok ? SCENARIO_OK : SCENARIO_FAIL;
}

static int test_home_changes_on_join_and_leave(void)
{
    char reason[STATUS_LINE_MAX];
    reason[0] = '\0';
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;

    for (uint32_t attempt = 1; attempt <= TEST_MAX_ATTEMPTS; attempt++) {
        uint16_t port_min = 0;
        uint16_t port_max = 0;

        if (!pick_port_window(&cursor, &port_min, &port_max)) {
            snprintf(reason, sizeof(reason),
                     "no queda un tramo libre de %u puertos en el bloque "
                     "%u-%u", TEST_PORT_WINDOW,
                     TEST_PORT_BLOCK_MIN, TEST_PORT_BLOCK_MAX);
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
                 "el traspaso fallo en el tramo %u-%u", port_min, port_max);
        break;
    }

    printf("   %s\n", reason[0] ? reason : "sin tramo de puertos disponible");
    return -1;
}

int main(void)
{
    printf("=== Test Suite: traspaso de home por cambio de anillo ===\n");
    RUN_TEST(test_home_changes_on_join_and_leave);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
