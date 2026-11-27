#include "test.h"
#include "dsm.h"

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
 * Flujo MSI minimo entre dos nodos sobre la misma pagina.
 *
 * El padre actua como nodo A y el hijo como nodo B. Los dos procesos comparten
 * el rango de puertos de la config, asi que el descubrimiento se serializa: A
 * completa dsm_init, dsm_thread_attach y dsm_alloc antes de conceder el arranque
 * de B por el pipe de comandos. Cada paso posterior se concede igual, de modo
 * que ningun proceso avanza antes de que el otro haya terminado el paso previo.
 *
 * El orden no es solo para evitar colisiones de puerto. El home de una pagina
 * lo decide el anillo de dispersion, que se reconstruye cuando cambia la epoca
 * de membresia, y el DSM no migra las paginas que cambian de home: el nodo que
 * pasa a ser home las sirve desde su propio alias, que aun no tiene la copia.
 * Es una limitacion del DSM, no del test, y por eso el primer acceso a la
 * pagina ocurre despues de que B confirme su union y A observe los dos nodos,
 * cuando ambos calculan el mismo home con la misma membresia.
 *
 * En ese punto la prueba exige el reparto de ranuras que describe el protocolo:
 * A arranca solo y toma el slot 0, B entra despues y toma el slot 1, y los dos
 * ven dos nodos vivos. Dos dsm_init simultaneos no garantizan ese reparto (los
 * dos procesos pueden quedarse con el slot 0) y sin embargo devuelven DSM_OK en
 * ambos, motivo por el cual la prueba no los lanza en paralelo.
 *
 * Los latidos usan un umbral amplio para que un par vivo no se declare muerto
 * mientras corre la prueba. Los plazos de conexion y descubrimiento acotan el
 * arranque cuando el tramo de puertos esta ocupado.
 *
 * El rango de puertos se elige en tiempo de ejecucion dentro del bloque
 * asignado al grupo. El descubrimiento recorre exactamente PORT_MIN..PORT_MAX y
 * aborta con DSM_ERR_CONFIG si encuentra un nodo con otro shared_hash, de modo que
 * un tramo propio y libre deja el escaneo limitado a los dos nodos de la prueba.
 * Como el tramo puede ser ocupado por otro proceso entre la comprobacion y el
 * arranque, ese caso se reintenta con otro tramo y no se cuenta como fallo.
 *
 * B redirige stdout a /dev/null y su stderr a un archivo temporal; el resultado
 * de cada paso viaja por el pipe de estado como "B <codigo> <descripcion>".
 */

#define PAGE_BYTES 4096u
#define PATTERN_A  0x5Au
#define PATTERN_B  0xA5u

#define STEP_TIMEOUT_MS 30000
#define EXIT_TIMEOUT_MS 10000
#define MEMBERSHIP_TIMEOUT_MS 5000
#define STATUS_LINE_MAX 256

/* Tramo del bloque de puertos del grupo que esta prueba reserva para si. */
#define TEST_PORT_BLOCK_MIN 27600u
#define TEST_PORT_BLOCK_MAX 27699u
#define TEST_PORT_WINDOW    4u
#define TEST_MAX_ATTEMPTS   3

#define CMD_START 'S'
#define CMD_READ  'R'
#define CMD_WRITE 'W'
#define CMD_QUIT  'Q'

/* Codigos de resultado que el hijo publica por el pipe de estado. */
enum {
    B_OK             = 0,
    B_INIT_FAIL      = 1,
    B_ATTACH_FAIL    = 2,
    B_ALLOC_FAIL     = 3,
    B_SLOT_MISMATCH  = 4,
    B_READ_MISMATCH  = 5,
    B_WRITE_MISMATCH = 6,
    B_COMMAND_FAIL   = 7,
    B_FINALIZE_FAIL  = 8
};

enum {
    SCENARIO_OK       =  0,
    SCENARIO_CONFLICT =  1, /* el tramo de puertos lo ocupo otro proceso */
    SCENARIO_FAIL     = -1
};

static const char *b_reason(int code)
{
    switch (code) {
    case B_OK:             return "paso completado";
    case B_INIT_FAIL:      return "dsm_init fallo";
    case B_ATTACH_FAIL:    return "dsm_thread_attach fallo";
    case B_ALLOC_FAIL:     return "dsm_alloc fallo";
    case B_SLOT_MISMATCH:  return "el nodo B no obtuvo el slot esperado";
    case B_READ_MISMATCH:  return "la lectura no devolvio el patron de A";
    case B_WRITE_MISMATCH: return "la relectura no devolvio el patron escrito";
    case B_COMMAND_FAIL:   return "sin orden del padre";
    case B_FINALIZE_FAIL:  return "dsm_finalize fallo";
    default:               return "codigo desconocido";
    }
}

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
 * superior exclusivo: los intentos anteriores ya usaron los tramos por encima
 * de el, y repetirlos solo agrega puertos en TIME_WAIT. Devuelve 0 si no queda
 * ningun tramo por debajo. Al elegir un tramo actualiza 'cursor' a su base.
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
            "REDUNDANCY=false\n"
            "NODE_MEM=67108864\n"
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
            port_min, port_max);
    fclose(f);
}

/* Crea un archivo temporal ya abierto y devuelve su ruta en 'path'. */
static int make_temp(char *path, size_t cap, int *out_fd)
{
    snprintf(path, cap, "/tmp/dsm_page_flow_XXXXXX");
    int fd = mkstemp(path);
    if (fd < 0) {
        path[0] = '\0';
        return 0;
    }
    *out_fd = fd;
    return 1;
}

/*
 * Espera a que el descriptor tenga datos. Devuelve 1 si hay datos, 0 ante
 * timeout y -1 si poll fallo.
 */
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

/* Lee exactamente len bytes. Devuelve 1 si los obtuvo y 0 si no. */
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

/*
 * Extrae una linea completa del pipe de estado y la interpreta. Devuelve 1 con
 * codigo 0, y 0 con el motivo en 'reason' ante timeout, EOF, codigo de error o
 * linea mal formada. El codigo recibido queda en 'out_code' cuando no es NULL.
 */
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

/* Concede un paso al hijo y espera su confirmacion. */
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

/* Vuelca el log del hijo para diagnosticar un fallo del nodo B. */
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

static int page_matches(const volatile uint8_t *page, uint8_t expected)
{
    for (size_t i = 0; i < PAGE_BYTES; i++) {
        if (page[i] != expected) return 0;
    }
    return 1;
}

static void page_fill(volatile uint8_t *page, uint8_t value)
{
    for (size_t i = 0; i < PAGE_BYTES; i++) {
        page[i] = value;
    }
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

/*
 * Espera a que la membresia local alcance 'expected' nodos vivos. La tabla se
 * actualiza desde el hilo listener, asi que la convergencia entre los dos nodos
 * no es instantanea; el plazo acota la espera y deja el fallo como un mensaje
 * explicito en lugar de un cuelgue.
 */
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
 * Cuerpo del nodo B. Nunca retorna. Espera la orden de arranque, se une al
 * cluster que ya levanto A y responde a cada paso concedido por el padre.
 */
static void run_child(const char *conf_b, int status_fd, int cmd_fd)
{
    /* El padre puede cerrar el pipe de estado al abandonar la prueba; el
     * reporte de un fallo debe terminar con _exit y no con SIGPIPE. */
    signal(SIGPIPE, SIG_IGN);

    char command = 0;
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_START) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de arranque");
        _exit(B_COMMAND_FAIL);
    }

    setenv("DSM_NODE_TAG", "page_flow_b", 1);
    if (dsm_init(conf_b, 0) != DSM_OK) {
        child_report(status_fd, B_INIT_FAIL, b_reason(B_INIT_FAIL));
        _exit(B_INIT_FAIL);
    }
    if (dsm_thread_attach() != DSM_OK) {
        child_report(status_fd, B_ATTACH_FAIL, b_reason(B_ATTACH_FAIL));
        _exit(B_ATTACH_FAIL);
    }

    void *base = NULL;
    if (dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        child_report(status_fd, B_ALLOC_FAIL, b_reason(B_ALLOC_FAIL));
        _exit(B_ALLOC_FAIL);
    }
    volatile uint8_t *page = (volatile uint8_t *)base;

    uint32_t slot = 0;
    uint32_t nodes = 0;
    if (dsm_node_id(&slot) != DSM_OK || dsm_node_count(&nodes) != DSM_OK ||
        slot != 1 || nodes != 2) {
        char detail[STATUS_LINE_MAX];
        snprintf(detail, sizeof(detail),
                 "B esperaba slot 1 de 2 nodos; obtuvo slot %u de %u nodos",
                 slot, nodes);
        child_report(status_fd, B_SLOT_MISMATCH, detail);
        _exit(B_SLOT_MISMATCH);
    }

    /* La union termino y el reparto de ranuras es el esperado. */
    child_report(status_fd, B_OK, "joined");
    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_READ) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de lectura");
        _exit(B_COMMAND_FAIL);
    }
    /* El fallo de lectura debe traer la escritura de A. */
    if (!page_matches(page, PATTERN_A)) {
        child_report(status_fd, B_READ_MISMATCH, b_reason(B_READ_MISMATCH));
        _exit(B_READ_MISMATCH);
    }
    child_report(status_fd, B_OK, "read");

    if (!read_exact(cmd_fd, &command, 1, STEP_TIMEOUT_MS) || command != CMD_WRITE) {
        child_report(status_fd, B_COMMAND_FAIL, "sin orden de escritura");
        _exit(B_COMMAND_FAIL);
    }
    /* El fallo de escritura invalida la copia de A antes de conceder M. */
    page_fill(page, PATTERN_B);
    if (!page_matches(page, PATTERN_B)) {
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

/*
 * Un tramo se considera ajeno cuando, tras un fallo de arranque, alguno de sus
 * puertos ya no se puede tomar. Cubre dos casos: otro proceso se quedó con el
 * puerto, o hay un nodo de otro cluster escuchando en el tramo y el HELLO se
 * rechaza por shared_hash distinto.
 */
static int window_taken_by_other(uint16_t port_min, uint16_t port_max)
{
    return !ports_available(port_min, (uint16_t)(port_max - port_min + 1));
}

/* Busca una subcadena en un archivo de texto. Devuelve 1 si aparece. */
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
 * El descubrimiento del nodo aborta con DSM_ERR_CONFIG al toparse con un nodo
 * de otro cluster, y con DSM_ERR_NET si se queda sin puerto libre. Los dos
 * mensajes identifican un tramo ocupado por otro proceso, no un fallo del
 * protocolo MSI.
 */
static int log_shows_window_conflict(const char *log_path)
{
    return file_contains(log_path, "discrepancia en dsm.conf") ||
           file_contains(log_path, "No hay puertos disponibles");
}

/*
 * Un intento completo sobre un tramo de puertos concreto. Deja el DSM del padre
 * finalizado y el hijo recogido en todas las salidas.
 */
static int run_scenario(uint16_t port_min, uint16_t port_max)
{
    char conf_a[64] = "";
    char conf_b[64] = "";
    char log_b[64] = "";
    int conf_a_fd = -1;
    int conf_b_fd = -1;
    int log_b_fd = -1;
    int command[2] = { -1, -1 }; /* A -> B: concesion de cada paso */
    int status[2]  = { -1, -1 }; /* B -> A: resultado de cada paso */
    pid_t child = -1;
    int result = SCENARIO_FAIL;
    int ok = 1;
    int b_code = B_OK;
    char reason[STATUS_LINE_MAX];

    if (!make_temp(conf_a, sizeof(conf_a), &conf_a_fd) ||
        !make_temp(conf_b, sizeof(conf_b), &conf_b_fd) ||
        !make_temp(log_b, sizeof(log_b), &log_b_fd)) {
        printf("   no se pudieron crear los archivos temporales\n");
        result = SCENARIO_FAIL;
        goto cleanup;
    }
    close(conf_a_fd);
    close(conf_b_fd);
    conf_a_fd = -1;
    conf_b_fd = -1;

    write_conf(conf_a, port_min, port_max);
    write_conf(conf_b, port_min, port_max);

    if (pipe(command) != 0) {
        printf("   no se pudo crear el pipe de comandos\n");
        result = SCENARIO_FAIL;
        goto cleanup;
    }
    if (pipe(status) != 0) {
        printf("   no se pudo crear el pipe de estado\n");
        result = SCENARIO_FAIL;
        goto cleanup;
    }

    child = fork();
    if (child < 0) {
        printf("   fork fallo: %s\n", strerror(errno));
        result = SCENARIO_FAIL;
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

    /* A arranca solo: nadie mas escucha en el tramo elegido. */
    setenv("DSM_NODE_TAG", "page_flow_a", 1);
    if (dsm_init(conf_a, 0) != DSM_OK) {
        if (window_taken_by_other(port_min, port_max)) {
            result = SCENARIO_CONFLICT;
            goto cleanup;
        }
        printf("   el nodo A no pudo arrancar en el tramo %u-%u\n",
               port_min, port_max);
        result = SCENARIO_FAIL;
        goto cleanup;
    }

    if (dsm_thread_attach() != DSM_OK) {
        printf("   dsm_thread_attach fallo en el nodo A\n");
        ok = 0;
        goto cleanup;
    }

    void *base = NULL;
    if (dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("   dsm_alloc fallo en el nodo A\n");
        ok = 0;
        goto cleanup;
    }
    volatile uint8_t *page = (volatile uint8_t *)base;

    uint32_t a_slot = 0;
    uint32_t a_nodes = 0;
    if (dsm_node_id(&a_slot) != DSM_OK || dsm_node_count(&a_nodes) != DSM_OK ||
        a_slot != 0 || a_nodes != 1) {
        printf("   A esperaba ser el slot 0 de 1 nodo; obtuvo slot %u de %u\n",
               a_slot, a_nodes);
        ok = 0;
        goto cleanup;
    }

    /* B se une al cluster y confirma; solo entonces A toca la pagina. */
    if (!grant_step(command[1], status[0], CMD_START, "union de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no se unio al cluster: %s\n", reason);
        ok = 0;
        if (b_code == B_INIT_FAIL &&
            (window_taken_by_other(port_min, port_max) ||
             log_shows_window_conflict(log_b))) {
            /* El descubrimiento de B aborta al toparse con un nodo ajeno
             * dentro del tramo; ese caso se reintenta con otro tramo. */
            result = SCENARIO_CONFLICT;
        }
        goto cleanup;
    }

    /* Con la membresia completa, el reparto de ranuras y el home coinciden. */
    if (!wait_node_count(2, MEMBERSHIP_TIMEOUT_MS, &a_nodes)) {
        printf("   B confirmo su union como slot 1, pero A sigue viendo %u nodo(s)"
               " tras %d ms\n", a_nodes, MEMBERSHIP_TIMEOUT_MS);
        ok = 0;
        goto cleanup;
    }

    page_fill(page, PATTERN_A);
    if (!page_matches(page, PATTERN_A)) {
        printf("   A no pudo escribir su patron en la pagina\n");
        ok = 0;
        goto cleanup;
    }

    if (!grant_step(command[1], status[0], CMD_READ, "lectura de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no completo la lectura: %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    if (!grant_step(command[1], status[0], CMD_WRITE, "escritura de B",
                    &b_code, reason, sizeof(reason))) {
        printf("   B no completo la escritura: %s\n", reason);
        ok = 0;
        goto cleanup;
    }

    /*
     * A relee despues de la escritura de B: la invalidacion debio retirarle la
     * copia, de modo que el fallo de lectura trae la version de B.
     */
    if (!page_matches(page, PATTERN_B)) {
        printf("   A relee un valor distinto del que escribio B: page[0]=0x%02X,"
               " esperaba 0x%02X\n", (unsigned)page[0], (unsigned)PATTERN_B);
        ok = 0;
    }

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

static int test_remote_page_fault_and_write_invalidation(void)
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

        /* Un tramo ocupado por otro proceso no es un fallo del flujo. */
        int rc = run_scenario(port_min, port_max);
        if (rc == SCENARIO_CONFLICT) {
            printf("   el tramo %u-%u lo ocupa otro proceso; se reintenta\n",
                   port_min, port_max);
            continue;
        }
        if (rc == SCENARIO_OK) return 0;

        snprintf(reason, sizeof(reason),
                 "el flujo fallo en el tramo %u-%u", port_min, port_max);
        break;
    }

    printf("   %s\n", reason[0] ? reason : "sin tramo de puertos disponible");
    return -1;
}

int main(void)
{
    printf("=== Test Suite: Flujo minimo de fallo de pagina MSI ===\n");
    RUN_TEST(test_remote_page_fault_and_write_invalidation);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
