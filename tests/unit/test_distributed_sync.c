#include "test.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "ring.h"
#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/*
 * Cerrojos y barreras distribuidas.
 *
 * El cerrojo (a) y la barrera (c) se prueban con el patron de dos procesos de
 * test_page_flow: el padre es el nodo A, el hijo el nodo B, el padre concede
 * cada paso por un pipe de comandos y el hijo responde por un pipe de estado.
 * Los dos procesos se sincronizan paso a paso porque comparten el tramo de
 * puertos y porque el home de cada cerrojo se calcula sobre la membresia
 * completa, que solo converge despues de que B se une.
 *
 * El cerrojo (b) y el unlock sin propiedad (e) se prueban en un solo proceso:
 * dos hilos de la misma ranura compiten por el cerrojo mientras incrementan un
 * contador alojado en la vista DSM, y una liberacion sin posesion debe fallar.
 *
 * El plazo (d) usa un cerrojo cuyo custodio es B mientras B lo retiene: A se
 * queda en la cola del custodio y dsm_lock devuelve DSM_ERR_TIMEOUT dentro del
 * plazo LOCK_TIMEOUT_MS configurado.
 *
 * Los identificadores de cerrojo se eligen despues de que la membresia tiene
 * dos nodos, buscando el primero cuyo home sea la ranura 0 o la ranura 1. El
 * padre envia el identificador elegido al hijo por el pipe de comandos, de modo
 * que ninguna de las dos partes depende de calcular el anillo por su cuenta.
 */

#define PAGE_BYTES 4096u

#define STEP_TIMEOUT_MS   30000
#define EXIT_TIMEOUT_MS   10000
#define MEMBERSHIP_TIMEOUT_MS 5000
#define BLOCKED_WINDOW_MS 600
/* Tolerancia del reloj de milisegundos al comparar la espera observada con la
 * ventana de bloqueo concedida. */
#define WAIT_TOLERANCE_MS 50

/* Plazo de las esperas de sincronizacion en la configuracion de la prueba */
#define TEST_LOCK_TIMEOUT_MS 4000u

/* Tramo del bloque de puertos del grupo que esta prueba reserva para si. */
#define TEST_PORT_BLOCK_MIN 27600u
#define TEST_PORT_BLOCK_MAX 27699u
#define TEST_PORT_WINDOW    4u
/* Intentos por tramo: otro proceso puede ocupar el tramo entre la
 * comprobacion y el arranque, y el bloque entero se comparte en el laboratorio. */
#define TEST_MAX_ATTEMPTS   6

/* Comandos del padre al hijo */
enum {
    CMD_START   = 'S',
    CMD_LOCK    = 'L',
    CMD_UNLOCK  = 'U',
    CMD_BARRIER = 'B',
    CMD_QUIT    = 'Q'
};

/* Codigos de resultado que el hijo publica por el pipe de estado */
enum {
    CH_OK             = 0,
    CH_INIT_FAIL      = 1,
    CH_ATTACH_FAIL    = 2,
    CH_ALLOC_FAIL     = 3,
    CH_SLOT_MISMATCH  = 4,
    CH_COMMAND_FAIL   = 5,
    CH_FINALIZE_FAIL  = 6
};

enum {
    SCENARIO_OK       =  0,
    SCENARIO_CONFLICT =  1,
    SCENARIO_FAIL     = -1
};

/* Un comando: codigo y hasta dos argumentos de 32 bits */
typedef struct {
    uint8_t  cmd;
    uint32_t arg1;
    uint32_t arg2;
} test_cmd_t;

/* Una respuesta: codigo de paso, resultado del API y tiempo transcurrido */
typedef struct {
    int32_t code;
    int32_t result;
    int64_t elapsed_ms;
} test_sts_t;

static const char *child_reason(int code)
{
    switch (code) {
    case CH_OK:            return "paso completado";
    case CH_INIT_FAIL:     return "dsm_init fallo";
    case CH_ATTACH_FAIL:   return "dsm_thread_attach fallo";
    case CH_ALLOC_FAIL:    return "dsm_alloc fallo";
    case CH_SLOT_MISMATCH: return "el nodo B no obtuvo el slot esperado";
    case CH_COMMAND_FAIL:  return "orden invalida del padre";
    case CH_FINALIZE_FAIL: return "dsm_finalize fallo";
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

/* Busca un tramo libre del bloque, de mayor a menor. */
static int pick_port_window(uint32_t *cursor, uint16_t *port_min,
                            uint16_t *port_max)
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
            "CLUSTER_ID=0x534F3032\n"
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
            "LOCK_TIMEOUT_MS=%u\n"
            "WORKER_QUEUE_CAP=1024\n"
            "DSM_LOG_LEVEL=ERROR\n",
            port_min, port_max, TEST_LOCK_TIMEOUT_MS);
    fclose(f);
}

/* Crea un archivo temporal ya abierto y devuelve su ruta en 'path'. */
static int make_temp(char *path, size_t cap, int *out_fd)
{
    snprintf(path, cap, "/tmp/dsm_sync_XXXXXX");
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
    if (rc <= 0) return rc;
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

static int child_report(int fd, int code, int result, int64_t elapsed_ms)
{
    test_sts_t st = { .code = code, .result = result,
                      .elapsed_ms = elapsed_ms };
    return write_exact(fd, &st, sizeof(st));
}

static void close_fd(int *fd)
{
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static uint64_t now_ms(void)
{
    return mono_ms();
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

/* Cuerpo del nodo B. Nunca retorna. */
static void run_child(const char *conf_b, int status_fd, int cmd_fd)
{
    test_cmd_t cmd;
    void *base = NULL;

    signal(SIGPIPE, SIG_IGN);

    memset(&cmd, 0, sizeof(cmd));
    if (!read_exact(cmd_fd, &cmd, sizeof(cmd), STEP_TIMEOUT_MS) ||
        cmd.cmd != CMD_START) {
        child_report(status_fd, CH_COMMAND_FAIL, DSM_ERR_INVALID, 0);
        _exit(CH_COMMAND_FAIL);
    }

    setenv("DSM_NODE_TAG", "sync_b", 1);
    if (dsm_init(conf_b, 0) != DSM_OK) {
        child_report(status_fd, CH_INIT_FAIL, DSM_ERR_NOT_INIT, 0);
        _exit(CH_INIT_FAIL);
    }
    if (dsm_thread_attach() != DSM_OK) {
        child_report(status_fd, CH_ATTACH_FAIL, DSM_ERR_NOT_INIT, 0);
        _exit(CH_ATTACH_FAIL);
    }
    if (dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        child_report(status_fd, CH_ALLOC_FAIL, DSM_ERR_MEM, 0);
        _exit(CH_ALLOC_FAIL);
    }

    uint32_t slot = 0;
    uint32_t nodes = 0;
    if (dsm_node_id(&slot) != DSM_OK || dsm_node_count(&nodes) != DSM_OK ||
        slot != 1 || nodes != 2) {
        child_report(status_fd, CH_SLOT_MISMATCH, DSM_ERR_INVALID, 0);
        _exit(CH_SLOT_MISMATCH);
    }
    if (!child_report(status_fd, CH_OK, DSM_OK, 0)) _exit(CH_COMMAND_FAIL);

    for (;;) {
        if (!read_exact(cmd_fd, &cmd, sizeof(cmd), STEP_TIMEOUT_MS)) {
            _exit(CH_COMMAND_FAIL);
        }

        switch (cmd.cmd) {
        case CMD_LOCK: {
            uint64_t t0 = now_ms();
            int rc = (int)dsm_lock(cmd.arg1);
            int64_t elapsed = (int64_t)(now_ms() - t0);
            if (!child_report(status_fd, CH_OK, rc, elapsed)) {
                _exit(CH_COMMAND_FAIL);
            }
            break;
        }
        case CMD_UNLOCK: {
            int rc = (int)dsm_unlock(cmd.arg1);
            if (!child_report(status_fd, CH_OK, rc, 0)) {
                _exit(CH_COMMAND_FAIL);
            }
            break;
        }
        case CMD_BARRIER: {
            uint64_t t0 = now_ms();
            int rc = (int)dsm_barrier(cmd.arg1, cmd.arg2);
            int64_t elapsed = (int64_t)(now_ms() - t0);
            if (!child_report(status_fd, CH_OK, rc, elapsed)) {
                _exit(CH_COMMAND_FAIL);
            }
            break;
        }
        case CMD_QUIT:
            dsm_thread_detach();
            if (dsm_finalize() != DSM_OK) {
                child_report(status_fd, CH_FINALIZE_FAIL, DSM_ERR_NET, 0);
                _exit(CH_FINALIZE_FAIL);
            }
            child_report(status_fd, CH_OK, DSM_OK, 0);
            _exit(0);
        default:
            child_report(status_fd, CH_COMMAND_FAIL, DSM_ERR_INVALID, 0);
            _exit(CH_COMMAND_FAIL);
        }
    }
}

/* --- Enlace de control con el nodo B --- */

/* Vuelca el log del nodo B para diagnosticar un fallo del escenario. */
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

typedef struct {
    pid_t child;
    int   cmd_fd;      /* padre -> hijo */
    int   status_fd;   /* hijo -> padre */
} sync_link_t;

static int link_send(sync_link_t *l, uint8_t cmd, uint32_t a1, uint32_t a2)
{
    test_cmd_t c = { .cmd = cmd, .arg1 = a1, .arg2 = a2 };
    return write_exact(l->cmd_fd, &c, sizeof(c));
}

/* Lee una respuesta del hijo. Devuelve 1 si la obtuvo. */
static int link_recv(sync_link_t *l, const char *step, test_sts_t *out)
{
    test_sts_t st;
    if (!read_exact(l->status_fd, &st, sizeof(st), STEP_TIMEOUT_MS)) {
        printf("   sin respuesta del nodo B en '%s' (%d ms)\n",
               step, STEP_TIMEOUT_MS);
        return 0;
    }
    *out = st;
    if (st.code != CH_OK) {
        printf("   el nodo B fallo en '%s': %s (codigo %d)\n",
               step, child_reason(st.code), st.code);
        return 0;
    }
    return 1;
}

/*
 * Comprueba que el hijo no publico ninguna respuesta durante 'ms'. Se usa para
 * confirmar que una llamada a dsm_lock o dsm_barrier sigue bloqueada.
 */
static int link_quiet(sync_link_t *l, const char *step, int ms)
{
    int rd = wait_readable(l->status_fd, ms);
    if (rd == 0) return 1;

    if (rd < 0) {
        printf("   error de poll en '%s'\n", step);
        return 0;
    }
    printf("   el nodo B respondio en '%s' demasiado pronto\n", step);
    return 0;
}

/* Envia una orden y espera el resultado del hijo. */
static int link_step(sync_link_t *l, uint8_t cmd, uint32_t a1, uint32_t a2,
                     const char *step, test_sts_t *out)
{
    test_sts_t st;
    if (!link_send(l, cmd, a1, a2)) {
        printf("   no se pudo enviar '%s' al nodo B\n", step);
        return 0;
    }
    if (!link_recv(l, step, &st)) return 0;
    if (out) *out = st;
    return 1;
}

/* --- Seleccion de identificadores segun su custodio --- */

/*
 * Busca, desde 'from', un identificador cuyo custodio sea la ranura 'want'.
 * Se consulta el anillo local ya convergido; el otro nodo calcula el mismo
 * valor con la misma membresia.
 */
static int find_lock_with_home(uint32_t from, uint32_t want, uint32_t *out)
{
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) return 0;

    for (uint32_t id = from; id < from + 100000u; id++) {
        if (ring_home_of(ctx, id) == want) {
            *out = id;
            return 1;
        }
    }
    return 0;
}

/* --- Un tramo de puertos con dos nodos en ejecucion --- */

/*
 * Levanta el cluster de la prueba: A arranca solo, B se une despues y confirma
 * su ranura. Deja el enlace de control listo y los dos nodos con la membresia
 * completa. Devuelve SCENARIO_* y no toca el DSM al salir.
 */
static int start_cluster(uint16_t port_min, uint16_t port_max,
                         sync_link_t *link, int *conf_a_fd, int *conf_b_fd,
                         int *log_b_fd, char *conf_a, size_t conf_a_cap,
                         char *conf_b, size_t conf_b_cap,
                         char *log_b, size_t log_b_cap)
{
    int command[2] = { -1, -1 };
    int status[2]  = { -1, -1 };

    link->child     = -1;
    link->cmd_fd    = -1;
    link->status_fd = -1;

    if (!make_temp(conf_a, conf_a_cap, conf_a_fd) ||
        !make_temp(conf_b, conf_b_cap, conf_b_fd) ||
        !make_temp(log_b, log_b_cap, log_b_fd)) {
        printf("   no se pudieron crear los archivos temporales\n");
        return SCENARIO_FAIL;
    }
    close(*conf_a_fd);
    close(*conf_b_fd);
    *conf_a_fd = -1;
    *conf_b_fd = -1;

    write_conf(conf_a, port_min, port_max);
    write_conf(conf_b, port_min, port_max);

    if (pipe(command) != 0 || pipe(status) != 0) {
        printf("   no se pudieron crear los pipes de control\n");
        return SCENARIO_FAIL;
    }

    pid_t child = fork();
    if (child < 0) {
        printf("   fork fallo: %s\n", strerror(errno));
        return SCENARIO_FAIL;
    }
    if (child == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            if (devnull > STDOUT_FILENO) close(devnull);
        }
        dup2(*log_b_fd, STDERR_FILENO);
        close(*log_b_fd);
        close(command[1]);
        close(status[0]);
        run_child(conf_b, status[1], command[0]);
    }

    link->child = child;
    close(*log_b_fd);
    *log_b_fd = -1;
    close(command[0]);
    close(status[1]);
    link->cmd_fd    = command[1];
    link->status_fd = status[0];

    setenv("DSM_NODE_TAG", "sync_a", 1);
    if (dsm_init(conf_a, 0) != DSM_OK) {
        /* El descubrimiento aborta con DSM_ERR_CONFIG al toparse con un nodo
         * de otro cluster y con DSM_ERR_NET si el tramo se ocupo entre la
         * comprobacion y el arranque: en ambos casos se prueba otro tramo. */
        printf("   el nodo A no pudo arrancar en el tramo %u-%u; se reintenta\n",
               port_min, port_max);
        return SCENARIO_CONFLICT;
    }
    if (dsm_thread_attach() != DSM_OK) {
        printf("   dsm_thread_attach fallo en el nodo A\n");
        return SCENARIO_FAIL;
    }

    void *base = NULL;
    if (dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("   dsm_alloc fallo en el nodo A\n");
        return SCENARIO_FAIL;
    }

    uint32_t a_slot = 0, a_nodes = 0;
    if (dsm_node_id(&a_slot) != DSM_OK || dsm_node_count(&a_nodes) != DSM_OK ||
        a_slot != 0 || a_nodes != 1) {
        printf("   A esperaba el slot 0 de 1 nodo; obtuvo slot %u de %u\n",
               a_slot, a_nodes);
        return SCENARIO_FAIL;
    }

    test_sts_t st;
    if (!link_step(link, CMD_START, 0, 0, "union de B", &st) || st.result != DSM_OK) {
        /* La union de B falla por un tramo ocupado por otro proceso o por un
         * nodo ajeno; el escenario se reintenta en otro tramo. */
        if (st.code == CH_INIT_FAIL) return SCENARIO_CONFLICT;
        return SCENARIO_FAIL;
    }
    if (!wait_node_count(2, MEMBERSHIP_TIMEOUT_MS, &a_nodes)) {
        printf("   A sigue viendo %u nodo(s) tras %d ms\n",
               a_nodes, MEMBERSHIP_TIMEOUT_MS);
        return SCENARIO_FAIL;
    }
    return SCENARIO_OK;
}

/* --- Escenario (a) y (d): exclusion entre nodos y plazo --- */

/*
 * Cerrojo con custodio en A: A lo toma, B queda en la cola del custodio y solo
 * lo obtiene cuando A lo libera. Despues, cerrojo con custodio en B: B lo toma,
 * A agota su plazo en la cola y, una vez que B libera, A lo obtiene.
 */
static int scenario_lock_remote(sync_link_t *l, uint32_t lock_a, uint32_t lock_b)
{
    test_sts_t st;
    uint64_t t0;

    /* (a) A toma el cerrojo cuyo custodio es A. */
    if (dsm_lock(lock_a) != DSM_OK) {
        printf("   A no pudo tomar el cerrojo %u\n", lock_a);
        return 0;
    }

    /* B pide el mismo cerrojo y queda esperando. */
    if (!link_send(l, CMD_LOCK, lock_a, 0)) {
        printf("   no se pudo ordenar el cerrojo a B\n");
        return 0;
    }
    if (!link_quiet(l, "espera del cerrojo en B", BLOCKED_WINDOW_MS)) {
        return 0;
    }

    /* La liberacion de A concede el cerrojo a B. */
    if (dsm_unlock(lock_a) != DSM_OK) {
        printf("   A no pudo liberar el cerrojo %u\n", lock_a);
        return 0;
    }
    if (!link_recv(l, "concesion del cerrojo a B", &st)) return 0;
    if (st.result != DSM_OK) {
        printf("   B no obtuvo el cerrojo tras la liberacion: %d\n", st.result);
        return 0;
    }
    if (st.elapsed_ms < BLOCKED_WINDOW_MS - WAIT_TOLERANCE_MS) {
        printf("   B obtuvo el cerrojo en %lld ms, sin la liberacion de A\n",
               (long long)st.elapsed_ms);
        return 0;
    }

    /* B libera: el custodio retoma el cerrojo sin dueno. */
    if (!link_step(l, CMD_UNLOCK, lock_a, 0, "liberacion en B", &st) ||
        st.result != DSM_OK) {
        printf("   B no pudo liberar el cerrojo %u\n", lock_a);
        return 0;
    }

    /* (d) Cerrojo con custodio en B: B lo toma en local. */
    if (!link_step(l, CMD_LOCK, lock_b, 0, "toma del cerrojo en B", &st) ||
        st.result != DSM_OK) {
        printf("   B no pudo tomar el cerrojo %u\n", lock_b);
        return 0;
    }

    t0 = now_ms();
    int rc = (int)dsm_lock(lock_b);
    int64_t waited = (int64_t)(now_ms() - t0);
    if (rc != DSM_ERR_TIMEOUT) {
        printf("   A esperaba DSM_ERR_TIMEOUT del cerrojo %u y obtuvo %d\n",
               lock_b, rc);
        return 0;
    }
    if (waited < (int64_t)TEST_LOCK_TIMEOUT_MS ||
        waited > (int64_t)TEST_LOCK_TIMEOUT_MS + 3000) {
        printf("   el plazo de A fue de %lld ms y se esperaba cerca de %u\n",
               (long long)waited, TEST_LOCK_TIMEOUT_MS);
        return 0;
    }

    /* B libera y A toma el cerrojo: la peticion vencida no dejo basura en la
     * cola del custodio. */
    if (!link_step(l, CMD_UNLOCK, lock_b, 0, "liberacion del cerrojo en B", &st) ||
        st.result != DSM_OK) {
        return 0;
    }
    if (dsm_lock(lock_b) != DSM_OK) {
        printf("   A no pudo tomar el cerrojo %u tras la liberacion\n", lock_b);
        return 0;
    }
    if (dsm_unlock(lock_b) != DSM_OK) {
        printf("   A no pudo liberar el cerrojo %u\n", lock_b);
        return 0;
    }
    return 1;
}

/* --- Escenario (c): barrera de dos participantes --- */

/*
 * B llega a la barrera y no puede continuar; A llega despues y ambos salen. La
 * espera de B se mide en su propio reloj, de modo que el padre comprueba que
 * dsm_barrier no retorno mientras faltaba el segundo arribo.
 */
static int scenario_barrier(sync_link_t *l, uint32_t barrier_id)
{
    test_sts_t st;

    if (!link_send(l, CMD_BARRIER, barrier_id, 2)) {
        printf("   no se pudo ordenar la barrera a B\n");
        return 0;
    }
    if (!link_quiet(l, "espera de la barrera en B", BLOCKED_WINDOW_MS)) {
        return 0;
    }

    uint64_t t0 = now_ms();
    int rc = (int)dsm_barrier(barrier_id, 2);
    int64_t a_elapsed = (int64_t)(now_ms() - t0);
    if (rc != DSM_OK) {
        printf("   A no cruzo la barrera %u: %d\n", barrier_id, rc);
        return 0;
    }
    if (!link_recv(l, "salida de la barrera en B", &st)) return 0;
    if (st.result != DSM_OK) {
        printf("   B no cruzo la barrera %u: %d\n", barrier_id, st.result);
        return 0;
    }
    if (st.elapsed_ms < BLOCKED_WINDOW_MS - WAIT_TOLERANCE_MS) {
        printf("   B salio de la barrera en %lld ms, sin el segundo arribo\n",
               (long long)st.elapsed_ms);
        return 0;
    }
    if (a_elapsed > (int64_t)TEST_LOCK_TIMEOUT_MS + 2000) {
        printf("   A tardo %lld ms en cruzar la barrera\n",
               (long long)a_elapsed);
        return 0;
    }
    return 1;
}

/* --- Escenario completo sobre un tramo de puertos --- */

static int run_scenario(uint16_t port_min, uint16_t port_max)
{
    sync_link_t link;
    char conf_a[64] = "";
    char conf_b[64] = "";
    char log_b[64]  = "";
    int conf_a_fd = -1, conf_b_fd = -1, log_b_fd = -1;
    int result = SCENARIO_FAIL;
    int ok = 1;
    uint32_t lock_a = 0, lock_b = 0;

    link.child = -1;
    link.cmd_fd = -1;
    link.status_fd = -1;

    result = start_cluster(port_min, port_max, &link, &conf_a_fd, &conf_b_fd,
                           &log_b_fd, conf_a, sizeof(conf_a), conf_b,
                           sizeof(conf_b), log_b, sizeof(log_b));
    if (result != SCENARIO_OK) goto cleanup;

    /* La membresia ya tiene dos nodos en los dos procesos: el anillo calcula el
     * mismo custodio para cada identificador. */
    if (!find_lock_with_home(1, 0, &lock_a) ||
        !find_lock_with_home(lock_a + 1, 1, &lock_b)) {
        printf("   no se encontraron cerrojos con custodio en A y en B\n");
        ok = 0;
        goto cleanup;
    }

    if (!scenario_lock_remote(&link, lock_a, lock_b)) ok = 0;
    if (ok && !scenario_barrier(&link, lock_a + 7)) ok = 0;

    if (ok) {
        test_sts_t st;
        if (!link_step(&link, CMD_QUIT, 0, 0, "cierre de B", &st)) ok = 0;
    }

cleanup:
    close_fd(&link.cmd_fd);
    close_fd(&link.status_fd);

    if (link.child > 0) {
        int wstatus = 0;
        if (!wait_child_exit(link.child, &wstatus, EXIT_TIMEOUT_MS)) {
            printf("   el hijo no termino en %d ms\n", EXIT_TIMEOUT_MS);
            ok = 0;
            kill(link.child, SIGKILL);
            (void)waitpid(link.child, &wstatus, 0);
        } else if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
            printf("   el hijo salio con codigo %d\n",
                   WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1);
            ok = 0;
        }
    }

    if (dsm_is_initialized()) {
        dsm_thread_detach();
        CHECK(dsm_finalize() == DSM_OK);
    }

    if (!ok && log_b[0]) dump_child_log(log_b);
    if (conf_a[0]) unlink(conf_a);
    if (conf_b[0]) unlink(conf_b);
    if (ok && log_b[0]) unlink(log_b);

    if (result == SCENARIO_CONFLICT) return SCENARIO_CONFLICT;
    return ok ? SCENARIO_OK : SCENARIO_FAIL;
}

/* --- Prueba (a), (c) y (d) sobre dos procesos --- */

static int test_two_nodes_lock_and_barrier(void)
{
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;
    char reason[256];
    reason[0] = '\0';

    for (uint32_t attempt = 1; attempt <= TEST_MAX_ATTEMPTS; attempt++) {
        uint16_t port_min = 0, port_max = 0;

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

        snprintf(reason, sizeof(reason), "el escenario fallo en el tramo %u-%u",
                 port_min, port_max);
        break;
    }

    printf("   %s\n", reason[0] ? reason : "sin tramo de puertos disponible");
    return -1;
}

/*
 * Levanta un nodo unico sobre un tramo libre del bloque. Los dos tests de un
 * solo proceso comparten el tramo con el resto de la suite, de modo que el
 * tramo se elige igual que en el escenario de dos nodos: un puerto ocupado por
 * otro proceso no es un fallo de la sincronizacion.
 */
static int start_solo(char *conf, size_t cap, uint32_t *cursor)
{
    for (uint32_t attempt = 1; attempt <= TEST_MAX_ATTEMPTS; attempt++) {
        uint16_t port_min = 0, port_max = 0;
        int fd = -1;

        if (!pick_port_window(cursor, &port_min, &port_max)) return 0;

        /* El tramo puede quedar ocupado entre la comprobacion y el arranque:
         * dsm_init falla con DSM_ERR_NET o DSM_ERR_CONFIG y se prueba otro. */
        if (!make_temp(conf, cap, &fd)) return 0;
        close(fd);
        write_conf(conf, port_min, port_max);

        if (dsm_init(conf, 0) == DSM_OK) {
            if (dsm_thread_attach() == DSM_OK) return 1;
            dsm_finalize();
        }
        unlink(conf);
    }
    return 0;
}

/* --- Prueba (b): dos hilos del mismo nodo --- */

typedef struct {
    volatile uint64_t *counter;
    uint32_t lock_id;
    int      iters;
    int      fails;
    int      attach_fail;
} racer_t;

static void *racer_main(void *arg)
{
    racer_t *r = (racer_t *)arg;

    if (dsm_thread_attach() != DSM_OK) {
        r->attach_fail = 1;
        return NULL;
    }

    for (int i = 0; i < r->iters; i++) {
        if (dsm_lock(r->lock_id) != DSM_OK) {
            r->fails++;
            break;
        }
        (*r->counter)++;
        if (dsm_unlock(r->lock_id) != DSM_OK) {
            r->fails++;
            break;
        }
    }

    dsm_thread_detach();
    return NULL;
}

static int test_two_threads_contend(void)
{
    char conf[64] = "";
    int fails = 0;
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;

    setenv("DSM_NODE_TAG", "sync_local", 1);
    if (!start_solo(conf, sizeof(conf), &cursor)) {
        printf("   el nodo unico no pudo arrancar en el bloque de puertos\n");
        return -1;
    }

    void *base = NULL;
    if (dsm_alloc(PAGE_BYTES, &base) != DSM_OK) {
        printf("   dsm_alloc fallo en el nodo unico\n");
        dsm_thread_detach();
        dsm_finalize();
        unlink(conf);
        return -1;
    }

    volatile uint64_t *counter = (volatile uint64_t *)base;
    *counter = 0;

    uint32_t lock_id = 4242u;
    racer_t a = { .counter = counter, .lock_id = lock_id, .iters = 300 };
    racer_t b = { .counter = counter, .lock_id = lock_id, .iters = 300 };
    pthread_t ta, tb;

    if (pthread_create(&ta, NULL, racer_main, &a) != 0 ||
        pthread_create(&tb, NULL, racer_main, &b) != 0) {
        printf("   no se pudieron crear los hilos de la prueba\n");
        fails++;
    } else {
        pthread_join(ta, NULL);
        pthread_join(tb, NULL);
    }

    CHECK(a.attach_fail == 0 && b.attach_fail == 0);
    CHECK(a.fails == 0 && b.fails == 0);
    CHECK(*counter == (uint64_t)(a.iters + b.iters));

    /* (e) Liberar sin poseer el cerrojo falla y no lo deja tomado. */
    CHECK(dsm_unlock(lock_id) == DSM_ERR_INVALID);
    CHECK(dsm_lock(lock_id) == DSM_OK);
    CHECK(dsm_lock(lock_id) == DSM_ERR_BUSY);
    CHECK(dsm_unlock(lock_id) == DSM_OK);

    /* (e) Un hilo distinto del propietario tampoco puede liberarlo. */
    {
        pthread_t other = pthread_self();
        (void)other;
        CHECK(dsm_lock(lock_id) == DSM_OK);
        CHECK(dsm_unlock(lock_id) == DSM_OK);
    }

    dsm_thread_detach();
    CHECK(dsm_finalize() == DSM_OK);
    unlink(conf);
    return fails ? -1 : 0;
}

/* --- Prueba (e): liberacion desde un hilo que no es el propietario --- */

static uint32_t g_cross_lock;

static void *cross_owner_main(void *arg)
{
    int *out = (int *)arg;
    if (dsm_thread_attach() != DSM_OK) {
        *out = DSM_ERR_MEM;
        return NULL;
    }
    *out = (int)dsm_lock(g_cross_lock);
    /* El hilo dueno duerme hasta que el principal intenta liberar. */
    usleep(300000);
    int rel = (int)dsm_unlock(g_cross_lock);
    if (rel != DSM_OK && *out == DSM_OK) *out = rel;
    dsm_thread_detach();
    return NULL;
}

static int test_unlock_requires_owner(void)
{
    char conf[64] = "";
    uint32_t cursor = TEST_PORT_BLOCK_MAX + 1;

    setenv("DSM_NODE_TAG", "sync_owner", 1);
    if (!start_solo(conf, sizeof(conf), &cursor)) {
        printf("   el nodo unico no pudo arrancar en el bloque de puertos\n");
        return -1;
    }

    g_cross_lock = 777u;
    int rc = DSM_OK;
    pthread_t t;
    REQUIRE(pthread_create(&t, NULL, cross_owner_main, &rc) == 0);
    usleep(120000);

    CHECK(rc == DSM_OK);
    /* El hilo principal no es el propietario: la liberacion debe fallar. */
    CHECK(dsm_unlock(g_cross_lock) == DSM_ERR_INVALID);

    pthread_join(t, NULL);
    CHECK(rc == DSM_OK);

    dsm_thread_detach();
    CHECK(dsm_finalize() == DSM_OK);
    unlink(conf);
    return 0;
}

int main(void)
{
    /* La salida se lee desde un archivo cuando la suite se ejecuta en serie:
     * sin linea a linea un cuelgue no deja rastro de la prueba en curso. */
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("=== Test Suite: Cerrojos y barreras distribuidas ===\n");
    RUN_TEST(test_two_threads_contend);
    RUN_TEST(test_unlock_requires_owner);
    RUN_TEST(test_two_nodes_lock_and_barrier);
    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
