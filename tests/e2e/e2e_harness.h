#ifndef DSM_E2E_HARNESS_H
#define DSM_E2E_HARNESS_H

/*
 * Utilidades comunes de las pruebas de extremo a extremo.
 *
 * Cada escenario reserva su propio tramo de puertos y nombra el archivo de
 * configuración con su PID, de modo que dos ejecuciones simultáneas de la
 * suite no comparten ni puertos, ni configuración, ni identidades de nodo.
 * Los hijos escriben su diagnóstico en un log por proceso que se imprime
 * cuando una espera falla, y un limpiador registrado en atexit y en las
 * señales habituales termina los procesos y libera los archivos aunque la
 * prueba aborte a mitad de camino.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define E2E_MAX_CHILDREN 8
#define E2E_LOG_REPORT   4096

typedef struct {
    pid_t pid;
    FILE *input;
    FILE *output;
    const char *name;
    char log_path[256];
    int exit_status;
    bool exited;
} e2e_child_t;

typedef struct {
    e2e_child_t *children[E2E_MAX_CHILDREN];
    size_t nchildren;
    char conf_path[256];
    char report_dir[256];
    bool prepared;
} e2e_state_t;

static e2e_state_t e2e_state;

static inline uint64_t e2e_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/* Identificador de clúster propio de la ejecución. Dos instancias de la suite
 * que coincidan en el tramo de puertos no se descubren entre sí: el
 * identificador viaja en cada mensaje y el par rechaza otro clúster. */
static inline uint32_t e2e_cluster_id(void)
{
    return 0x534F0000u | ((uint32_t)getpid() & 0xFFFFu);
}

static inline void e2e_sleep_ms(uint64_t ms)
{
    struct timespec nap;
    nap.tv_sec  = (time_t)(ms / 1000u);
    nap.tv_nsec = (long)((ms % 1000u) * 1000000u);
    nanosleep(&nap, NULL);
}

/* Cierra los extremos que quedaron abiertos y borra los archivos propios.
 * Solo toca procesos que la prueba registró, nunca ajenos. */
static inline void e2e_cleanup_all(void)
{
    for (size_t i = 0; i < e2e_state.nchildren; i++) {
        e2e_child_t *child = e2e_state.children[i];
        if (!child) continue;
        if (child->pid > 0) {
            kill(child->pid, SIGKILL);
            waitpid(child->pid, NULL, 0);
            child->pid = -1;
        }
        if (child->input) fclose(child->input);
        if (child->output) fclose(child->output);
        child->input = NULL;
        child->output = NULL;
    }
    if (e2e_state.conf_path[0]) {
        unlink(e2e_state.conf_path);
    }
}

static inline void e2e_signal_handler(int signo)
{
    for (size_t i = 0; i < e2e_state.nchildren; i++) {
        e2e_child_t *child = e2e_state.children[i];
        if (child && child->pid > 0) {
            kill(child->pid, SIGKILL);
        }
    }
    if (e2e_state.conf_path[0]) {
        unlink(e2e_state.conf_path);
    }
    _exit(128 + signo);
}

static inline void e2e_prepare(const char *scenario)
{
    if (e2e_state.prepared) return;

    long pid = (long)getpid();
    snprintf(e2e_state.conf_path, sizeof(e2e_state.conf_path),
             "/tmp/dsm_e2e_%s_%ld.conf", scenario, pid);
    snprintf(e2e_state.report_dir, sizeof(e2e_state.report_dir),
             "/tmp/dsm_e2e_%s_reporte_%ld", scenario, pid);
    e2e_state.prepared = true;

    signal(SIGPIPE, SIG_IGN);
    atexit(e2e_cleanup_all);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = e2e_signal_handler;
    sigemptyset(&sa.sa_mask);
    (void)sigaction(SIGINT, &sa, NULL);
    (void)sigaction(SIGTERM, &sa, NULL);
    (void)sigaction(SIGHUP, &sa, NULL);
    (void)sigaction(SIGQUIT, &sa, NULL);
}

/* Comprueba que un bloque de puertos esté libre. Los sockets de prueba se
 * mantienen abiertos hasta terminar el bloque completo y se cierran antes de
 * devolver el tramo, que los nodos enlazan a continuación. */
static inline bool e2e_block_free(uint16_t start, uint16_t need)
{
    int fds[16];
    uint16_t opened = 0;
    bool free_block = true;

    for (uint16_t i = 0; i < need; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            free_block = false;
            break;
        }

        int opt = 1;
        (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port        = htons((uint16_t)(start + i));

        if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
            listen(fd, 8) != 0) {
            close(fd);
            free_block = false;
            break;
        }
        fds[opened++] = fd;
    }

    for (uint16_t i = 0; i < opened; i++) {
        close(fds[i]);
    }
    return free_block;
}

/* Elige un bloque libre dentro del tramo del escenario. El desplazamiento por
 * PID separa dos ejecuciones simultáneas de la misma prueba. */
static inline bool e2e_pick_ports(uint16_t base, uint16_t span, uint16_t need,
                                  uint16_t *lo, uint16_t *hi)
{
    if (need == 0 || need > 16 || need > span) return false;

    uint16_t offset = (uint16_t)((getpid() % 8u) * 8u);
    if (offset + need > span) offset = 0;

    for (uint16_t off = offset; off + need <= span; off = (uint16_t)(off + 8u)) {
        if (e2e_block_free((uint16_t)(base + off), need)) {
            *lo = (uint16_t)(base + off);
            *hi = (uint16_t)(base + off + need - 1u);
            return true;
        }
    }
    for (uint16_t off = 0; off + need <= offset; off = (uint16_t)(off + 8u)) {
        if (e2e_block_free((uint16_t)(base + off), need)) {
            *lo = (uint16_t)(base + off);
            *hi = (uint16_t)(base + off + need - 1u);
            return true;
        }
    }
    return false;
}

static inline bool e2e_read_line(e2e_child_t *child, char *line, size_t capacity,
                                 uint64_t deadline)
{
    if (!child || !child->output || capacity == 0) return false;

    int fd = fileno(child->output);
    for (;;) {
        uint64_t now = e2e_now_ms();
        if (now >= deadline) return false;

        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int rc = poll(&pfd, 1, (int)(deadline - now));
        if (rc < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (rc == 0) return false;

        return fgets(line, (int)capacity, child->output) != NULL;
    }
}

/* Imprime el estado final del hijo y el final de su log. */
static inline bool e2e_wait_exit(e2e_child_t *child, uint64_t deadline,
                                 int *status_out);

static inline void e2e_report_child(e2e_child_t *child)
{
    if (!child) return;

    int status = child->exit_status;
    if (child->pid > 0) {
        if (!e2e_wait_exit(child, e2e_now_ms() + 1000u, &status)) {
            printf("  el proceso %s sigue en ejecución\n", child->name);
            status = -1;
        }
    }

    if (status >= 0) {
        if (child->exited && WIFSIGNALED(status)) {
            printf("  el proceso %s terminó por la señal %d\n",
                   child->name, WTERMSIG(status));
        } else if (child->exited && WIFEXITED(status)) {
            printf("  el proceso %s terminó con código %d\n",
                   child->name, WEXITSTATUS(status));
        }
    }

    FILE *log = fopen(child->log_path, "r");
    if (!log) {
        printf("  sin log de %s (%s)\n", child->name, child->log_path);
        return;
    }

    long offset = 0;
    if (fseek(log, 0, SEEK_END) == 0) {
        long size = ftell(log);
        if (size > (long)E2E_LOG_REPORT) offset = size - (long)E2E_LOG_REPORT;
    }
    rewind(log);
    if (offset > 0 && fseek(log, offset, SEEK_SET) != 0) offset = 0;

    bool any = false;
    char line[512];
    while (fgets(line, sizeof(line), log)) {
        if (!any) {
            printf("  log de %s (%s):\n", child->name, child->log_path);
            any = true;
        }
        printf("    %s", line);
    }
    if (!any) {
        printf("  log de %s (%s): vacío\n", child->name, child->log_path);
    }
    fclose(log);
}

static inline bool e2e_wait_line(e2e_child_t *child, const char *marker,
                                 char *out, size_t capacity, uint64_t deadline)
{
    char line[1024];

    for (;;) {
        if (!e2e_read_line(child, line, sizeof(line), deadline)) {
            printf("  FAIL: no se recibió '%s'\n", marker);
            e2e_report_child(child);
            return false;
        }
        printf("  %s", line);
        if (strstr(line, marker)) {
            if (out && capacity > 0) {
                snprintf(out, capacity, "%s", line);
            }
            return true;
        }
    }
}

static inline bool e2e_wait_any(e2e_child_t *child, const char *const *markers,
                                size_t nmarkers, char *out, size_t capacity,
                                uint64_t deadline)
{
    char line[1024];

    for (;;) {
        if (!e2e_read_line(child, line, sizeof(line), deadline)) {
            printf("  FAIL: no se recibió ninguno de los marcadores esperados\n");
            e2e_report_child(child);
            return false;
        }
        printf("  %s", line);
        for (size_t i = 0; i < nmarkers; i++) {
            if (strstr(line, markers[i])) {
                if (out && capacity > 0) {
                    snprintf(out, capacity, "%s", line);
                }
                return true;
            }
        }
    }
}

static inline e2e_child_t *e2e_start_child(const char *name, const char *tag,
                                           const char *mode,
                                           char *const argv[])
{
    if (e2e_state.nchildren >= E2E_MAX_CHILDREN) return NULL;

    e2e_child_t *child = calloc(1, sizeof(*child));
    if (!child) return NULL;
    child->pid  = -1;
    child->name = name;
    snprintf(child->log_path, sizeof(child->log_path),
             "/tmp/dsm_e2e_%s_%ld.log", name, (long)getpid());

    int input_pipe[2];
    int output_pipe[2];
    if (pipe2(input_pipe, O_CLOEXEC) != 0) {
        free(child);
        return NULL;
    }
    if (pipe2(output_pipe, O_CLOEXEC) != 0) {
        close(input_pipe[0]);
        close(input_pipe[1]);
        free(child);
        return NULL;
    }

    pid_t pid = fork();
    if (pid < 0) {
        close(input_pipe[0]);
        close(input_pipe[1]);
        close(output_pipe[0]);
        close(output_pipe[1]);
        free(child);
        return NULL;
    }

    if (pid == 0) {
        int log_fd = open(child->log_path, O_CREAT | O_WRONLY | O_TRUNC, 0600);
        if (dup2(input_pipe[0], STDIN_FILENO) < 0 ||
            dup2(output_pipe[1], STDOUT_FILENO) < 0 ||
            (log_fd >= 0 && dup2(log_fd, STDERR_FILENO) < 0)) {
            _exit(126);
        }
        close(input_pipe[0]);
        close(input_pipe[1]);
        close(output_pipe[0]);
        close(output_pipe[1]);
        if (log_fd >= 0) close(log_fd);

        if (tag) setenv("DSM_NODE_TAG", tag, 1);
        if (mode) {
            setenv("DSM_NODE_MODE", mode, 1);
        } else {
            unsetenv("DSM_NODE_MODE");
        }
        execv(argv[0], argv);
        _exit(127);
    }

    close(input_pipe[0]);
    close(output_pipe[1]);

    child->pid    = pid;
    child->input  = fdopen(input_pipe[1], "w");
    child->output = fdopen(output_pipe[0], "r");
    if (!child->input || !child->output) {
        if (child->input) fclose(child->input);
        if (child->output) fclose(child->output);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        free(child);
        return NULL;
    }
    setvbuf(child->input, NULL, _IOLBF, 0);
    setvbuf(child->output, NULL, _IONBF, 0);

    e2e_state.children[e2e_state.nchildren++] = child;
    return child;
}

static inline void e2e_send(e2e_child_t *child, const char *command)
{
    if (!child || !child->input || child->pid <= 0) return;
    fputs(command, child->input);
    fputc('\n', child->input);
    fflush(child->input);
}

/* Espera la salida del hijo sin reapearlo en otra parte. */
static inline bool e2e_wait_exit(e2e_child_t *child, uint64_t deadline,
                                 int *status_out)
{
    if (!child || child->pid <= 0) return true;

    int status = 0;
    for (;;) {
        pid_t got = waitpid(child->pid, &status, WNOHANG);
        if (got == child->pid) {
            child->exit_status = status;
            child->exited = true;
            if (status_out) *status_out = status;
            child->pid = -1;
            return true;
        }
        if (got < 0) {
            if (errno == EINTR) continue;
            child->pid = -1;
            return true;
        }
        if (e2e_now_ms() >= deadline) return false;
        e2e_sleep_ms(20);
    }
}

/* Cierra la entrada para que el hijo salga por EOF, concede un plazo al
 * cierre ordenado y escala a SIGTERM y SIGKILL. */
static inline void e2e_stop_child(e2e_child_t *child)
{
    if (!child) return;

    if (child->pid > 0) {
        e2e_send(child, "QUIT");
    }
    if (child->input) {
        fclose(child->input);
        child->input = NULL;
    }

    if (child->pid > 0) {
        int status = 0;
        if (!e2e_wait_exit(child, e2e_now_ms() + 3000u, &status)) {
            kill(child->pid, SIGTERM);
            if (!e2e_wait_exit(child, e2e_now_ms() + 2000u, &status)) {
                kill(child->pid, SIGKILL);
                waitpid(child->pid, &status, 0);
                child->pid = -1;
            }
        }
    }
    if (child->output) {
        fclose(child->output);
        child->output = NULL;
    }
}

/* Ejecuta una herramienta con plazo. La salida y el diagnóstico se recogen
 * por la misma tubería; si la herramienta no termina, se mata y se informa. */
static inline int e2e_run_tool(const char *tool, const char *conf, uint16_t port,
                               char *const args[], char *out, size_t capacity,
                               uint64_t deadline)
{
    if (!out || capacity == 0) return -1;
    out[0] = '\0';

    char port_text[8];
    snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);

    char *argv[16];
    size_t argc = 0;
    argv[argc++] = (char *)tool;
    argv[argc++] = (char *)"-c";
    argv[argc++] = (char *)conf;
    argv[argc++] = (char *)"-p";
    argv[argc++] = port_text;
    for (size_t i = 0; args && args[i] && argc + 2 < 16; i++) {
        argv[argc++] = args[i];
    }
    argv[argc] = NULL;

    int pipefd[2];
    if (pipe2(pipefd, O_CLOEXEC) != 0) return -1;

    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    if (pid == 0) {
        close(pipefd[0]);
        if (dup2(pipefd[1], STDOUT_FILENO) < 0 ||
            dup2(pipefd[1], STDERR_FILENO) < 0) {
            _exit(126);
        }
        close(pipefd[1]);
        execv(argv[0], argv);
        _exit(127);
    }

    close(pipefd[1]);
    size_t used = 0;
    for (;;) {
        uint64_t now = e2e_now_ms();
        if (now >= deadline) break;

        struct pollfd pfd = { .fd = pipefd[0], .events = POLLIN };
        int rc = poll(&pfd, 1, (int)(deadline - now));
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (rc == 0) break;

        char chunk[1024];
        ssize_t got = read(pipefd[0], chunk, sizeof(chunk));
        if (got <= 0) break;
        size_t copy = (size_t)got;
        if (used + copy >= capacity) copy = capacity - 1 - used;
        if (copy > 0) {
            memcpy(out + used, chunk, copy);
            used += copy;
            out[used] = '\0';
        }
    }
    close(pipefd[0]);

    int status = 0;
    if (!e2e_wait_exit(&(e2e_child_t){ .pid = pid }, deadline, &status)) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        return -1;
    }
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : -1;
}

static inline bool e2e_parse_uint_after(const char *line, const char *key,
                                        unsigned *out)
{
    const char *pos = strstr(line, key);
    if (!pos) return false;
    pos += strlen(key);
    char *end = NULL;
    unsigned long value = strtoul(pos, &end, 10);
    if (end == pos) return false;
    *out = (unsigned)value;
    return true;
}

#endif /* DSM_E2E_HARNESS_H */
