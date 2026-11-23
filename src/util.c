#include "util.h"
#include "dsm_types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <limits.h>

uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

void mono_deadline_ts(uint64_t deadline_ms, struct timespec *ts)
{
    ts->tv_sec  = (time_t)(deadline_ms / 1000ULL);
    ts->tv_nsec = (long)((deadline_ms % 1000ULL) * 1000000ULL);
}

void sleep_ms(uint32_t ms)
{
    struct timespec ts;
    ts.tv_sec  = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000);
    /* nanosleep se reanuda tras una señal para respetar la espera completa. */
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
    }
}

int dsm_write_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        p += n;
        remaining -= (size_t)n;
    }
    return 0;
}

int dsm_send_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        /* MSG_NOSIGNAL suprime SIGPIPE: el peer puede cerrar el socket a
         * mitad de la escritura y aun asi recibimos EPIPE como error. */
        ssize_t n = send(fd, p, remaining, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        p += n;
        remaining -= (size_t)n;
    }
    return 0;
}

int dsm_send_full_deadline(int fd, const void *buf, size_t len, uint32_t timeout_ms)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = len;
    uint64_t deadline = mono_ms() + timeout_ms;
    int rc = 0;

    while (remaining > 0) {
        uint64_t now = mono_ms();
        if (now >= deadline) {
            rc = -1;
            break;
        }

        int timeout_rem = (int)(deadline - now);
        struct pollfd pfd = { .fd = fd, .events = POLLOUT, .revents = 0 };
        int pr = poll(&pfd, 1, timeout_rem);

        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            rc = -1;
            break;
        }
        if (pr == 0) {
            rc = -1;
            break;
        }
        if (pfd.revents & POLLNVAL) {
            rc = -1;
            break;
        }
        if (!(pfd.revents & (POLLOUT | POLLHUP | POLLERR))) {
            continue;
        }

        /* La espera ya acoto el bloqueo: MSG_DONTWAIT impide que send() se
         * duerma si el peer deja de drenar entre poll() y la escritura. */
        ssize_t n = send(fd, p, remaining, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            rc = -1;
            break;
        }
        if (n == 0) {
            rc = -1;
            break;
        }
        p += n;
        remaining -= (size_t)n;
    }

    /* El descriptor se entrega en modo bloqueante al resto de la pila. */
    int flags = fcntl(fd, F_GETFL);
    if (flags >= 0 && (flags & O_NONBLOCK)) {
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }
    return rc;
}

int dsm_read_full(int fd, void *buf, size_t len)
{
    uint8_t *p = (uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = read(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1; /* EOF prematuro */
        }
        p += n;
        remaining -= (size_t)n;
    }
    return 0;
}

int dsm_read_full_deadline(int fd, void *buf, size_t len, uint32_t timeout_ms)
{
    uint8_t *p = (uint8_t *)buf;
    size_t remaining = len;
    uint64_t deadline = mono_ms() + timeout_ms;

    while (remaining > 0) {
        uint64_t now = mono_ms();
        if (now >= deadline) {
            return DSM_ERR_TIMEOUT;
        }

        int timeout_rem = (int)(deadline - now);
        struct pollfd pfd = { .fd = fd, .events = POLLIN, .revents = 0 };
        int pr = poll(&pfd, 1, timeout_rem);

        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            return DSM_ERR_NET;
        }
        if (pr == 0) {
            return DSM_ERR_TIMEOUT;
        }
        if (!(pfd.revents & (POLLIN | POLLHUP | POLLERR))) {
            continue;
        }

        ssize_t n = read(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return DSM_ERR_NET;
        }
        if (n == 0) {
            return DSM_ERR_NET; /* Conexión cerrada */
        }
        p += n;
        remaining -= (size_t)n;
    }
    return DSM_OK;
}

int set_cloexec(int fd)
{
    int flags = fcntl(fd, F_GETFD);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
}

int set_nonblock(int fd, bool nonblock)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0) return -1;
    if (nonblock) {
        flags |= O_NONBLOCK;
    } else {
        flags &= ~O_NONBLOCK;
    }
    return fcntl(fd, F_SETFL, flags);
}

int dsm_connect_timeout(const struct sockaddr_in *addr, uint32_t timeout_ms)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return DSM_ERR_NET;
    }
    if (set_cloexec(fd) != 0) {
        close(fd);
        return DSM_ERR_NET;
    }

    /* Banderas originales: el descriptor se entrega en modo bloqueante como
     * el resto de la pila, para que dsm_send_full y dsm_read_full_deadline
     * mantengan su semantica. */
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return DSM_ERR_NET;
    }

    int rc = connect(fd, (const struct sockaddr *)addr, sizeof(*addr));
    if (rc != 0) {
        if (errno != EINPROGRESS) {
            close(fd);
            return DSM_ERR_NET;
        }

        struct pollfd pfd = { .fd = fd, .events = POLLOUT, .revents = 0 };
        int pr;
        do {
            pr = poll(&pfd, 1, (int)timeout_ms);
        } while (pr < 0 && errno == EINTR);

        if (pr == 0) {
            close(fd);
            return DSM_ERR_TIMEOUT;
        }
        if (pr < 0) {
            close(fd);
            return DSM_ERR_NET;
        }

        /* Con POLLOUT la respuesta definitiva la da SO_ERROR: un connect
         * rechazado o inalcanzable tambien despierta el poll. */
        int soerr = 0;
        socklen_t slen = sizeof(soerr);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) < 0) {
            close(fd);
            return DSM_ERR_NET;
        }
        if (soerr != 0) {
            close(fd);
            return (soerr == ETIMEDOUT) ? DSM_ERR_TIMEOUT : DSM_ERR_NET;
        }
    }

    if (fcntl(fd, F_SETFL, flags) < 0) {
        close(fd);
        return DSM_ERR_NET;
    }

    return fd;
}

static int get_random_bytes(void *buf, size_t len)
{
    ssize_t ret = getrandom(buf, len, 0);
    if (ret == (ssize_t)len) {
        return 0;
    }
    /* Fallback a /dev/urandom */
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return -1;
    }
    int rc = dsm_read_full(fd, buf, len);
    close(fd);
    return rc;
}

int uuid_generate(uint8_t out[16])
{
    if (get_random_bytes(out, 16) != 0) {
        return DSM_ERR_MEM;
    }
    out[6] = (uint8_t)((out[6] & 0x0F) | 0x40);
    out[8] = (uint8_t)((out[8] & 0x3F) | 0x80);
    return DSM_OK;
}

int uuid_load_or_create_file(uint32_t cluster_id, const char *tag, uint8_t out[16])
{
    const char *home = getenv("HOME");
    if (!home) {
        home = "/tmp";
    }

    char dir_path[PATH_MAX];
    snprintf(dir_path, sizeof(dir_path), "%s/.dsm", home);
    /* EEXIST es el caso normal; cualquier otro error impide persistir el UUID. */
    if (mkdir(dir_path, 0700) != 0 && errno != EEXIST) {
        return DSM_ERR_MEM;
    }

    char hostname[128];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        strncpy(hostname, "localhost", sizeof(hostname));
    }
    hostname[sizeof(hostname) - 1] = '\0';

    const char *tag_env = getenv("DSM_NODE_TAG");
    if (tag_env && tag_env[0] != '\0') {
        tag = tag_env;
    } else if (!tag || tag[0] == '\0') {
        tag = "default";
    }

    char file_path[PATH_MAX];
    int fp_len = snprintf(file_path, sizeof(file_path), "%s/%08x-%.64s-%.64s.uuid",
                          dir_path, cluster_id, hostname, tag);
    if (fp_len < 0 || (size_t)fp_len >= sizeof(file_path)) {
        return DSM_ERR_INVALID;
    }

    /* Intentar leer archivo existente */
    int fd = open(file_path, O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        ssize_t n = read(fd, out, 16);
        close(fd);
        if (n == 16) {
            return DSM_OK;
        }
    }

    if (uuid_generate(out) != DSM_OK) {
        return DSM_ERR_MEM;
    }

    /* Escribir en un temporal y renombrar atómicamente: el archivo final
     * nunca queda a medio escribir aunque el proceso muera. */
    char tmp_path[PATH_MAX];
    int tp_len = snprintf(tmp_path, sizeof(tmp_path), "%.4000s.tmp.%d", file_path, (int)getpid());
    if (tp_len < 0 || (size_t)tp_len >= sizeof(tmp_path)) {
        return DSM_ERR_INVALID;
    }

    int wfd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0600);
    if (wfd < 0) {
        return DSM_ERR_MEM;
    }

    if (dsm_write_full(wfd, out, 16) != 0) {
        close(wfd);
        unlink(tmp_path);
        return DSM_ERR_MEM;
    }
    /* Un cierre fallido deja el UUID sin garantizar en disco. */
    if (close(wfd) != 0) {
        unlink(tmp_path);
        return DSM_ERR_MEM;
    }

    if (rename(tmp_path, file_path) != 0) {
        /* Si otro proceso ganó la carrera, leer el que quedó */
        unlink(tmp_path);
        fd = open(file_path, O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            ssize_t n = read(fd, out, 16);
            close(fd);
            if (n == 16) {
                return DSM_OK;
            }
        }
        return DSM_ERR_MEM;
    }

    return DSM_OK;
}

void uuid_to_str(const uint8_t uuid[16], char out[37])
{
    snprintf(out, 37,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             uuid[0], uuid[1], uuid[2], uuid[3],
             uuid[4], uuid[5], uuid[6], uuid[7],
             uuid[8], uuid[9], uuid[10], uuid[11],
             uuid[12], uuid[13], uuid[14], uuid[15]);
}
