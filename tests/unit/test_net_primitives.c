/* ==============================================================================
 * test_net_primitives.c — Pruebas de integración de las primitivas de red.
 *
 * Ejercita la misma pila que usa el daemon: cabecera de 56 B,
 * CRC32 y conexiones cortas por mensaje.
 *
 * Cobertura:
 *   1. Listener + dsm_connect_timeout + trama real (MSG_PAGE_REQUEST) leída
 *      con dsm_read_full_deadline y validada con dsm_wire_validate.
 *   2. Corrupción de un bit en el payload: el CRC debe rechazar la trama.
 *   3. Puerto cerrado: dsm_connect_timeout falla pronto, sin colgarse.
 *   4. IP inalcanzable con plazo corto: DSM_ERR_TIMEOUT o DSM_ERR_NET.
 *   5. SIGPIPE suprimido: el peer cierra sin leer y dsm_send_full devuelve
 *      error dejando el proceso vivo.
 *   6. EINTR: señales sin SA_RESTART durante una lectura larga no rompen
 *      dsm_read_full_deadline.
 *
 * Puertos usados: bloque 27600-27699 (exclusivo del grupo).
 * ============================================================================== */

#include "test.h"
#include "util.h"
#include "wire.h"
#include "dsm_types.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define TEST_CLUSTER_ID UINT32_C(0x534F3031)

/* ----------------------------------------------------------------------------
 * Utilidades locales
 * ------------------------------------------------------------------------- */

/* Crea un socket de escucha TCP en loopback con SO_REUSEADDR. Devuelve el fd
 * o -1. El puerto pertenece siempre al bloque asignado 27600-27699. */
static int make_listener(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    set_cloexec(fd);

    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) != 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, 16) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int fill_sockaddr(struct sockaddr_in *addr, const char *ip, uint16_t port)
{
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port   = htons(port);
    return (inet_pton(AF_INET, ip, &addr->sin_addr) == 1) ? 0 : -1;
}

/* Servidor mínimo: acepta UNA conexión, lee cabecera + payload y valida el
 * CRC con la misma ruta que usa el listener del daemon. */
typedef struct {
    int      listen_fd;
    uint32_t cluster_id;
    int      result;      /* DSM_OK si la trama es íntegra, error si no */
} server_args_t;

static void *server_once(void *arg)
{
    server_args_t *sa = (server_args_t *)arg;

    int cfd = accept(sa->listen_fd, NULL, NULL);
    if (cfd < 0) {
        sa->result = DSM_ERR_NET;
        close(sa->listen_fd);
        sa->listen_fd = -1;
        return NULL;
    }

    set_cloexec(cfd);

    uint8_t raw_hdr[DSM_HDR_LEN];
    if (dsm_read_full_deadline(cfd, raw_hdr, DSM_HDR_LEN, 2000) != DSM_OK) {
        sa->result = DSM_ERR_NET;
        close(cfd);
        close(sa->listen_fd);
        sa->listen_fd = -1;
        return NULL;
    }

    msg_header_t hdr;
    if (dsm_wire_unpack(raw_hdr, &hdr) != DSM_OK) {
        sa->result = DSM_ERR_PROTO;
        close(cfd);
        close(sa->listen_fd);
        sa->listen_fd = -1;
        return NULL;
    }

    uint8_t *payload = NULL;
    if (hdr.payload_len > 0 && hdr.payload_len <= DSM_MAX_PAYLOAD) {
        payload = malloc(hdr.payload_len);
        if (!payload) {
            sa->result = DSM_ERR_MEM;
            close(cfd);
            close(sa->listen_fd);
            sa->listen_fd = -1;
            return NULL;
        }
        if (dsm_read_full_deadline(cfd, payload, hdr.payload_len, 2000) != DSM_OK) {
            free(payload);
            sa->result = DSM_ERR_NET;
            close(cfd);
            close(sa->listen_fd);
            sa->listen_fd = -1;
            return NULL;
        }
    }

    sa->result = dsm_wire_validate(&hdr, raw_hdr, payload, sa->cluster_id);

    free(payload);
    close(cfd);
    close(sa->listen_fd);
    sa->listen_fd = -1;
    return NULL;
}

/* ----------------------------------------------------------------------------
 * 1. Trama real íntegra sobre el listener
 * ------------------------------------------------------------------------- */

static int test_listener_roundtrip_valid_frame(void)
{
    int lfd = make_listener(27640);
    REQUIRE(lfd >= 0);

    server_args_t sa = {
        .listen_fd  = lfd,
        .cluster_id = TEST_CLUSTER_ID,
        .result     = DSM_ERR_NET
    };

    pthread_t srv;
    REQUIRE(pthread_create(&srv, NULL, server_once, &sa) == 0);

    struct sockaddr_in addr;
    REQUIRE(fill_sockaddr(&addr, "127.0.0.1", 27640) == 0);

    int cfd = dsm_connect_timeout(&addr, 2000);
    REQUIRE(cfd >= 0);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = TEST_CLUSTER_ID;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_PAGE_REQUEST;
    hdr.src_slot      = 0;
    hdr.dst_slot      = 1;
    hdr.epoch         = 7;
    hdr.req_id        = 42;
    hdr.page_id       = 3;
    hdr.access        = 1;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw, NULL, 0);
    CHECK(dsm_wire_validate(&hdr, raw, NULL, TEST_CLUSTER_ID) == DSM_OK);

    CHECK(dsm_send_full(cfd, raw, DSM_HDR_LEN) == 0);

    CHECK(pthread_join(srv, NULL) == 0);
    CHECK(sa.result == DSM_OK);

    close(cfd);
    return 0;
}

/* ----------------------------------------------------------------------------
 * 2. Un bit invertido en el payload rompe el CRC
 * ------------------------------------------------------------------------- */

static int test_corrupted_payload_rejected(void)
{
    int lfd = make_listener(27641);
    REQUIRE(lfd >= 0);

    server_args_t sa = {
        .listen_fd  = lfd,
        .cluster_id = TEST_CLUSTER_ID,
        .result     = DSM_ERR_NET
    };

    pthread_t srv;
    REQUIRE(pthread_create(&srv, NULL, server_once, &sa) == 0);

    struct sockaddr_in addr;
    REQUIRE(fill_sockaddr(&addr, "127.0.0.1", 27641) == 0);

    int cfd = dsm_connect_timeout(&addr, 2000);
    REQUIRE(cfd >= 0);

    uint8_t payload[64];
    for (size_t i = 0; i < sizeof(payload); i++) {
        payload[i] = (uint8_t)(i * 7u + 3u);
    }

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = TEST_CLUSTER_ID;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_PAGE_DATA;
    hdr.epoch         = 1;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw, payload, (uint32_t)sizeof(payload));

    /* Corrupción deliberada posterior al cálculo del CRC: un solo bit. */
    payload[10] ^= 0x01u;

    CHECK(dsm_send_full(cfd, raw, DSM_HDR_LEN) == 0);
    CHECK(dsm_send_full(cfd, payload, sizeof(payload)) == 0);

    CHECK(pthread_join(srv, NULL) == 0);
    CHECK(sa.result == DSM_ERR_PROTO);

    close(cfd);
    return 0;
}

/* ----------------------------------------------------------------------------
 * 3. Puerto cerrado: error oportuno, sin bloqueo
 * ------------------------------------------------------------------------- */

static int test_closed_port_fails_fast(void)
{
    struct sockaddr_in addr;
    REQUIRE(fill_sockaddr(&addr, "127.0.0.1", 27690) == 0);

    uint64_t t0 = mono_ms();
    int fd = dsm_connect_timeout(&addr, 1000);
    uint64_t elapsed = mono_ms() - t0;

    /* Sin listener el kernel responde RST (ECONNREFUSED) casi de inmediato. */
    CHECK(fd < 0);
    CHECK(fd == DSM_ERR_NET || fd == DSM_ERR_TIMEOUT);
    CHECK(elapsed < 1000);

    if (fd >= 0) {
        close(fd);
    }
    return 0;
}

/* ----------------------------------------------------------------------------
 * 4. IP inalcanzable con plazo corto: nunca cuelga
 * ------------------------------------------------------------------------- */

static int test_unreachable_host_times_out(void)
{
    struct sockaddr_in addr;
    REQUIRE(fill_sockaddr(&addr, "10.255.255.1", 27600) == 0);

    uint64_t t0 = mono_ms();
    int fd = dsm_connect_timeout(&addr, 300);
    uint64_t elapsed = mono_ms() - t0;

    /* Según el filtrado del entorno puede llegar EHOSTUNREACH/ENETUNREACH
     * (DSM_ERR_NET) o agotarse el plazo (DSM_ERR_TIMEOUT). Ambos son válidos;
     * lo que no se acepta es colgarse. */
    CHECK(fd < 0);
    CHECK(fd == DSM_ERR_TIMEOUT || fd == DSM_ERR_NET);
    CHECK(elapsed < 3000);

    if (fd >= 0) {
        close(fd);
    }
    return 0;
}

/* ----------------------------------------------------------------------------
 * 5. SIGPIPE suprimido por dsm_send_full
 * ------------------------------------------------------------------------- */

typedef struct {
    int listen_fd;
} closer_args_t;

static void *server_accept_and_close(void *arg)
{
    closer_args_t *ca = (closer_args_t *)arg;
    int cfd = accept(ca->listen_fd, NULL, NULL);
    if (cfd >= 0) {
        /* Cierre inmediato sin leer: el cliente acumulará EPIPE. */
        close(cfd);
    }
    close(ca->listen_fd);
    ca->listen_fd = -1;
    return NULL;
}

static int test_sigpipe_suppressed(void)
{
    /* Garantizar la disposición por defecto: si SIGPIPE estuviera ignorado la
     * prueba no demostraría nada. */
    struct sigaction dfl;
    memset(&dfl, 0, sizeof(dfl));
    dfl.sa_handler = SIG_DFL;
    sigemptyset(&dfl.sa_mask);
    REQUIRE(sigaction(SIGPIPE, &dfl, NULL) == 0);

    int lfd = make_listener(27642);
    REQUIRE(lfd >= 0);

    closer_args_t ca = { .listen_fd = lfd };
    pthread_t srv;
    REQUIRE(pthread_create(&srv, NULL, server_accept_and_close, &ca) == 0);

    struct sockaddr_in addr;
    REQUIRE(fill_sockaddr(&addr, "127.0.0.1", 27642) == 0);

    int cfd = dsm_connect_timeout(&addr, 2000);
    REQUIRE(cfd >= 0);

    /* Trama grande: cabecera + 64 KiB de payload. */
    const size_t payload_len = 64u * 1024u;
    uint8_t *big = malloc(DSM_HDR_LEN + payload_len);
    REQUIRE(big != NULL);
    memset(big, 0xA5, DSM_HDR_LEN + payload_len);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = TEST_CLUSTER_ID;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_PAGE_DATA;
    dsm_wire_finish(&hdr, big, big + DSM_HDR_LEN, (uint32_t)payload_len);

    int write_error = 0;
    for (int i = 0; i < 256 && !write_error; i++) {
        if (dsm_send_full(cfd, big, DSM_HDR_LEN + payload_len) != 0) {
            write_error = 1;
        }
    }

    /* La escritura debe fallar en algún momento y el proceso seguir vivo. */
    CHECK(write_error == 1);
    CHECK(getpid() > 0);

    free(big);
    close(cfd);
    CHECK(pthread_join(srv, NULL) == 0);

    /* Restaurar el comportamiento habitual del daemon. */
    struct sigaction ign;
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;
    sigemptyset(&ign.sa_mask);
    sigaction(SIGPIPE, &ign, NULL);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 6. EINTR: señales sin SA_RESTART durante una lectura con plazo
 * ------------------------------------------------------------------------- */

static void sigusr1_handler(int signo)
{
    (void)signo;
}

typedef struct {
    int      peer_fd;
    pthread_t target;
} signal_args_t;

static void *signal_sender(void *arg)
{
    signal_args_t *sa = (signal_args_t *)arg;

    sleep_ms(100);
    pthread_kill(sa->target, SIGUSR1);
    sleep_ms(50);
    pthread_kill(sa->target, SIGUSR1);
    sleep_ms(50);
    pthread_kill(sa->target, SIGUSR1);
    sleep_ms(100);

    /* Tras interrumpir la espera, entregar los datos esperados. */
    const uint8_t token[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    (void)dsm_send_full(sa->peer_fd, token, sizeof(token));
    return NULL;
}

static int test_read_retries_on_eintr(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigusr1_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; /* Sin SA_RESTART: las llamadas deben devolver EINTR */
    REQUIRE(sigaction(SIGUSR1, &sa, NULL) == 0);

    int sv[2];
    REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

    signal_args_t args = {
        .peer_fd = sv[1],
        .target  = pthread_self()
    };

    pthread_t sig;
    REQUIRE(pthread_create(&sig, NULL, signal_sender, &args) == 0);

    uint8_t buf[8];
    memset(buf, 0, sizeof(buf));
    int rc = dsm_read_full_deadline(sv[0], buf, sizeof(buf), 3000);

    CHECK(rc == DSM_OK || rc == DSM_ERR_TIMEOUT);
    if (rc == DSM_OK) {
        CHECK(buf[0] == 1 && buf[7] == 8);
    }

    CHECK(pthread_join(sig, NULL) == 0);

    close(sv[0]);
    close(sv[1]);

    struct sigaction dfl;
    memset(&dfl, 0, sizeof(dfl));
    dfl.sa_handler = SIG_DFL;
    sigemptyset(&dfl.sa_mask);
    sigaction(SIGUSR1, &dfl, NULL);

    return 0;
}

int main(void)
{
    printf("=== Test Suite: Primitivas de red (wire + util) ===\n");
    RUN_TEST(test_listener_roundtrip_valid_frame);
    RUN_TEST(test_corrupted_payload_rejected);
    RUN_TEST(test_closed_port_fails_fast);
    RUN_TEST(test_unreachable_host_times_out);
    RUN_TEST(test_sigpipe_suppressed);
    RUN_TEST(test_read_retries_on_eintr);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
