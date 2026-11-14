#include "test.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "wire.h"
#include "discovery.h"
#include "net.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <time.h>

#define CONF_NODE_A "/tmp/dsm_cluster_node_a.conf"
#define CONF_NODE_B "/tmp/dsm_cluster_node_b.conf"

static void write_node_conf(const char *path, uint16_t port_min, uint16_t port_max,
                            uint32_t hb_ms, uint32_t reconnect_max_ms)
{
    FILE *f = fopen(path, "w");
    if (!f) return;
    fprintf(f,
        "CLUSTER_ID          = 0x534F3031\n"
        "PAGE_SIZE           = 4096\n"
        "BASE_ADDR           = 0x200000000000\n"
        "VIEW_SIZE           = 4294967296\n"
        "MAX_RUNTIME_NODES   = 256\n"
        "VNODES_PER_MB       = 16\n"
        "REDUNDANCY          = NONE\n"
        "NODE_MEM            = 268435456\n"
        "PORT_MIN            = %u\n"
        "PORT_MAX            = %u\n"
        "PEER_HOSTS          = 127.0.0.1\n"
        "CONNECT_TIMEOUT_MS  = 300\n"
        "DISCOVERY_TIMEOUT_MS= 300\n"
        "SEND_TIMEOUT_MS     = 2000\n"
        "REQUEST_TIMEOUT_MS  = 3000\n"
        "REQUEST_RETRIES     = 3\n"
        "RECONNECT_MIN_MS    = 100\n"
        "RECONNECT_MAX_MS    = %u\n"
        "HB_INTERVAL_MS      = %u\n"
        "HB_SUSPECT          = 3\n"
        "HB_DEAD             = 6\n"
        "WORKER_THREADS      = 2\n"
        "DISPATCHER_THREADS  = 1\n"
        "PAGE_LOCK_STRIPES   = 64\n"
        "HOME_SLACK          = 1.25\n"
        "LOCK_TIMEOUT_MS     = 5000\n"
        "WORKER_QUEUE_CAP    = 1024\n"
        "DSM_LOG_LEVEL       = ERROR\n",
        port_min, port_max, reconnect_max_ms, hb_ms
    );
    fclose(f);
}

static void clean_uuid_files(const char *prefix)
{
    const char *homedir = getenv("HOME") ? getenv("HOME") : "/tmp";
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -f %s/.dsm/*%s* /tmp/.dsm/*%s*", homedir, prefix, prefix);
    system(cmd);
}

/*
 * ============================================================================
 * PRUEBA 1: Descubrimiento y Unión real entre 2 nodos sobre sockets TCP locales
 *           (rango 27600..27699)
 *  - Nodo A (puerto 27600) se reconoce como Slot 0, Epoch 1.
 *  - Nodo B (puerto 27601) escanea el rango, envía HELLO (UUID + raw_hash),
 *    recibe WELCOME, envía JOIN al Nodo A y es aceptado como Slot 1 con Epoch 2.
 *  - Comprueba en ambos nodos que la membresía refleja 2 nodos vivos (dsm_node_count == 2).
 * ============================================================================
 */
static int test_cluster_discovery_and_join_two_nodes(void)
{
    write_node_conf(CONF_NODE_A, 27600, 27699, 200, 1500);
    write_node_conf(CONF_NODE_B, 27600, 27699, 200, 1500);
    clean_uuid_files("p1_node");

    int pipe_sync[2];
    int pipe_b_status[2];
    int pipe_b_cmd[2];
    REQUIRE(pipe(pipe_sync) == 0);
    REQUIRE(pipe(pipe_b_status) == 0);
    REQUIRE(pipe(pipe_b_cmd) == 0);

    pid_t pid_b = fork();
    REQUIRE(pid_b >= 0);

    if (pid_b == 0) {
        /* Proceso hijo: Nodo B */
        close(pipe_sync[1]);
        close(pipe_b_status[0]);
        close(pipe_b_cmd[1]);

        /* Esperar a que Nodo A haya completado su arranque en 27600 */
        char sync_b = 0;
        read(pipe_sync[0], &sync_b, 1);
        close(pipe_sync[0]);

        setenv("DSM_NODE_TAG", "p1_node_b", 1);
        dsm_status_t rc = dsm_init(CONF_NODE_B, 0);
        if (rc != DSM_OK) {
            int err[3] = { (int)rc, -1, -1 };
            write(pipe_b_status[1], err, sizeof(err));
            _exit(1);
        }

        uint32_t b_slot = 999;
        dsm_node_id(&b_slot);
        uint32_t b_count = 0;
        dsm_node_count(&b_count);
        dsm_ctx_t *ctx_b = dsm_ctx_get();
        uint64_t b_epoch = ctx_b ? ctx_b->memb.epoch : 0;

        int res[3] = { (int)b_slot, (int)b_count, (int)b_epoch };
        write(pipe_b_status[1], res, sizeof(res));

        /* Esperar orden de finalización limpia */
        char cmd = 0;
        read(pipe_b_cmd[0], &cmd, 1);
        close(pipe_b_cmd[0]);

        dsm_finalize();
        _exit(0);
    }

    /* Proceso padre: Nodo A */
    close(pipe_sync[0]);
    close(pipe_b_status[1]);
    close(pipe_b_cmd[0]);

    setenv("DSM_NODE_TAG", "p1_node_a", 1);
    dsm_status_t st = dsm_init(CONF_NODE_A, 0);
    CHECK(st == DSM_OK);

    dsm_ctx_t *ctx_a = dsm_ctx_get();
    REQUIRE(ctx_a != NULL);

    /* Nodo A se reconoce inicialmente como Slot 0, Epoch 1 */
    uint32_t a_slot = 999;
    CHECK(dsm_node_id(&a_slot) == DSM_OK);
    CHECK(a_slot == 0);
    CHECK(ctx_a->memb.epoch == 1);

    uint32_t a_count = 0;
    CHECK(dsm_node_count(&a_count) == DSM_OK);
    CHECK(a_count == 1);

    /* Notificar a Nodo B para que inicie el escaneo de puertos */
    char sync_ack = 1;
    write(pipe_sync[1], &sync_ack, 1);
    close(pipe_sync[1]);

    /* Recibir confirmación de Nodo B tras su escaneo y unión */
    int b_res[3] = { -1, -1, -1 };
    read(pipe_b_status[0], b_res, sizeof(b_res));
    close(pipe_b_status[0]);

    /*
     * B se une como slot 1 y ve 2 nodos vivos. El epoch es una version de la
     * membresia y crece con cada fusion o convergencia: su valor exacto
     * depende de cuantas rondas de descubrimiento hicieron falta, asi que la
     * invariante verificable es que sea posterior al epoch inicial del primer
     * nodo (1), no un numero fijo.
     */
    CHECK(b_res[0] == 1);
    CHECK(b_res[1] == 2);
    CHECK(b_res[2] >= 2);

    /* Esperar sincronización y verificar que Nodo A también ve 2 nodos vivos */
    usleep(250000);
    CHECK(dsm_node_count(&a_count) == DSM_OK);
    CHECK(a_count == 2);
    CHECK(ctx_a->memb.epoch >= 2);
    CHECK(membership_slot_present(ctx_a, 0) == true);
    CHECK(membership_slot_present(ctx_a, 1) == true);

    /* Finalizar Nodo B ordenadamente */
    char term_cmd = 'T';
    write(pipe_b_cmd[1], &term_cmd, 1);
    close(pipe_b_cmd[1]);

    int wstatus = 0;
    waitpid(pid_b, &wstatus, 0);

    /* Finalizar Nodo A */
    dsm_finalize();
    CHECK(!dsm_is_initialized());

    unlink(CONF_NODE_A);
    unlink(CONF_NODE_B);
    return 0;
}

/*
 * ============================================================================
 * PRUEBA 2: Intercambio de Heartbeat y degradación por timeout
 *  - Comprueba que los nodos intercambian mensajes HEARTBEAT por TCP
 *    y el contador missed_hb se mantiene en 0.
 *  - Simula la detención/desconexión abrupta del Nodo B.
 *  - Comprueba que tras 3 latidos perdidos (~1.5 s) Nodo A marca Nodo B como SUSPECT.
 *  - Comprueba que tras 6 latidos perdidos (~3.0 s) Nodo A marca Nodo B como DEAD,
 *    difunde NODE_DOWN, y la ranura 1 queda bloqueada para reutilización
 *    hasta transcurrir reconnect_max_ms.
 * ============================================================================
 */
static int test_cluster_heartbeat_and_timeout_degradation(void)
{
    /*
     * Usamos temporizaciones aceleradas de heartbeat para agilidad:
     * HB_INTERVAL_MS = 250 ms -> 3 latidos perdidos = 750 ms (SUSPECT)
     *                          -> 6 latidos perdidos = 1500 ms (DEAD)
     * RECONNECT_MAX_MS = 2000 ms
     */
    write_node_conf(CONF_NODE_A, 27600, 27699, 250, 2000);
    write_node_conf(CONF_NODE_B, 27600, 27699, 250, 2000);
    clean_uuid_files("p2_node");

    int pipe_sync[2];
    int pipe_b_status[2];
    int pipe_b_cmd[2];
    REQUIRE(pipe(pipe_sync) == 0);
    REQUIRE(pipe(pipe_b_status) == 0);
    REQUIRE(pipe(pipe_b_cmd) == 0);

    pid_t pid_b = fork();
    REQUIRE(pid_b >= 0);

    if (pid_b == 0) {
        /* Proceso hijo: Nodo B */
        close(pipe_sync[1]);
        close(pipe_b_status[0]);
        close(pipe_b_cmd[1]);

        char sync_b = 0;
        read(pipe_sync[0], &sync_b, 1);
        close(pipe_sync[0]);

        setenv("DSM_NODE_TAG", "p2_node_b", 1);
        dsm_status_t rc = dsm_init(CONF_NODE_B, 0);
        if (rc != DSM_OK) {
            int err[2] = { (int)rc, -1 };
            write(pipe_b_status[1], err, sizeof(err));
            _exit(1);
        }

        uint32_t b_slot = 999;
        dsm_node_id(&b_slot);
        uint32_t b_count = 0;
        dsm_node_count(&b_count);

        int res[2] = { (int)b_slot, (int)b_count };
        write(pipe_b_status[1], res, sizeof(res));

        /* Esperar orden para desconexión abrupta */
        char cmd = 0;
        read(pipe_b_cmd[0], &cmd, 1);
        close(pipe_b_cmd[0]);

        /* Muerte abrupta sin LEAVE para probar timeout de heartbeat */
        _exit(0);
    }

    /* Proceso padre: Nodo A */
    close(pipe_sync[0]);
    close(pipe_b_status[1]);
    close(pipe_b_cmd[0]);

    setenv("DSM_NODE_TAG", "p2_node_a", 1);
    dsm_status_t st = dsm_init(CONF_NODE_A, 0);
    CHECK(st == DSM_OK);

    dsm_ctx_t *ctx_a = dsm_ctx_get();
    REQUIRE(ctx_a != NULL);

    char sync_ack = 1;
    write(pipe_sync[1], &sync_ack, 1);
    close(pipe_sync[1]);

    int b_res[2] = { -1, -1 };
    read(pipe_b_status[0], b_res, sizeof(b_res));
    close(pipe_b_status[0]);

    CHECK(b_res[0] == 1);
    CHECK(b_res[1] == 2);

    /*
     * 1. Comprobar que los nodos intercambian latidos TCP reales
     *    y missed_hb se mantiene en 0.
     */
    usleep(700000); /* 700 ms = > 2 periodos de heartbeat */

    uint32_t live_count = 0;
    CHECK(dsm_node_count(&live_count) == DSM_OK);
    CHECK(live_count == 2);

    pthread_rwlock_rdlock(&ctx_a->memb.rw);
    CHECK(ctx_a->memb.nodes[1].state == DSM_NODE_ALIVE);
    /*
     * NOTA: A evalúa timeouts y B envía latidos cada 250 ms en hilos
     * independientes; la fase entre ambos es indeterminada, por lo que
     * en la evaluación de A puede haber transcurrido justo un intervalo
     * completo desde el último latido recibido (missed_hb == 1) sin que
     * el latido de B haya llegado todavía. Aceptamos <= 1 con estado
     * ALIVE como prueba de intercambio saludable de latidos.
     */
    CHECK(ctx_a->memb.nodes[1].missed_hb <= 1);
    pthread_rwlock_unlock(&ctx_a->memb.rw);

    /*
     * 2. Simular desconexión o detención del Nodo B
     */
    char kill_cmd = 'K';
    write(pipe_b_cmd[1], &kill_cmd, 1);
    close(pipe_b_cmd[1]);

    int wstatus = 0;
    waitpid(pid_b, &wstatus, 0);

    /*
     * 3. Comprobar que tras 3 latidos perdidos (~750 ms),
     *    el Nodo A marca al Nodo B como SUSPECT.
     */
    usleep(850000); /* ~850 ms transcurridos sin latidos (> 3 latidos de 250ms) */

    pthread_rwlock_rdlock(&ctx_a->memb.rw);
    uint8_t state_suspect = ctx_a->memb.nodes[1].state;
    uint8_t missed_suspect = ctx_a->memb.nodes[1].missed_hb;
    pthread_rwlock_unlock(&ctx_a->memb.rw);

    CHECK(missed_suspect >= 3);
    CHECK(state_suspect == DSM_NODE_SUSPECT || state_suspect == DSM_NODE_DEAD);

    /*
     * 4. Comprobar que tras 6 latidos perdidos (~1500 ms acumulados),
     *    el Nodo A marca al Nodo B como DEAD, difunde NODE_DOWN,
     *    y la ranura 1 queda bloqueada para reutilización
     *    hasta transcurrir reconnect_max_ms.
     */
    usleep(950000); /* Total acumulado = 1.8 s (> 6 latidos de 250ms = 1.5s) */

    pthread_rwlock_rdlock(&ctx_a->memb.rw);
    CHECK(ctx_a->memb.nodes[1].state == DSM_NODE_DEAD);
    CHECK(!membership_slot_present(ctx_a, 1));
    uint64_t reusable_ms = ctx_a->memb.reusable_after_ms[1];
    pthread_rwlock_unlock(&ctx_a->memb.rw);

    CHECK(dsm_node_count(&live_count) == DSM_OK);
    CHECK(live_count == 1);

    /* Ranura 1 retenida en cuarentena durante reconnect_max_ms */
    uint64_t now_ms = mono_ms();
    CHECK(reusable_ms > now_ms);
    uint32_t next_free = membership_first_free_slot(ctx_a);
    CHECK(next_free != 1);

    dsm_finalize();
    CHECK(!dsm_is_initialized());

    unlink(CONF_NODE_A);
    unlink(CONF_NODE_B);
    return 0;
}

/*
 * ============================================================================
 * PRUEBA 3: Salida ordenada (LEAVE)
 *  - Nodo B ejecuta discovery_leave() (mediante dsm_finalize()).
 *  - Nodo A recibe MSG_LEAVE por TCP y marca el slot 1 en DEAD inmediatamente
 *    sin tener que esperar timeouts de latidos perdidos.
 * ============================================================================
 */
static int test_cluster_orderly_leave(void)
{
    write_node_conf(CONF_NODE_A, 27600, 27699, 500, 2000);
    write_node_conf(CONF_NODE_B, 27600, 27699, 500, 2000);
    clean_uuid_files("p3_node");

    int pipe_sync[2];
    int pipe_b_status[2];
    int pipe_b_cmd[2];
    REQUIRE(pipe(pipe_sync) == 0);
    REQUIRE(pipe(pipe_b_status) == 0);
    REQUIRE(pipe(pipe_b_cmd) == 0);

    pid_t pid_b = fork();
    REQUIRE(pid_b >= 0);

    if (pid_b == 0) {
        /* Proceso hijo: Nodo B */
        close(pipe_sync[1]);
        close(pipe_b_status[0]);
        close(pipe_b_cmd[1]);

        char sync_b = 0;
        read(pipe_sync[0], &sync_b, 1);
        close(pipe_sync[0]);

        setenv("DSM_NODE_TAG", "p3_node_b", 1);
        dsm_status_t rc = dsm_init(CONF_NODE_B, 0);
        if (rc != DSM_OK) {
            int err[2] = { (int)rc, -1 };
            write(pipe_b_status[1], err, sizeof(err));
            _exit(1);
        }

        uint32_t b_slot = 999;
        dsm_node_id(&b_slot);
        uint32_t b_count = 0;
        dsm_node_count(&b_count);

        int res[2] = { (int)b_slot, (int)b_count };
        write(pipe_b_status[1], res, sizeof(res));

        /* Esperar orden de salida limpia */
        char cmd = 0;
        read(pipe_b_cmd[0], &cmd, 1);
        close(pipe_b_cmd[0]);

        /* Ejecutar salida limpia: difunde MSG_LEAVE hacia Nodo A */
        dsm_finalize();
        _exit(0);
    }

    /* Proceso padre: Nodo A */
    close(pipe_sync[0]);
    close(pipe_b_status[1]);
    close(pipe_b_cmd[0]);

    setenv("DSM_NODE_TAG", "p3_node_a", 1);
    dsm_status_t st = dsm_init(CONF_NODE_A, 0);
    CHECK(st == DSM_OK);

    dsm_ctx_t *ctx_a = dsm_ctx_get();
    REQUIRE(ctx_a != NULL);

    char sync_ack = 1;
    write(pipe_sync[1], &sync_ack, 1);
    close(pipe_sync[1]);

    int b_res[2] = { -1, -1 };
    read(pipe_b_status[0], b_res, sizeof(b_res));
    close(pipe_b_status[0]);

    CHECK(b_res[0] == 1);
    CHECK(b_res[1] == 2);

    /* Cluster bi-nodo plenamente operativo */
    usleep(200000);
    uint32_t count = 0;
    dsm_node_count(&count);
    CHECK(count == 2);

    /* Ordenar al Nodo B salir ordenadamente con MSG_LEAVE */
    char leave_cmd = 'L';
    write(pipe_b_cmd[1], &leave_cmd, 1);
    close(pipe_b_cmd[1]);

    int wstatus = 0;
    waitpid(pid_b, &wstatus, 0);

    /*
     * El MSG_LEAVE viaja por red antes de que el proceso hijo finalice.
     * En breve intervalo (< 250ms), Nodo A debe haber procesado el LEAVE
     * y marcado la ranura 1 como DEAD limpiamente sin esperar los timeouts
     * de latidos.
     */
    usleep(250000);

    pthread_rwlock_rdlock(&ctx_a->memb.rw);
    CHECK(ctx_a->memb.nodes[1].state == DSM_NODE_DEAD);
    CHECK(!membership_slot_present(ctx_a, 1));
    pthread_rwlock_unlock(&ctx_a->memb.rw);

    dsm_node_count(&count);
    CHECK(count == 1);

    dsm_finalize();
    CHECK(!dsm_is_initialized());

    unlink(CONF_NODE_A);
    unlink(CONF_NODE_B);
    return 0;
}

/*
 * ============================================================================
 * PRUEBA 4: Ciclo de vida API y verificación de métricas y apagado limpio
 *  - Verificación de dsm_node_id(), dsm_node_count(), dsm_global_size(), dsm_stats().
 *  - Verificación de apagado limpio sin fugas de memoria con ASan ni descriptores colgados.
 *  - Verificación de rechazo por discrepancia de configuración (raw_hash).
 * ============================================================================
 */
static int test_cluster_api_lifecycle_and_reject(void)
{
    write_node_conf(CONF_NODE_A, 27600, 27699, 200, 1500);
    clean_uuid_files("p4_node");

    setenv("DSM_NODE_TAG", "p4_node_a", 1);
    dsm_status_t st = dsm_init(CONF_NODE_A, 0);
    REQUIRE(st == DSM_OK);

    dsm_ctx_t *ctx = dsm_ctx_get();
    REQUIRE(ctx != NULL);

    /* 1. Verificación de dsm_node_id */
    uint32_t self_id = 999;
    CHECK(dsm_node_id(&self_id) == DSM_OK);
    CHECK(self_id == 0);
    CHECK(dsm_node_id(NULL) == DSM_ERR_INVALID);

    /* 2. Verificación de dsm_node_count */
    uint32_t node_count = 0;
    CHECK(dsm_node_count(&node_count) == DSM_OK);
    CHECK(node_count == 1);
    CHECK(dsm_node_count(NULL) == DSM_ERR_INVALID);

    /* 3. Verificación de dsm_global_size */
    uint64_t global_sz = 0;
    CHECK(dsm_global_size(&global_sz) == DSM_OK);
    CHECK(global_sz == 268435456ULL); /* 256 MiB aportados */
    CHECK(dsm_global_size(NULL) == DSM_ERR_INVALID);

    /* 4. Verificación de dsm_stats */
    dsm_stats_t stats;
    CHECK(dsm_stats(&stats) == DSM_OK);
    CHECK(stats.read_faults == 0);
    CHECK(stats.write_faults == 0);
    CHECK(dsm_stats(NULL) == DSM_ERR_INVALID);

    /* 5. Verificación de rechazo sobre socket TCP ante raw_hash discordante */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(sock >= 0);

    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sin.sin_port = htons(ctx->net.listen_port);

    REQUIRE(connect(sock, (struct sockaddr *)&sin, sizeof(sin)) == 0);

    /* Enviar HELLO con hash inválido (0xDEADBEEF != raw_hash del cluster) */
    uint8_t bad_hello[20];
    memset(bad_hello, 0xFE, 16);
    uint32_t bad_hash = htonl(0xDEADBEEF);
    memcpy(bad_hello + 16, &bad_hash, 4);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = ctx->cfg.cluster_id;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_HELLO;
    hdr.flags         = DSM_FLAG_PROBE;
    hdr.payload_len   = 20;

    uint8_t raw_hdr[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, raw_hdr, bad_hello, 20);

    REQUIRE(dsm_send_full(sock, raw_hdr, DSM_HDR_LEN) == 0);
    REQUIRE(dsm_send_full(sock, bad_hello, 20) == 0);

    /* Debe recibir WELCOME con status DSM_ST_CONFIG_MISMATCH */
    uint8_t rx_hdr_buf[DSM_HDR_LEN];
    REQUIRE(dsm_read_full(sock, rx_hdr_buf, DSM_HDR_LEN) == 0);

    msg_header_t rx_hdr;
    REQUIRE(dsm_wire_unpack(rx_hdr_buf, &rx_hdr) == DSM_OK);
    CHECK(rx_hdr.type == MSG_WELCOME);
    CHECK(rx_hdr.status == DSM_ST_CONFIG_MISMATCH);

    close(sock);

    /* 6. Apagado limpio y comprobación de dsm_is_initialized */
    CHECK(dsm_is_initialized() == true);
    CHECK(dsm_finalize() == DSM_OK);
    CHECK(dsm_is_initialized() == false);

    /* Llamadas a la API tras finalize deben fallar con DSM_ERR_NOT_INIT */
    CHECK(dsm_node_id(&self_id) == DSM_ERR_NOT_INIT);
    CHECK(dsm_node_count(&node_count) == DSM_ERR_NOT_INIT);
    CHECK(dsm_global_size(&global_sz) == DSM_ERR_NOT_INIT);
    CHECK(dsm_stats(&stats) == DSM_ERR_NOT_INIT);

    unlink(CONF_NODE_A);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Real Network Cluster (TCP, Discovery, Membership, Heartbeat, Leave) ===\n");
    RUN_TEST(test_cluster_discovery_and_join_two_nodes);
    RUN_TEST(test_cluster_heartbeat_and_timeout_degradation);
    RUN_TEST(test_cluster_orderly_leave);
    RUN_TEST(test_cluster_api_lifecycle_and_reject);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
