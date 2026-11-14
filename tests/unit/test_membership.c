#include "test.h"
#include "dsm_internal.h"
#include "discovery.h"
#include <unistd.h>

static int test_membership_table_ops(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.page_size = 4096;
    ctx.cfg.hb_interval_ms = 500;
    ctx.cfg.hb_suspect = 3;
    ctx.cfg.hb_dead = 6;
    ctx.cfg.reconnect_max_ms = 2000;

    membership_init(&ctx);

    /* Verificar que inicialmente no hay slots ocupados */
    uint32_t slot = membership_first_free_slot(&ctx);
    CHECK(slot == 0);

    /* Añadirse a sí mismo como slot 0 */
    ctx.memb.self_slot = 0;
    memset(ctx.memb.self_uuid, 0x55, 16);
    membership_add_self(&ctx);

    CHECK(membership_slot_present(&ctx, 0) == true);
    CHECK(membership_is_alive(&ctx, 0) == true);

    /* Siguiente slot libre debe ser 1 */
    slot = membership_first_free_slot(&ctx);
    CHECK(slot == 1);

    /* Total de páginas aportadas */
    uint32_t total = membership_total_pages(&ctx);
    CHECK(total == (uint32_t)((256ULL << 20) / 4096));

    membership_destroy(&ctx);
    return 0;
}

static int test_membership_evaluate_timeouts(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.hb_interval_ms = 500;
    ctx.cfg.hb_suspect = 3;
    ctx.cfg.hb_dead = 6;
    ctx.cfg.reconnect_max_ms = 2000;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    membership_add_self(&ctx);

    /* Simular nodo remoto en slot 1 */
    ctx.memb.nodes[1].slot = 1;
    ctx.memb.nodes[1].state = DSM_NODE_ALIVE;
    ctx.memb.nodes[1].last_hb_ms = 1000;
    ctx.memb.present[0] |= 0x02; /* Bit 1 en present */

    /* Evaluación a 1000 + 1000 ms: 2 missed (ALIVE) */
    membership_evaluate(&ctx, 2000);
    CHECK(ctx.memb.nodes[1].state == DSM_NODE_ALIVE);

    /* Evaluación a 1000 + 2000 ms: 4 missed (SUSPECT) */
    membership_evaluate(&ctx, 3000);
    CHECK(ctx.memb.nodes[1].state == DSM_NODE_SUSPECT);

    /* Evaluación a 1000 + 3500 ms: 7 missed (DEAD) */
    membership_evaluate(&ctx, 4500);
    CHECK(ctx.memb.nodes[1].state == DSM_NODE_DEAD);
    CHECK(membership_slot_present(&ctx, 1) == false);

    membership_destroy(&ctx);
    return 0;
}

static int test_membership_reusable_slot_delay(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.hb_interval_ms = 500;
    ctx.cfg.hb_suspect = 3;
    ctx.cfg.hb_dead = 6;
    ctx.cfg.reconnect_max_ms = 2000;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    membership_add_self(&ctx);

    /* Añadir nodo 1 */
    ctx.memb.nodes[1].slot = 1;
    ctx.memb.nodes[1].state = DSM_NODE_ALIVE;
    ctx.memb.nodes[1].last_hb_ms = 1000;
    ctx.memb.present[0] |= 0x02;

    /* Siguiente slot libre con 0 y 1 ocupados debe ser 2 */
    uint32_t slot = membership_first_free_slot(&ctx);
    CHECK(slot == 2);

    /* Matar al nodo 1 en now = 5000 (>= 6 missed hbs) */
    membership_evaluate(&ctx, 5000);
    CHECK(ctx.memb.nodes[1].state == DSM_NODE_DEAD);
    CHECK(membership_slot_present(&ctx, 1) == false);
    CHECK(ctx.memb.reusable_after_ms[1] == 5000 + 2000);

    /* En t = 6000 (< 7000), el slot 1 aún está en periodo de gracia/cuarentena.
     * membership_first_free_slot consulta mono_ms(), asi que se verifica el
     * vencimiento directamente sobre reusable_after_ms. */
    CHECK(ctx.memb.reusable_after_ms[1] > 6000);

    membership_destroy(&ctx);
    return 0;
}

static int test_membership_epoch_and_leave(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.reconnect_max_ms = 1500;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    ctx.memb.epoch = 1;
    membership_add_self(&ctx);

    /* Simular mensaje MSG_JOIN entrante de slot 1 con epoch 1 */
    dsm_msg_t join_msg;
    memset(&join_msg, 0, sizeof(join_msg));
    join_msg.hdr.type = MSG_JOIN;
    join_msg.hdr.src_slot = 1;
    join_msg.hdr.epoch = 1;
    join_msg.hdr.payload_len = DSM_MEMBERSHIP_WIRE_LEN;

    membership_wire_t wire;
    memset(&wire, 0, sizeof(wire));
    memset(wire.uuid, 0xAA, 16);
    wire.slot = 1;
    wire.ip = htonl(INADDR_LOOPBACK);
    wire.port = 27601;
    wire.state = DSM_NODE_ALIVE;
    wire.incarnation = 1;
    wire.mem_pages = 65536;

    uint8_t payload[DSM_MEMBERSHIP_WIRE_LEN];
    dsm_pack_membership(&wire, payload);
    join_msg.payload = payload;

    membership_on_join(&ctx, &join_msg);

    CHECK(membership_slot_present(&ctx, 1) == true);
    CHECK(membership_is_alive(&ctx, 1) == true);
    CHECK(ctx.memb.epoch == 2); /* Epoch incrementado */

    /* Simular MSG_LEAVE desde slot 1 */
    dsm_msg_t leave_msg;
    memset(&leave_msg, 0, sizeof(leave_msg));
    leave_msg.hdr.type = MSG_LEAVE;
    leave_msg.hdr.src_slot = 1;
    membership_on_leave(&ctx, &leave_msg);

    CHECK(ctx.memb.nodes[1].state == DSM_NODE_DEAD);
    CHECK(membership_slot_present(&ctx, 1) == false);
    CHECK(membership_is_alive(&ctx, 1) == false);

    membership_destroy(&ctx);
    return 0;
}

static int test_membership_heartbeat_revival(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.hb_interval_ms = 500;
    ctx.cfg.hb_suspect = 3;
    ctx.cfg.hb_dead = 6;
    ctx.cfg.reconnect_max_ms = 2000;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    membership_add_self(&ctx);

    /* Simular nodo 1 */
    ctx.memb.nodes[1].slot = 1;
    ctx.memb.nodes[1].state = DSM_NODE_ALIVE;
    ctx.memb.nodes[1].last_hb_ms = 1000;
    ctx.memb.present[0] |= 0x02;

    /* Transición a SUSPECT tras 4 latidos perdidos */
    membership_evaluate(&ctx, 3000);
    CHECK(ctx.memb.nodes[1].state == DSM_NODE_SUSPECT);
    CHECK(ctx.memb.nodes[1].missed_hb == 4);

    /* Recibir HEARTBEAT real de nodo 1: debe revivir a ALIVE y resetear missed_hb a 0 */
    dsm_msg_t hb_msg;
    memset(&hb_msg, 0, sizeof(hb_msg));
    hb_msg.hdr.type = MSG_HEARTBEAT;
    hb_msg.hdr.src_slot = 1;
    hb_msg.hdr.dst_slot = 0;
    hb_msg.hdr.payload_len = 12;

    uint8_t payload[12];
    uint64_t inc_net = dsm_htonll(1);
    uint32_t free_p = htonl(32768);
    memcpy(payload, &inc_net, 8);
    memcpy(payload + 8, &free_p, 4);
    hb_msg.payload = payload;

    membership_on_heartbeat(&ctx, &hb_msg);

    CHECK(ctx.memb.nodes[1].state == DSM_NODE_ALIVE);
    CHECK(ctx.memb.nodes[1].missed_hb == 0);
    CHECK(ctx.memb.nodes[1].free_pages == 32768);

    membership_destroy(&ctx);
    return 0;
}

static int test_membership_node_down_handling(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 256;
    ctx.cfg.reconnect_max_ms = 2000;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    membership_add_self(&ctx);

    /* Agregar nodo 2 */
    ctx.memb.nodes[2].slot = 2;
    ctx.memb.nodes[2].state = DSM_NODE_ALIVE;
    ctx.memb.nodes[2].last_hb_ms = 1000;
    ctx.memb.present[0] |= 0x04; /* Bit 2 */

    CHECK(membership_is_alive(&ctx, 2) == true);

    /* Notificación externa NODE_DOWN indicando caída del slot 2 */
    dsm_msg_t down_msg;
    memset(&down_msg, 0, sizeof(down_msg));
    down_msg.hdr.type = MSG_NODE_DOWN;
    down_msg.hdr.src_slot = 1;
    down_msg.hdr.page_id = 2; /* Slot afectado */

    membership_on_node_down(&ctx, &down_msg);

    CHECK(ctx.memb.nodes[2].state == DSM_NODE_DEAD);
    CHECK(membership_slot_present(&ctx, 2) == false);
    CHECK(membership_is_alive(&ctx, 2) == false);
    CHECK(ctx.memb.reusable_after_ms[2] > 0);

    membership_destroy(&ctx);
    return 0;
}

typedef struct {
    dsm_ctx_t *ctx;
    atomic_bool *stop;
    uint32_t thread_id;
} stress_thread_arg_t;

static void *stress_reader_thread(void *arg)
{
    stress_thread_arg_t *a = (stress_thread_arg_t *)arg;
    while (!atomic_load(a->stop)) {
        membership_is_alive(a->ctx, a->thread_id % 16);
        membership_slot_present(a->ctx, a->thread_id % 16);
        membership_total_pages(a->ctx);
        membership_first_free_slot(a->ctx);
    }
    return NULL;
}

static void *stress_writer_thread(void *arg)
{
    stress_thread_arg_t *a = (stress_thread_arg_t *)arg;
    uint32_t s = (a->thread_id % 14) + 1; /* slots 1..14 */
    while (!atomic_load(a->stop)) {
        dsm_msg_t hb;
        memset(&hb, 0, sizeof(hb));
        hb.hdr.type = MSG_HEARTBEAT;
        hb.hdr.src_slot = s;
        membership_on_heartbeat(a->ctx, &hb);

        uint64_t now = mono_ms();
        membership_evaluate(a->ctx, now);
        usleep(500);
    }
    return NULL;
}

static int test_membership_concurrent_stress(void)
{
    dsm_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.cfg.page_size = 4096;
    ctx.cfg.node_mem = 256ULL << 20;
    ctx.cfg.max_runtime_nodes = 32;
    ctx.cfg.hb_interval_ms = 100;
    ctx.cfg.hb_suspect = 3;
    ctx.cfg.hb_dead = 6;
    ctx.cfg.reconnect_max_ms = 500;

    membership_init(&ctx);
    ctx.memb.self_slot = 0;
    membership_add_self(&ctx);

    /* Pre-poblar varios slots */
    for (uint32_t i = 1; i < 16; i++) {
        ctx.memb.nodes[i].slot = i;
        ctx.memb.nodes[i].state = DSM_NODE_ALIVE;
        ctx.memb.nodes[i].last_hb_ms = mono_ms();
        ctx.memb.nodes[i].mem_pages = 1024;
        ctx.memb.present[i / 8] |= (uint8_t)(1u << (i % 8));
    }

    atomic_bool stop = false;
    pthread_t rthreads[8];
    pthread_t wthreads[4];
    stress_thread_arg_t rargs[8];
    stress_thread_arg_t wargs[4];

    for (int i = 0; i < 8; i++) {
        rargs[i].ctx = &ctx;
        rargs[i].stop = &stop;
        rargs[i].thread_id = (uint32_t)i;
        pthread_create(&rthreads[i], NULL, stress_reader_thread, &rargs[i]);
    }
    for (int i = 0; i < 4; i++) {
        wargs[i].ctx = &ctx;
        wargs[i].stop = &stop;
        wargs[i].thread_id = (uint32_t)i;
        pthread_create(&wthreads[i], NULL, stress_writer_thread, &wargs[i]);
    }

    /* Ejecutar estrés concurrente durante 300 ms */
    usleep(300000);
    atomic_store(&stop, true);

    for (int i = 0; i < 8; i++) pthread_join(rthreads[i], NULL);
    for (int i = 0; i < 4; i++) pthread_join(wthreads[i], NULL);

    membership_destroy(&ctx);
    return 0;
}

int main(void)
{
    printf("=== Test Suite: Membership ===\n");
    RUN_TEST(test_membership_table_ops);
    RUN_TEST(test_membership_evaluate_timeouts);
    RUN_TEST(test_membership_reusable_slot_delay);
    RUN_TEST(test_membership_epoch_and_leave);
    RUN_TEST(test_membership_heartbeat_revival);
    RUN_TEST(test_membership_node_down_handling);
    RUN_TEST(test_membership_concurrent_stress);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
