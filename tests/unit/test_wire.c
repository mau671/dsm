/* ==============================================================================
 * test_wire.c — Suite de pruebas del protocolo de cable (src/wire.c).
 *
 * Cobertura:
 *   1. Cabecera: 56 B en el cable vs 64 B en memoria, offsets exactos de cada
 *      campo, orden de red y round-trip pack/unpack sin perdida.
 *   2. CRC32 IEEE 802.3: vectores conocidos, incrementalidad, y barrido
 *      EXHAUSTIVO de inversion de cada bit individual (cabecera y payload)
 *      comprobando que ninguna corrupcion de 1 bit pasa desapercibida.
 *   3. Empaquetado/desempaquetado de MEMBERSHIP (40 B memoria / 36 B cable)
 *      con matrices reales de nodos y deltas de layout.
 *   4. Empaquetado/desempaquetado de WELCOME con matrices reales: 0, 1, 2,
 *      pares, impares, MAX_NODES y todos los allocs; truncamientos y cotas.
 *   5. Validacion de longitudes por tipo (wire_payload_ok) y clasificacion.
 *   6. Dir entry y alloc record: layout exacto y round-trip.
 *   7. Rigor adicional: offsets en memoria vs cable (epoch en 12 y leido con
 *      memcpy), barrido de (56 + 4096) * 8 = 33216 bits que exige
 *      DSM_ERR_PROTO en cada uno, cargas fijas con datos realistas y matriz
 *      completa de wire_payload_ok() por tipo de mensaje.
 * ============================================================================== */

#include "test.h"
#include "wire.h"

#include <arpa/inet.h>

/* CRC32 IEEE de referencia para "123456789" */
#define CRC32_CHECK_VECTOR 0xCBF43926u

/* ----------------------------------------------------------------------------
 * 1. Cabecera: tamanos, offsets y round-trip
 * ------------------------------------------------------------------------- */

static int test_header_layout(void)
{
    /* Tamano en memoria vs tamano en el cable */
    CHECK(sizeof(msg_header_t) == 64);
    CHECK(DSM_HDR_LEN == 56u);
    CHECK(sizeof(msg_header_t) > DSM_HDR_LEN);
    CHECK(_Alignof(msg_header_t) == 8);

    /* Los offsets documentados deben cubrir exactamente los 56 bytes, sin
     * solapamientos ni huecos no explicados. */
    CHECK(OFF_MAGIC == 0);
    CHECK(OFF_CLUSTER_ID == 4);
    CHECK(OFF_PROTO_VER == 8);
    CHECK(OFF_TYPE == 10);
    CHECK(OFF_EPOCH == 12);
    CHECK(OFF_SRC_SLOT == 20);
    CHECK(OFF_DST_SLOT == 24);
    CHECK(OFF_REQ_ID == 28);
    CHECK(OFF_PAGE_ID == 32);
    CHECK(OFF_TSEQ == 36);
    CHECK(OFF_DATA_VERSION == 40);
    CHECK(OFF_ACCESS == 44);
    CHECK(OFF_FLAGS == 45);
    CHECK(OFF_STATUS == 46);
    CHECK(OFF_PAYLOAD_LEN == 48);
    CHECK(OFF_CRC32 == 52);
    CHECK(OFF_CRC32 + 4 == DSM_HDR_LEN);

    /* El padding de 8 bytes de la struct debe estar al final: el ultimo campo
     * util (crc32) termina en el offset 56 de la struct. */
    msg_header_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.crc32 = 0x5A5A5A5Au;
    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_pack(&probe, raw);

    /* dsm_wire_pack deja CRC32 = 0 por contrato: el CRC solo lo escribe
     * dsm_wire_finish, una vez empaquetada la cabecera completa. */
    CHECK(raw[OFF_CRC32 + 0] == 0x00);
    CHECK(raw[OFF_CRC32 + 1] == 0x00);
    CHECK(raw[OFF_CRC32 + 2] == 0x00);
    CHECK(raw[OFF_CRC32 + 3] == 0x00);

    /* dsm_wire_pack debe escribir exactamente 56 B: no toca el byte 56 */
    uint8_t guard[64];
    memset(guard, 0xEE, sizeof(guard));
    dsm_wire_pack(&probe, guard);
    CHECK(guard[56] == 0xEE);
    CHECK(guard[57] == 0xEE);
    CHECK(guard[63] == 0xEE);

    return 0;
}

static int test_header_endianness(void)
{
    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_PAGE_DATA;
    h.epoch         = 0x0102030405060708ULL;
    h.src_slot      = 0x11223344;
    h.dst_slot      = 0x55667788;
    h.req_id        = 0x99AABBCC;
    h.page_id       = 0xDDEEFF00;
    h.tseq          = 0x0A0B0C0D;
    h.data_version  = 0x0E0F1011;
    h.access        = 0x42;
    h.flags         = 0xA5;
    h.status        = 0xBEEF;
    h.payload_len   = 0xCAFEBABE;
    h.crc32         = 0x13579BDF;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_pack(&h, raw);

    /* dsm_wire_pack siempre escribe CRC=0 en el cable: el valor de h.crc32 no
     * se transmite hasta llamar a dsm_wire_finish. */
    CHECK(raw[OFF_CRC32 + 0] == 0 && raw[OFF_CRC32 + 3] == 0);

    /* Spot-check byte a byte de los campos multi-byte (big-endian) */
    CHECK(raw[OFF_MAGIC + 0] == 0x44 && raw[OFF_MAGIC + 3] == 0x32);
    CHECK(raw[OFF_CLUSTER_ID + 0] == 0x53 && raw[OFF_CLUSTER_ID + 3] == 0x31);
    CHECK(raw[OFF_PROTO_VER + 0] == 0x00 && raw[OFF_PROTO_VER + 1] == 0x02);
    CHECK(raw[OFF_TYPE + 0] == 0x00 && raw[OFF_TYPE + 1] == 0x11);
    CHECK(raw[OFF_EPOCH + 0] == 0x01 && raw[OFF_EPOCH + 7] == 0x08);
    CHECK(raw[OFF_SRC_SLOT + 0] == 0x11 && raw[OFF_SRC_SLOT + 3] == 0x44);
    CHECK(raw[OFF_DST_SLOT + 0] == 0x55 && raw[OFF_DST_SLOT + 3] == 0x88);
    CHECK(raw[OFF_REQ_ID + 0] == 0x99 && raw[OFF_REQ_ID + 3] == 0xCC);
    CHECK(raw[OFF_PAGE_ID + 0] == 0xDD && raw[OFF_PAGE_ID + 3] == 0x00);
    CHECK(raw[OFF_TSEQ + 0] == 0x0A && raw[OFF_TSEQ + 3] == 0x0D);
    CHECK(raw[OFF_DATA_VERSION + 0] == 0x0E && raw[OFF_DATA_VERSION + 3] == 0x11);
    CHECK(raw[OFF_ACCESS] == 0x42);
    CHECK(raw[OFF_FLAGS] == 0xA5);
    CHECK(raw[OFF_STATUS + 0] == 0xBE && raw[OFF_STATUS + 1] == 0xEF);
    CHECK(raw[OFF_PAYLOAD_LEN + 0] == 0xCA && raw[OFF_PAYLOAD_LEN + 3] == 0xBE);

    /* Round-trip completo */
    msg_header_t out;
    REQUIRE(dsm_wire_unpack(raw, &out) == DSM_OK);
    CHECK(out.magic == h.magic);
    CHECK(out.cluster_id == h.cluster_id);
    CHECK(out.proto_version == h.proto_version);
    CHECK(out.type == h.type);
    CHECK(out.epoch == h.epoch);
    CHECK(out.src_slot == h.src_slot);
    CHECK(out.dst_slot == h.dst_slot);
    CHECK(out.req_id == h.req_id);
    CHECK(out.page_id == h.page_id);
    CHECK(out.tseq == h.tseq);
    CHECK(out.data_version == h.data_version);
    CHECK(out.access == h.access);
    CHECK(out.flags == h.flags);
    CHECK(out.status == h.status);
    CHECK(out.payload_len == h.payload_len);
    CHECK(out.crc32 == 0);                 /* pack no transporta el CRC del struct */

    /* dsm_htonll/ntohll deben ser inversos exactos */
    CHECK(dsm_ntohll(dsm_htonll(0x0123456789ABCDEFULL)) == 0x0123456789ABCDEFULL);
    CHECK(dsm_ntohll(dsm_htonll(0)) == 0);
    CHECK(dsm_ntohll(dsm_htonll(UINT64_MAX)) == UINT64_MAX);

    return 0;
}

static int test_header_rejects_bad_magic_and_version(void)
{
    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_HEARTBEAT;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&h, raw, NULL, 0);

    /* Magic corrupto: unpack debe rechazar */
    uint8_t mut[DSM_HDR_LEN];
    memcpy(mut, raw, DSM_HDR_LEN);
    mut[OFF_MAGIC + 3] ^= 0xFF;
    msg_header_t out;
    CHECK(dsm_wire_unpack(mut, &out) != DSM_OK);

    /* Version distinta: rechazo */
    memcpy(mut, raw, DSM_HDR_LEN);
    mut[OFF_PROTO_VER + 1] = 3;
    CHECK(dsm_wire_unpack(mut, &out) != DSM_OK);

    /* Cluster ID distinto en validate: rechazo */
    msg_header_t ok;
    REQUIRE(dsm_wire_unpack(raw, &ok) == DSM_OK);
    CHECK(dsm_wire_validate(&ok, raw, NULL, 0x534F3031) == DSM_OK);
    CHECK(dsm_wire_validate(&ok, raw, NULL, 0x534F3032) != DSM_OK);

    /* payload_len fuera de DSM_MAX_PAYLOAD: rechazo */
    msg_header_t big = ok;
    big.payload_len = DSM_MAX_PAYLOAD + 1;
    uint8_t bigraw[DSM_HDR_LEN];
    dsm_wire_pack(&big, bigraw);
    big.crc32 = dsm_crc32(0, bigraw, DSM_HDR_LEN);
    CHECK(dsm_wire_validate(&big, bigraw, NULL, 0x534F3031) != DSM_OK);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 2. CRC32: vectores, incrementalidad y sensibilidad a cada bit
 * ------------------------------------------------------------------------- */

static int test_crc32_vectors(void)
{
    CHECK(dsm_crc32(0, "123456789", 9) == CRC32_CHECK_VECTOR);
    CHECK(dsm_crc32(0, "", 0) == 0x00000000u);
    CHECK(dsm_crc32(0, "a", 1) == 0xE8B7BE43u);
    CHECK(dsm_crc32(0, "abc", 3) == 0x352441C2u);
    CHECK(dsm_crc32(0, "The quick brown fox jumps over the lazy dog", 43)
          == 0x414FA339u);

    /* Un solo bit de diferencia en un byte debe cambiar el CRC */
    const uint8_t a[4] = { 0x00, 0x00, 0x00, 0x00 };
    uint8_t b[4];
    for (int bit = 0; bit < 8; bit++) {
        memcpy(b, a, 4);
        b[2] = (uint8_t)(1u << bit);
        CHECK(dsm_crc32(0, a, 4) != dsm_crc32(0, b, 4));
    }

    /* La funcion es incremental: crc(crc(0, A), B) == crc(0, A||B) */
    const char *s = "cabecera-de-56-bytes-y-carga";
    size_t n = strlen(s);
    for (size_t split = 0; split <= n; split++) {
        uint32_t inc = dsm_crc32(0, s, split);
        inc = dsm_crc32(inc, s + split, n - split);
        CHECK(inc == dsm_crc32(0, s, n));
    }

    return 0;
}

/* Invierte un bit concreto (0..len*8-1) sobre un buffer */
static void flip_bit(uint8_t *buf, size_t bit_index)
{
    buf[bit_index / 8] ^= (uint8_t)(1u << (bit_index % 8));
}

static int test_crc32_header_single_bit_sweep(void)
{
    /* Barrido exhaustivo: 56 bytes * 8 bits = 448 posiciones. Ninguna
     * inversion de un solo bit debe producir un CRC valido. */
    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_PAGE_DATA;
    h.src_slot      = 7;
    h.dst_slot      = 3;
    h.page_id       = 12345;
    h.payload_len   = 0;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&h, raw, NULL, 0);

    msg_header_t base;
    REQUIRE(dsm_wire_unpack(raw, &base) == DSM_OK);
    REQUIRE(dsm_wire_validate(&base, raw, NULL, 0x534F3031) == DSM_OK);

    unsigned detected = 0;
    unsigned total = DSM_HDR_LEN * 8;

    for (size_t bit = 0; bit < total; bit++) {
        uint8_t mut[DSM_HDR_LEN];
        memcpy(mut, raw, DSM_HDR_LEN);
        flip_bit(mut, bit);

        /* Reinterpretar la cabecera mutada tal como haria el receptor */
        msg_header_t m;
        int up = dsm_wire_unpack(mut, &m);

        /* Si unpack rechaza ya es deteccion; si no, validate debe rechazar.
         * Excepcion legitima: mutar el byte de CRC solo cambia el valor
         * esperado y validate compara contra el calculado, asi que tambien
         * debe fallar. */
        int rejected = (up != DSM_OK) ||
                       (dsm_wire_validate(&m, mut, NULL, 0x534F3031) != DSM_OK);
        if (rejected) {
            detected++;
        } else {
            printf("   bit %zu de la cabecera NO fue detectado\n", bit);
            g_fails++;
        }
    }

    CHECK(detected == total);
    printf("   barrido cabecera: %u/%u bits corruptos detectados\n", detected, total);
    return 0;
}

static int test_crc32_payload_single_bit_sweep(void)
{
    /* Payload de 64 bytes: 512 posiciones de bit. Todas deben detectarse. */
    enum { PLEN = 64 };
    uint8_t payload[PLEN];
    for (int i = 0; i < PLEN; i++) payload[i] = (uint8_t)(0xA0 + i);

    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_PAGE_DATA;
    h.page_id       = 1;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&h, raw, payload, PLEN);

    msg_header_t base;
    REQUIRE(dsm_wire_unpack(raw, &base) == DSM_OK);
    REQUIRE(dsm_wire_validate(&base, raw, payload, 0x534F3031) == DSM_OK);

    unsigned total = PLEN * 8;
    unsigned detected = 0;

    for (size_t bit = 0; bit < total; bit++) {
        uint8_t mut[PLEN];
        memcpy(mut, payload, PLEN);
        flip_bit(mut, bit);

        if (dsm_wire_validate(&base, raw, mut, 0x534F3031) != DSM_OK) {
            detected++;
        } else {
            printf("   bit %zu del payload NO fue detectado\n", bit);
            g_fails++;
        }
    }

    CHECK(detected == total);
    printf("   barrido payload: %u/%u bits corruptos detectados\n", detected, total);
    return 0;
}

static int test_crc32_payload_truncation_and_extension(void)
{
    enum { PLEN = 32 };
    uint8_t payload[PLEN];
    memset(payload, 0x5C, PLEN);

    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_JOIN;
    h.payload_len   = PLEN;

    uint8_t raw[DSM_HDR_LEN];
    dsm_wire_finish(&h, raw, payload, PLEN);

    msg_header_t base;
    REQUIRE(dsm_wire_unpack(raw, &base) == DSM_OK);
    REQUIRE(dsm_wire_validate(&base, raw, payload, 0x534F3031) == DSM_OK);

    /* payload NULL cuando el header declara datos: no puede validar OK */
    CHECK(dsm_wire_validate(&base, raw, NULL, 0x534F3031) != DSM_OK);

    /* Indice comun de un payload distinto: debe fallar */
    uint8_t other[PLEN];
    memset(other, 0x5D, PLEN);
    CHECK(dsm_wire_validate(&base, raw, other, 0x534F3031) != DSM_OK);

    /* Un solo byte distinto en la ultima posicion */
    other[PLEN - 1] = 0x5C;
    CHECK(dsm_wire_validate(&base, raw, other, 0x534F3031) != DSM_OK);

    /* dsm_wire_finish debe ser determinista: mismo mensaje, mismo CRC */
    uint8_t raw2[DSM_HDR_LEN];
    msg_header_t h2;
    memcpy(&h2, &h, sizeof(h2));
    uint32_t c2 = dsm_wire_finish(&h2, raw2, payload, PLEN);
    CHECK(c2 == base.crc32);
    CHECK(memcmp(raw, raw2, DSM_HDR_LEN) == 0);

    /* Y el CRC no debe depender del valor previo del campo crc32 */
    msg_header_t h3;
    memcpy(&h3, &h, sizeof(h3));
    h3.crc32 = 0xFFFFFFFF;
    uint8_t raw3[DSM_HDR_LEN];
    uint32_t c3 = dsm_wire_finish(&h3, raw3, payload, PLEN);
    CHECK(c3 == base.crc32);
    CHECK(h3.crc32 == c3);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 3. MEMBERSHIP: 40 B memoria / 36 B cable
 * ------------------------------------------------------------------------- */

static int test_membership_pack(void)
{
    CHECK(sizeof(membership_wire_t) == 40);
    CHECK(DSM_MEMBERSHIP_WIRE_LEN == 36u);
    CHECK(sizeof(membership_wire_t) - DSM_MEMBERSHIP_WIRE_LEN == 4); /* padding final */

    membership_wire_t m;
    memset(&m, 0, sizeof(m));
    for (int i = 0; i < 16; i++) m.uuid[i] = (uint8_t)(0x10 + i);
    m.ip          = 0x7F000001;      /* 127.0.0.1 */
    m.port        = 27600;
    m.slot        = 17;
    m.state       = DSM_NODE_ALIVE;
    m.incarnation = 0x0102030405060708ULL;
    m.mem_pages   = 65536;

    uint8_t out[DSM_MEMBERSHIP_WIRE_LEN];
    memset(out, 0xEE, sizeof(out));
    size_t n = dsm_pack_membership(&m, out);
    CHECK(n == DSM_MEMBERSHIP_WIRE_LEN);

    /* Layout exacto en el cable */
    CHECK(memcmp(out, m.uuid, 16) == 0);
    CHECK(out[16] == 0x7F && out[17] == 0x00 && out[18] == 0x00 && out[19] == 0x01);
    CHECK(out[20] == (uint8_t)(27600 >> 8) && out[21] == (uint8_t)(27600 & 0xFF));
    CHECK(out[22] == 17);
    CHECK(out[23] == DSM_NODE_ALIVE);
    CHECK(out[24] == 0x01 && out[31] == 0x08);
    CHECK(out[32] == 0x00 && out[35] == 0x00);

    membership_wire_t r;
    memset(&r, 0xAA, sizeof(r));
    dsm_unpack_membership(out, &r);
    CHECK(memcmp(r.uuid, m.uuid, 16) == 0);
    CHECK(r.ip == m.ip);
    CHECK(r.port == m.port);
    CHECK(r.slot == m.slot);
    CHECK(r.state == m.state);
    CHECK(r.incarnation == m.incarnation);
    CHECK(r.mem_pages == m.mem_pages);

    /* El padding de la struct en memoria no debe contaminar el cable:
     * aunque se ensucie, el pack escribe exactamente 36 B deterministas. */
    membership_wire_t dirty;
    memcpy(&dirty, &m, sizeof(m));
    memset((uint8_t *)&dirty + 36, 0xFF, 4);
    uint8_t out2[DSM_MEMBERSHIP_WIRE_LEN];
    dsm_pack_membership(&dirty, out2);
    CHECK(memcmp(out, out2, DSM_MEMBERSHIP_WIRE_LEN) == 0);

    /* Casos limite */
    membership_wire_t lim;
    memset(&lim, 0xFF, sizeof(lim));
    lim.port = UINT16_MAX;
    lim.slot = 255;
    uint8_t lo[DSM_MEMBERSHIP_WIRE_LEN];
    dsm_pack_membership(&lim, lo);
    dsm_unpack_membership(lo, &r);
    CHECK(r.port == UINT16_MAX);
    CHECK(r.slot == 255);
    CHECK(r.mem_pages == UINT32_MAX);
    CHECK(r.incarnation == UINT64_MAX);

    return 0;
}

static int test_membership_matrix_roundtrip(void)
{
    /* Matriz realista de nodos con valores distintos y verificables */
    enum { N = 32 };
    static membership_wire_t nodes[N];
    static uint8_t packed[N * DSM_MEMBERSHIP_WIRE_LEN];

    for (int i = 0; i < N; i++) {
        memset(&nodes[i], 0, sizeof(nodes[i]));
        for (int b = 0; b < 16; b++) {
            nodes[i].uuid[b] = (uint8_t)((i * 7 + b * 13) & 0xFF);
        }
        nodes[i].ip          = 0x0A000001u + (uint32_t)i;   /* 10.0.0.x */
        nodes[i].port        = (uint16_t)(27600 + i);
        nodes[i].slot        = (uint8_t)i;
        nodes[i].state       = (i % 3 == 0) ? DSM_NODE_ALIVE
                             : (i % 3 == 1) ? DSM_NODE_SUSPECT : DSM_NODE_DEAD;
        nodes[i].incarnation = 0x100000000ULL * (uint64_t)i + (uint64_t)i;
        nodes[i].mem_pages   = 1000u + (uint32_t)i * 7u;
    }

    for (int i = 0; i < N; i++) {
        size_t n = dsm_pack_membership(&nodes[i], packed + i * DSM_MEMBERSHIP_WIRE_LEN);
        CHECK(n == DSM_MEMBERSHIP_WIRE_LEN);
    }

    for (int i = 0; i < N; i++) {
        membership_wire_t r;
        memset(&r, 0, sizeof(r));
        dsm_unpack_membership(packed + i * DSM_MEMBERSHIP_WIRE_LEN, &r);
        CHECK(memcmp(r.uuid, nodes[i].uuid, 16) == 0);
        CHECK(r.ip == nodes[i].ip);
        CHECK(r.port == nodes[i].port);
        CHECK(r.slot == nodes[i].slot);
        CHECK(r.state == nodes[i].state);
        CHECK(r.incarnation == nodes[i].incarnation);
        CHECK(r.mem_pages == nodes[i].mem_pages);
    }

    /* Un cambio de un bit en cualquier registro no debe propagarse a los
     * registros vecinos al desempaquetar (cada uno ocupa exactamente 36 B). */
    membership_wire_t antes, despues;
    dsm_unpack_membership(packed + 10 * 36, &antes);
    packed[10 * 36 + 22] ^= 0x01;   /* bit bajo del slot del nodo 10 */
    dsm_unpack_membership(packed + 10 * 36, &despues);
    CHECK(despues.slot == (antes.slot ^ 0x01));
    dsm_unpack_membership(packed + 11 * 36, &despues);
    CHECK(memcmp(despues.uuid, nodes[11].uuid, 16) == 0);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 4. WELCOME: matrices reales de nodos
 * ------------------------------------------------------------------------- */

/* Llena un welcome_t con 'count' miembros y 'allocs' registros deterministas */
static void make_welcome(welcome_t *w, uint32_t count, uint32_t allocs, uint64_t epoch)
{
    memset(w, 0, sizeof(*w));
    w->epoch = epoch;
    for (int i = 0; i < 16; i++) w->origin_uuid[i] = (uint8_t)(0xC0 + i);

    w->count = count;
    for (uint32_t i = 0; i < count && i < MAX_NODES; i++) {
        membership_wire_t *m = &w->members[i];
        for (int b = 0; b < 16; b++) m->uuid[b] = (uint8_t)(i ^ (uint32_t)b);
        m->ip          = 0x0A000001u + i;
        m->port        = (uint16_t)(27600 + i);
        m->slot        = (uint8_t)i;
        m->state       = DSM_NODE_ALIVE;
        m->incarnation = 1000u + i;
        m->mem_pages   = 65536u + i;
    }

    w->alloc_count = allocs;
    for (uint32_t i = 0; i < allocs && i < 64; i++) {
        w->allocs[i].alloc_id   = i + 1;
        w->allocs[i].first_page = i * 16;
        w->allocs[i].npages     = 8 + i;
        w->allocs[i].slot       = i % 4;
        w->allocs[i].pid        = 1000 + i;
        w->allocs[i].reserved   = 0;
    }
}

static int verify_welcome(const welcome_t *in, const welcome_t *out, const char *label)
{
    int before = g_fails;
    CHECK(out->epoch == in->epoch);
    CHECK(memcmp(out->origin_uuid, in->origin_uuid, 16) == 0);
    CHECK(out->count == in->count);
    CHECK(out->alloc_count == in->alloc_count);

    for (uint32_t i = 0; i < in->count; i++) {
        CHECK(memcmp(out->members[i].uuid, in->members[i].uuid, 16) == 0);
        CHECK(out->members[i].ip == in->members[i].ip);
        CHECK(out->members[i].port == in->members[i].port);
        CHECK(out->members[i].slot == in->members[i].slot);
        CHECK(out->members[i].state == in->members[i].state);
        CHECK(out->members[i].incarnation == in->members[i].incarnation);
        CHECK(out->members[i].mem_pages == in->members[i].mem_pages);
    }
    for (uint32_t i = 0; i < in->alloc_count; i++) {
        CHECK(out->allocs[i].alloc_id == in->allocs[i].alloc_id);
        CHECK(out->allocs[i].first_page == in->allocs[i].first_page);
        CHECK(out->allocs[i].npages == in->allocs[i].npages);
        CHECK(out->allocs[i].slot == in->allocs[i].slot);
        CHECK(out->allocs[i].pid == in->allocs[i].pid);
    }
    if (g_fails != before) {
        printf("   fallo en matriz '%s' (count=%u allocs=%u)\n",
               label, in->count, in->alloc_count);
        return -1;
    }
    return 0;
}

static int test_welcome_matrix(void)
{
    /* Tamanos representativos: vacio, unitario, par, impar, potencia de dos,
     * casi el maximo y el maximo absoluto. */
    static const uint32_t counts[] = { 0, 1, 2, 3, 7, 8, 16, 63, 64, 127, 255, 256 };
    static const uint32_t allocs[] = { 0, 1, 2, 5, 16, 63, 64 };
    static uint8_t buf[65536];

    for (size_t ci = 0; ci < sizeof(counts) / sizeof(counts[0]); ci++) {
        for (size_t ai = 0; ai < sizeof(allocs) / sizeof(allocs[0]); ai++) {
            static welcome_t in, out;

            /* max_out debe bastar: 28 + N*36 + 4 + A*24 */
            size_t need = 28 + (size_t)counts[ci] * 36 + 4 + (size_t)allocs[ai] * 24;
            if (need > sizeof(buf)) continue;

            make_welcome(&in, counts[ci], allocs[ai], 0xAF00000000000000ULL + ci);

            size_t n = dsm_pack_welcome(&in, buf, sizeof(buf));
            CHECK(n == need);

            memset(&out, 0, sizeof(out));
            int rc = dsm_unpack_welcome(buf, n, &out);
            CHECK(rc == DSM_OK);
            if (rc != DSM_OK) continue;

            char label[64];
            snprintf(label, sizeof(label), "N=%u,A=%u", counts[ci], allocs[ai]);
            verify_welcome(&in, &out, label);

            /* Determinismo: empaquetar dos veces produce bytes identicos */
            static uint8_t buf2[65536];
            size_t n2 = dsm_pack_welcome(&in, buf2, sizeof(buf2));
            CHECK(n2 == n);
            CHECK(memcmp(buf, buf2, n) == 0);
        }
    }

    return 0;
}

static int test_welcome_buffer_bounds(void)
{
    static welcome_t in;
    static uint8_t buf[65536];
    make_welcome(&in, 10, 5, 42);

    size_t need = 28 + 10 * 36 + 4 + 5 * 24;
    CHECK(need == 512);

    /* Exactamente justo: acepta */
    CHECK(dsm_pack_welcome(&in, buf, need) == need);

    /* Un byte menos: debe rechazar y NO escribir parcialmente */
    memset(buf, 0xCC, 64);
    CHECK(dsm_pack_welcome(&in, buf, need - 1) == 0);
    CHECK(buf[0] == 0xCC);

    /* count fuera de MAX_NODES: rechazo sin leer fuera de la struct */
    welcome_t bad = in;
    bad.count = MAX_NODES + 1;
    CHECK(dsm_pack_welcome(&bad, buf, sizeof(buf)) == 0);

    /* alloc_count fuera de 64: rechazo */
    bad = in;
    bad.alloc_count = 65;
    CHECK(dsm_pack_welcome(&bad, buf, sizeof(buf)) == 0);

    /* NULL: rechazo */
    CHECK(dsm_pack_welcome(NULL, buf, sizeof(buf)) == 0);
    CHECK(dsm_pack_welcome(&in, NULL, sizeof(buf)) == 0);

    /* WELCOME maximo real (256 nodos + 64 allocs) cabe en el presupuesto de
     * DSM_MAX_PAYLOAD y en un buffer de 4 KiB no: debe rechazar limpiamente. */
    static welcome_t maxw;
    make_welcome(&maxw, MAX_NODES, 64, 1);
    size_t max_need = 28 + (size_t)MAX_NODES * 36 + 4 + 64 * 24;
    CHECK(max_need == 10784);
    CHECK(max_need <= DSM_MAX_PAYLOAD);
    static uint8_t big[20000];
    CHECK(dsm_pack_welcome(&maxw, big, sizeof(big)) == max_need);
    CHECK(dsm_pack_welcome(&maxw, big, 4096) == 0);

    return 0;
}

static int test_welcome_unpack_robustness(void)
{
    static welcome_t in, out;
    static uint8_t buf[20000];
    make_welcome(&in, 4, 2, 7);
    size_t n = dsm_pack_welcome(&in, buf, sizeof(buf));
    REQUIRE(n > 0);
    REQUIRE(dsm_unpack_welcome(buf, n, &out) == DSM_OK);

    /* Truncado en cada frontera relevante: nunca debe devolver DSM_OK */
    CHECK(dsm_unpack_welcome(buf, 0, &out) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(buf, 1, &out) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(buf, 31, &out) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(buf, 32, &out) != DSM_OK);
    CHECK(dsm_unpack_welcome(buf, n - 1, &out) != DSM_OK);
    CHECK(dsm_unpack_welcome(buf, n, &out) == DSM_OK);

    /* count declarado mayor que los miembros presentes */
    uint8_t mut[20000];
    memcpy(mut, buf, n);
    mut[27] = 0xFF;                       /* count = 255 (big-endian offset 24..27) */
    CHECK(dsm_unpack_welcome(mut, n, &out) != DSM_OK);

    /* count > MAX_NODES */
    memcpy(mut, buf, n);
    mut[24] = 0x00; mut[25] = 0x00; mut[26] = 0x01; mut[27] = 0x01; /* 257 */
    CHECK(dsm_unpack_welcome(mut, n, &out) == DSM_ERR_PROTO);

    /* alloc_count > 64: ubicarlo justo despues de los miembros */
    memcpy(mut, buf, n);
    size_t off = 28 + 4 * 36;
    mut[off + 0] = 0x00; mut[off + 1] = 0x00;
    mut[off + 2] = 0x00; mut[off + 3] = 0x41;   /* 65 */
    CHECK(dsm_unpack_welcome(mut, n, &out) == DSM_ERR_PROTO);

    /* claim de alloc_count que excede la longitud real */
    memcpy(mut, buf, n);
    mut[off + 3] = 0x04;                        /* 4 allocs, solo hay 2 */
    CHECK(dsm_unpack_welcome(mut, n, &out) == DSM_ERR_PROTO);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 5. Validacion de longitudes por tipo de mensaje
 * ------------------------------------------------------------------------- */

static int test_wire_payload_ok_matrix(void)
{
    const uint32_t ps = 4096;

    /* HELLO: uuid[16] + raw_hash[4] = 20 exacto */
    CHECK(wire_payload_ok(MSG_HELLO, 0, 20, ps) == true);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 19, ps) == false);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 21, ps) == false);

    /* WELCOME: >= 32 y <= MAX_PAYLOAD */
    CHECK(wire_payload_ok(MSG_WELCOME, 0, 32, ps) == true);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, 31, ps) == false);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, DSM_MAX_PAYLOAD, ps) == true);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, DSM_MAX_PAYLOAD + 1, ps) == false);

    /* JOIN: membership exacta */
    CHECK(wire_payload_ok(MSG_JOIN, 0, DSM_MEMBERSHIP_WIRE_LEN, ps) == true);
    CHECK(wire_payload_ok(MSG_JOIN, 0, DSM_MEMBERSHIP_WIRE_LEN - 1, ps) == false);
    CHECK(wire_payload_ok(MSG_JOIN, 0, DSM_MEMBERSHIP_WIRE_LEN + 1, ps) == false);

    /* JOIN_ACK: sin carga */
    CHECK(wire_payload_ok(MSG_JOIN_ACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_JOIN_ACK, 0, 1, ps) == false);

    /* HEARTBEAT: 12 exacto */
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 12, ps) == true);
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 11, ps) == false);
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 13, ps) == false);

    /* LEAVE / NODE_DOWN / MEMBERSHIP_REQ: 0 o 28 */
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 28, ps) == true);
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 27, ps) == false);
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 28, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_REQ, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_REQ, 0, 28, ps) == true);

    /* MENSAJES sin carga */
    CHECK(wire_payload_ok(MSG_PAGE_REQUEST, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_PAGE_REQUEST, 0, 1, ps) == false);
    CHECK(wire_payload_ok(MSG_GRANT, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_NACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE, 0, ps, ps) == false);

    /* PAGE_DATA / WRITEBACK: exactamente page_size */
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4096, ps) == true);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4095, ps) == false);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 8192, ps) == false);
    CHECK(wire_payload_ok(MSG_WRITEBACK, 0, 4096, ps) == true);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 8192, 8192) == true);

    /* ACK con/sin datos segun flag */
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, DSM_FLAG_HAS_DATA, ps, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, DSM_FLAG_HAS_DATA, 0, ps) == false);
    CHECK(wire_payload_ok(MSG_DOWNGRADE_ACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_DOWNGRADE_ACK, DSM_FLAG_HAS_DATA, ps, ps) == true);

    /* Barrier */
    CHECK(wire_payload_ok(MSG_BARRIER_ARRIVE, 0, 8, ps) == true);
    CHECK(wire_payload_ok(MSG_BARRIER_ARRIVE, 0, 7, ps) == false);
    CHECK(wire_payload_ok(MSG_BARRIER_GO, 0, 4, ps) == true);
    CHECK(wire_payload_ok(MSG_BARRIER_GO, 0, 8, ps) == false);

    /* Tipo desconocido: solo la cota global */
    CHECK(wire_payload_ok(0x7FFE, 0, 0, ps) == true);
    CHECK(wire_payload_ok(0x7FFE, 0, DSM_MAX_PAYLOAD, ps) == true);
    CHECK(wire_payload_ok(0x7FFE, 0, DSM_MAX_PAYLOAD + 1, ps) == false);

    return 0;
}

static int test_message_classification(void)
{
    /* Nombres de tipo: cada mensaje definido debe tener nombre != UNKNOWN */
    static const uint16_t all_types[] = {
        MSG_HELLO, MSG_WELCOME, MSG_JOIN, MSG_JOIN_ACK, MSG_HEARTBEAT,
        MSG_LEAVE, MSG_NODE_DOWN, MSG_MEMBERSHIP_REQ, MSG_MEMBERSHIP_RESP,
        MSG_PAGE_MOVE, MSG_PAGE_MOVE_ACK, MSG_PAGE_REQUEST, MSG_PAGE_DATA,
        MSG_GRANT, MSG_INVALIDATE, MSG_INVALIDATE_ACK, MSG_DOWNGRADE,
        MSG_DOWNGRADE_ACK, MSG_WRITEBACK, MSG_WRITEBACK_ACK,
        MSG_PARITY_DELTA, MSG_PARITY_ACK, MSG_STRIPE_PUT, MSG_STRIPE_PUT_ACK,
        MSG_STRIPE_READ, MSG_STRIPE_DATA, MSG_STATE_REPORT, MSG_DIR_DONE,
        MSG_LOCK_REQ, MSG_LOCK_GRANT, MSG_LOCK_REL, MSG_BARRIER_ARRIVE,
        MSG_BARRIER_GO, MSG_QUERY_REQ, MSG_QUERY_RESP, MSG_NACK
    };
    for (size_t i = 0; i < sizeof(all_types) / sizeof(all_types[0]); i++) {
        const char *name = dsm_msg_type_name(all_types[i]);
        CHECK(strcmp(name, "UNKNOWN") != 0);
        CHECK(name[0] != '\0');
    }
    CHECK(strcmp(dsm_msg_type_name(MSG_PAGE_DATA), "PAGE_DATA") == 0);
    CHECK(strcmp(dsm_msg_type_name(MSG_WELCOME), "WELCOME") == 0);
    CHECK(strcmp(dsm_msg_type_name(0xFFFF), "UNKNOWN") == 0);

    /* Respuestas */
    CHECK(dsm_msg_is_response(MSG_WELCOME) == true);
    CHECK(dsm_msg_is_response(MSG_JOIN_ACK) == true);
    CHECK(dsm_msg_is_response(MSG_PAGE_DATA) == true);
    CHECK(dsm_msg_is_response(MSG_GRANT) == true);
    CHECK(dsm_msg_is_response(MSG_NACK) == true);
    CHECK(dsm_msg_is_response(MSG_HELLO) == false);
    CHECK(dsm_msg_is_response(MSG_JOIN) == false);
    CHECK(dsm_msg_is_response(MSG_PAGE_REQUEST) == false);
    CHECK(dsm_msg_is_response(0xFFFF) == false);

    /* Membresia */
    CHECK(dsm_msg_is_membership(MSG_HELLO) == true);
    CHECK(dsm_msg_is_membership(MSG_WELCOME) == true);
    CHECK(dsm_msg_is_membership(MSG_JOIN) == true);
    CHECK(dsm_msg_is_membership(MSG_MEMBERSHIP_RESP) == true);
    CHECK(dsm_msg_is_membership(MSG_PAGE_DATA) == false);
    CHECK(dsm_msg_is_membership(MSG_LOCK_REQ) == false);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 6. Alloc record y dir entry
 * ------------------------------------------------------------------------- */

static int test_alloc_and_dir_layout(void)
{
    CHECK(sizeof(alloc_rec_t) == 24);
    CHECK(DSM_ALLOC_REC_LEN == 24u);
    CHECK(sizeof(dir_entry_t) == 72);
    CHECK(DSM_DIR_ENTRY_LEN == 72u);

    alloc_rec_t a;
    memset(&a, 0, sizeof(a));
    a.alloc_id   = 0x01020304;
    a.first_page = 0x0A0B0C0D;
    a.npages     = 0x11223344;
    a.slot       = 0x55667788;
    a.pid        = 0x99AABBCC;
    a.reserved   = 0xDDEEFF00;

    uint8_t ab[DSM_ALLOC_REC_LEN];
    CHECK(dsm_pack_alloc_rec(&a, ab) == DSM_ALLOC_REC_LEN);
    CHECK(ab[0] == 0x01 && ab[3] == 0x04);
    CHECK(ab[4] == 0x0A && ab[7] == 0x0D);
    CHECK(ab[8] == 0x11 && ab[11] == 0x44);
    CHECK(ab[12] == 0x55 && ab[15] == 0x88);
    CHECK(ab[16] == 0x99 && ab[19] == 0xCC);
    CHECK(ab[20] == 0xDD && ab[23] == 0x00);

    alloc_rec_t ar;
    dsm_unpack_alloc_rec(ab, &ar);
    CHECK(memcmp(&ar, &a, sizeof(a)) == 0);

    dir_entry_t d;
    memset(&d, 0, sizeof(d));
    d.page_id   = 1;   d.owner    = 2;   d.version = 3;  d.tseq = 4;
    d.frame     = 5;   d.stripe_id = 6;  d.wait_head = 7; d.wait_tail = 8;
    d.txn       = 9;   d.state    = 0xA; d.busy     = 0xB; d.reserved = 0x0C0D;
    for (int i = 0; i < MAX_NODES / 8; i++) d.copyset[i] = (uint8_t)(0xF0 ^ i);

    uint8_t db[DSM_DIR_ENTRY_LEN];
    dsm_pack_dir_entry(&d, db);
    CHECK(db[0] == 0 && db[3] == 1);
    CHECK(db[36] == 0x0A);
    CHECK(db[37] == 0x0B);
    CHECK(db[38] == 0x0C && db[39] == 0x0D);
    CHECK(memcmp(db + 40, d.copyset, 32) == 0);

    dir_entry_t dr;
    memset(&dr, 0, sizeof(dr));
    dsm_unpack_dir_entry(db, &dr);
    CHECK(memcmp(&dr, &d, sizeof(d)) == 0);

    /* Matriz de 256 dir entries: round-trip sin deriva */
    static dir_entry_t ents[256];
    static uint8_t packed[256 * DSM_DIR_ENTRY_LEN];
    for (int i = 0; i < 256; i++) {
        memset(&ents[i], 0, sizeof(ents[i]));
        ents[i].page_id = (uint32_t)i;
        ents[i].owner   = (uint32_t)i * 3;
        ents[i].version = (uint32_t)i * 5;
        ents[i].tseq    = (uint32_t)i * 7;
        ents[i].state   = (uint8_t)(i % 3);
        ents[i].copyset[i / 8] = (uint8_t)(1u << (i % 8));
        dsm_pack_dir_entry(&ents[i], packed + i * DSM_DIR_ENTRY_LEN);
    }
    for (int i = 0; i < 256; i++) {
        dir_entry_t r;
        dsm_unpack_dir_entry(packed + i * DSM_DIR_ENTRY_LEN, &r);
        CHECK(memcmp(&r, &ents[i], sizeof(r)) == 0);
    }

    return 0;
}

/* ----------------------------------------------------------------------------
 * 7. Rigor de layout: memoria vs cable y epoch en offset 12
 * ------------------------------------------------------------------------- */

static int test_rig_layout_bytes_y_epoch(void)
{
    /* Tamanos y alineacion que sostienen todo el protocolo. */
    CHECK(sizeof(msg_header_t) == 64);
    CHECK(DSM_HDR_LEN == 56u);
    CHECK(_Alignof(msg_header_t) == 8);
    CHECK((sizeof(msg_header_t) - DSM_HDR_LEN) == 8);   /* relleno final */

    /* Offsets en MEMORIA: el relleno de 4 B cae entre type y epoch. */
    CHECK(offsetof(msg_header_t, magic)         == 0);
    CHECK(offsetof(msg_header_t, cluster_id)    == 4);
    CHECK(offsetof(msg_header_t, proto_version) == 8);
    CHECK(offsetof(msg_header_t, type)          == 10);
    CHECK(offsetof(msg_header_t, epoch)         == 16);
    CHECK(offsetof(msg_header_t, src_slot)      == 24);
    CHECK(offsetof(msg_header_t, dst_slot)      == 28);
    CHECK(offsetof(msg_header_t, req_id)        == 32);
    CHECK(offsetof(msg_header_t, page_id)       == 36);
    CHECK(offsetof(msg_header_t, tseq)          == 40);
    CHECK(offsetof(msg_header_t, data_version)  == 44);
    CHECK(offsetof(msg_header_t, access)        == 48);
    CHECK(offsetof(msg_header_t, flags)         == 49);
    CHECK(offsetof(msg_header_t, status)        == 50);
    CHECK(offsetof(msg_header_t, payload_len)   == 52);
    CHECK(offsetof(msg_header_t, crc32)         == 56);
    CHECK(offsetof(msg_header_t, crc32) + 4     == 60);   /* y 4 B de relleno final */
    CHECK(sizeof(msg_header_t) - (offsetof(msg_header_t, crc32) + 4) == 4);

    /* Los offsets del CABLE no coinciden con los de memoria justo por eso:
     * el epoch salta de 16 en memoria a 12 en el cable. */
    CHECK(OFF_EPOCH == 12);
    CHECK(OFF_EPOCH != offsetof(msg_header_t, epoch));
    CHECK(OFF_PAYLOAD_LEN == 48 && offsetof(msg_header_t, payload_len) == 52);
    CHECK(OFF_CRC32 == 52 && offsetof(msg_header_t, crc32) == 56);

    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = 0x534F3031;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_PAGE_REQUEST;
    h.epoch         = 0x0123456789ABCDEFULL;
    h.src_slot      = 0x0A0B0C0D;
    h.dst_slot      = 0x01020304;
    h.req_id        = 0xDEADBEEF;
    h.page_id       = 0xCAFEBABE;
    h.tseq          = 0x00112233;
    h.data_version  = 0x44556677;
    h.access        = 0x11;
    h.flags         = 0x22;
    h.status        = 0x3344;
    h.payload_len   = 0;
    h.crc32         = 0xFFFFFFFF;   /* no debe viajar con dsm_wire_pack */

    /* Buffer alineado a 8: asi raw + OFF_EPOCH (12) queda desalineado a 4,
     * que es exactamente el caso que obliga a usar memcpy y prohibe el cast. */
    _Alignas(8) uint8_t raw[DSM_HDR_LEN];
    dsm_wire_pack(&h, raw);

    CHECK(((uintptr_t)(raw + OFF_EPOCH) & 7u) == 4u);

    /* epoch en orden de red: big-endian puro en 12..19, escrito/leido con
     * memcpy (nunca con *(uint64_t *)(raw + 12)). */
    static const uint8_t epoch_be[8] = {
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF
    };
    CHECK(memcmp(raw + OFF_EPOCH, epoch_be, 8) == 0);

    uint64_t net;
    memcpy(&net, raw + OFF_EPOCH, sizeof(net));
    CHECK(net == dsm_htonll(h.epoch));
    CHECK(dsm_ntohll(net) == h.epoch);

    /* El resto de campos multi-byte, tambien big-endian y en su offset. */
    uint32_t u32; uint16_t u16;
    memcpy(&u32, raw + OFF_MAGIC, 4);         CHECK(ntohl(u32) == DSM_MAGIC);
    memcpy(&u32, raw + OFF_CLUSTER_ID, 4);    CHECK(ntohl(u32) == 0x534F3031u);
    memcpy(&u16, raw + OFF_PROTO_VER, 2);     CHECK(ntohs(u16) == DSM_PROTO_VERSION);
    memcpy(&u16, raw + OFF_TYPE, 2);          CHECK(ntohs(u16) == MSG_PAGE_REQUEST);
    memcpy(&u32, raw + OFF_SRC_SLOT, 4);      CHECK(ntohl(u32) == 0x0A0B0C0Du);
    memcpy(&u32, raw + OFF_DST_SLOT, 4);      CHECK(ntohl(u32) == 0x01020304u);
    memcpy(&u32, raw + OFF_REQ_ID, 4);        CHECK(ntohl(u32) == 0xDEADBEEFu);
    memcpy(&u32, raw + OFF_PAGE_ID, 4);       CHECK(ntohl(u32) == 0xCAFEBABEu);
    memcpy(&u32, raw + OFF_TSEQ, 4);          CHECK(ntohl(u32) == 0x00112233u);
    memcpy(&u32, raw + OFF_DATA_VERSION, 4);  CHECK(ntohl(u32) == 0x44556677u);
    CHECK(raw[OFF_ACCESS] == 0x11);
    CHECK(raw[OFF_FLAGS]  == 0x22);
    memcpy(&u16, raw + OFF_STATUS, 2);        CHECK(ntohs(u16) == 0x3344);
    memcpy(&u32, raw + OFF_PAYLOAD_LEN, 4);   CHECK(ntohl(u32) == 0);
    memcpy(&u32, raw + OFF_CRC32, 4);         CHECK(u32 == 0);  /* pack escribe 0 */

    /* Round-trip: el epoch vuelve intacto tras pasar por el cable. */
    msg_header_t out;
    REQUIRE(dsm_wire_unpack(raw, &out) == DSM_OK);
    CHECK(out.epoch == h.epoch);
    CHECK(out.magic == DSM_MAGIC);
    CHECK(out.payload_len == 0);
    CHECK(out.crc32 == 0);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 8. CRC32: barrido bit a bit de (56 + 4096) * 8 bits
 * ------------------------------------------------------------------------- */

static void rig_flip_bit(uint8_t *buf, size_t bit)
{
    buf[bit / 8] ^= (uint8_t)(1u << (bit % 8));
}

static int test_rig_crc32_frame_4096_bitflip(void)
{
    enum { PLEN = 4096 };
    const uint32_t cluster = 0x534F3031u;

    /* Payload realista de pagina: patron determinista y no trivial. */
    static uint8_t payload[DSM_MAX_PAYLOAD];
    memset(payload, 0, sizeof(payload));
    for (size_t i = 0; i < PLEN; i++) {
        payload[i] = (uint8_t)((i * 131u + 7u) & 0xFFu);
    }
    CHECK(payload[0] == 0x07 && payload[PLEN - 1] ==
          (uint8_t)(((PLEN - 1) * 131u + 7u) & 0xFFu));

    msg_header_t h;
    memset(&h, 0, sizeof(h));
    h.magic         = DSM_MAGIC;
    h.cluster_id    = cluster;
    h.proto_version = DSM_PROTO_VERSION;
    h.type          = MSG_PAGE_DATA;
    h.epoch         = 0x0123456789ABCDEFULL;
    h.src_slot      = 7;
    h.dst_slot      = 3;
    h.req_id        = 0x11223344u;
    h.page_id       = 1000;
    h.tseq          = 5;
    h.data_version  = 9;
    h.access        = 1;
    h.flags         = DSM_FLAG_HAS_DATA;
    h.payload_len   = PLEN;

    uint8_t raw[DSM_HDR_LEN];
    uint32_t crc = dsm_wire_finish(&h, raw, payload, PLEN);
    CHECK(crc == h.crc32);
    REQUIRE(dsm_wire_validate(&h, raw, payload, cluster) == DSM_OK);

    /* CRC de referencia calculado por fuera de wire.c, con la misma
     * definicion: cabecera con crc=0 seguida de los 4096 bytes. */
    uint8_t raw_zero[DSM_HDR_LEN];
    memcpy(raw_zero, raw, DSM_HDR_LEN);
    memset(raw_zero + OFF_CRC32, 0, 4);
    uint32_t ref = dsm_crc32(0, raw_zero, DSM_HDR_LEN);
    ref = dsm_crc32(ref, payload, PLEN);
    CHECK(ref == crc);

    /* Cabecera: 56 * 8 = 448 inversiones de un bit. */
    unsigned hdr_total = DSM_HDR_LEN * 8u;
    unsigned hdr_bad = 0;
    for (unsigned bit = 0; bit < hdr_total; bit++) {
        uint8_t mut[DSM_HDR_LEN];
        memcpy(mut, raw, DSM_HDR_LEN);
        rig_flip_bit(mut, bit);

        msg_header_t m;
        memset(&m, 0, sizeof(m));
        (void)dsm_wire_unpack(mut, &m);   /* rellena aunque rechace magic/version */

        int rc = dsm_wire_validate(&m, mut, payload, cluster);
        if (rc != DSM_ERR_PROTO) {
            printf("   cabecera: bit %u devolvio %d, se esperaba DSM_ERR_PROTO\n",
                   bit, rc);
            hdr_bad++;
        }
    }
    CHECK(hdr_bad == 0);
    CHECK(hdr_total == 448u);
    printf("   cabecera: %u/%u bits corruptos rechazados con DSM_ERR_PROTO\n",
           hdr_total, hdr_total);

    /* Payload: 4096 * 8 = 32768 inversiones de un bit. Se trabajan sobre una
     * copia de 1 MiB (tamano maximo legal) para que una mutacion del campo
     * payload_len nunca haga que validate lea fuera del buffer. */
    unsigned pl_total = PLEN * 8u;
    unsigned pl_bad = 0;
    for (unsigned bit = 0; bit < pl_total; bit++) {
        rig_flip_bit(payload, bit);
        int rc = dsm_wire_validate(&h, raw, payload, cluster);
        if (rc != DSM_ERR_PROTO) {
            printf("   payload: bit %u devolvio %d, se esperaba DSM_ERR_PROTO\n",
                   bit, rc);
            pl_bad++;
        }
        rig_flip_bit(payload, bit);       /* restaurar */
    }
    CHECK(pl_bad == 0);
    CHECK(pl_total == 32768u);
    printf("   payload: %u/%u bits corruptos rechazados con DSM_ERR_PROTO\n",
           pl_total, pl_total);

    /* Total del barrido pedido: (56 + 4096) * 8 = 33216 comprobaciones. */
    CHECK(hdr_total + pl_total == (DSM_HDR_LEN + PLEN) * 8u);
    CHECK((DSM_HDR_LEN + PLEN) * 8u == 33216u);

    /* Con la trama intacta sigue validando: el barrido no dejo residuos. */
    CHECK(dsm_wire_validate(&h, raw, payload, cluster) == DSM_OK);

    /* Una cabecera que declara carga y llega sin buffer NO puede validar
     * como integra: el CRC de la carga no se habria comprobado. */
    msg_header_t hsin;
    memcpy(&hsin, &h, sizeof(hsin));
    uint8_t raw_sin[DSM_HDR_LEN];
    uint32_t crc_sin = dsm_wire_finish(&hsin, raw_sin, NULL, PLEN);
    uint8_t raw_zero_sin[DSM_HDR_LEN];
    memcpy(raw_zero_sin, raw_sin, DSM_HDR_LEN);
    memset(raw_zero_sin + OFF_CRC32, 0, 4);
    CHECK(crc_sin == dsm_crc32(0, raw_zero_sin, DSM_HDR_LEN));
    CHECK(hsin.payload_len == PLEN);
    CHECK(dsm_wire_validate(&hsin, raw_sin, NULL, cluster) == DSM_ERR_PROTO);

    /* Una longitud por encima de DSM_MAX_PAYLOAD se rechaza por la cota, no
     * por el CRC: se construye una trama coherente de 1 MiB + 1 byte, de modo
     * que solo el limite de tamano puede hacerla fallar. */
    static uint8_t over[DSM_MAX_PAYLOAD + 1];
    memset(over, 0x5A, sizeof(over));
    msg_header_t hmax;
    memset(&hmax, 0, sizeof(hmax));
    hmax.magic         = DSM_MAGIC;
    hmax.cluster_id    = cluster;
    hmax.proto_version = DSM_PROTO_VERSION;
    hmax.type          = MSG_PAGE_DATA;
    uint8_t raw_max[DSM_HDR_LEN];
    dsm_wire_finish(&hmax, raw_max, over, (uint32_t)sizeof(over));
    CHECK(hmax.payload_len == DSM_MAX_PAYLOAD + 1);
    CHECK(dsm_wire_validate(&hmax, raw_max, over, cluster) == DSM_ERR_PROTO);
    /* El mismo mensaje con el tamano maximo legal si es coherente. */
    dsm_wire_finish(&hmax, raw_max, over, DSM_MAX_PAYLOAD);
    CHECK(hmax.payload_len == DSM_MAX_PAYLOAD);
    CHECK(dsm_wire_validate(&hmax, raw_max, over, cluster) == DSM_OK);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 9. Round-trip de cargas fijas con datos realistas
 * ------------------------------------------------------------------------- */

static int test_rig_cargas_fijas_realistas(void)
{
    /* membership_wire_t: un nodo real 192.168.1.42:27601 en estado SUSPECT. */
    membership_wire_t m;
    memset(&m, 0xCC, sizeof(m));          /* padding sucio a proposito */
    for (int i = 0; i < 16; i++) m.uuid[i] = (uint8_t)(0xA0 + i);
    m.ip          = 0xC0A8012Au;          /* 192.168.1.42 en orden de host */
    m.port        = 27601;
    m.slot        = 42;
    m.state       = DSM_NODE_SUSPECT;
    m.incarnation = 0x0000000100000002ULL;
    m.mem_pages   = 65536;

    CHECK(sizeof(membership_wire_t) == 40);
    CHECK(DSM_MEMBERSHIP_WIRE_LEN == 36);

    uint8_t mb[DSM_MEMBERSHIP_WIRE_LEN];
    CHECK(dsm_pack_membership(&m, mb) == DSM_MEMBERSHIP_WIRE_LEN);
    CHECK(memcmp(mb, m.uuid, 16) == 0);
    CHECK(mb[16] == 0xC0 && mb[17] == 0xA8 && mb[18] == 0x01 && mb[19] == 0x2A);
    CHECK(mb[20] == 0x6B && mb[21] == 0xD1);          /* 27601 = 0x6BD1 */
    CHECK(mb[22] == 42);
    CHECK(mb[23] == DSM_NODE_SUSPECT);
    CHECK(mb[24] == 0x00 && mb[27] == 0x01);          /* incarnation alto */
    CHECK(mb[28] == 0x00 && mb[31] == 0x02);          /* incarnation bajo */
    CHECK(mb[32] == 0x00 && mb[33] == 0x01 && mb[35] == 0x00);

    membership_wire_t mr;
    memset(&mr, 0x5A, sizeof(mr));
    dsm_unpack_membership(mb, &mr);
    CHECK(memcmp(mr.uuid, m.uuid, 16) == 0);
    CHECK(mr.ip == m.ip && mr.port == m.port && mr.slot == m.slot);
    CHECK(mr.state == m.state && mr.incarnation == m.incarnation);
    CHECK(mr.mem_pages == m.mem_pages);

    /* alloc_rec_t: reserva de 12 paginas del PID 4242 en el slot 3. */
    alloc_rec_t a;
    a.alloc_id   = 0x0000000Bu;
    a.first_page = 8192;
    a.npages     = 12;
    a.slot       = 3;
    a.pid        = 4242;
    a.reserved   = 0;

    uint8_t ab[DSM_ALLOC_REC_LEN];
    CHECK(dsm_pack_alloc_rec(&a, ab) == DSM_ALLOC_REC_LEN);
    static const uint8_t alloc_expected[24] = {
        0x00,0x00,0x00,0x0B,  0x00,0x00,0x20,0x00,
        0x00,0x00,0x00,0x0C,  0x00,0x00,0x00,0x03,
        0x00,0x00,0x10,0x92,  0x00,0x00,0x00,0x00
    };
    CHECK(memcmp(ab, alloc_expected, sizeof(alloc_expected)) == 0);

    alloc_rec_t ar;
    dsm_unpack_alloc_rec(ab, &ar);
    CHECK(memcmp(&ar, &a, sizeof(a)) == 0);

    /* dir_entry_t: pagina 777 en M, owner 5, copyset con los slots
     * 0, 3, 7 y MAX_NODES-1, y contadores de espera reales. */
    dir_entry_t d;
    memset(&d, 0, sizeof(d));
    d.page_id   = 777;
    d.owner     = 5;
    d.version   = 0x01020304u;
    d.tseq      = 0x0A0B0C0Du;
    d.frame     = 4096;
    d.stripe_id = 63;
    d.wait_head = 0;
    d.wait_tail = 2;
    d.txn       = 0xFFFFFFFFu;
    d.state     = DSM_DIR_MODIFIED;
    d.busy      = 1;
    d.reserved  = 0xABCD;
    d.copyset[0 / 8] |= (uint8_t)(1u << (0 % 8));
    d.copyset[3 / 8] |= (uint8_t)(1u << (3 % 8));
    d.copyset[7 / 8] |= (uint8_t)(1u << (7 % 8));
    d.copyset[(MAX_NODES - 1) / 8] |= (uint8_t)(1u << ((MAX_NODES - 1) % 8));

    CHECK(sizeof(dir_entry_t) == 72);
    uint8_t db[DSM_DIR_ENTRY_LEN];
    CHECK(dsm_pack_dir_entry(&d, db) == DSM_DIR_ENTRY_LEN);
    CHECK(db[0] == 0x00 && db[1] == 0x00 && db[2] == 0x03 && db[3] == 0x09);
    CHECK(db[16] == 0x00 && db[18] == 0x10 && db[19] == 0x00);   /* frame */
    CHECK(db[32] == 0xFF && db[35] == 0xFF);                     /* txn */
    CHECK(db[36] == DSM_DIR_MODIFIED && db[37] == 1);
    CHECK(db[38] == 0xAB && db[39] == 0xCD);
    CHECK(db[40] == 0x89);                                       /* bits 0,3,7 */
    CHECK(db[40 + (MAX_NODES - 1) / 8] == 0x80);                 /* bit 255 */

    dir_entry_t dr;
    memset(&dr, 0xEE, sizeof(dr));
    dsm_unpack_dir_entry(db, &dr);
    CHECK(memcmp(&dr, &d, sizeof(d)) == 0);

    /* welcome_t: 5 nodos, 3 reservas y UUID de origen distinguible. */
    static welcome_t w, wr;
    memset(&w, 0, sizeof(w));
    w.epoch = 0x1122334455667788ULL;
    for (int i = 0; i < 16; i++) w.origin_uuid[i] = (uint8_t)(0x30 + i);
    w.count = 5;
    for (uint32_t i = 0; i < w.count; i++) {
        for (int b = 0; b < 16; b++) {
            w.members[i].uuid[b] = (uint8_t)(i * 16 + (uint32_t)b);
        }
        w.members[i].ip          = 0x0A000001u + i;
        w.members[i].port        = (uint16_t)(27600 + i);
        w.members[i].slot        = (uint8_t)i;
        w.members[i].state       = (uint8_t)(DSM_NODE_ALIVE + (i % 2));
        w.members[i].incarnation = 100u + i;
        w.members[i].mem_pages   = 65536u + 100u * i;
    }
    w.alloc_count = 3;
    for (uint32_t i = 0; i < w.alloc_count; i++) {
        w.allocs[i].alloc_id   = 1000u + i;
        w.allocs[i].first_page = i * 64u;
        w.allocs[i].npages     = 16u + i;
        w.allocs[i].slot       = i;
        w.allocs[i].pid        = 3000u + i;
        w.allocs[i].reserved   = 0;
    }

    static uint8_t wb[20000];
    size_t need = 28 + 5 * 36 + 4 + 3 * 24;
    CHECK(dsm_pack_welcome(&w, wb, sizeof(wb)) == need);
    CHECK(need == 284);

    msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic         = DSM_MAGIC;
    hdr.cluster_id    = 0x534F3031u;
    hdr.proto_version = DSM_PROTO_VERSION;
    hdr.type          = MSG_WELCOME;
    hdr.payload_len   = (uint32_t)need;

    uint8_t hraw[DSM_HDR_LEN];
    dsm_wire_finish(&hdr, hraw, wb, hdr.payload_len);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, hdr.payload_len, 4096) == true);
    REQUIRE(dsm_wire_validate(&hdr, hraw, wb, 0x534F3031u) == DSM_OK);
    CHECK(dsm_wire_unpack(hraw, &hdr) == DSM_OK);
    REQUIRE(dsm_unpack_welcome(wb, need, &wr) == DSM_OK);

    CHECK(wr.epoch == w.epoch);
    CHECK(memcmp(wr.origin_uuid, w.origin_uuid, 16) == 0);
    CHECK(wr.count == 5 && wr.alloc_count == 3);
    for (uint32_t i = 0; i < 5; i++) {
        CHECK(memcmp(wr.members[i].uuid, w.members[i].uuid, 16) == 0);
        CHECK(wr.members[i].ip == w.members[i].ip);
        CHECK(wr.members[i].port == w.members[i].port);
        CHECK(wr.members[i].slot == w.members[i].slot);
        CHECK(wr.members[i].state == w.members[i].state);
        CHECK(wr.members[i].incarnation == w.members[i].incarnation);
        CHECK(wr.members[i].mem_pages == w.members[i].mem_pages);
    }
    for (uint32_t i = 0; i < 3; i++) {
        CHECK(wr.allocs[i].alloc_id == w.allocs[i].alloc_id);
        CHECK(wr.allocs[i].first_page == w.allocs[i].first_page);
        CHECK(wr.allocs[i].npages == w.allocs[i].npages);
        CHECK(wr.allocs[i].slot == w.allocs[i].slot);
        CHECK(wr.allocs[i].pid == w.allocs[i].pid);
    }

    /* Layout del cable del WELCOME: epoch, UUID, count y luego 36 B/nodo. */
    CHECK(wb[0] == 0x11 && wb[7] == 0x88);
    CHECK(memcmp(wb + 8, w.origin_uuid, 16) == 0);
    CHECK(wb[24] == 0 && wb[27] == 5);
    CHECK(wb[28 + 22] == 0);                       /* slot del primer miembro */
    CHECK(wb[28 + 36 + 22] == 1);                  /* slot del segundo */
    CHECK(wb[28 + 5 * 36 + 3] == 3);               /* alloc_count big-endian */

    /* Truncar un byte por cada frontera siempre debe fallar. */
    CHECK(dsm_unpack_welcome(wb, need - 1, &wr) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(wb, 28 + 5 * 36 + 3, &wr) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(wb, 28 + 5 * 36 + 2, &wr) == DSM_ERR_PROTO);
    CHECK(dsm_unpack_welcome(wb, 31, &wr) == DSM_ERR_PROTO);

    return 0;
}

/* ----------------------------------------------------------------------------
 * 10. wire_payload_ok(): matriz completa por tipo
 * ------------------------------------------------------------------------- */

static int test_rig_payload_ok_por_tipo(void)
{
    const uint32_t ps  = 4096;
    const uint32_t max = DSM_MAX_PAYLOAD;

    /* Mensajes sin carga: solo 0 es legal. */
    static const uint16_t vacios[] = {
        MSG_JOIN_ACK, MSG_PAGE_REQUEST, MSG_GRANT, MSG_INVALIDATE,
        MSG_DOWNGRADE, MSG_WRITEBACK_ACK, MSG_LOCK_REQ, MSG_LOCK_GRANT,
        MSG_LOCK_REL, MSG_NACK
    };
    for (size_t i = 0; i < sizeof(vacios) / sizeof(vacios[0]); i++) {
        CHECK(wire_payload_ok(vacios[i], 0, 0, ps) == true);
        CHECK(wire_payload_ok(vacios[i], 0, 1, ps) == false);
        CHECK(wire_payload_ok(vacios[i], 0, 36, ps) == false);
        CHECK(wire_payload_ok(vacios[i], 0, max, ps) == false);
        CHECK(wire_payload_ok(vacios[i], 0, max + 1, ps) == false);
    }

    /* HELLO: 20 exacto (uuid[16] + raw_hash[4]). */
    CHECK(wire_payload_ok(MSG_HELLO, 0, 20, ps) == true);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 0, ps) == false);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 16, ps) == false);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 19, ps) == false);
    CHECK(wire_payload_ok(MSG_HELLO, 0, 21, ps) == false);

    /* JOIN: 36 exacto (membership_wire_t serializado). */
    CHECK(wire_payload_ok(MSG_JOIN, 0, 36, ps) == true);
    CHECK(wire_payload_ok(MSG_JOIN, 0, 35, ps) == false);
    CHECK(wire_payload_ok(MSG_JOIN, 0, 37, ps) == false);
    CHECK(wire_payload_ok(MSG_JOIN, 0, 40, ps) == false);   /* 40 = struct en RAM */

    /* HEARTBEAT: 12 exacto (incarnation[8] + free_pages[4]). */
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 12, ps) == true);
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 0, ps) == false);
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 11, ps) == false);
    CHECK(wire_payload_ok(MSG_HEARTBEAT, 0, 13, ps) == false);

    /* NODE_DOWN: 0 (implementacion actual) o 28 (slot + incarnation + uuid). */
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 28, ps) == true);
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 27, ps) == false);
    CHECK(wire_payload_ok(MSG_NODE_DOWN, 0, 12, ps) == false);

    /* LEAVE: 0 (salida sin carga) o 28 (forma extendida aceptada). */
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 28, ps) == true);
    CHECK(wire_payload_ok(MSG_LEAVE, 0, 27, ps) == false);

    /* MEMBERSHIP_REQ: 0 o 28. */
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_REQ, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_REQ, 0, 28, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_REQ, 0, 13, ps) == false);

    /* MEMBERSHIP_RESP: 12 + 36*n, con n >= 0. */
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 12, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 12 + 36, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 12 + 2 * 36, ps) == true);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 11, ps) == false);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 13, ps) == false);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, 12 + 35, ps) == false);
    CHECK(wire_payload_ok(MSG_MEMBERSHIP_RESP, 0, max + 1, ps) == false);

    /* WELCOME: min 32 (count=0, alloc_count=0) y cota global. */
    CHECK(wire_payload_ok(MSG_WELCOME, 0, 32, ps) == true);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, 31, ps) == false);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, 284, ps) == true);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, max, ps) == true);
    CHECK(wire_payload_ok(MSG_WELCOME, 0, max + 1, ps) == false);

    /* PAGE_DATA / WRITEBACK: exactamente page_size, que se conoce en runtime. */
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4096, ps) == true);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4095, ps) == false);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4097, ps) == false);
    CHECK(wire_payload_ok(MSG_WRITEBACK, 0, 4096, ps) == true);
    CHECK(wire_payload_ok(MSG_WRITEBACK, 0, 4095, ps) == false);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 8192, 8192) == true);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 4096, 8192) == false);
    CHECK(wire_payload_ok(MSG_PAGE_DATA, 0, 0, 0) == false); /* page_size 0 no existe */

    /* ACK con HAS_DATA: la carga es una pagina; sin flag, cero bytes. */
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, DSM_FLAG_HAS_DATA, ps, ps) == true);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, DSM_FLAG_HAS_DATA, 0, ps) == false);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, DSM_FLAG_HAS_DATA, ps - 1, ps) == false);
    CHECK(wire_payload_ok(MSG_INVALIDATE_ACK, 0, ps, ps) == false);
    CHECK(wire_payload_ok(MSG_DOWNGRADE_ACK, 0, 0, ps) == true);
    CHECK(wire_payload_ok(MSG_DOWNGRADE_ACK, DSM_FLAG_HAS_DATA, ps, ps) == true);
    CHECK(wire_payload_ok(MSG_DOWNGRADE_ACK, DSM_FLAG_HAS_DATA, ps + 1, ps) == false);

    /* Barreras. */
    CHECK(wire_payload_ok(MSG_BARRIER_ARRIVE, 0, 8, ps) == true);
    CHECK(wire_payload_ok(MSG_BARRIER_ARRIVE, 0, 7, ps) == false);
    CHECK(wire_payload_ok(MSG_BARRIER_ARRIVE, 0, 12, ps) == false);
    CHECK(wire_payload_ok(MSG_BARRIER_GO, 0, 4, ps) == true);
    CHECK(wire_payload_ok(MSG_BARRIER_GO, 0, 0, ps) == false);
    CHECK(wire_payload_ok(MSG_BARRIER_GO, 0, 5, ps) == false);

    /* Tipos solo acotados por DSM_MAX_PAYLOAD (P2/variables). */
    static const uint16_t variables[] = {
        MSG_PAGE_MOVE, MSG_PAGE_MOVE_ACK, MSG_PARITY_DELTA, MSG_PARITY_ACK,
        MSG_STRIPE_PUT, MSG_STRIPE_PUT_ACK, MSG_STRIPE_READ, MSG_STRIPE_DATA,
        MSG_STATE_REPORT, MSG_DIR_DONE, MSG_QUERY_REQ, MSG_QUERY_RESP
    };
    for (size_t i = 0; i < sizeof(variables) / sizeof(variables[0]); i++) {
        CHECK(wire_payload_ok(variables[i], 0, 0, ps) == true);
        CHECK(wire_payload_ok(variables[i], 0, max, ps) == true);
        CHECK(wire_payload_ok(variables[i], 0, max + 1, ps) == false);
    }

    /* Tipo desconocido: nunca por encima del maximo global. */
    CHECK(wire_payload_ok(0x0000, 0, 0, ps) == true);
    CHECK(wire_payload_ok(0x00FF, 0, max, ps) == true);
    CHECK(wire_payload_ok(0x00FF, 0, max + 1, ps) == false);
    CHECK(wire_payload_ok(0xFFFF, 0, max + 1, ps) == false);

    return 0;
}

/* ------------------------------------------------------------------------- */

int main(void)
{
    printf("=== Test Suite: Wire Protocol ===\n");

    RUN_TEST(test_header_layout);
    RUN_TEST(test_header_endianness);
    RUN_TEST(test_header_rejects_bad_magic_and_version);
    RUN_TEST(test_crc32_vectors);
    RUN_TEST(test_crc32_header_single_bit_sweep);
    RUN_TEST(test_crc32_payload_single_bit_sweep);
    RUN_TEST(test_crc32_payload_truncation_and_extension);
    RUN_TEST(test_membership_pack);
    RUN_TEST(test_membership_matrix_roundtrip);
    RUN_TEST(test_welcome_matrix);
    RUN_TEST(test_welcome_buffer_bounds);
    RUN_TEST(test_welcome_unpack_robustness);
    RUN_TEST(test_wire_payload_ok_matrix);
    RUN_TEST(test_message_classification);
    RUN_TEST(test_alloc_and_dir_layout);
    RUN_TEST(test_rig_layout_bytes_y_epoch);
    RUN_TEST(test_rig_crc32_frame_4096_bitflip);
    RUN_TEST(test_rig_cargas_fijas_realistas);
    RUN_TEST(test_rig_payload_ok_por_tipo);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
