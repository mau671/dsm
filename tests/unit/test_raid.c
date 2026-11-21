/* ==============================================================================
 * test_raid.c — Suite de pruebas de la reconstruccion XOR de RAID5 (src/raid.c).
 *
 * Cobertura: identidad de reconstruccion con perdida de un dato, contradiccion
 * con la reconstruccion que omite la paridad, reconstruccion de la paridad y
 * aplicacion byte a byte sobre una pagina multibyte.
 * ============================================================================== */

#include "test.h"
#include "raid.h"

static int test_rebuild_dato_con_paridad(void)
{
    /* K=2: D1=0xAA, D2=0x55 y P=D1^D2=0xFF. Cae el home de D1. */
    uint8_t p = 0xFFu;
    uint8_t d2 = 0x55u;
    const uint8_t *parts[2] = { &p, &d2 };
    uint8_t out = 0u;

    raid_xor_rebuild(parts, 2u, 1u, &out);

    CHECK(out == 0xAAu);
    CHECK(out != 0x55u);
    return 0;
}

static int test_rebuild_solo_dato_superviviente(void)
{
    /* Sin la paridad el resultado es el dato superviviente, no el perdido. */
    uint8_t d2 = 0x55u;
    const uint8_t *parts[1] = { &d2 };
    uint8_t out = 0xFFu;

    raid_xor_rebuild(parts, 1u, 1u, &out);

    CHECK(out == 0x55u);
    CHECK(out != 0xAAu);
    return 0;
}

static int test_rebuild_paridad_con_datos(void)
{
    uint8_t d1 = 0xAAu;
    uint8_t d2 = 0x55u;
    const uint8_t *parts[2] = { &d1, &d2 };
    uint8_t out = 0u;

    raid_xor_rebuild(parts, 2u, 1u, &out);

    CHECK(out == 0xFFu);
    return 0;
}

static int test_rebuild_pagina_multibyte(void)
{
    /* K=3 con tres bytes por pieza: D1 se recupera de P, D2 y D3. */
    static const uint8_t d1[3] = { 0xAAu, 0x00u, 0xFFu };
    static const uint8_t d2[3] = { 0x55u, 0x12u, 0x0Fu };
    static const uint8_t d3[3] = { 0x0Fu, 0x34u, 0xF0u };
    static uint8_t p[3];
    static uint8_t out[3];

    for (uint32_t b = 0u; b < 3u; b++) {
        p[b] = (uint8_t)(d1[b] ^ d2[b] ^ d3[b]);
    }
    const uint8_t *parts[4] = { p, d2, d3 };

    raid_xor_rebuild(parts, 3u, 3u, out);

    for (uint32_t b = 0u; b < 3u; b++) {
        CHECK(out[b] == d1[b]);
    }
    return 0;
}

int main(void)
{
    printf("=== Test Suite: RAID rebuild ===\n");

    RUN_TEST(test_rebuild_dato_con_paridad);
    RUN_TEST(test_rebuild_solo_dato_superviviente);
    RUN_TEST(test_rebuild_paridad_con_datos);
    RUN_TEST(test_rebuild_pagina_multibyte);

    printf("%d pruebas ejecutadas, %d fallos\n", g_tests, g_fails);
    return g_fails ? 1 : 0;
}
