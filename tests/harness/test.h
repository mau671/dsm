#ifndef DSM_TEST_H
#define DSM_TEST_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_tests = 0;
static int g_fails = 0;

#define RUN_TEST(fn) do {                                   \
    g_tests++;                                              \
    printf("-- %s\n", #fn);                                 \
    int r_ = fn();                                          \
    if (r_ != 0) { g_fails++; printf("   FALLO\n"); }       \
    else { printf("   OK\n"); }                             \
} while (0)

#define CHECK(cond) do {                                    \
    if (!(cond)) {                                          \
        printf("   %s:%d CHECK(%s) FALLO\n", __FILE__, __LINE__, #cond); \
        g_fails++;                                          \
    }                                                       \
} while (0)

#define REQUIRE(cond) do {                                  \
    if (!(cond)) {                                          \
        printf("   %s:%d REQUIRE(%s) FALLO\n", __FILE__, __LINE__, #cond); \
        return -1;                                          \
    }                                                       \
} while (0)

#endif /* DSM_TEST_H */
