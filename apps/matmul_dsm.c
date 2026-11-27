#include "dsm.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define N_DEFECTO      256u
#define HILOS_DEFECTO  4u
#define N_MAXIMO       2048u
#define HILOS_MAXIMOS  256u
#define TOLERANCIA     1e-6

typedef struct trabajo {
    const double *a;
    const double *b;
    double       *c;
    uint32_t      n;
    uint32_t      primera_fila;
    uint32_t      ultima_fila;
    int           fallo;
} trabajo_t;

static uint64_t instante_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Patrones deterministas que caben en un entero y se reproducen igual en la
 * vista DSM y en la referencia local. */
static double valor_a(uint32_t i, uint32_t j)
{
    return (double)((i * 31u + j * 17u) % 100u);
}

static double valor_b(uint32_t i, uint32_t j)
{
    return (double)((i * 13u + j * 7u) % 100u);
}

static void llenar_a(double *m, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t j = 0; j < n; j++) {
            m[(size_t)i * n + j] = valor_a(i, j);
        }
    }
}

static void llenar_b(double *m, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        for (uint32_t j = 0; j < n; j++) {
            m[(size_t)i * n + j] = valor_b(i, j);
        }
    }
}

/* Recorre k en el bucle externo para leer B por filas completas y mantener la
 * fila de salida en una sola página de la vista. */
static void multiplicar_fila(const double *a, const double *b, double *salida,
                             uint32_t n)
{
    for (uint32_t j = 0; j < n; j++) {
        salida[j] = 0.0;
    }
    for (uint32_t k = 0; k < n; k++) {
        double factor = a[k];
        const double *fila_b = b + (size_t)k * n;
        for (uint32_t j = 0; j < n; j++) {
            salida[j] += factor * fila_b[j];
        }
    }
}

static void *hilo_multiplicacion(void *arg)
{
    trabajo_t *t = (trabajo_t *)arg;

    if (dsm_thread_attach() != DSM_OK) {
        t->fallo = 1;
        return NULL;
    }

    for (uint32_t i = t->primera_fila; i < t->ultima_fila; i++) {
        multiplicar_fila(t->a + (size_t)i * t->n, t->b,
                         t->c + (size_t)i * t->n, t->n);
    }

    dsm_thread_detach();
    return NULL;
}

/* Reparte las filas en bloques contiguos, uno por hilo. */
static void repartir_filas(trabajo_t *trabajos, uint32_t hilos, uint32_t n)
{
    uint32_t base = n / hilos;
    uint32_t resto = n % hilos;
    uint32_t inicio = 0;

    for (uint32_t i = 0; i < hilos; i++) {
        uint32_t cantidad = base + (i < resto ? 1u : 0u);
        trabajos[i].primera_fila = inicio;
        trabajos[i].ultima_fila = inicio + cantidad;
        inicio += cantidad;
    }
}

/* Repite la multiplicación en memoria local y compara fila por fila contra la
 * vista DSM, además de acumular los dos checksums en el mismo orden. */
static int comparar_resultado(const double *c_dsm, const double *a_local,
                              const double *b_local, uint32_t n, double *fila,
                              double *suma_dsm, double *suma_ref,
                              double *diferencia_maxima, uint64_t *distintos)
{
    double acumulado_dsm = 0.0;
    double acumulado_ref = 0.0;
    double maxima = 0.0;
    uint64_t malos = 0;

    for (uint32_t i = 0; i < n; i++) {
        multiplicar_fila(a_local + (size_t)i * n, b_local, fila, n);
        const double *fila_dsm = c_dsm + (size_t)i * n;

        for (uint32_t j = 0; j < n; j++) {
            double valor_dsm = fila_dsm[j];
            double valor_ref = fila[j];
            double diferencia = valor_dsm - valor_ref;

            if (diferencia < 0.0) diferencia = -diferencia;
            if (diferencia > maxima) maxima = diferencia;
            if (diferencia > TOLERANCIA) malos++;

            acumulado_dsm += valor_dsm;
            acumulado_ref += valor_ref;
        }
    }

    *suma_dsm = acumulado_dsm;
    *suma_ref = acumulado_ref;
    *diferencia_maxima = maxima;
    *distintos = malos;

    double diferencia_suma = acumulado_dsm - acumulado_ref;
    if (diferencia_suma < 0.0) diferencia_suma = -diferencia_suma;
    double escala = (acumulado_ref < 0.0) ? -acumulado_ref : acumulado_ref;
    return malos == 0 && diferencia_suma <= TOLERANCIA * (1.0 + escala);
}

static bool parsear_entero(const char *texto, uint32_t maximo, uint32_t *salida)
{
    if (!texto || *texto == '\0') return false;
    errno = 0;
    char *fin = NULL;
    unsigned long valor = strtoul(texto, &fin, 10);
    if (errno != 0 || !fin || *fin != '\0' || valor == 0 || valor > maximo) {
        return false;
    }
    *salida = (uint32_t)valor;
    return true;
}

static void uso(const char *programa)
{
    fprintf(stderr,
            "Uso: %s [N] [--conf <archivo>] [--hilos H]\n"
            "\n"
            "  N              lado de las matrices cuadradas (por defecto %u,"
            " máximo %u)\n"
            "  --conf         archivo de configuración; por defecto dsm.conf\n"
            "  --hilos        hilos que calculan filas de C (por defecto %u)\n"
            "  -h, --help     muestra esta ayuda\n",
            programa, N_DEFECTO, N_MAXIMO, HILOS_DEFECTO);
}

int main(int argc, char **argv)
{
    const char *conf = NULL;
    uint32_t n = N_DEFECTO;
    uint32_t hilos = HILOS_DEFECTO;
    bool hay_n = false;
    int rc = 1;

    setvbuf(stdout, NULL, _IOLBF, 0);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--conf") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "%s: falta el archivo de --conf\n", argv[0]);
                return 2;
            }
            conf = argv[++i];
        } else if (strcmp(argv[i], "--hilos") == 0) {
            if (i + 1 >= argc || !parsear_entero(argv[++i], HILOS_MAXIMOS, &hilos)) {
                fprintf(stderr, "%s: --hilos requiere un entero entre 1 y %u\n",
                        argv[0], HILOS_MAXIMOS);
                return 2;
            }
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            uso(argv[0]);
            return 0;
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "%s: opción desconocida '%s'\n", argv[0], argv[i]);
            uso(argv[0]);
            return 2;
        } else if (!hay_n) {
            if (!parsear_entero(argv[i], N_MAXIMO, &n)) {
                fprintf(stderr, "%s: N debe ser un entero entre 1 y %u\n",
                        argv[0], N_MAXIMO);
                return 2;
            }
            hay_n = true;
        } else {
            fprintf(stderr, "%s: sobra el argumento '%s'\n", argv[0], argv[i]);
            uso(argv[0]);
            return 2;
        }
    }

    if (hilos > n) hilos = n;

    uint64_t n64 = (uint64_t)n;
    uint64_t bytes = n64 * n64 * sizeof(double);
    void *mem_a = NULL;
    void *mem_b = NULL;
    void *mem_c = NULL;
    pthread_t *ids = NULL;
    trabajo_t *trabajos = NULL;
    double *a_local = NULL;
    double *b_local = NULL;
    double *fila = NULL;
    uint32_t lanzados = 0;

    /* Las aplicaciones se unen como clientes: usan la memoria del clúster sin
     * aportar nodos ni custodiar páginas. */
    setenv("DSM_NODE_MODE", "client", 1);

    dsm_status_t st = dsm_init(conf, 0);
    if (st != DSM_OK) {
        fprintf(stderr, "matmul_dsm: dsm_init falló: %s\n", dsm_strerror(st));
        if (st == DSM_ERR_NO_SPACE) {
            fprintf(stderr, "matmul_dsm: inicie al menos un nodo del clúster "
                            "antes de ejecutar la aplicación\n");
        }
        return 1;
    }

    st = dsm_thread_attach();
    if (st != DSM_OK) {
        fprintf(stderr, "matmul_dsm: dsm_thread_attach falló: %s\n",
                dsm_strerror(st));
        goto fin_dsm;
    }

    uint32_t nodos = 0;
    uint64_t total = 0;
    (void)dsm_node_count(&nodos);
    (void)dsm_global_size(&total);

    printf("Multiplicación de matrices sobre memoria DSM\n");
    printf("Dimensiones: %u x %u, elemento double\n", n, n);
    printf("Hilos: %u\n", hilos);
    printf("Matriz: %.2f MiB; A, B y C: %.2f MiB\n",
           (double)bytes / (1024.0 * 1024.0),
           3.0 * (double)bytes / (1024.0 * 1024.0));
    printf("Nodos activos: %u; memoria del clúster: %.2f MiB\n", nodos,
           (double)total / (1024.0 * 1024.0));

    if (total != 0 && 3u * bytes > total) {
        fprintf(stderr, "matmul_dsm: A, B y C necesitan %.2f MiB y el clúster "
                        "ofrece %.2f MiB; reduzca N o aumente NODE_MEM\n",
                3.0 * (double)bytes / (1024.0 * 1024.0),
                (double)total / (1024.0 * 1024.0));
        goto fin_dsm;
    }

    st = dsm_alloc(bytes, &mem_a);
    if (st != DSM_OK) {
        fprintf(stderr, "matmul_dsm: dsm_alloc de A falló: %s\n", dsm_strerror(st));
        goto fin_dsm;
    }
    st = dsm_alloc(bytes, &mem_b);
    if (st != DSM_OK) {
        fprintf(stderr, "matmul_dsm: dsm_alloc de B falló: %s\n", dsm_strerror(st));
        goto fin_a;
    }
    st = dsm_alloc(bytes, &mem_c);
    if (st != DSM_OK) {
        fprintf(stderr, "matmul_dsm: dsm_alloc de C falló: %s\n", dsm_strerror(st));
        goto fin_b;
    }

    llenar_a((double *)mem_a, n);
    llenar_b((double *)mem_b, n);

    ids = calloc(hilos, sizeof(*ids));
    trabajos = calloc(hilos, sizeof(*trabajos));
    if (!ids || !trabajos) {
        fprintf(stderr, "matmul_dsm: sin memoria local para %u hilos\n", hilos);
        goto fin_c;
    }

    repartir_filas(trabajos, hilos, n);
    for (uint32_t i = 0; i < hilos; i++) {
        trabajos[i].a = (const double *)mem_a;
        trabajos[i].b = (const double *)mem_b;
        trabajos[i].c = (double *)mem_c;
        trabajos[i].n = n;
        trabajos[i].fallo = 0;
    }

    uint64_t inicio = instante_ns();
    for (uint32_t i = 0; i < hilos; i++) {
        if (pthread_create(&ids[i], NULL, hilo_multiplicacion, &trabajos[i]) != 0) {
            fprintf(stderr, "matmul_dsm: no se pudo crear el hilo %u\n", i);
            break;
        }
        lanzados++;
    }

    int fallo = (lanzados == hilos) ? 0 : 1;
    for (uint32_t i = 0; i < lanzados; i++) {
        pthread_join(ids[i], NULL);
        if (trabajos[i].fallo) fallo = 1;
    }
    uint64_t fin = instante_ns();

    if (fallo) {
        fprintf(stderr, "matmul_dsm: algún hilo no pudo calcular su bloque de filas\n");
        goto fin_c;
    }

    uint64_t inicio_sync = instante_ns();
    st = dsm_sync();
    uint64_t fin_sync = instante_ns();

    if (st != DSM_OK) {
        printf("dsm_sync: error (%s)\n", dsm_strerror(st));
        goto fin_c;
    }
    printf("dsm_sync: OK (%.3f s)\n",
           (double)(fin_sync - inicio_sync) / 1e9);

    a_local = malloc((size_t)bytes);
    b_local = malloc((size_t)bytes);
    fila = malloc((size_t)n * sizeof(double));
    if (!a_local || !b_local || !fila) {
        fprintf(stderr, "matmul_dsm: sin memoria local para la referencia "
                        "secuencial de %.2f MiB\n",
                2.0 * (double)bytes / (1024.0 * 1024.0));
        goto fin_c;
    }

    llenar_a(a_local, n);
    llenar_b(b_local, n);

    uint64_t inicio_ref = instante_ns();
    double suma_dsm = 0.0;
    double suma_ref = 0.0;
    double diferencia_maxima = 0.0;
    uint64_t distintos = 0;
    int coincide = comparar_resultado((const double *)mem_c, a_local, b_local, n,
                                      fila, &suma_dsm, &suma_ref,
                                      &diferencia_maxima, &distintos);
    uint64_t fin_ref = instante_ns();

    printf("Tiempo de cálculo: %.3f s\n", (double)(fin - inicio) / 1e9);
    printf("Tiempo de referencia secuencial: %.3f s\n",
           (double)(fin_ref - inicio_ref) / 1e9);
    printf("Checksum DSM: %.6f\n", suma_dsm);
    printf("Checksum referencia: %.6f\n", suma_ref);
    printf("Diferencia máxima: %.6e\n", diferencia_maxima);
    printf("Elementos distintos: %llu de %llu\n",
           (unsigned long long)distintos,
           (unsigned long long)(n64 * n64));
    printf("VERIFICACION: %s\n", coincide ? "OK" : "FALLO");

    rc = coincide ? 0 : 1;

fin_c:
    if (mem_c != NULL) {
        dsm_status_t fr = dsm_free(mem_c, bytes);
        if (fr != DSM_OK) {
            fprintf(stderr, "matmul_dsm: dsm_free de C falló: %s\n",
                    dsm_strerror(fr));
            rc = 1;
        }
    }
fin_b:
    if (mem_b != NULL) {
        dsm_status_t fr = dsm_free(mem_b, bytes);
        if (fr != DSM_OK) {
            fprintf(stderr, "matmul_dsm: dsm_free de B falló: %s\n",
                    dsm_strerror(fr));
            rc = 1;
        }
    }
fin_a:
    if (mem_a != NULL) {
        dsm_status_t fr = dsm_free(mem_a, bytes);
        if (fr != DSM_OK) {
            fprintf(stderr, "matmul_dsm: dsm_free de A falló: %s\n",
                    dsm_strerror(fr));
            rc = 1;
        }
    }
fin_dsm:
    free(a_local);
    free(b_local);
    free(fila);
    free(ids);
    free(trabajos);
    dsm_thread_detach();
    dsm_finalize();
    return rc;
}
