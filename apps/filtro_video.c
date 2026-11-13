/*
 * Filtro de blanco y negro sobre video.
 *
 * Aplicacion de prueba del sistema DSM: lee cuadros RGB24 desde un archivo,
 * calcula la luminancia de cada pixel y escribe el resultado en escala de
 * grises. La entrada y la salida viven en la memoria compartida distribuida,
 * de modo que los cuadros se cargan una sola vez y cualquier nodo del cluster
 * puede procesar cualquier rango de cuadros.
 *
 * Uso:
 *   filtro_video <config> <entrada> <salida> [hilos]
 *   filtro_video --generar <archivo> <ancho> <alto> <cuadros>
 *
 * Formato del archivo: cabecera de tres enteros de 32 bits (ancho, alto,
 * cuadros) seguida de ancho*alto*cuadros pixeles RGB24. La salida tiene la
 * misma cabecera y un byte de luminancia por pixel.
 */
#include "dsm.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CABECERA_BYTES   12u
#define CANALES_RGB      3u
#define MAX_HILOS        256u
#define MAX_LADO         65536u
#define MAX_PIXELES      (1ull << 32)
#define MAX_CUADROS      16777216u
#define MAX_BYTES        (1ull << 34)

/* Tamano del video que se fabrica cuando el archivo de entrada no existe. */
#define ANCHO_SINTETICO   640u
#define ALTO_SINTETICO    480u
#define CUADROS_SINTETICOS 30u

typedef struct {
    uint32_t ancho;
    uint32_t alto;
    uint32_t cuadros;
} video_info_t;

typedef struct {
    const uint8_t *entrada;
    uint8_t       *salida;
    video_info_t   info;
    size_t         bytes_cuadro;
    uint32_t       primer_cuadro;
    uint32_t       ultimo_cuadro;
    int            fallo;
} trabajo_t;

static void info_desde_crudo(video_info_t *info, const uint32_t crudo[3])
{
    info->ancho   = crudo[0];
    info->alto    = crudo[1];
    info->cuadros = crudo[2];
}

/* Valida los limites del video para que las multiplicaciones no desborden. */
static bool info_valida(const video_info_t *info)
{
    if (info->ancho == 0 || info->alto == 0 || info->cuadros == 0) {
        return false;
    }
    if ((uint64_t)info->ancho * info->alto > MAX_PIXELES) {
        return false;
    }
    if (info->cuadros > MAX_CUADROS) {
        return false;
    }
    uint64_t bytes = (uint64_t)info->ancho * info->alto * CANALES_RGB;
    return bytes > 0 && bytes <= MAX_BYTES &&
           (uint64_t)info->cuadros * bytes <= MAX_BYTES;
}

static uint64_t bytes_entrada(const video_info_t *info)
{
    return (uint64_t)info->ancho * info->alto * CANALES_RGB * info->cuadros;
}

static uint64_t bytes_salida(const video_info_t *info)
{
    return (uint64_t)info->ancho * info->alto * info->cuadros;
}

static uint64_t instante_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/*
 * Lee exactamente 'n' bytes desde el descriptor, tolerando cortes parciales.
 * La copia se hace en dos pasos: read() directo sobre la vista DSM falla con
 * EFAULT en las paginas que aun no estan presentes, porque el kernel no puede
 * resolver el fallo desde el contexto de la llamada al sistema. Por eso se lee
 * a un buffer intermedio y se copia con memcpy, que si dispara SIGSEGV y deja
 * que el despachador instale la pagina antes de reintentar la instruccion.
 */
static bool leer_completo(int fd, void *dst, size_t n)
{
    uint8_t *p = (uint8_t *)dst;
    size_t restante = n;
    uint8_t buffer[65536];

    while (restante > 0) {
        size_t bloque = restante < sizeof(buffer) ? restante : sizeof(buffer);
        size_t leidos = 0;
        while (leidos < bloque) {
            ssize_t r = read(fd, buffer + leidos, bloque - leidos);
            if (r < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            if (r == 0) return false;
            leidos += (size_t)r;
        }
        memcpy(p, buffer, bloque);
        p += bloque;
        restante -= bloque;
    }
    return true;
}

static bool escribir_completo(int fd, const void *src, size_t n)
{
    const uint8_t *p = (const uint8_t *)src;
    size_t restante = n;
    while (restante > 0) {
        ssize_t w = write(fd, p, restante);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        p += (size_t)w;
        restante -= (size_t)w;
    }
    return true;
}

/*
 * Escribe 'n' bytes en el descriptor copiando desde la vista DSM a traves de
 * un buffer intermedio, por la misma razon que leer_completo: el kernel no
 * puede resolver un fallo de pagina DSM desde dentro de write().
 */
static bool escribir_desde_dsm(int fd, const void *src, size_t n)
{
    const uint8_t *p = (const uint8_t *)src;
    size_t restante = n;
    uint8_t buffer[65536];

    while (restante > 0) {
        size_t bloque = restante < sizeof(buffer) ? restante : sizeof(buffer);
        memcpy(buffer, p, bloque);
        if (!escribir_completo(fd, buffer, bloque)) {
            return false;
        }
        p += bloque;
        restante -= bloque;
    }
    return true;
}

/* Abre el archivo de entrada y valida su cabecera contra el tamano real. */
static int abrir_entrada(const char *ruta, int *fd_out, video_info_t *info)
{
    int fd = open(ruta, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "filtro_video: no se pudo abrir '%s': %s\n",
                ruta, strerror(errno));
        return -1;
    }

    uint32_t crudo[CABECERA_BYTES / sizeof(uint32_t)];
    if (!leer_completo(fd, crudo, CABECERA_BYTES)) {
        fprintf(stderr, "filtro_video: cabecera incompleta en '%s'\n", ruta);
        close(fd);
        return -1;
    }
    info_desde_crudo(info, crudo);

    if (!info_valida(info)) {
        fprintf(stderr, "filtro_video: dimensiones invalidas en '%s' "
                        "(%u x %u, %u cuadros)\n",
                ruta, info->ancho, info->alto, info->cuadros);
        close(fd);
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        fprintf(stderr, "filtro_video: fstat fallo sobre '%s': %s\n",
                ruta, strerror(errno));
        close(fd);
        return -1;
    }
    uint64_t esperado = CABECERA_BYTES + bytes_entrada(info);
    if ((uint64_t)st.st_size != esperado) {
        fprintf(stderr, "filtro_video: '%s' mide %llu bytes y la cabecera "
                        "declara %llu\n",
                ruta, (unsigned long long)st.st_size,
                (unsigned long long)esperado);
        close(fd);
        return -1;
    }

    *fd_out = fd;
    return 0;
}

/* Calcula la luminancia BT.601 de un pixel RGB24. */
static inline uint8_t luminancia(uint8_t r, uint8_t g, uint8_t b)
{
    uint32_t suma = 299u * r + 587u * g + 114u * b;
    return (uint8_t)((suma + 500u) / 1000u);
}

/* Convierte los cuadros asignados al hilo, un pixel de entrada por byte de salida. */
static void *hilo_filtro(void *arg)
{
    trabajo_t *t = (trabajo_t *)arg;

    if (dsm_thread_attach() != DSM_OK) {
        t->fallo = 1;
        return NULL;
    }

    size_t pixeles = (size_t)t->info.ancho * t->info.alto;
    for (uint32_t c = t->primer_cuadro; c < t->ultimo_cuadro; c++) {
        const uint8_t *src = t->entrada + (size_t)c * t->bytes_cuadro;
        uint8_t *dst = t->salida + (size_t)c * pixeles;
        for (size_t p = 0; p < pixeles; p++) {
            const uint8_t *px = src + p * CANALES_RGB;
            dst[p] = luminancia(px[0], px[1], px[2]);
        }
    }

    dsm_thread_detach();
    return NULL;
}

/* Reparte los cuadros en bloques contiguos, uno por hilo. */
static void repartir_cuadros(trabajo_t *trabajos, uint32_t hilos,
                             uint32_t cuadros)
{
    uint32_t base = cuadros / hilos;
    uint32_t resto = cuadros % hilos;
    uint32_t inicio = 0;

    for (uint32_t i = 0; i < hilos; i++) {
        uint32_t cantidad = base + (i < resto ? 1u : 0u);
        trabajos[i].primer_cuadro = inicio;
        trabajos[i].ultimo_cuadro = inicio + cantidad;
        inicio += cantidad;
    }
}

static void imprimir_estadisticas_dsm(void)
{
    dsm_stats_t stats;
    if (dsm_stats(&stats) != DSM_OK) {
        printf("Estadisticas DSM: no disponibles\n");
        return;
    }
    printf("Fallos de pagina: lectura=%lu escritura=%lu\n",
           (unsigned long)stats.read_faults,
           (unsigned long)stats.write_faults);
    printf("Paginas locales: M=%lu S=%lu I=%lu\n",
           (unsigned long)stats.pages_m,
           (unsigned long)stats.pages_s,
           (unsigned long)stats.pages_i);
    printf("Mensajes: enviados=%lu recibidos=%lu\n",
           (unsigned long)stats.msgs_sent,
           (unsigned long)stats.msgs_recv);
}

static int generar(const char *ruta, uint32_t ancho, uint32_t alto,
                   uint32_t cuadros);

static int procesar(const char *conf, const char *ruta_entrada,
                    const char *ruta_salida, uint32_t hilos)
{
    video_info_t info;
    int fd_entrada = -1;
    int fd_salida = -1;
    void *mem_entrada = NULL;
    void *mem_salida = NULL;
    pthread_t *ids = NULL;
    trabajo_t *trabajos = NULL;
    uint64_t bytes_in;
    uint64_t bytes_out;
    int rc = 1;

    if (abrir_entrada(ruta_entrada, &fd_entrada, &info) != 0) {
        /* Sin archivo de entrada no hay nada que filtrar, pero la aplicacion
         * puede fabricar un video sintetico en esa misma ruta para que la
         * prueba no dependa de datos externos. */
        if (access(ruta_entrada, F_OK) == 0) {
            return 1;
        }
        printf("Entrada '%s' inexistente: se genera un video sintetico\n",
               ruta_entrada);
        if (generar(ruta_entrada, ANCHO_SINTETICO, ALTO_SINTETICO,
                    CUADROS_SINTETICOS) != 0 ||
            abrir_entrada(ruta_entrada, &fd_entrada, &info) != 0) {
            return 1;
        }
    }
    bytes_in = bytes_entrada(&info);
    bytes_out = bytes_salida(&info);

    /* La aplicacion es un cliente del cluster: usa la memoria distribuida sin
     * aportar ninguna, de modo que ejecutarla no agrega un nodo. */
    setenv("DSM_NODE_MODE", "client", 1);

    dsm_status_t st = dsm_init(conf, 0);
    if (st != DSM_OK) {
        fprintf(stderr, "filtro_video: dsm_init fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        goto fin;
    }

    /* El hilo principal tambien toca la vista DSM al cargar el archivo, asi
     * que necesita su ranura de fallo y su pila alternativa. */
    st = dsm_thread_attach();
    if (st != DSM_OK) {
        fprintf(stderr, "filtro_video: dsm_thread_attach fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        goto fin_dsm;
    }

    st = dsm_alloc(bytes_in, &mem_entrada);
    if (st != DSM_OK) {
        fprintf(stderr, "filtro_video: dsm_alloc de entrada (%llu bytes) "
                        "fallo: %s (%d)\n",
                (unsigned long long)bytes_in, dsm_strerror(st), (int)st);
        goto fin_dsm;
    }
    st = dsm_alloc(bytes_out, &mem_salida);
    if (st != DSM_OK) {
        fprintf(stderr, "filtro_video: dsm_alloc de salida (%llu bytes) "
                        "fallo: %s (%d)\n",
                (unsigned long long)bytes_out, dsm_strerror(st), (int)st);
        goto fin_entrada;
    }

    if (!leer_completo(fd_entrada, mem_entrada, (size_t)bytes_in)) {
        fprintf(stderr, "filtro_video: lectura incompleta de '%s' "
                        "(%llu bytes en la region DSM en %p)\n",
                ruta_entrada, (unsigned long long)bytes_in, mem_entrada);
        goto fin_salida;
    }
    close(fd_entrada);
    fd_entrada = -1;

    ids = calloc(hilos, sizeof(*ids));
    trabajos = calloc(hilos, sizeof(*trabajos));
    if (!ids || !trabajos) {
        fprintf(stderr, "filtro_video: sin memoria para %u hilos\n", hilos);
        goto fin_salida;
    }
    repartir_cuadros(trabajos, hilos, info.cuadros);

    printf("Video: %u x %u, %u cuadros (entrada %llu B, salida %llu B)\n",
           info.ancho, info.alto, info.cuadros,
           (unsigned long long)bytes_in,
           (unsigned long long)bytes_out);
    printf("Hilos de aplicacion: %u\n", hilos);

    uint64_t inicio = instante_ns();
    uint32_t lanzados = 0;
    for (uint32_t i = 0; i < hilos; i++) {
        trabajos[i].entrada      = (const uint8_t *)mem_entrada;
        trabajos[i].salida       = (uint8_t *)mem_salida;
        trabajos[i].info         = info;
        trabajos[i].bytes_cuadro = (size_t)info.ancho * info.alto * CANALES_RGB;
        trabajos[i].fallo        = 0;
        if (pthread_create(&ids[i], NULL, hilo_filtro, &trabajos[i]) != 0) {
            fprintf(stderr, "filtro_video: no se pudo crear el hilo %u\n", i);
            break;
        }
        lanzados++;
    }

    int fallo = (lanzados == hilos) ? 0 : 1;
    for (uint32_t i = 0; i < lanzados; i++) {
        pthread_join(ids[i], NULL);
        if (trabajos[i].fallo) fallo = 1;
    }
    if (fallo) {
        fprintf(stderr, "filtro_video: algun hilo no pudo procesar su rango\n");
        goto fin_salida;
    }

    st = dsm_sync();
    if (st != DSM_OK) {
        fprintf(stderr, "filtro_video: dsm_sync fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        goto fin_salida;
    }
    uint64_t fin_proceso = instante_ns();

    fd_salida = open(ruta_salida, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd_salida < 0) {
        fprintf(stderr, "filtro_video: no se pudo crear '%s': %s\n",
                ruta_salida, strerror(errno));
        goto fin_salida;
    }
    uint32_t crudo[3] = { info.ancho, info.alto, info.cuadros };
    if (!escribir_completo(fd_salida, crudo, CABECERA_BYTES) ||
        !escribir_desde_dsm(fd_salida, mem_salida, (size_t)bytes_out)) {
        fprintf(stderr, "filtro_video: escritura incompleta en '%s': %s\n",
                ruta_salida, strerror(errno));
        goto fin_salida;
    }
    close(fd_salida);
    fd_salida = -1;
    uint64_t fin_total = instante_ns();

    double segundos = (double)(fin_proceso - inicio) / 1e9;
    double segundos_total = (double)(fin_total - inicio) / 1e9;
    double mib = (double)(bytes_in + bytes_out) / (1024.0 * 1024.0);
    double mb_s = (segundos > 0.0) ? mib / segundos : 0.0;

    printf("Cuadros procesados: %u\n", info.cuadros);
    printf("Tiempo de filtrado: %.3f s (%.3f s con escritura a disco)\n",
           segundos, segundos_total);
    printf("Rendimiento: %.2f MiB/s (lectura + escritura)\n", mb_s);
    imprimir_estadisticas_dsm();
    printf("Salida escrita en '%s'\n", ruta_salida);
    rc = 0;

fin_salida:
    if (fd_salida >= 0) close(fd_salida);
    free(ids);
    free(trabajos);
    if (mem_salida) dsm_free(mem_salida, bytes_out);
fin_entrada:
    if (mem_entrada) dsm_free(mem_entrada, bytes_in);
fin_dsm:
    dsm_thread_detach();
    dsm_finalize();
fin:
    if (fd_entrada >= 0) close(fd_entrada);
    return rc;
}

/* Genera un video sintetico con un gradiente que recorre los tres canales. */
static int generar(const char *ruta, uint32_t ancho, uint32_t alto,
                   uint32_t cuadros)
{
    video_info_t info = { ancho, alto, cuadros };
    if (!info_valida(&info)) {
        fprintf(stderr, "filtro_video: dimensiones fuera de rango\n");
        return 1;
    }

    int fd = open(ruta, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "filtro_video: no se pudo crear '%s': %s\n",
                ruta, strerror(errno));
        return 1;
    }

    uint32_t crudo[3] = { ancho, alto, cuadros };
    if (!escribir_completo(fd, crudo, CABECERA_BYTES)) {
        fprintf(stderr, "filtro_video: escritura fallida en '%s'\n", ruta);
        close(fd);
        return 1;
    }

    size_t ancho_bytes = (size_t)ancho * CANALES_RGB;
    uint8_t *fila = malloc(ancho_bytes);
    if (!fila) {
        fprintf(stderr, "filtro_video: sin memoria para el cuadro sintetico\n");
        close(fd);
        return 1;
    }

    int ok = 1;
    for (uint32_t c = 0; c < cuadros && ok; c++) {
        for (uint32_t y = 0; y < alto && ok; y++) {
            for (uint32_t x = 0; x < ancho; x++) {
                uint8_t *px = fila + (size_t)x * CANALES_RGB;
                px[0] = (uint8_t)(((uint64_t)x * 255u) /
                                  (ancho > 1 ? ancho - 1 : 1));
                px[1] = (uint8_t)(((uint64_t)y * 255u) /
                                  (alto > 1 ? alto - 1 : 1));
                px[2] = (uint8_t)(((uint64_t)c * 255u) /
                                  (cuadros > 1 ? cuadros - 1 : 1));
            }
            if (!escribir_completo(fd, fila, ancho_bytes)) {
                fprintf(stderr, "filtro_video: escritura fallida en '%s'\n", ruta);
                ok = 0;
            }
        }
    }

    free(fila);
    close(fd);
    if (ok) {
        printf("Generado '%s': %u x %u, %u cuadros\n", ruta, ancho, alto, cuadros);
    }
    return ok ? 0 : 1;
}

static bool parsear_entero(const char *texto, uint64_t maximo, uint32_t *out)
{
    if (!texto || *texto == '\0') return false;
    errno = 0;
    char *fin = NULL;
    unsigned long v = strtoul(texto, &fin, 10);
    if (errno != 0 || !fin || *fin != '\0' || v == 0 || v > maximo) {
        return false;
    }
    *out = (uint32_t)v;
    return true;
}

static void uso(const char *programa)
{
    fprintf(stderr,
            "Uso: %s [--conf <archivo>] <entrada> <salida> [hilos]\n"
            "     %s --generar <archivo> <ancho> <alto> <cuadros>\n"
            "\n"
            "Sin --conf se usa dsm.conf del directorio actual. La aplicacion se\n"
            "une al cluster como cliente: usa su memoria sin aportar ninguna.\n",
            programa, programa);
}

int main(int argc, char **argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc == 6 && strcmp(argv[1], "--generar") == 0) {
        uint32_t ancho, alto, cuadros;
        if (!parsear_entero(argv[3], MAX_LADO, &ancho) ||
            !parsear_entero(argv[4], MAX_LADO, &alto) ||
            !parsear_entero(argv[5], MAX_CUADROS, &cuadros)) {
            fprintf(stderr, "filtro_video: dimensiones invalidas\n");
            uso(argv[0]);
            return 2;
        }
        return generar(argv[2], ancho, alto, cuadros);
    }

    const char *conf = NULL;
    int arg = 1;

    if (argc > 2 && strcmp(argv[1], "--conf") == 0) {
        conf = argv[2];
        arg = 3;
    }

    int restantes = argc - arg;
    if (restantes < 2 || restantes > 3) {
        uso(argv[0]);
        return 2;
    }

    uint32_t hilos = 4;
    if (restantes == 3 && !parsear_entero(argv[arg + 2], MAX_HILOS, &hilos)) {
        fprintf(stderr, "filtro_video: numero de hilos invalido '%s'\n", argv[arg + 2]);
        uso(argv[0]);
        return 2;
    }

    return procesar(conf, argv[arg], argv[arg + 1], hilos);
}
