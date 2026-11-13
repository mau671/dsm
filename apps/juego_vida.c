/*
 * Juego de la vida de Conway sobre un tablero grande en memoria DSM.
 *
 * Aplicacion de prueba del sistema DSM: reserva dos tableros en la memoria
 * compartida distribuida, uno con la generacion actual y otro con la
 * siguiente, y aplica las reglas del automata celular generacion tras
 * generacion intercambiando los punteros (doble buffer, sin copiar). Cada
 * celda ocupa un byte para que el tablero consuma la mayor cantidad posible
 * de memoria por celda.
 *
 * Uso:
 *   juego_vida [--verificar] [--patron <nombre>] <config> <ancho> <alto>
 *              <generaciones> [hilos]
 *
 * El borde es toroidal: la ultima fila es vecina de la primera y la ultima
 * columna de la primera. La misma convencion se usa en la simulacion
 * secuencial de referencia, de modo que la comparacion de --verificar mide
 * coherencia del DSM y no diferencias de convencion. En tableros pequenos el
 * borde toroidal altera la evolucion de los patrones clasicos (un glider
 * reaparece por el lado opuesto), pero el resultado sigue siendo determinista.
 */
#include "dsm.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#define LADO_MINIMO          3u
#define LADO_MAXIMO          65536u
#define MAX_HILOS            256u
#define MAX_GENERACIONES     10000000u
#define MAX_CELDAS           (1ull << 32)
#define MAX_CELDAS_VERIF     (1ull << 28)

/* Semilla fija del generador inicial: la misma entrada produce el mismo
 * tablero en cada ejecucion, lo que hace reproducible la referencia
 * secuencial de --verificar. */
#define SEMILLA_INICIAL      0x5EED1234u
#define DENSIDAD_PORCENTAJE  30u

#define UN_MIB               1048576ull

typedef struct {
    uint8_t *tablero_a;
    uint8_t *tablero_b;
    uint32_t ancho;
    uint32_t alto;
    uint32_t generaciones;
    pthread_barrier_t barrera;
    pthread_mutex_t puerta_mu;
    pthread_cond_t  puerta_cond;
    /* 0 = los hilos esperan el arranque, 1 = arrancar, -1 = abortar. La puerta
     * evita que un hilo empiece a recorrer generaciones antes de que existan
     * todos los participantes de la barrera. */
    int puerta;
} comun_t;

typedef struct {
    comun_t *comun;
    uint32_t primera_fila;
    uint32_t ultima_fila;
    uint64_t vivas;
    int fallo;
} trabajo_t;

typedef struct {
    const char *nombre;
    uint32_t filas;
    uint32_t columnas;
    const char *celdas;
} patron_t;

static const patron_t PATRONES[] = {
    { "block",   2, 2, "XX"
                       "XX" },
    { "blinker", 1, 3, "XXX" },
    { "glider",  3, 3, ".X."
                       "..X"
                       "XXX" }
};

static uint64_t instante_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t lcg(uint32_t *estado)
{
    *estado = (*estado * 1664525u) + 1013904223u;
    return *estado;
}

static const patron_t *buscar_patron(const char *nombre)
{
    for (size_t i = 0; i < sizeof(PATRONES) / sizeof(PATRONES[0]); i++) {
        if (strcmp(PATRONES[i].nombre, nombre) == 0) {
            return &PATRONES[i];
        }
    }
    return NULL;
}

/*
 * Lee una imagen en escala de grises de exactamente ancho x alto píxeles y la
 * devuelve como un byte por celda. La imagen debe ser materia real, por ejemplo
 * una foto convertida con ffmpeg:
 *
 *   ffmpeg -i foto.jpg -vf scale=<ancho>:<alto> -pix_fmt gray -f rawvideo out.gray
 *
 * Se exige el tamaño exacto porque cada píxel se convierte en una celda del
 * tablero; cualquier otra dimensión dejaría celdas sin sembrar o sobraría
 * información.
 */
static uint8_t *leer_imagen_gris(const char *ruta, uint32_t ancho, uint32_t alto)
{
    struct stat st;
    if (stat(ruta, &st) != 0) {
        fprintf(stderr, "juego_vida: no se pudo leer '%s': %s\n", ruta,
                strerror(errno));
        return NULL;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "juego_vida: '%s' no es un archivo regular\n", ruta);
        return NULL;
    }

    uint64_t esperado = (uint64_t)ancho * alto;
    if ((uint64_t)st.st_size != esperado) {
        fprintf(stderr,
                "juego_vida: '%s' mide %lld bytes y el tablero de %u x %u "
                "necesita exactamente %llu bytes en escala de grises\n",
                ruta, (long long)st.st_size, ancho, alto,
                (unsigned long long)esperado);
        return NULL;
    }

    uint8_t *datos = malloc((size_t)esperado);
    if (!datos) {
        fprintf(stderr, "juego_vida: sin memoria para la imagen\n");
        return NULL;
    }

    int fd = open(ruta, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "juego_vida: no se pudo abrir '%s': %s\n", ruta,
                strerror(errno));
        free(datos);
        return NULL;
    }

    size_t leidos = 0;
    while (leidos < (size_t)esperado) {
        ssize_t n = read(fd, datos + leidos, (size_t)esperado - leidos);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            fprintf(stderr, "juego_vida: fallo al leer '%s': %s\n", ruta,
                    strerror(errno));
            close(fd);
            free(datos);
            return NULL;
        }
        if (n == 0) {
            break;
        }
        leidos += (size_t)n;
    }
    close(fd);

    if (leidos != (size_t)esperado) {
        fprintf(stderr, "juego_vida: '%s' quedo incompleto\n", ruta);
        free(datos);
        return NULL;
    }

    return datos;
}

/*
 * Reglas de Conway para una celda: nace con 3 vecinas vivas y sobrevive con 2.
 * Los indices de las tres columnas llegan ya resueltos por el llamador para
 * que el borde toroidal se calcule una sola vez por fila.
 */
static inline uint8_t celda_siguiente(const uint8_t *arriba, const uint8_t *fila,
                                      const uint8_t *abajo, uint32_t izquierda,
                                      uint32_t centro, uint32_t derecha)
{
    uint32_t vecinos = (uint32_t)arriba[izquierda] + arriba[centro] + arriba[derecha]
                     + (uint32_t)fila[izquierda] + fila[derecha]
                     + (uint32_t)abajo[izquierda] + abajo[centro] + abajo[derecha];

    if (vecinos == 3u) return 1;
    if (vecinos == 2u && fila[centro] != 0) return 1;
    return 0;
}

/* Calcula una fila completa y devuelve cuantas celdas quedaron vivas.
 * Requiere ancho >= 3, condicion que main valida antes de reservar memoria:
 * con menos columnas los indices de borde se solaparian con el interior. */
static uint32_t fila_siguiente(const uint8_t *arriba, const uint8_t *fila,
                               const uint8_t *abajo, uint8_t *destino,
                               uint32_t ancho)
{
    uint32_t vivas = 0;

    destino[0] = celda_siguiente(arriba, fila, abajo, ancho - 1, 0, 1);
    vivas += destino[0];

    for (uint32_t x = 1; x + 1 < ancho; x++) {
        destino[x] = celda_siguiente(arriba, fila, abajo, x - 1, x, x + 1);
        vivas += destino[x];
    }

    destino[ancho - 1] = celda_siguiente(arriba, fila, abajo, ancho - 2,
                                         ancho - 1, 0);
    vivas += destino[ancho - 1];
    return vivas;
}

/* Escribe el tablero completo: ceros y, segun el modo, una semilla
 * pseudoaleatoria reproducible, un patron clasico centrado o una imagen de
 * partida leida de disco. Devuelve la cantidad de celdas vivas iniciales. */
static uint64_t sembrar_tablero(uint8_t *tablero, uint32_t ancho, uint32_t alto,
                                const patron_t *patron, const uint8_t *imagen)
{
    size_t celdas = (size_t)ancho * alto;
    uint64_t vivas = 0;

    memset(tablero, 0, celdas);

    if (imagen) {
        /*
         * Una imagen en escala de grises sirve de tablero real: cada celda
         * queda viva cuando el pixel supera el umbral medio, de modo que las
         * formas de la imagen se convierten en el estado inicial del automata.
         */
        for (size_t i = 0; i < celdas; i++) {
            if (imagen[i] >= 128u) {
                tablero[i] = 1;
                vivas++;
            }
        }
        return vivas;
    }

    if (!patron) {
        uint32_t estado = SEMILLA_INICIAL;
        for (size_t i = 0; i < celdas; i++) {
            if ((lcg(&estado) >> 16) % 100u < DENSIDAD_PORCENTAJE) {
                tablero[i] = 1;
                vivas++;
            }
        }
        return vivas;
    }

    uint32_t fila0 = (alto - patron->filas) / 2;
    uint32_t col0 = (ancho - patron->columnas) / 2;
    for (uint32_t r = 0; r < patron->filas; r++) {
        for (uint32_t c = 0; c < patron->columnas; c++) {
            if (patron->celdas[r * patron->columnas + c] == 'X') {
                tablero[(size_t)(fila0 + r) * ancho + col0 + c] = 1;
                vivas++;
            }
        }
    }
    return vivas;
}

static void paso_secuencial(const uint8_t *actual, uint8_t *siguiente,
                            uint32_t ancho, uint32_t alto)
{
    for (uint32_t y = 0; y < alto; y++) {
        uint32_t arriba = (y == 0) ? alto - 1 : y - 1;
        uint32_t abajo = (y + 1 == alto) ? 0 : y + 1;
        (void)fila_siguiente(actual + (size_t)arriba * ancho,
                             actual + (size_t)y * ancho,
                             actual + (size_t)abajo * ancho,
                             siguiente + (size_t)y * ancho, ancho);
    }
}

/*
 * Hilo trabajador: se enlaza al DSM, espera la senal de arranque y recorre las
 * generaciones sobre su bloque contiguo de filas. El tablero de origen y el de
 * destino se eligen por paridad de la generacion, de modo que el intercambio
 * de doble buffer no necesita estado compartido mutable: todos los hilos
 * deducen el mismo par a partir del numero de generacion.
 */
static void *hilo_vida(void *arg)
{
    trabajo_t *t = (trabajo_t *)arg;
    comun_t *c = t->comun;

    if (dsm_thread_attach() != DSM_OK) {
        t->fallo = 1;
    }

    pthread_mutex_lock(&c->puerta_mu);
    while (c->puerta == 0) {
        pthread_cond_wait(&c->puerta_cond, &c->puerta_mu);
    }
    bool abortar = (c->puerta < 0);
    pthread_mutex_unlock(&c->puerta_mu);

    if (abortar) {
        dsm_thread_detach();
        return NULL;
    }

    /* Un hilo sin enlace no puede tocar la vista, pero debe seguir llegando a
     * la barrera: si abandona, los demas quedan bloqueados esperando un
     * participante que ya no existe. */
    if (t->fallo) {
        for (uint32_t g = 0; g < c->generaciones; g++) {
            pthread_barrier_wait(&c->barrera);
        }
        dsm_thread_detach();
        return NULL;
    }

    uint32_t ancho = c->ancho;
    uint32_t alto = c->alto;

    for (uint32_t g = 0; g < c->generaciones; g++) {
        const uint8_t *actual = (g & 1u) ? c->tablero_b : c->tablero_a;
        uint8_t *siguiente = (g & 1u) ? c->tablero_a : c->tablero_b;
        uint64_t vivas = 0;

        for (uint32_t y = t->primera_fila; y < t->ultima_fila; y++) {
            uint32_t arriba = (y == 0) ? alto - 1 : y - 1;
            uint32_t abajo = (y + 1 == alto) ? 0 : y + 1;
            vivas += fila_siguiente(actual + (size_t)arriba * ancho,
                                    actual + (size_t)y * ancho,
                                    actual + (size_t)abajo * ancho,
                                    siguiente + (size_t)y * ancho, ancho);
        }

        t->vivas = vivas;

        /* La barrera separa generaciones: nadie lee el tablero de destino
         * hasta que todos los hilos terminaron de escribirlo. El reparto es
         * entre hilos del mismo proceso, no entre nodos. */
        pthread_barrier_wait(&c->barrera);
    }

    dsm_thread_detach();
    return NULL;
}

/* Reparte las filas en bloques contiguos, uno por hilo. */
static void repartir_filas(trabajo_t *trabajos, uint32_t hilos, uint32_t alto)
{
    uint32_t base = alto / hilos;
    uint32_t resto = alto % hilos;
    uint32_t inicio = 0;

    for (uint32_t i = 0; i < hilos; i++) {
        uint32_t cantidad = base + (i < resto ? 1u : 0u);
        trabajos[i].primera_fila = inicio;
        trabajos[i].ultima_fila = inicio + cantidad;
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
    printf("Latencia promedio por fallo: %lu us (%lu fallos atendidos)\n",
           (unsigned long)stats.fault_latency_us_avg,
           (unsigned long)stats.faults_serviced);
    printf("Mensajes: enviados=%lu recibidos=%lu\n",
           (unsigned long)stats.msgs_sent,
           (unsigned long)stats.msgs_recv);
}

/* Explica el rechazo con las dos cifras que necesita el usuario para
 * dimensionar el tablero: lo que pide la aplicacion y lo que aporta el
 * cluster. El total del cluster incluye la copia de respaldo del nodo local,
 * que es la que en la practica agota primero el espacio. */
static void informar_sin_espacio(uint64_t bytes_tablero)
{
    uint64_t total = 0;
    uint32_t nodos = 0;
    bool hay_total = (dsm_global_size(&total) == DSM_OK);
    bool hay_nodos = (dsm_node_count(&nodos) == DSM_OK);

    fprintf(stderr, "juego_vida: el tablero no cabe en la memoria DSM\n");
    fprintf(stderr, "  se necesitan %llu MiB (dos tableros de %llu B)\n",
            (unsigned long long)((2 * bytes_tablero + UN_MIB - 1) / UN_MIB),
            (unsigned long long)bytes_tablero);
    if (hay_total && hay_nodos) {
        fprintf(stderr, "  el cluster aporta %llu MiB en %u nodo(s)\n",
                (unsigned long long)(total / UN_MIB), nodos);
    } else if (hay_total) {
        fprintf(stderr, "  el cluster aporta %llu MiB\n",
                (unsigned long long)(total / UN_MIB));
    } else {
        fprintf(stderr, "  no se pudo consultar el tamano del cluster\n");
    }
    fprintf(stderr, "  reduzca <ancho> o <alto> para que el tablero quepa\n");
}

/* Compara lo que pide la aplicacion con el tamano nominal del cluster antes de
 * reservar. dsm_alloc solo controla el espacio de la vista, que en dsm.conf es
 * mucho mayor que la memoria aportada; sin esta comprobacion un tablero
 * demasiado grande se reserva y el respaldo se agota mas tarde, en pleno
 * procesamiento, cuando un fallo de escritura ya no se puede resolver.
 *
 * La comparacion usa la memoria nominal, sin el margen HOME_SLACK del
 * respaldo: eso rechaza la franja estrecha entre memoria nominal y memoria
 * respaldada, a cambio de no aceptar un tablero que solo cabe por el margen y
 * deja la simulacion a medio camino. El mensaje de rechazo muestra las dos
 * cifras para que el usuario ajuste el tamano. */
static bool capacidad_suficiente(uint64_t bytes_tablero)
{
    uint64_t total = 0;
    if (dsm_global_size(&total) != DSM_OK) {
        return true;
    }
    if (2 * bytes_tablero <= total) {
        return true;
    }
    informar_sin_espacio(bytes_tablero);
    return false;
}

/* Compara el tablero final del DSM con la referencia secuencial e informa la
 * primera diferencia, que ubica el error sin volcar el tablero completo. */
static bool comparar_tableros(const uint8_t *dsm, const uint8_t *referencia,
                              uint32_t ancho, uint32_t alto,
                              uint64_t *diferencias)
{
    uint64_t distintas = 0;
    bool primera = true;
    uint32_t fila_primera = 0;
    uint32_t col_primera = 0;
    uint8_t valor_dsm = 0;
    uint8_t valor_ref = 0;

    for (uint32_t y = 0; y < alto; y++) {
        const uint8_t *fila_dsm = dsm + (size_t)y * ancho;
        const uint8_t *fila_ref = referencia + (size_t)y * ancho;
        for (uint32_t x = 0; x < ancho; x++) {
            if (fila_dsm[x] == fila_ref[x]) {
                continue;
            }
            distintas++;
            if (primera) {
                primera = false;
                fila_primera = y;
                col_primera = x;
                valor_dsm = fila_dsm[x];
                valor_ref = fila_ref[x];
            }
        }
    }

    *diferencias = distintas;
    if (distintas == 0) {
        return true;
    }

    printf("  DIVERGENCIAS: %llu celdas distintas de %llu\n",
           (unsigned long long)distintas,
           (unsigned long long)((uint64_t)ancho * alto));
    printf("  Primera diferencia en fila %u, columna %u: DSM=%u secuencial=%u\n",
           fila_primera, col_primera, (unsigned)valor_dsm, (unsigned)valor_ref);
    return false;
}

/* Ejecuta el mismo automata en memoria normal y compara el resultado final. */
static int verificar_secuencial(const uint8_t *tablero_dsm,
                                uint32_t ancho, uint32_t alto,
                                uint32_t generaciones, const patron_t *patron,
                                const uint8_t *imagen)
{
    size_t celdas = (size_t)ancho * alto;
    uint8_t *ref_a = malloc(celdas);
    uint8_t *ref_b = malloc(celdas);
    int rc = 1;

    if (!ref_a || !ref_b) {
        fprintf(stderr, "juego_vida: sin memoria local para la referencia "
                        "secuencial (%llu MiB)\n",
                (unsigned long long)((2 * (uint64_t)celdas + UN_MIB - 1) / UN_MIB));
        free(ref_a);
        free(ref_b);
        return 1;
    }

    (void)sembrar_tablero(ref_a, ancho, alto, patron, imagen);

    uint64_t inicio = instante_ns();
    for (uint32_t g = 0; g < generaciones; g++) {
        const uint8_t *actual = (g & 1u) ? ref_b : ref_a;
        uint8_t *siguiente = (g & 1u) ? ref_a : ref_b;
        paso_secuencial(actual, siguiente, ancho, alto);
    }
    double segundos = (double)(instante_ns() - inicio) / 1e9;

    const uint8_t *final_ref = (generaciones & 1u) ? ref_b : ref_a;

    printf("Verificacion secuencial\n");
    printf("  Simulacion de referencia: %u generaciones en %.3f s\n",
           generaciones, segundos);

    uint64_t diferencias = 0;
    if (comparar_tableros(tablero_dsm, final_ref, ancho, alto, &diferencias)) {
        printf("  Coincidencia exacta: el tablero DSM y la referencia "
               "secuencial son identicos (%llu celdas)\n",
               (unsigned long long)((uint64_t)ancho * alto));
        rc = 0;
    }

    free(ref_a);
    free(ref_b);
    return rc;
}

static int procesar(const char *conf, uint32_t ancho, uint32_t alto,
                    uint32_t generaciones, uint32_t hilos, bool verificar,
                    const patron_t *patron, const uint8_t *imagen)
{
    uint64_t celdas = (uint64_t)ancho * alto;
    uint64_t bytes_tablero = celdas;
    void *mem_a = NULL;
    void *mem_b = NULL;
    pthread_t *ids = NULL;
    trabajo_t *trabajos = NULL;
    comun_t comun;
    uint32_t lanzados = 0;
    int rc = 1;

    /* La aplicacion se une al cluster como cliente: usa la memoria distribuida
     * sin aportar ninguna, de modo que ejecutarla no agrega un nodo. */
    setenv("DSM_NODE_MODE", "client", 1);

    dsm_status_t st = dsm_init(conf, 0);
    if (st != DSM_OK) {
        fprintf(stderr, "juego_vida: dsm_init fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        return 1;
    }

    /* El hilo principal siembra los tableros y luego recorre el resultado
     * final, asi que tambien necesita su ranura de fallo y su pila
     * alternativa. */
    st = dsm_thread_attach();
    if (st != DSM_OK) {
        fprintf(stderr, "juego_vida: dsm_thread_attach fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        goto fin_dsm;
    }

    if (!capacidad_suficiente(bytes_tablero)) {
        goto fin_dsm;
    }

    st = dsm_alloc(bytes_tablero, &mem_a);
    if (st != DSM_OK) {
        fprintf(stderr, "juego_vida: dsm_alloc del primer tablero fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        informar_sin_espacio(bytes_tablero);
        goto fin_dsm;
    }
    st = dsm_alloc(bytes_tablero, &mem_b);
    if (st != DSM_OK) {
        fprintf(stderr, "juego_vida: dsm_alloc del segundo tablero fallo: %s (%d)\n",
                dsm_strerror(st), (int)st);
        informar_sin_espacio(bytes_tablero);
        goto fin_a;
    }

    /* La siembra se hace con escrituras normales sobre la vista: el primer
     * acceso a cada pagina dispara SIGSEGV y el despachador instala el permiso
     * de escritura. Nunca se usa read()/write() sobre esta region. */
    uint64_t vivas_iniciales =
        sembrar_tablero((uint8_t *)mem_a, ancho, alto, patron, imagen);

    printf("Juego de la vida sobre memoria DSM\n");
    printf("Tablero: %u x %u celdas\n", ancho, alto);
    printf("Reserva DSM: dos tableros de %llu B (%.2f MiB por tablero, "
           "%.2f MiB en total)\n",
           (unsigned long long)bytes_tablero,
           (double)bytes_tablero / (double)UN_MIB,
           2.0 * (double)bytes_tablero / (double)UN_MIB);
    if (imagen) {
        printf("Estado inicial: imagen real de %u x %u, celdas vivas: %llu\n",
               ancho, alto, (unsigned long long)vivas_iniciales);
    } else if (patron) {
        printf("Patron: %s (%u x %u centrado), celdas vivas iniciales: %llu\n",
               patron->nombre, patron->filas, patron->columnas,
               (unsigned long long)vivas_iniciales);
    } else {
        printf("Semilla: 0x%08X (densidad %u%%), celdas vivas iniciales: %llu\n",
               SEMILLA_INICIAL, DENSIDAD_PORCENTAJE,
               (unsigned long long)vivas_iniciales);
    }
    printf("Generaciones: %u, hilos: %u, borde toroidal\n", generaciones, hilos);

    ids = calloc(hilos, sizeof(*ids));
    trabajos = calloc(hilos, sizeof(*trabajos));
    if (!ids || !trabajos) {
        fprintf(stderr, "juego_vida: sin memoria para %u hilos\n", hilos);
        goto fin_b;
    }

    memset(&comun, 0, sizeof(comun));
    comun.tablero_a = (uint8_t *)mem_a;
    comun.tablero_b = (uint8_t *)mem_b;
    comun.ancho = ancho;
    comun.alto = alto;
    comun.generaciones = generaciones;
    comun.puerta = 0;

    if (pthread_barrier_init(&comun.barrera, NULL, hilos) != 0) {
        fprintf(stderr, "juego_vida: no se pudo inicializar la barrera\n");
        goto fin_b;
    }
    if (pthread_mutex_init(&comun.puerta_mu, NULL) != 0) {
        fprintf(stderr, "juego_vida: no se pudo inicializar el mutex de arranque\n");
        pthread_barrier_destroy(&comun.barrera);
        goto fin_b;
    }
    if (pthread_cond_init(&comun.puerta_cond, NULL) != 0) {
        fprintf(stderr, "juego_vida: no se pudo inicializar la condicion de arranque\n");
        pthread_mutex_destroy(&comun.puerta_mu);
        pthread_barrier_destroy(&comun.barrera);
        goto fin_b;
    }

    repartir_filas(trabajos, hilos, alto);
    for (uint32_t i = 0; i < hilos; i++) {
        trabajos[i].comun = &comun;
        trabajos[i].fallo = 0;
        trabajos[i].vivas = 0;
    }

    uint64_t inicio = instante_ns();
    for (uint32_t i = 0; i < hilos; i++) {
        if (pthread_create(&ids[i], NULL, hilo_vida, &trabajos[i]) != 0) {
            fprintf(stderr, "juego_vida: no se pudo crear el hilo %u\n", i);
            break;
        }
        lanzados++;
    }

    /* Abrir la puerta solo si estan todos: un hilo que falte dejaria la
     * barrera esperando para siempre. */
    pthread_mutex_lock(&comun.puerta_mu);
    comun.puerta = (lanzados == hilos) ? 1 : -1;
    pthread_cond_broadcast(&comun.puerta_cond);
    pthread_mutex_unlock(&comun.puerta_mu);

    int fallo = (lanzados == hilos) ? 0 : 1;
    for (uint32_t i = 0; i < lanzados; i++) {
        pthread_join(ids[i], NULL);
        if (trabajos[i].fallo) fallo = 1;
    }
    uint64_t fin = instante_ns();

    if (fallo) {
        fprintf(stderr, "juego_vida: algun hilo no pudo procesar su bloque de filas\n");
        goto fin_comun;
    }

    /* Tras N generaciones, la generacion 0 lee A y escribe B; el estado final
     * queda en B si N es impar y en A si N es par. */
    uint8_t *final = (generaciones & 1u) ? (uint8_t *)mem_b : (uint8_t *)mem_a;

    uint64_t vivas = 0;
    for (uint32_t i = 0; i < hilos; i++) {
        vivas += trabajos[i].vivas;
    }

    double segundos = (double)(fin - inicio) / 1e9;
    double evaluadas = (double)generaciones * (double)celdas;
    double por_segundo = (segundos > 0.0) ? evaluadas / segundos : 0.0;

    printf("Generaciones ejecutadas: %u\n", generaciones);
    printf("Celdas vivas finales: %llu\n", (unsigned long long)vivas);
    printf("Tiempo total: %.3f s\n", segundos);
    printf("Rendimiento: %.0f celdas/s (%.2f Mcel/s)\n",
           por_segundo, por_segundo / 1e6);
    printf("Tablero: %.2f MiB por generacion\n",
           (double)bytes_tablero / (double)UN_MIB);
    imprimir_estadisticas_dsm();

    if (verificar &&
        verificar_secuencial(final, ancho, alto, generaciones, patron, imagen) != 0) {
        goto fin_comun;
    }

    rc = 0;

fin_comun:
    pthread_cond_destroy(&comun.puerta_cond);
    pthread_mutex_destroy(&comun.puerta_mu);
    pthread_barrier_destroy(&comun.barrera);
fin_b:
    free(ids);
    free(trabajos);
    if (mem_b) dsm_free(mem_b, bytes_tablero);
fin_a:
    if (mem_a) dsm_free(mem_a, bytes_tablero);
fin_dsm:
    dsm_thread_detach();
    dsm_finalize();
    return rc;
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
            "Uso: %s [--verificar] [--patron <nombre>] [--conf <archivo>]\n"
            "          <ancho> <alto> <generaciones> [hilos]\n"
            "\n"
            "  --verificar        compara el tablero final con una simulacion\n"
            "                     secuencial en memoria normal\n"
            "  --patron <nombre>  siembra un patron clasico: %s, %s o %s\n"
            "  --imagen <archivo> siembra el tablero con una imagen en escala\n"
            "                     de grises del tamano exacto del tablero\n"
            "  --conf <archivo>   configuracion del cluster; por defecto dsm.conf\n",
            programa,
            PATRONES[0].nombre, PATRONES[1].nombre, PATRONES[2].nombre);
}

int main(int argc, char **argv)
{
    const char *posicional[4];
    const char *nombre_patron = NULL;
    const char *conf = NULL;
    const char *ruta_imagen = NULL;
    bool verificar = false;
    int npos = 0;

    setvbuf(stdout, NULL, _IOLBF, 0);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--verificar") == 0) {
            verificar = true;
        } else if (strcmp(argv[i], "--conf") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "juego_vida: --conf requiere una ruta\n");
                uso(argv[0]);
                return 2;
            }
            conf = argv[++i];
        } else if (strcmp(argv[i], "--imagen") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "juego_vida: --imagen requiere una ruta\n");
                uso(argv[0]);
                return 2;
            }
            ruta_imagen = argv[++i];
        } else if (strcmp(argv[i], "--patron") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "juego_vida: --patron requiere un nombre\n");
                uso(argv[0]);
                return 2;
            }
            nombre_patron = argv[++i];
        } else if (strcmp(argv[i], "--ayuda") == 0 || strcmp(argv[i], "-h") == 0) {
            uso(argv[0]);
            return 0;
        } else if (argv[i][0] == '-' && argv[i][1] == '-') {
            fprintf(stderr, "juego_vida: opcion desconocida '%s'\n", argv[i]);
            uso(argv[0]);
            return 2;
        } else {
            if (npos == 4) {
                fprintf(stderr, "juego_vida: demasiados argumentos\n");
                uso(argv[0]);
                return 2;
            }
            posicional[npos++] = argv[i];
        }
    }

    if (npos < 3) {
        uso(argv[0]);
        return 2;
    }

    /*
     * La imagen, si se pide, tiene que coincidir exactamente con el tablero:
     * el estado inicial se lee de ella pixel a pixel y cualquier otra
     * dimension dejaria celdas sin sembrar.
     */
    uint8_t *imagen = NULL;
    if (ruta_imagen) {
        uint32_t ancho_img = 0;
        uint32_t alto_img = 0;
        if (!parsear_entero(posicional[0], LADO_MAXIMO, &ancho_img) ||
            !parsear_entero(posicional[1], LADO_MAXIMO, &alto_img)) {
            uso(argv[0]);
            return 2;
        }
        imagen = leer_imagen_gris(ruta_imagen, ancho_img, alto_img);
        if (!imagen) {
            return 2;
        }
    }

    const patron_t *patron = NULL;
    if (nombre_patron) {
        patron = buscar_patron(nombre_patron);
        if (!patron) {
            fprintf(stderr, "juego_vida: patron desconocido '%s'\n", nombre_patron);
            uso(argv[0]);
            return 2;
        }
    }

    uint32_t ancho, alto, generaciones;
    if (!parsear_entero(posicional[0], LADO_MAXIMO, &ancho) ||
        !parsear_entero(posicional[1], LADO_MAXIMO, &alto)) {
        fprintf(stderr, "juego_vida: dimensiones invalidas '%s' x '%s' "
                        "(rango 1..%u)\n",
                posicional[0], posicional[1], LADO_MAXIMO);
        return 2;
    }
    if (ancho < LADO_MINIMO || alto < LADO_MINIMO) {
        fprintf(stderr, "juego_vida: el tablero debe medir al menos %u x %u\n",
                LADO_MINIMO, LADO_MINIMO);
        return 2;
    }
    if ((uint64_t)ancho * alto > MAX_CELDAS) {
        fprintf(stderr, "juego_vida: %u x %u supera el maximo de %llu celdas\n",
                ancho, alto, (unsigned long long)MAX_CELDAS);
        return 2;
    }
    if (!parsear_entero(posicional[2], MAX_GENERACIONES, &generaciones)) {
        fprintf(stderr, "juego_vida: numero de generaciones invalido '%s' "
                        "(rango 1..%u)\n",
                posicional[2], MAX_GENERACIONES);
        return 2;
    }

    uint32_t hilos = 4;
    if (npos == 4 && !parsear_entero(posicional[3], MAX_HILOS, &hilos)) {
        fprintf(stderr, "juego_vida: numero de hilos invalido '%s' "
                        "(rango 1..%u)\n",
                posicional[3], MAX_HILOS);
        return 2;
    }
    /* Un hilo por fila como maximo: los hilos sobrantes no tendrian trabajo y
     * solo agregarian participantes a la barrera. */
    if (hilos > alto) {
        printf("Aviso: %u hilos para %u filas; se usan %u hilos\n",
               hilos, alto, alto);
        hilos = alto;
    }

    if (verificar && (uint64_t)ancho * alto > MAX_CELDAS_VERIF) {
        fprintf(stderr, "juego_vida: --verificar admite hasta %llu celdas; "
                        "el tablero tiene %llu y la referencia secuencial "
                        "necesitaria %llu MiB de memoria local\n",
                (unsigned long long)MAX_CELDAS_VERIF,
                (unsigned long long)((uint64_t)ancho * alto),
                (unsigned long long)((2 * (uint64_t)ancho * alto +
                                      UN_MIB - 1) / UN_MIB));
        return 2;
    }

    int resultado = procesar(conf, ancho, alto, generaciones, hilos, verificar,
                             patron, imagen);
    free(imagen);
    return resultado;
}
