/*
 * video_pack: convierte una secuencia RGB24 cruda en el formato que consume
 * filtro_video, que es una cabecera de tres enteros de 32 bits con el ancho, el
 * alto y la cantidad de cuadros, seguida de los píxeles.
 *
 * Uso: video_pack <entrada.rgb> <salida.rgb> <ancho> <alto>
 *
 * Sirve para alimentar la aplicación con video real, por ejemplo el que produce
 * ffmpeg:
 *
 *   ffmpeg -i entrada.mp4 -pix_fmt rgb24 -f rawvideo crudo.rgb
 *   video_pack crudo.rgb video.rgb 1280 720
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void uso(const char *programa)
{
    fprintf(stderr,
            "Uso: %s <entrada.rgb> <salida.rgb> <ancho> <alto>\n"
            "\n"
            "  entrada  secuencia RGB24 cruda, sin cabecera\n"
            "  salida   misma secuencia precedida del ancho, el alto y el\n"
            "           numero de cuadros, que es lo que lee filtro_video\n",
            programa);
}

static int entero_positivo(const char *texto, uint32_t *out)
{
    char *fin = NULL;
    unsigned long valor = strtoul(texto, &fin, 10);

    if (!texto[0] || !fin || *fin != '\0' || valor == 0 || valor > 65535UL) {
        return 0;
    }
    *out = (uint32_t)valor;
    return 1;
}

int main(int argc, char **argv)
{
    if (argc != 5) {
        uso(argv[0]);
        return 2;
    }

    uint32_t ancho = 0;
    uint32_t alto = 0;
    if (!entero_positivo(argv[3], &ancho) || !entero_positivo(argv[4], &alto)) {
        fprintf(stderr, "video_pack: dimensiones invalidas\n");
        uso(argv[0]);
        return 2;
    }

    uint64_t bytes_cuadro = (uint64_t)ancho * alto * 3u;
    struct stat st;
    if (stat(argv[1], &st) != 0) {
        fprintf(stderr, "video_pack: no se pudo leer '%s'\n", argv[1]);
        return 1;
    }
    if ((uint64_t)st.st_size % bytes_cuadro != 0) {
        fprintf(stderr,
                "video_pack: '%s' mide %lld bytes, que no es multiple de un "
                "cuadro de %u x %u (%llu bytes)\n",
                argv[1], (long long)st.st_size, ancho, alto,
                (unsigned long long)bytes_cuadro);
        return 1;
    }

    uint64_t cuadros = (uint64_t)st.st_size / bytes_cuadro;
    if (cuadros > UINT32_MAX) {
        fprintf(stderr, "video_pack: demasiados cuadros\n");
        return 1;
    }

    FILE *entrada = fopen(argv[1], "rb");
    if (!entrada) {
        fprintf(stderr, "video_pack: no se pudo abrir '%s'\n", argv[1]);
        return 1;
    }
    FILE *salida = fopen(argv[2], "wb");
    if (!salida) {
        fclose(entrada);
        fprintf(stderr, "video_pack: no se pudo crear '%s'\n", argv[2]);
        return 1;
    }

    uint32_t cabecera[3] = { ancho, alto, (uint32_t)cuadros };
    if (fwrite(cabecera, sizeof(cabecera), 1, salida) != 1) {
        fprintf(stderr, "video_pack: fallo al escribir la cabecera\n");
        fclose(entrada);
        fclose(salida);
        return 1;
    }

    unsigned char *bloque = malloc((size_t)bytes_cuadro);
    if (!bloque) {
        fclose(entrada);
        fclose(salida);
        return 1;
    }

    for (uint64_t i = 0; i < cuadros; i++) {
        if (fread(bloque, (size_t)bytes_cuadro, 1, entrada) != 1 ||
            fwrite(bloque, (size_t)bytes_cuadro, 1, salida) != 1) {
            fprintf(stderr, "video_pack: fallo al copiar el cuadro %llu\n",
                    (unsigned long long)i);
            free(bloque);
            fclose(entrada);
            fclose(salida);
            return 1;
        }
    }

    free(bloque);
    fclose(entrada);
    fclose(salida);

    printf("Empaquetado '%s' -> '%s': %u x %u, %llu cuadros (%llu bytes)\n",
           argv[1], argv[2], ancho, alto, (unsigned long long)cuadros,
           (unsigned long long)(12 + st.st_size));
    return 0;
}
