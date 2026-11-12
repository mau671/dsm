#include "dsm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
    if (argc < 2) {
        printf("Uso: %s <comando> [opciones]\n", argv[0]);
        printf("Comandos disponibles:\n");
        printf("  nodes   - Lista los nodos conocidos en la membresia\n");
        printf("  stats   - Muestra metricas de rendimiento del cluster\n");
        printf("  help    - Muestra esta ayuda\n");
        return 0;
    }

    const char *cmd = argv[1];
    if (strcmp(cmd, "nodes") == 0) {
        printf("DSM Control: Consultando nodos activos...\n");
        /* En prototipo 1, stub de conexion de diagnostico efimera */
        printf("Slot 0: 127.0.0.1 (ALIVE)\n");
    } else if (strcmp(cmd, "stats") == 0) {
        printf("DSM Control: Metricas del sistema DSM...\n");
        printf("Paginacion bajo demanda: Activa (Protocolo MSI)\n");
    } else {
        printf("Comando desconocido: '%s'\n", cmd);
        return 1;
    }

    return 0;
}
