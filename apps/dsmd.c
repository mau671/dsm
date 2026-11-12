#include "dsm.h"
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop = 0;

static void on_sigint(int sig)
{
    (void)sig;
    g_stop = 1;
}

int main(int argc, char **argv)
{
    const char *conf_path = (argc > 1) ? argv[1] : "dsm.conf";

    printf("Iniciando nodo DSM Daemon con configuracion '%s'...\n", conf_path);

    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    dsm_status_t st = dsm_init(conf_path, 0);
    if (st != DSM_OK) {
        fprintf(stderr, "Error al inicializar DSM: %s (%d)\n", dsm_strerror(st), st);
        return 1;
    }

    uint32_t slot = 0;
    dsm_node_id(&slot);
    printf("Nodo activo y en ejecucion (Slot %u). Presione Ctrl+C para detener.\n", slot);

    while (!g_stop) {
        sleep(1);
    }

    printf("\nRecibida señal de terminacion. Finalizando nodo...\n");

    dsm_stats_t stats;
    if (dsm_stats(&stats) == DSM_OK) {
        printf("--- Estadisticas finales del nodo ---\n");
        printf("Mensajes enviados : %lu\n", (unsigned long)stats.msgs_sent);
        printf("Mensajes recibidos: %lu\n", (unsigned long)stats.msgs_recv);
        printf("Fallos de lectura : %lu\n", (unsigned long)stats.read_faults);
        printf("Fallos de escrit. : %lu\n", (unsigned long)stats.write_faults);
    }

    dsm_finalize();
    printf("Nodo detenido correctamente.\n");
    return 0;
}
