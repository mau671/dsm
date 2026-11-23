#include "dsm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

    printf("Iniciando el nodo DSM con la configuración '%s'.\n", conf_path);

    /* Registro con sigaction y SA_RESTART, como exige el manejo de señales
     * del proyecto: una interrupción no aborta llamadas al sistema en curso
     * y el ciclo principal solo observa la bandera de parada. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sa.sa_flags   = SA_RESTART;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) != 0 ||
        sigaction(SIGTERM, &sa, NULL) != 0) {
        fprintf(stderr, "No se pudo registrar el manejador de terminación.\n");
        return 1;
    }

    /* Las señales de parada se bloquean antes de crear los hilos del nodo:
     * todos heredan la máscara y la señal queda pendiente hasta que el hilo
     * principal la espera con sigsuspend. Así no se pierde un aviso emitido
     * entre la comprobación de la bandera y la espera. */
    sigset_t stopmask;
    sigset_t oldmask;
    sigemptyset(&stopmask);
    sigaddset(&stopmask, SIGINT);
    sigaddset(&stopmask, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &stopmask, &oldmask) != 0) {
        fprintf(stderr, "No se pudo preparar la espera de terminación.\n");
        return 1;
    }

    dsm_status_t st = dsm_init(conf_path, 0);
    if (st != DSM_OK) {
        fprintf(stderr, "Error al inicializar DSM: %s\n", dsm_strerror(st));
        sigprocmask(SIG_SETMASK, &oldmask, NULL);
        return 1;
    }

    uint32_t slot = 0;
    dsm_node_id(&slot);
    printf("Nodo activo en la ranura %u. Pulse Ctrl+C para detenerlo.\n", slot);

    while (!g_stop) {
        sigsuspend(&oldmask);
    }
    sigprocmask(SIG_SETMASK, &oldmask, NULL);

    printf("\nRecibida señal de terminación. Finalizando nodo.\n");

    dsm_stats_t stats;
    if (dsm_stats(&stats) == DSM_OK) {
        printf("Estadísticas finales del nodo\n");
        printf("Mensajes enviados   : %lu\n", (unsigned long)stats.msgs_sent);
        printf("Mensajes recibidos  : %lu\n", (unsigned long)stats.msgs_recv);
        printf("Fallos de lectura   : %lu\n", (unsigned long)stats.read_faults);
        printf("Fallos de escritura : %lu\n", (unsigned long)stats.write_faults);
    }

    dsm_finalize();
    printf("Nodo detenido correctamente.\n");
    return 0;
}
