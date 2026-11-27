#include "dsm.h"
#include "log.h"
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

    dsm_log_init_from_env();
    DSM_LOG_INFO("Iniciando el nodo DSM con la configuración '%s'", conf_path);

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
        DSM_LOG_ERR("No se pudo registrar el manejador de terminación");
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
        DSM_LOG_ERR("No se pudo preparar la espera de terminación");
        return 1;
    }

    dsm_status_t st = dsm_init(conf_path, 0);
    if (st != DSM_OK) {
        DSM_LOG_ERR("Error al inicializar DSM: %s", dsm_strerror(st));
        sigprocmask(SIG_SETMASK, &oldmask, NULL);
        return 1;
    }

    uint32_t slot = 0;
    dsm_node_id(&slot);
    DSM_LOG_INFO("Nodo activo en la ranura %u. Pulse Ctrl+C para detenerlo.",
                 slot);

    while (!g_stop) {
        sigsuspend(&oldmask);
    }
    sigprocmask(SIG_SETMASK, &oldmask, NULL);

    DSM_LOG_INFO("Recibida señal de terminación; se detiene el nodo");

    dsm_stats_t stats;
    if (dsm_stats(&stats) == DSM_OK) {
        DSM_LOG_INFO("Estadísticas finales del nodo");
        DSM_LOG_INFO("Mensajes enviados   : %lu",
                     (unsigned long)stats.msgs_sent);
        DSM_LOG_INFO("Mensajes recibidos  : %lu",
                     (unsigned long)stats.msgs_recv);
        DSM_LOG_INFO("Fallos de lectura   : %lu",
                     (unsigned long)stats.read_faults);
        DSM_LOG_INFO("Fallos de escritura : %lu",
                     (unsigned long)stats.write_faults);
    }

    dsm_finalize();
    DSM_LOG_INFO("Nodo detenido correctamente");
    return 0;
}
