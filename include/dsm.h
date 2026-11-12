#ifndef DSM_H
#define DSM_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "dsm_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* --- Ciclo de vida del nodo -------------------------------------------- */

/**
 * Inicializa el subsistema DSM en el proceso actual.
 * Carga y valida la configuración, reserva memoria virtual con memfd_create,
 * instala el manejador de SIGSEGV, inicia hilos de red/workers y ejecuta
 * el descubrimiento/unión al cluster.
 *
 * @param conf_path Ruta al archivo dsm.conf (NULL para "dsm.conf").
 * @param node_mem_override Memoria física a aportar en bytes (0 para usar valor de conf).
 * @return DSM_OK en éxito o código de error negativo.
 */
dsm_status_t dsm_init(const char *conf_path, uint64_t node_mem_override);

/**
 * Finaliza el nodo DSM ordenadamente.
 * Difunde mensaje LEAVE, persiste páginas modificadas, despierta hilos,
 * cierra sockets y libera memoria.
 *
 * @return DSM_OK en éxito o código de error negativo.
 */
dsm_status_t dsm_finalize(void);

/**
 * Consulta si el subsistema DSM está inicializado.
 */
bool dsm_is_initialized(void);

/* --- Gestión de hilos de aplicación ------------------------------------- */

/**
 * Vincula el hilo actual al sistema DSM asignándole una ranura de fallo
 * y una pila alternativa (sigaltstack) de 64 KiB para SIGSEGV.
 * Todo hilo que acceda al espacio de memoria compartida DEBE llamar a esta función.
 */
dsm_status_t dsm_thread_attach(void);

/**
 * Desvincula el hilo actual del sistema DSM y libera su pila alternativa.
 */
dsm_status_t dsm_thread_detach(void);

/* --- Memoria compartida distribuida ------------------------------------- */

/**
 * Reserva colectiva de memoria contigua en el espacio global DSM.
 * Debe invocarse colectivamente en idéntico orden en todos los nodos.
 *
 * @param size Cantidad de bytes a reservar (se redondea a páginas completas).
 * @param out Puntero de salida donde se almacena la dirección virtual base.
 * @return DSM_OK en éxito o código de error.
 */
dsm_status_t dsm_alloc(uint64_t size, void **out);

/**
 * Liberación colectiva de memoria asignada previamente con dsm_alloc.
 *
 * @param ptr Puntero retornado previamente por dsm_alloc.
 * @param size Cantidad de bytes liberados.
 */
dsm_status_t dsm_free(void *ptr, uint64_t size);

/**
 * Fuerza la persistencia (WRITEBACK) de todas las páginas modificadas
 * localmente (estado M) hacia sus nodos custodios.
 */
dsm_status_t dsm_sync(void);

/* --- Primitivas de sincronización distribuida -------------------------- */

/**
 * Adquiere un cerrojo distribuido (lock_id). Bloquea al hilo hasta obtenerlo.
 */
dsm_status_t dsm_lock(uint32_t lock_id);

/**
 * Libera un cerrojo distribuido previamente adquirido.
 */
dsm_status_t dsm_unlock(uint32_t lock_id);

/**
 * Bloquea al hilo en una barrera distribuida hasta que 'n' participantes
 * arriben a la generación actual de barrier_id.
 */
dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n);

/* --- Consultas y diagnóstico ------------------------------------------- */

/**
 * Obtiene la ranura (slot) asignada al nodo local en el cluster.
 */
dsm_status_t dsm_node_id(uint32_t *out);

/**
 * Obtiene el número total de nodos vivos en la membresía del cluster.
 */
dsm_status_t dsm_node_count(uint32_t *out);

/**
 * Obtiene el tamaño total de memoria compartida agregada por el cluster (bytes).
 */
dsm_status_t dsm_global_size(uint64_t *out);

/**
 * Obtiene una instantánea de las estadísticas operativas del nodo.
 */
dsm_status_t dsm_stats(dsm_stats_t *out);

/**
 * Retorna una descripción textual estática del código de estado DSM.
 */
const char *dsm_strerror(dsm_status_t st);

/**
 * Vuelca el estado de la tabla local y directorio en formato de texto
 * al descriptor de archivo indicado (para depuración y pruebas).
 */
dsm_status_t dsm_debug_dump_fd(int fd);

#ifdef __cplusplus
}
#endif

#endif /* DSM_H */
