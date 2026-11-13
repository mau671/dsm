#ifndef DSM_H
#define DSM_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "dsm_types.h"

/**
 * @file dsm.h
 * @brief API pública del sistema de memoria compartida distribuida (DSM).
 *
 * Modelo de uso: un proceso llama a dsm_init() una vez al arrancar, cada hilo
 * que toque la memoria compartida llama a dsm_thread_attach() antes de usarla,
 * las aplicaciones reservan memoria con dsm_alloc() y acceden a ella mediante
 * punteros normales (los fallos de página los resuelve el sistema), y al
 * terminar se llama a dsm_finalize().
 *
 * Convención de errores: todas las funciones que devuelven dsm_status_t
 * retornan DSM_OK (0) en éxito y un código negativo en error. El conjunto de
 * códigos posibles se documenta en cada función; dsm_strerror() traduce cada
 * código a texto. Cuando una función devuelve error, no escribe en sus
 * parámetros de salida salvo que se indique lo contrario.
 *
 * dsm_lock(), dsm_unlock() y dsm_barrier() se apoyan en un custodio
 * determinista por identificador: el nodo que el anillo de dispersión asigna
 * al cerrojo o a la barrera mantiene su estado y el resto le pide la
 * concesión. La propiedad de un cerrojo es por hilo.
 */

#ifdef __cplusplus
extern "C" {
#endif

/* --- Ciclo de vida del nodo -------------------------------------------- */

/**
 * @brief Inicializa el subsistema DSM en el proceso actual.
 *
 * Carga y valida la configuración, eleva el límite de descriptores, ignora
 * SIGPIPE, inicializa la cola de fallos, la membresía, la red, el directorio
 * MSI y la memoria virtual (vista mapeada en la dirección base configurada
 * más un alias de lectura/escritura sobre el mismo memfd), instala el
 * manejador de SIGSEGV, arranca los hilos de red, workers y despachadores, y
 * ejecuta el descubrimiento y la unión al cluster. Si no se detecta ningún
 * par, el nodo asume el rol de primer nodo (ranura 0, época 1).
 *
 * Si el subsistema ya está inicializado, la llamada no hace nada, registra
 * una advertencia y devuelve DSM_OK.
 *
 * @param conf_path Ruta al archivo dsm.conf. Si es NULL se usa "dsm.conf".
 * @param node_mem_override Memoria física que aporta este nodo, en bytes.
 *        Si es 0 se usa el valor NODE_MEM del archivo de configuración; si es
 *        mayor que 0 reemplaza ese valor y la configuración se valida de nuevo.
 * @return DSM_OK si el nodo quedó inicializado; en caso contrario:
 *         - DSM_ERR_CONFIG: no se pudo abrir el archivo, no es un archivo
 *           regular, supera 1 MiB, tiene líneas o valores inválidos, o un par
 *           rechazó la unión por discrepancia de configuración.
 *         - DSM_ERR_MEM: fallo de asignación de memoria, de creación de hilos
 *           o semáforos, de memfd_create/mmap/ftruncate, o de instalación del
 *           manejador de SIGSEGV.
 *         - DSM_ERR_NET: no hay puerto libre en el rango configurado, fallo de
 *           listen, o fallo de conexión durante el descubrimiento.
 *         - DSM_ERR_PROTO: respuesta de descubrimiento con cabecera inválida,
 *           versión incompatible o CRC32 incorrecto.
 *         - DSM_ERR_NO_SPACE: no quedan ranuras de nodo libres en el cluster.
 *         - DSM_ERR_INVALID: la ruta del archivo de identidad del nodo excede
 *           el tamaño máximo permitido.
 *         En cualquier error, los recursos ya adquiridos se liberan y el
 *         subsistema queda sin inicializar.
 */
dsm_status_t dsm_init(const char *conf_path, uint64_t node_mem_override);

/**
 * @brief Finaliza el nodo DSM de forma ordenada y libera sus recursos.
 *
 * Detiene el ciclo de ejecución, anuncia la salida al cluster (mensaje LEAVE)
 * y detiene el heartbeat, los despachadores y los workers, cierra el listener
 * y las conexiones, restaura el manejador de SIGSEGV previo, desmapea la
 * memoria compartida y destruye los módulos internos. Tras una finalización
 * exitosa, dsm_is_initialized() devuelve false y el subsistema puede
 * inicializarse de nuevo con dsm_init().
 *
 * @return DSM_OK si el nodo se finalizó; DSM_ERR_NOT_INIT si el subsistema no
 *         estaba inicializado (en ese caso no se modifica nada).
 */
dsm_status_t dsm_finalize(void);

/**
 * @brief Indica si el subsistema DSM está inicializado en el proceso actual.
 *
 * @return true si dsm_init() completó con éxito y dsm_finalize() no se ha
 *         ejecutado todavía; false en caso contrario.
 */
bool dsm_is_initialized(void);

/* --- Gestión de hilos de aplicación ------------------------------------- */

/**
 * @brief Vincula el hilo actual al sistema DSM.
 *
 * Asigna al hilo una pila alternativa de 64 KiB mediante sigaltstack, que el
 * manejador de SIGSEGV necesita para atender los fallos de página de la vista
 * compartida. Todo hilo que acceda a la memoria reservada con dsm_alloc() debe
 * llamar a esta función antes de hacerlo. Si el hilo ya estaba vinculado, la
 * llamada no hace nada y devuelve DSM_OK.
 *
 * @return DSM_OK si el hilo quedó vinculado; DSM_ERR_NOT_INIT si el subsistema
 *         no está inicializado; DSM_ERR_MEM si falla el mmap de la pila
 *         alternativa o la instalación con sigaltstack.
 */
dsm_status_t dsm_thread_attach(void);

/**
 * @brief Desvincula el hilo actual del sistema DSM.
 *
 * Desactiva la pila alternativa (sigaltstack con SS_DISABLE) y libera su
 * memoria. Si el hilo no estaba vinculado, la llamada no hace nada y devuelve
 * DSM_OK.
 *
 * @return DSM_OK siempre que el subsistema esté inicializado; DSM_ERR_NOT_INIT
 *         si el subsistema no está inicializado.
 */
dsm_status_t dsm_thread_detach(void);

/* --- Memoria compartida distribuida ------------------------------------- */

/**
 * @brief Reserva memoria en la vista compartida del nodo.
 *
 * Asigna un bloque contiguo de páginas dentro de la vista DSM del nodo local y
 * devuelve su dirección virtual base. El tamaño se redondea hacia arriba a un
 * múltiplo del tamaño de página configurado. La búsqueda es first-fit: primero
 * recorre la lista de huecos que dejaron las liberaciones previas y toma el
 * primer rango que alcanza el tamaño pedido, partiendo el sobrante si el hueco
 * es mayor; solo si ningún hueco encaja avanza el cursor de asignación sobre
 * las páginas que nunca se reservaron. La reserva es local al nodo: no
 * coordina la asignación con otros nodos ni notifica el bloque al cluster. La
 * llamada es segura entre hilos del mismo proceso.
 *
 * Cada reserva ocupa una entrada de la tabla interna de bloques vivos, de
 * capacidad acotada. Un bloque liberado con dsm_free() deja libre su entrada y
 * puede volver a entregarse a otra reserva.
 *
 * @param size Cantidad de bytes a reservar. Debe ser mayor que 0.
 * @param out Puntero de salida donde se escribe la dirección virtual base del
 *        bloque reservado. No puede ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL o @p size es 0;
 *         DSM_ERR_NO_SPACE si el bloque no cabe en las páginas de la vista ni
 *         en ningún hueco libre, o si la tabla de bloques vivos está llena.
 */
dsm_status_t dsm_alloc(uint64_t size, void **out);

/**
 * @brief Liberación de memoria reservada con dsm_alloc().
 *
 * Devuelve el bloque al espacio libre del nodo y lo deja disponible para una
 * reserva posterior. La llamada valida que @p ptr sea la dirección base de un
 * bloque vivo de la vista, que esté alineado a página y que @p size redondee
 * al mismo número de páginas con que se reservó; si alguna condición falla
 * devuelve DSM_ERR_INVALID sin modificar el estado. En éxito, todas las
 * páginas del bloque quedan en estado Invalid y sin permisos de acceso,
 * marcadas como inaccesibles para que la próxima reserva que las ocupe no
 * observe el contenido anterior, y el rango se anexa a la lista de huecos,
 * fusionándose con los rangos contiguos que ya estuvieran libres.
 *
 * La liberación es local al nodo, igual que la reserva. Cada llamada consume
 * una entrada de la lista de huecos, de capacidad acotada.
 *
 * @param ptr Dirección base devuelta por dsm_alloc(). No puede ser NULL.
 * @param size Cantidad de bytes del bloque. Debe coincidir, tras redondear a
 *        páginas, con el tamaño reservado.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p ptr es NULL, no pertenece a la
 *         vista, está desalineado, no corresponde a un bloque vivo o @p size
 *         no coincide; DSM_ERR_NO_SPACE si la lista de huecos está llena.
 */
dsm_status_t dsm_free(void *ptr, uint64_t size);

/**
 * @brief Persiste las páginas modificadas localmente.
 *
 * Recorre la tabla local de páginas y, para cada página en estado Modified,
 * persiste su contenido a través de su nodo custodio. Las páginas en estado
 * Shared o Invalid no se tocan.
 *
 * @return DSM_OK si todas las páginas se persistieron; el primer código de
 *         error encontrado si alguna falla. DSM_ERR_NOT_INIT si el subsistema no
 *         está inicializado.
 */
dsm_status_t dsm_sync(void);

/* --- Primitivas de sincronización distribuida -------------------------- */

/**
 * @brief Adquiere un cerrojo distribuido.
 *
 * El custodio del cerrojo es el nodo que el anillo de dispersión asigna a
 * @p lock_id. Si el llamador es el custodio y el cerrojo está libre, la
 * concesión es local; en caso contrario se envía MSG_LOCK_REQ al custodio y el
 * hilo espera MSG_LOCK_GRANT con un plazo de LOCK_TIMEOUT_MS. La propiedad se
 * registra por hilo, de modo que dos hilos del mismo nodo quedan serializados
 * igual que dos nodos distintos.
 *
 * @param lock_id Identificador del cerrojo.
 * @return DSM_OK si el cerrojo quedó adquirido; DSM_ERR_NOT_INIT si el
 *         subsistema no está inicializado; DSM_ERR_BUSY si el hilo ya posee
 *         ese cerrojo; DSM_ERR_TIMEOUT si el custodio no concede dentro
 *         del plazo; DSM_ERR_NO_SPACE si se agotan las entradas locales de
 *         espera o la cola del custodio; DSM_ERR_NET si el enlace con el
 *         custodio falla.
 */
dsm_status_t dsm_lock(uint32_t lock_id);

/**
 * @brief Libera un cerrojo distribuido.
 *
 * El llamador debe ser el hilo que adquirió el cerrojo. Si es el custodio, la
 * concesión pasa al siguiente de la cola o el cerrojo queda libre; si no,
 * envía MSG_LOCK_REL al custodio.
 *
 * @param lock_id Identificador del cerrojo.
 * @return DSM_OK si el cerrojo quedó liberado; DSM_ERR_NOT_INIT si el
 *         subsistema no está inicializado; DSM_ERR_INVALID si el hilo no
 *         posee ese cerrojo; DSM_ERR_NET si el enlace con el custodio falla.
 */
dsm_status_t dsm_unlock(uint32_t lock_id);

/**
 * @brief Barrera de sincronización distribuida.
 *
 * El custodio cuenta los arribos y, al alcanzar @p n, difunde MSG_BARRIER_GO
 * a los participantes y los libera. Cada llamada bloquea al hilo hasta que la
 * ronda se cierra o vence LOCK_TIMEOUT_MS.
 *
 * @param barrier_id Identificador de la barrera.
 * @param n Cantidad de participantes esperados. Debe ser mayor que cero.
 * @return DSM_OK si la ronda se cerró; DSM_ERR_NOT_INIT si el subsistema no
 *         está inicializado; DSM_ERR_INVALID si @p n es cero; DSM_ERR_TIMEOUT
 *         si no llegan todos los participantes dentro del plazo;
 *         DSM_ERR_NO_SPACE si se agotan las entradas locales de espera o las
 *         ranuras de barrera del custodio; DSM_ERR_NET si el enlace con el
 *         custodio falla.
 */
dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n);

/* --- Consultas y diagnóstico ------------------------------------------- */

/**
 * @brief Obtiene la ranura del nodo local en el cluster.
 *
 * @param out Puntero de salida donde se escribe la ranura asignada en la
 *        membresía (0..MAX_NODES-1). No puede ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL.
 */
dsm_status_t dsm_node_id(uint32_t *out);

/**
 * @brief Cuenta los nodos vivos del cluster.
 *
 * Recorre la tabla de membresía y cuenta las ranuras presentes cuyo estado no
 * es DSM_NODE_DEAD, es decir, las ranuras ALIVE y SUSPECT. El nodo local se
 * incluye en el conteo.
 *
 * @param out Puntero de salida donde se escribe la cantidad de nodos. No puede
 *        ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL.
 */
dsm_status_t dsm_node_count(uint32_t *out);

/**
 * @brief Obtiene el tamaño agregado de la memoria compartida del cluster.
 *
 * Suma las páginas aportadas por cada nodo presente y no muerto de la
 * membresía (ALIVE y SUSPECT) y multiplica el total por el tamaño de página
 * configurado.
 *
 * @param out Puntero de salida donde se escribe el tamaño total en bytes. No
 *        puede ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL.
 */
dsm_status_t dsm_global_size(uint64_t *out);

/**
 * @brief Obtiene una instantánea de las estadísticas del nodo local.
 *
 * Rellena @p out con los contadores acumulados del nodo (fallos de lectura y
 * escritura, mensajes y bytes enviados y recibidos, invalidaciones, latencia
 * acumulada de fallos, operaciones pendientes, uso del conjunto de espera,
 * fallos encolados y despertados) y con el conteo actual de páginas locales en
 * estado Modified, Shared e Invalid. La estructura se sobrescribe por completo:
 * los valores previos de @p out se descartan.
 *
 * @param out Puntero a la estructura que recibe la instantánea. No puede ser
 *        NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL.
 */
dsm_status_t dsm_stats(dsm_stats_t *out);

/**
 * @brief Traduce un código de estado DSM a texto.
 *
 * @param st Código de estado a describir.
 * @return Puntero a una cadena estática de solo lectura con la descripción en
 *         español del código. Para valores no reconocidos devuelve
 *         "Error desconocido". La cadena no debe liberarse ni modificarse.
 */
const char *dsm_strerror(dsm_status_t st);

/**
 * @brief Vuelca el estado del nodo al descriptor de archivo indicado.
 *
 * Escribe líneas de texto con la ranura y la época del nodo local, la
 * dirección base y el tamaño de la vista compartida, y la cantidad de nodos
 * activos. Pensada para depuración y pruebas.
 *
 * @param fd Descriptor de archivo abierto para escritura (por ejemplo,
 *        STDERR_FILENO o el descriptor de un archivo de log). Los errores de
 *        escritura no se detectan ni se informan.
 * @return DSM_OK si el subsistema está inicializado; DSM_ERR_NOT_INIT si no lo
 *         está. No valida @p fd y no devuelve ningún otro código.
 */
dsm_status_t dsm_debug_dump_fd(int fd);

#ifdef __cplusplus
}
#endif

#endif /* DSM_H */
