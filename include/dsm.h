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
 *
 * @note Los fallos de página concurrentes sobre una misma página se coordinan
 * con una cola FIFO por página: la solicitud que llega durante una transición
 * en curso se encola y aguarda en una variable de condición hasta que la
 * transición termina, con plazo LOCK_TIMEOUT_MS. No hay rechazos por sondeo ni
 * esperas de duración fija; cada finalización despierta a todos los
 * esperadores de la página para que reintenten una sola vez. Si el dueño
 * remoto de una copia modificada murió, el home responde con un rechazo
 * terminal que se traduce a DSM_ERR_NODE_DOWN: la página no es servible hasta
 * que la recuperación de baja reasigne el directorio, y el hilo de aplicación
 * vuelve a fallar al reacceder. Ese estado nunca se reencola como BUSY.
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
 * múltiplo del tamaño de página configurado. La asignación se coordina con el
 * resto del clúster: el nodo vivo de menor ranura mantiene el mapa global de la
 * vista y entrega rangos únicos; los demás nodos le piden el rango por red. Si
 * el llamador es el coordinador, la búsqueda es first-fit sobre los huecos que
 * dejaron las liberaciones previas y solo después avanza el cursor de
 * asignación. La llamada es segura entre hilos del mismo proceso.
 *
 * Cada página del bloque queda anotada con el PID del proceso y con su índice
 * dentro del bloque (orden), datos que exponen dsmctl y los informes. Cada
 * reserva ocupa una entrada de la tabla interna de bloques vivos, de capacidad
 * acotada. Un bloque liberado con dsm_free() deja libre su entrada y puede
 * volver a entregarse a otra reserva.
 *
 * Si el coordinador no responde, la llamada devuelve DSM_ERR_NET: no hay una
 * reserva local silenciosa que pueda colisionar con la de otro nodo.
 *
 * Todos los nodos mapean la vista en la misma dirección base, pero el mapa
 * global entrega rangos distintos a cada reserva. Dos procesos que llaman a
 * dsm_alloc() no obtienen el mismo bloque: para compartir memoria, el creador
 * debe comunicar a los demás la dirección virtual que recibió y ellos usan esa
 * dirección dentro de su propia vista. La dirección es válida en todo el
 * clúster porque la vista se mapea con BASE_ADDR idéntica en cada nodo.
 *
 * @param size Cantidad de bytes a reservar. Debe ser mayor que 0.
 * @param out Puntero de salida donde se escribe la dirección virtual base del
 *        bloque reservado. No puede ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL o @p size es 0;
 *         DSM_ERR_NO_SPACE si el bloque no cabe en las páginas de la vista ni
 *         en ningún hueco libre, o si la tabla de bloques vivos está llena;
 *         DSM_ERR_NET si el coordinador no responde a la petición de rango;
 *         DSM_ERR_AGAIN si la respuesta proviene de un nodo que ya no es el
 *         coordinador; DSM_ERR_BUSY si el coordinador aún no reconstruyó el
 *         mapa global tras un relevo; DSM_ERR_PROTO si la respuesta del
 *         coordinador no describe un rango válido dentro de la vista.
 */
dsm_status_t dsm_alloc(uint64_t size, void **out);

/**
 * @brief Liberación de memoria reservada con dsm_alloc().
 *
 * Devuelve el bloque al espacio libre y lo deja disponible para una reserva
 * posterior. La llamada valida que @p ptr sea la dirección base de un bloque
 * vivo de la vista, que esté alineado a página y que @p size redondee al mismo
 * número de páginas con que se reservó; si alguna condición falla devuelve
 * DSM_ERR_INVALID sin modificar el estado. En éxito, todas las páginas del
 * bloque quedan en estado Invalid y sin permisos de acceso, marcadas como
 * inaccesibles para que la próxima reserva que las ocupe no observe el
 * contenido anterior, y se limpian el PID y el orden anotados en dsm_alloc().
 *
 * El rango se libera en el mapa global del coordinador y en la copia local del
 * nodo. Si el coordinador no confirma la liberación, el bloque sigue vivo y la
 * llamada devuelve DSM_ERR_NET para que pueda reintentarse.
 *
 * @param ptr Dirección base devuelta por dsm_alloc(). No puede ser NULL.
 * @param size Cantidad de bytes del bloque. Debe coincidir, tras redondear a
 *        páginas, con el tamaño reservado.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p ptr es NULL, no pertenece a la
 *         vista, está desalineado, no corresponde a un bloque vivo o @p size
 *         no coincide; DSM_ERR_NO_SPACE si el mapa global no admite más
 *         huecos; DSM_ERR_NET si el coordinador no responde o la página no se
 *         pudo persistir en su custodio; DSM_ERR_AGAIN si la respuesta
 *         proviene de un nodo que ya no es el coordinador; DSM_ERR_BUSY si
 *         alguna página del bloque tiene una transición de coherencia en
 *         curso o una copia remota no pudo invalidarse.
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
 * igual que dos nodos distintos. Si el hilo poseedor termina sin liberar, el
 * cerrojo permanece retenido hasta LOCK_TIMEOUT_MS. Al caer el custodio, los
 * nodos supervivientes migran la propiedad y la cola al nuevo custodio; si el
 * dueño también cayó, el cerrojo queda libre y se concede al primero de la
 * cola. Las concesiones de una época anterior se descartan.
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
 *         está inicializado; DSM_ERR_INVALID si @p n es cero o no coincide
 *         con el umbral ya fijado por el custodio; DSM_ERR_TIMEOUT
 *         si no llegan todos los participantes dentro del plazo;
 *         DSM_ERR_NO_SPACE si se agotan las entradas locales de espera o las
 *         ranuras de barrera del custodio; DSM_ERR_NET si el enlace con el
 *         custodio falla.
 */
dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n);

/* --- Ejecución distribuida --------------------------------------------- */

/**
 * Kernel de demostración disponible en todos los nodos.
 *
 * Copia @c nbytes desde la dirección de origen a la de destino de la
 * partición; si @c src_va es cero, rellena el destino con ceros. El resultado
 * informa en @c items la cantidad de bytes procesados y el CRC32 lo calcula
 * el nodo trabajador sobre el rango de salida.
 */
#define DSM_DIST_KERNEL_DEMO UINT32_C(0xD15D0001)

/**
 * @brief Descriptor de una partición de una ejecución distribuida.
 *
 * Define el rango de la vista compartida que un kernel procesa en un nodo
 * trabajador. Las direcciones @p src_va y @p dst_va pertenecen a la vista DSM
 * y @p nbytes expresa la longitud del rango de entrada en bytes.
 */
typedef struct {
    /** Identificador de la partición dentro del trabajo. */
    uint32_t id;
    /** Dirección virtual base del rango de entrada en la vista compartida. */
    uint64_t src_va;
    /** Dirección virtual base del rango de salida en la vista compartida. */
    uint64_t dst_va;
    /** Cantidad de bytes del rango de entrada. */
    uint64_t nbytes;
} dsm_dist_part_t;

/**
 * @brief Resultado devuelto por el kernel de una partición distribuida.
 *
 * El nodo trabajador rellena esta estructura al terminar la partición. El
 * campo @p crc32 resume los datos de salida y permite verificar el trabajo con
 * dsm_dist_verify().
 */
typedef struct {
    /** Ranura del nodo que ejecutó la partición. */
    uint32_t slot;
    /** Identificador de la partición a la que corresponde el resultado. */
    uint32_t part_id;
    /** Estado de la ejecución: cero indica éxito y distinto de cero un fallo. */
    uint32_t status;
    /** Cantidad de elementos procesados por la partición. */
    uint64_t items;
    /** CRC32 calculado sobre los datos de salida de la partición. */
    uint64_t crc32;
    /** Duración de la ejecución en microsegundos. */
    uint64_t us;
    /** Fallos de lectura presentados durante la ejecución de la partición. */
    uint32_t faults_r;
    /** Fallos de escritura presentados durante la ejecución de la partición. */
    uint32_t faults_w;
} dsm_dist_result_t;

/**
 * @brief Función kernel invocada para cada partición de un trabajo distribuido.
 *
 * @param arg Argumento opaco entregado al registrar el kernel con
 *        dsm_dist_register().
 * @param part Descriptor de la partición que esta invocación debe procesar.
 * @param out Estructura donde el kernel escribe el resultado de la partición.
 */
typedef void (*dsm_dist_kernel_fn)(void *arg, const dsm_dist_part_t *part,
                                   dsm_dist_result_t *out);

/**
 * @brief Registra una función kernel de ejecución distribuida.
 *
 * Asocia @p kernel_id con la función @p fn y con el argumento opaco @p arg,
 * que se entrega a cada invocación del kernel. Registrar un identificador ya
 * existente reemplaza su función y su argumento. El registro es local al nodo
 * y no se propaga al resto del cluster: el coordinador solo envía el
 * identificador de la partición, nunca el código. Por eso, un kernel debe
 * estar compilado en cada nodo que lo ejecute o registrado por la aplicación
 * en ese nodo después de dsm_init(). La biblioteca registra en todos los nodos
 * el kernel de demostración DSM_DIST_KERNEL_DEMO, de modo que un daemon sin
 * kernels de aplicación puede atender una copia o un CRC de rango.
 *
 * @param kernel_id Identificador del kernel.
 * @param fn Función a registrar. No puede ser NULL.
 * @param arg Argumento opaco que se entrega a @p fn en cada ejecución; puede
 *        ser NULL.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p fn es NULL; DSM_ERR_NO_SPACE si
 *         no hay entradas libres en la tabla local de kernels.
 */
dsm_status_t dsm_dist_register(uint32_t kernel_id, dsm_dist_kernel_fn fn,
                               void *arg);

/**
 * @brief Lanza un trabajo distribuido sobre un conjunto de nodos trabajadores.
 *
 * Asocia @p job_id con el kernel @p kernel_id y reparte las particiones de
 * @p parts entre los nodos de @p workers. Cada trabajador ejecuta el kernel
 * sobre las particiones que le corresponden y acumula su resultado, que se
 * consulta después con dsm_dist_collect(). El plazo @p timeout_ms acota la
 * fase de envío y arranque de las particiones.
 *
 * @param job_id Identificador del trabajo. No debe estar ya en uso.
 * @param kernel_id Identificador de un kernel registrado con
 *        dsm_dist_register().
 * @param parts Arreglo de particiones a ejecutar. No puede ser NULL si
 *        @p nparts es mayor que cero.
 * @param nparts Cantidad de particiones del arreglo @p parts.
 * @param workers Arreglo de ranuras de nodos trabajadores. No puede ser NULL
 *        si @p nworkers es mayor que cero.
 * @param nworkers Cantidad de nodos del arreglo @p workers. Debe ser mayor
 *        que cero.
 * @param timeout_ms Plazo en milisegundos para arrancar el trabajo. Debe ser
 *        mayor que cero.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si algún puntero obligatorio es NULL
 *         o algún conteo o plazo es cero; DSM_ERR_INVALID si @p kernel_id no
 *         está registrado, @p job_id ya está en uso o alguna ranura de
 *         @p workers no pertenece a la membresía; DSM_ERR_NO_SPACE si no hay
 *         entradas libres para el trabajo o sus particiones; DSM_ERR_NET si
 *         falla el envío a un trabajador; DSM_ERR_TIMEOUT si no se puede
 *         arrancar el trabajo dentro del plazo.
 */
dsm_status_t dsm_dist_launch(uint32_t job_id, uint32_t kernel_id,
                             const dsm_dist_part_t *parts, uint32_t nparts,
                             const uint32_t *workers, uint32_t nworkers,
                             uint32_t timeout_ms);

/**
 * @brief Recoge los resultados disponibles de un trabajo distribuido.
 *
 * Escribe en @p out hasta la capacidad indicada por @p n y devuelve en @p n la
 * cantidad real de resultados obtenidos. Las entradas se entregan en el orden
 * en que los trabajadores terminaron sus particiones. Una llamada posterior
 * devuelve los resultados que aún no se habían recogido.
 *
 * @param job_id Identificador del trabajo lanzado con dsm_dist_launch().
 * @param out Arreglo de salida donde se escriben los resultados. No puede ser
 *        NULL.
 * @param n Puntero de entrada y salida: contiene la capacidad de @p out y
 *        recibe la cantidad de resultados escritos. No puede ser NULL.
 * @param timeout_ms Plazo en milisegundos para esperar resultados. Cero indica
 *        una consulta no bloqueante.
 * @return DSM_OK en éxito, incluso si no hay resultados todavía;
 *         DSM_ERR_NOT_INIT si el subsistema no está inicializado;
 *         DSM_ERR_INVALID si @p out o @p n son NULL, si @p job_id no existe o
 *         si la capacidad indicada por @p n es cero sin ser una consulta no
 *         bloqueante; DSM_ERR_TIMEOUT si no llega ningún resultado dentro del
 *         plazo; DSM_ERR_NET si falla la recepción desde un trabajador.
 */
dsm_status_t dsm_dist_collect(uint32_t job_id, dsm_dist_result_t *out,
                              uint32_t *n, uint32_t timeout_ms);

/**
 * @brief Verifica el CRC32 agregado de un trabajo distribuido.
 *
 * Calcula el CRC32 del conjunto de resultados del trabajo @p job_id y lo
 * compara con @p expected_crc32. El valor agregado es el XOR de los CRC32 de
 * todas las particiones, de modo que no depende del orden de llegada; una
 * partición fallida aporta cero. La llamada espera a que todas las particiones
 * hayan terminado antes de decidir, con el plazo configurado en
 * DIST_TIMEOUT_MS.
 *
 * @param job_id Identificador del trabajo lanzado con dsm_dist_launch().
 * @param expected_crc32 CRC32 esperado para la salida agregada del trabajo.
 * @param ok Puntero de salida donde se escribe true si los CRC32 coinciden y
 *        false en caso contrario. No puede ser NULL.
 * @return DSM_OK si la comparación se pudo completar, con independencia del
 *         valor de @p ok; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p ok es NULL o @p job_id no
 *         existe; DSM_ERR_TIMEOUT si el trabajo no termina dentro del plazo;
 *         DSM_ERR_NET si falla la recepción desde un trabajador.
 */
dsm_status_t dsm_dist_verify(uint32_t job_id, uint64_t expected_crc32,
                             bool *ok);

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
 * incluye en el conteo. Quedan fuera los clientes y las ranuras que no
 * aportan memoria al clúster.
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
 * @brief Obtiene la tabla global de páginas del cluster en formato de texto.
 *
 * Recorre la información de páginas de todos los nodos presentes de la
 * membresía y escribe en @p out una tabla de texto con el identificador de
 * página, el nodo propietario, el orden de la página dentro de su bloque, el
 * copyset de nodos que conservan copia y la franja de paridad a la que
 * pertenece. La tabla termina con un carácter nulo si caben en @p cap bytes.
 *
 * @param first Índice de la primera página que se incluye en la tabla.
 * @param max Cantidad máxima de filas que se escriben.
 * @param out Búfer de salida donde se escribe la tabla. No puede ser NULL.
 * @param cap Capacidad del búfer @p out en bytes. Debe ser mayor que cero.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out es NULL o @p cap es cero;
 *         DSM_ERR_NO_SPACE si la tabla no cabe completa en @p cap bytes;
 *         DSM_ERR_NET si falla la consulta a algún nodo remoto.
 */
dsm_status_t dsm_pages_global(uint32_t first, uint32_t max, char *out,
                              size_t cap);

/**
 * @brief Genera los informes HTML de páginas del cluster.
 *
 * Crea un archivo HTML por nodo presente y un archivo global resumen dentro de
 * @p out_dir. El directorio se crea si no existe y el proceso debe tener
 * permisos de escritura. Cada archivo describe la tabla de páginas de su nodo
 * con el propietario, el proceso que reservó cada página, su orden dentro del
 * bloque, el estado MSI, el copyset y la franja de paridad; el archivo global
 * agrega las tablas de todos los nodos. Los clientes se muestran con su rol y
 * no se cuentan como nodos. Solo se consultan las páginas materializadas de la
 * vista local, de modo que la generación no recorre un millón de páginas sin
 * datos.
 *
 * @param out_dir Ruta al directorio donde se escriben los informes. Si es NULL
 *        o una cadena vacía se usa OUTPUT_DIR de la configuración.
 * @return DSM_OK en éxito; DSM_ERR_NOT_INIT si el subsistema no está
 *         inicializado; DSM_ERR_INVALID si @p out_dir y OUTPUT_DIR están
 *         vacíos; DSM_ERR_NO_SPACE si no se puede crear o escribir algún
 *         archivo en @p out_dir; DSM_ERR_NET si falla la consulta a algún nodo
 *         remoto.
 */
dsm_status_t dsm_html_report(const char *out_dir);

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
