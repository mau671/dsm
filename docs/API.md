# API del sistema DSM — Prototipo 1

Documento de referencia del API pública definida en `include/dsm.h` y
`include/dsm_types.h`. Describe cada función, sus parámetros, su valor de
retorno, los códigos de error que puede devolver y su estado real de
implementación. Toda la información corresponde al código actual de `src/`.

## Convenciones

- Todas las funciones devuelven `dsm_status_t`. `DSM_OK` (0) indica éxito; los
  valores negativos son errores.
- `dsm_strerror(st)` traduce cualquier código a texto.
- Cuando una función devuelve error no escribe en sus parámetros de salida.
- El rol del proceso se elige con la variable de entorno `DSM_NODE_MODE`, no con
  `dsm.conf`: `node` (por defecto) aporta memoria al clúster y puede ser
  custodio de páginas, mientras que `client` usa la memoria del clúster sin
  aportar ninguna y nunca es custodio. Un cliente no cuenta en
  `dsm_node_count` ni en `dsm_global_size`.
- La columna **Estado** distingue:
  - **Implementada**: la función realiza la operación descrita.
  - **Implementada (parcial)**: la función ejecuta parte de la operación y el
    alcance real se indica en la descripción.
  - **Stub**: la función solo valida que el subsistema esté inicializado y
    devuelve `DSM_OK`; no realiza la operación y no ofrece ninguna garantía
    sobre ella.

## Códigos de estado (`dsm_status_t`)

| Código | Valor | Significado | Devuelto por el API pública |
|---|---:|---|---|
| `DSM_OK` | 0 | Operación exitosa | Sí |
| `DSM_ERR_CONFIG` | -1 | Error de configuración: archivo ausente, no regular, mayor a 1 MiB, línea o valor inválido, o discrepancia de `raw_hash` con un par | `dsm_init` |
| `DSM_ERR_NOT_INIT` | -2 | El subsistema no está inicializado en este proceso | Todas salvo `dsm_init`, `dsm_is_initialized`, `dsm_strerror` |
| `DSM_ERR_MEM` | -3 | Fallo de asignación de memoria, de hilos, de semáforos, de `memfd_create`/`mmap`/`ftruncate` o de instalación de SIGSEGV | `dsm_init`, `dsm_thread_attach` |
| `DSM_ERR_NET` | -4 | Sin puerto libre, fallo de `listen`, conexión rechazada o socket caído | `dsm_init` |
| `DSM_ERR_TIMEOUT` | -5 | Tiempo de espera agotado (`request_timeout_ms`) | No (uso interno) |
| `DSM_ERR_NO_SPACE` | -6 | Vista compartida sin páginas libres o sin ranuras de nodo | `dsm_init`, `dsm_alloc` |
| `DSM_ERR_INVALID` | -7 | Puntero de salida NULL, tamaño cero o índice fuera de rango | `dsm_init`, `dsm_alloc`, `dsm_node_id`, `dsm_node_count`, `dsm_global_size`, `dsm_stats` |
| `DSM_ERR_NODE_DOWN` | -8 | Nodo destino fuera de la membresía activa | No (uso interno) |
| `DSM_ERR_BUSY` | -9 | Página con un fallo de página en curso | No (uso interno) |
| `DSM_ERR_PROTO` | -10 | Cabecera inválida, versión incompatible, tipo inesperado o CRC32 incorrecto | `dsm_init` |
| `DSM_ERR_INTR` | -11 | Llamada interrumpida por señal | No |
| `DSM_ERR_AGAIN` | -12 | Recurso no disponible de inmediato | No |
| `DSM_ERR_NOT_FOUND` | -13 | Sin pares en el cluster durante el descubrimiento | No (`dsm_init` lo consume para asumir el rol de primer nodo) |

## Ciclo de vida del nodo

| Función | Firma | Descripción | Parámetros | Retorno | Errores | Estado |
|---|---|---|---|---|---|---|
| `dsm_init` | `dsm_status_t dsm_init(const char *conf_path, uint64_t node_mem_override)` | Carga y valida la configuración, eleva el límite de descriptores, ignora SIGPIPE, inicializa cola de fallos, membresía, red, directorio MSI y memoria virtual (vista en la dirección base más alias RW sobre el mismo memfd), instala el manejador de SIGSEGV, arranca listener, workers y despachadores, y ejecuta descubrimiento y unión al cluster. Si no hay pares, un nodo asume la ranura 0 con época 1, mientras que un cliente falla con `DSM_ERR_NO_SPACE` porque no puede aportar la memoria que ninguna página tendría como custodio. Si ya está inicializado, no hace nada y devuelve `DSM_OK`. | `conf_path`: ruta a `dsm.conf`, `NULL` para `"dsm.conf"`.<br>`node_mem_override`: bytes de memoria física a aportar; 0 usa `NODE_MEM` del archivo, >0 lo reemplaza y valida de nuevo. Un cliente ignora este valor. | `DSM_OK` si el nodo quedó inicializado. En error, los recursos ya adquiridos se liberan y el subsistema queda sin inicializar. | `DSM_ERR_CONFIG`, `DSM_ERR_MEM`, `DSM_ERR_NET`, `DSM_ERR_PROTO`, `DSM_ERR_NO_SPACE`, `DSM_ERR_INVALID` | Implementada |
| `dsm_finalize` | `dsm_status_t dsm_finalize(void)` | Detiene el ciclo de ejecución, anuncia LEAVE, detiene heartbeat, despachadores y workers, cierra listener y conexiones, restaura el manejador de SIGSEGV previo, desmapea la memoria y destruye los módulos. Después, `dsm_is_initialized()` devuelve `false` y el subsistema puede inicializarse de nuevo. | Ninguno | `DSM_OK` si se finalizó. | `DSM_ERR_NOT_INIT` | Implementada |
| `dsm_is_initialized` | `bool dsm_is_initialized(void)` | Indica si existe contexto DSM activo en el proceso. | Ninguno | `true` entre un `dsm_init` exitoso y `dsm_finalize`; `false` en otro caso. | Ninguno | Implementada |

## Gestión de hilos de aplicación

| Función | Firma | Descripción | Parámetros | Retorno | Errores | Estado |
|---|---|---|---|---|---|---|
| `dsm_thread_attach` | `dsm_status_t dsm_thread_attach(void)` | Asigna al hilo una pila alternativa de 64 KiB con `sigaltstack`, necesaria para que el manejador de SIGSEGV atienda los fallos de la vista compartida. Obligatoria en todo hilo que acceda a memoria reservada con `dsm_alloc`. Si el hilo ya estaba vinculado, no hace nada y devuelve `DSM_OK`. | Ninguno | `DSM_OK` si el hilo quedó vinculado. | `DSM_ERR_NOT_INIT`, `DSM_ERR_MEM` | Implementada |
| `dsm_thread_detach` | `dsm_status_t dsm_thread_detach(void)` | Desactiva la pila alternativa (`SS_DISABLE`) y libera su memoria. Si el hilo no estaba vinculado, no hace nada y devuelve `DSM_OK`. | Ninguno | `DSM_OK` si el subsistema está inicializado. | `DSM_ERR_NOT_INIT` | Implementada |

## Memoria compartida distribuida

| Función | Firma | Descripción | Parámetros | Retorno | Errores | Estado |
|---|---|---|---|---|---|---|
| `dsm_alloc` | `dsm_status_t dsm_alloc(uint64_t size, void **out)` | Asigna un bloque contiguo de páginas a partir del primer bloque libre de la vista local, redondeando `size` a páginas completas, y devuelve su dirección base. La reserva es local al nodo: no coordina la asignación con otros nodos ni la notifica al cluster. Segura entre hilos del proceso. El bloque no se libera (ver `dsm_free`). | `size`: bytes a reservar, mayor que 0.<br>`out`: salida con la dirección virtual base; no puede ser NULL. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID`, `DSM_ERR_NO_SPACE` | Implementada |
| `dsm_free` | `dsm_status_t dsm_free(void *ptr, uint64_t size)` | Libera un bloque reservado con `dsm_alloc` y devuelve sus páginas a la lista de huecos libres, que se mantiene ordenada por primera página y se fusiona con los rangos contiguos. La reutilización es first-fit: la siguiente reserva toma el primer hueco que alcance el tamaño pedido, partiendo el resto si sobra. Las páginas liberadas quedan en ceros, con `PROT_NONE` y estado `I`, de modo que ninguna reserva posterior observa contenido anterior. | `ptr`: dirección devuelta por `dsm_alloc`, alineada a página.<br>`size`: bytes del bloque, deben redondear a las mismas páginas que la reserva original. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID`, `DSM_ERR_NO_SPACE` | Implementada |
| `dsm_sync` | `dsm_status_t dsm_sync(void)` | Recorre la tabla local y persiste las páginas en estado `M`: si el nodo es home de la página copia el contenido al marco de respaldo, y si no lo es envía `WRITEBACK` al home y espera su confirmación. Las páginas `S` e `I` no se tocan. El resultado refleja el primer fallo de persistencia encontrado, de modo que un respaldo incompleto no se reporta como éxito. | Ninguno | `DSM_OK` si todas las páginas se persistieron. | `DSM_ERR_NOT_INIT`, `DSM_ERR_NET`, `DSM_ERR_TIMEOUT`, `DSM_ERR_MEM` | Implementada |

## Primitivas de sincronización distribuida

| Función | Firma | Descripción | Parámetros | Retorno | Errores | Estado |
|---|---|---|---|---|---|---|
| `dsm_lock` | `dsm_status_t dsm_lock(uint32_t lock_id)` | Adquiere el cerrojo distribuido `lock_id`. El custodio se calcula de forma determinista con el anillo de dispersión, así que todos los nodos coinciden. Si el custodio es el nodo local y el cerrojo está libre, la concesión es inmediata y sin red; si está ocupado, el solicitante entra en la cola FIFO del custodio y espera con `sem_timedwait` durante `LOCK_TIMEOUT_MS`. La propiedad se registra por hilo, de modo que solo el hilo que lo tomó puede liberarlo. | `lock_id`: identificador del cerrojo. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID`, `DSM_ERR_BUSY`, `DSM_ERR_TIMEOUT`, `DSM_ERR_NET` | Implementada |
| `dsm_unlock` | `dsm_status_t dsm_unlock(uint32_t lock_id)` | Libera el cerrojo distribuido `lock_id` que el hilo llamador posee. Si el custodio es local, la concesión pasa al siguiente de la cola FIFO o el cerrojo queda libre; si es remoto, se envía `LOCK_REL` al custodio. Un hilo que no posee el cerrojo recibe `DSM_ERR_INVALID`. | `lock_id`: identificador del cerrojo. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID`, `DSM_ERR_NET` | Implementada |
| `dsm_barrier` | `dsm_status_t dsm_barrier(uint32_t barrier_id, uint32_t n)` | Barrera distribuida de `n` participantes. El custodio, calculado con el anillo de dispersión sobre un dominio distinto al de los cerrojos, cuenta los arribos y difunde `BARRIER_GO` al alcanzar el umbral. Cada espera se libera con una generación posterior a la suya, así que un aviso atrasado o duplicado no destapa una ronda futura. Los participantes esperan con `sem_timedwait` durante `LOCK_TIMEOUT_MS`. | `barrier_id`: identificador de la barrera.<br>`n`: participantes esperados en la ronda. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID`, `DSM_ERR_TIMEOUT`, `DSM_ERR_NET` | Implementada |

## Consultas y diagnóstico

| Función | Firma | Descripción | Parámetros | Retorno | Errores | Estado |
|---|---|---|---|---|---|---|
| `dsm_node_id` | `dsm_status_t dsm_node_id(uint32_t *out)` | Escribe la ranura del nodo local en la membresía (0..`MAX_NODES`-1). | `out`: salida; no puede ser NULL. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID` | Implementada |
| `dsm_node_count` | `dsm_status_t dsm_node_count(uint32_t *out)` | Cuenta las ranuras presentes de la membresía cuyo estado no es `DSM_NODE_DEAD` (es decir, ALIVE y SUSPECT) y que aportan memoria. Incluye al nodo local y excluye a los clientes, de modo que lanzar una aplicación no altera el conteo. | `out`: salida; no puede ser NULL. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID` | Implementada |
| `dsm_global_size` | `dsm_status_t dsm_global_size(uint64_t *out)` | Suma las páginas aportadas por los nodos presentes y no muertos (ALIVE y SUSPECT), sin contar a los clientes, y multiplica por el tamaño de página configurado. | `out`: salida en bytes; no puede ser NULL. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID` | Implementada |
| `dsm_stats` | `dsm_status_t dsm_stats(dsm_stats_t *out)` | Sobrescribe `out` con los contadores acumulados del nodo y el conteo actual de páginas locales en estado M, S e I. Los valores previos de `out` se descartan. | `out`: estructura de salida; no puede ser NULL. | `DSM_OK` en éxito. | `DSM_ERR_NOT_INIT`, `DSM_ERR_INVALID` | Implementada |
| `dsm_strerror` | `const char *dsm_strerror(dsm_status_t st)` | Devuelve la descripción estática en español del código. Para valores desconocidos devuelve `"Error desconocido"`. La cadena no debe liberarse ni modificarse. | `st`: código a describir. | Puntero a cadena estática de solo lectura; nunca NULL. | Ninguno | Implementada |
| `dsm_debug_dump_fd` | `dsm_status_t dsm_debug_dump_fd(int fd)` | Escribe al descriptor las líneas con ranura y época del nodo, dirección base y tamaño de la vista, y cantidad de nodos activos. Los errores de escritura no se detectan. | `fd`: descriptor abierto para escritura (por ejemplo `STDERR_FILENO`). | `DSM_OK` si está inicializado. | `DSM_ERR_NOT_INIT` | Implementada |

## Tipos públicos

### `dsm_status_t`
Códigos de retorno del API. Detalle en la tabla de códigos de estado.

### `dsm_page_state_t` — estado MSI local
| Valor | Número | Significado |
|---|---:|---|
| `DSM_PAGE_I` | 0 | Invalid: página ausente o no válida localmente; el acceso genera fallo. |
| `DSM_PAGE_S` | 1 | Shared: presente en modo lectura, compartible con otros nodos. |
| `DSM_PAGE_M` | 2 | Modified: única copia válida, local y modificada. |
| `DSM_PAGE_IS_PEND` | 3 | Invalid-Shared pendiente: se pidió copia de lectura y se espera respuesta del home. |
| `DSM_PAGE_IM_PEND` | 4 | Invalid-Modified pendiente: se pidió escritura desde I y se espera respuesta. |
| `DSM_PAGE_SM_PEND` | 5 | Shared-Modified pendiente: se pidió escritura desde S y se espera la invalidación de las demás copias. |

Los valores numéricos viajan en los mensajes del protocolo: no reordenar.

### `dsm_dir_state_t` — estado en el directorio del home
| Valor | Número | Significado |
|---|---:|---|
| `DSM_DIR_UNCACHED` | 0 | Ningún nodo conserva copia; el home es la única fuente. |
| `DSM_DIR_SHARED` | 1 | Varios nodos conservan copias de lectura; el home tiene el contenido válido. |
| `DSM_DIR_MODIFIED` | 2 | Un nodo concreto posee la copia modificada; el home no tiene el contenido actualizado. |

### `dsm_node_state_t` — estado de membresía
| Valor | Número | Significado |
|---|---:|---|
| `DSM_NODE_ALIVE` | 1 | Responde a los heartbeats y participa del cluster. |
| `DSM_NODE_SUSPECT` | 2 | Dejó de responder pero aún no se le declara muerto. |
| `DSM_NODE_DEAD` | 3 | Se retiró o fue declarado muerto; su ranura queda libre. |

### `dsm_status_code_t` — diagnóstico de mensajes
Códigos que viajan en el campo `status` de la cabecera de los mensajes. No son
valores de retorno del API pública.

| Valor | Número | Significado |
|---|---:|---|
| `DSM_ST_OK` | 0 | Sin condición de rechazo. |
| `DSM_ST_STALE_EPOCH` | 1 | Mensaje de una época anterior a la vigente. |
| `DSM_ST_NOT_HOME` | 2 | El receptor no es el home de la página. |
| `DSM_ST_NOT_CLUSTER` | 3 | El emisor no pertenece al cluster indicado. |
| `DSM_ST_BAD_STATE` | 4 | La página no admite la transición solicitada. |
| `DSM_ST_BUSY` | 5 | Recurso ocupado por otra operación. |
| `DSM_ST_SLOT_TAKEN` | 6 | La ranura ya está asignada a otro nodo. |
| `DSM_ST_NO_SPACE` | 7 | Sin páginas o ranuras disponibles. |
| `DSM_ST_CONFIG_MISMATCH` | 8 | `raw_hash` distinto entre pares. |

### Banderas de cabecera
| Bandera | Valor | Significado |
|---|---:|---|
| `DSM_FLAG_WANT_DATA` | `1u << 0` | El emisor solicita el contenido de la página. |
| `DSM_FLAG_HAS_DATA` | `1u << 1` | El mensaje transporta el contenido de una página. |
| `DSM_FLAG_UPGRADE` | `1u << 2` | La solicitud pide permiso de escritura. |
| `DSM_FLAG_RETRY` | `1u << 3` | Reintento de una solicitud anterior. |
| `DSM_FLAG_PROBE` | `1u << 4` | Sonda de descubrimiento de pares. |

### `dsm_stats_t`
| Campo | Tipo | Significado |
|---|---|---|
| `read_faults` | `uint64_t` | Fallos de página de lectura atendidos por SIGSEGV. |
| `write_faults` | `uint64_t` | Fallos de página de escritura atendidos por SIGSEGV. |
| `msgs_sent` | `uint64_t` | Mensajes enviados a otros nodos. |
| `msgs_recv` | `uint64_t` | Mensajes recibidos de otros nodos. |
| `bytes_sent` | `uint64_t` | Bytes enviados, incluida cabecera y payload. |
| `bytes_recv` | `uint64_t` | Bytes recibidos, incluida cabecera y payload. |
| `invalidations` | `uint64_t` | Invalidaciones de copias remotas emitidas o recibidas. |
| `fault_latency_us_total` | `uint64_t` | Suma acumulada de latencia de resolución de fallos, en microsegundos. |
| `pages_m` | `uint64_t` | Páginas locales en estado Modified. |
| `pages_s` | `uint64_t` | Páginas locales en estado Shared. |
| `pages_i` | `uint64_t` | Páginas locales en estado Invalid. |
| `pending_count` | `uint32_t` | Operaciones de página pendientes de respuesta. |
| `wait_pool_used` | `uint32_t` | Entradas ocupadas del conjunto de espera. |
| `slots_enqueued` | `uint32_t` | Fallos encolados para el despachador. |
| `slots_woken` | `uint32_t` | Fallos resueltos y despertados por el despachador. |

### Constantes
| Constante | Valor | Significado |
|---|---|---|
| `MAX_NODES` | 256 | Ranuras máximas de membresía. |
| `DSM_SLOT_NONE` | `0xFFFFFFFF` | Sin ranura asignada. |
| `DSM_PAGE_ID_NONE` | `0xFFFFFFFF` | Sin página válida. |
| `DSM_TXN_NONE` | `0xFFFFFFFF` | Sin transacción asociada. |
| `DSM_WAIT_NONE` | `0xFFFFFFFF` | Sin entrada en cola de espera. |
| `DSM_MAX_PAYLOAD` | 1 MiB | Payload máximo de un mensaje. |
| `DSM_FAULTQ_CAP` | 4096 | Capacidad de la cola interna de fallos. |

## Ejemplo de uso

```c
#include "dsm.h"
#include <stdio.h>

int main(void)
{
    if (dsm_init(NULL, 0) != DSM_OK) {
        fprintf(stderr, "dsm_init: %s\n", dsm_strerror(DSM_ERR_CONFIG));
        return 1;
    }
    if (dsm_thread_attach() != DSM_OK) {
        dsm_finalize();
        return 1;
    }

    void *buf = NULL;
    dsm_status_t st = dsm_alloc(4096, &buf);
    if (st != DSM_OK) {
        fprintf(stderr, "dsm_alloc: %s\n", dsm_strerror(st));
    } else {
        ((int *)buf)[0] = 42;   /* el fallo de página lo resuelve el sistema */
    }

    uint32_t self = 0, nodes = 0;
    uint64_t total = 0;
    dsm_stats_t stats;
    dsm_node_id(&self);
    dsm_node_count(&nodes);
    dsm_global_size(&total);
    dsm_stats(&stats);
    printf("nodo %u de %u, %llu bytes, %llu fallos de lectura\n",
           self, nodes, (unsigned long long)total,
           (unsigned long long)stats.read_faults);

    dsm_thread_detach();
    dsm_finalize();
    return 0;
}
```
