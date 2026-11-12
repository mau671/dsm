# Entregable 2: Prototipo 1 y arquitectura actualizada

## 1. Resumen

El Prototipo 1 integra una primera versión ejecutable del sistema de memoria
compartida distribuida (DSM) sobre Linux, pthreads y TCP. El objetivo principal
de esta etapa fue validar la inicialización de dos nodos, el descubrimiento de
pares, el transporte de mensajes y el flujo mínimo de coherencia MSI para una
página:

1. El nodo A escribe una página.
2. El nodo B solicita la página y la lee.
3. El nodo B solicita la propiedad de escritura, invalida la copia de A y
   escribe un nuevo valor.
4. El nodo A vuelve a solicitar la página y observa el valor actualizado.

El resultado corresponde a un prototipo funcional acotado, no a una
implementación completa de todas las primitivas previstas para DSM.

## 2. Cambios respecto al diseño del Entregable 1

### 2.1 Sustitución de la capa de red inicial

El diseño inicial contemplaba `network.c` y `protocol.h` como una capa de red
separada. Esa organización fue eliminada y reemplazada por una integración más
modular:

- `src/net.c` / `include/net.h`: sockets TCP, conexiones con timeout, envío y
  recepción, listener, RPC y correlación de respuestas mediante `req_id`.
- `src/wire.c` / `include/wire.h`: serialización y validación del protocolo,
  empaquetado de cabeceras y cargas, nombres de mensajes y comprobación de
  longitudes.
- `src/util.c` / `include/util.h`: operaciones comunes de E/S completa,
  deadlines, `poll`, conexión con timeout, `MSG_NOSIGNAL` y utilidades de
  descriptores.

La sustitución evita mantener dos implementaciones de red en paralelo y
reutiliza las primitivas robustas que ya habían sido probadas de forma aislada.
También permite que descubrimiento, membresía, MSI y workers utilicen la misma
representación de mensajes.

### 2.2 Protocolo unificado

El protocolo actual utiliza una cabecera lógica serializada de **56 bytes**.
La estructura C en memoria no se transmite directamente: `wire.c` empaqueta
cada campo en offsets definidos y convierte los enteros al orden de red.

La cabecera incluye, entre otros campos:

- `magic` y versión del protocolo;
- identificador de cluster;
- época y slots de origen/destino;
- `req_id` para correlacionar RPC;
- identificador de página y secuencia de transición;
- tipo de acceso, flags, estado y longitud de carga;
- CRC32.

El CRC32 IEEE 802.3 se calcula sobre la cabecera con el campo CRC en cero y
sobre la carga. La recepción valida la cabecera, el cluster, la longitud y la
integridad antes de entregar el mensaje al worker. Esto reemplaza la
dependencia de estructuras transmitidas crudas y hace explícito el formato del
cable.

### 2.3 Descubrimiento y membresía

Se incorporó `src/discovery.c` como componente propio de descubrimiento y
adhesión al cluster. El nodo:

1. carga o crea su UUID;
2. reserva el primer puerto disponible del rango configurado;
3. sondea los hosts y puertos configurados mediante `HELLO`;
4. recibe `WELCOME` con la membresía;
5. valida la configuración mediante el hash de `dsm.conf`;
6. envía la solicitud de unión;
7. mantiene la membresía mediante listener y heartbeats.

El UUID puede distinguirse mediante `DSM_NODE_TAG`, necesario para ejecutar dos
nodos locales que compartan la misma configuración sin que se consideren el
mismo proceso.

### 2.4 Separación entre señal, despacho y workers

El manejador de `SIGSEGV` no resuelve la página ni realiza operaciones de red.
Registra el fallo en `faultq` y bloquea el hilo de aplicación mediante futex.
Un dispatcher separado consume la cola y ejecuta `msi_local_fault()`. Los
workers reciben mensajes de red y despachan cada tipo de mensaje a su handler
MSI correspondiente.

Esta separación mantiene el handler de señal limitado a operaciones
compatibles con el contexto de señal y deja la lógica bloqueante fuera de
`SIGSEGV`.

### 2.5 Memoria virtual y MSI

La vista DSM de la aplicación se reserva con `mmap` inicialmente como
`PROT_NONE`. El sistema conserva un alias interno de lectura/escritura para
instalar y copiar datos sin acceder directamente a la vista protegida.

Los estados locales implementados son:

- `I`, `S`, `M`;
- `IS_PEND`, `IM_PEND`, `SM_PEND`.

El directorio del home utiliza `UNCACHED`, `SHARED` y `MODIFIED`, además de
owner y conjunto de copias. Las transiciones principales se realizan mediante
`PAGE_REQUEST`, `PAGE_DATA`, `GRANT`, `INVALIDATE` y `DOWNGRADE`.

## 3. Qué quedó implementado y probado

### 3.1 Implementado

Quedó implementado el siguiente subconjunto:

- carga y validación de configuración;
- reserva de memoria virtual y alias interno;
- instalación del manejador de `SIGSEGV`;
- cola de fallos y dispatcher;
- descubrimiento, unión, membresía y heartbeat;
- listener y workers TCP;
- protocolo serializado con cabecera de 56 bytes y CRC32;
- envío y recepción con timeouts;
- RPC con correlación por `req_id`;
- fallo local de lectura y escritura;
- adquisición remota de página;
- downgrade de una copia `M` a `S`;
- invalidación de copias remotas;
- aplicación de permisos `PROT_NONE`, `PROT_READ` y
  `PROT_READ | PROT_WRITE`;
- estadísticas públicas mediante `dsm_stats`;
- aplicación de prueba basada solamente en `include/dsm.h`;
- arnés de integración de dos nodos.

### 3.2 Pruebas ejecutadas

Se consideran únicamente los resultados disponibles del estado actual:

#### Pruebas unitarias

Comando:

```bash
make test-unit
```

Resultado reportado: **53 pruebas pasaron sin fallos**.

#### Compilación con sanitizadores

Comandos:

```bash
make asan
make tsan
```

Resultado reportado: ambos perfiles **compilaron correctamente**. Esto no se
presenta como una ejecución completa bajo ASan o TSan, sino como una
compilación exitosa de dichos perfiles.

#### Integración de dos nodos

Comando:

```bash
make test-int
```

Resultado observado:

```text
B_READ PASS
B_AFTER_WRITE PASS
A_AFTER_B_WRITE PASS
PASS: flujo de página entre dos nodos
```

La prueba también imprimió estadísticas antes y después de las operaciones.
Los contadores mostraron los fallos de lectura y escritura esperados, así como
el cambio de estados entre `M`, `S` e `I`.

## 4. Pendientes y plan para el Prototipo 2

### 4.1 Pendientes conocidos

El repositorio todavía contiene partes deliberadamente incompletas:

- locks distribuidos: los handlers de solicitud y liberación aún no ejecutan
  el protocolo completo;
- barreras distribuidas: los handlers de llegada y continuación son stubs;
- `dsm_free`: la liberación colectiva todavía no está completada;
- writeback: `msi_on_writeback()` no está implementado y
  `msi_writeback_page()` devuelve éxito sin persistir una página;
- `dsm_mem_install_local()`, `dsm_mem_frame_alloc()` y
  `dsm_mem_frame_free()` son placeholders del manejo avanzado de frames;
- `msi_on_invalidate_ack()` y `msi_on_downgrade_ack()` no contienen lógica
  propia; las esperas del flujo actual se apoyan en la correlación RPC;
- `net_probe()` permanece como stub;
- no está implementada la recuperación completa ante caída de un home o
  transferencia de ownership;
- la asignación colectiva y la gestión general del directorio todavía son
  mínimas.

Por lo tanto, el resultado probado es el flujo mínimo de una página entre dos
nodos, no un DSM listo para cargas generales ni para tolerancia a fallos.

### 4.2 Plan propuesto para el Prototipo 2

1. **Cerrar el ciclo de coherencia**
   - completar writeback y persistencia de páginas `M`;
   - validar versiones y secuencias (`tseq`) en todas las respuestas;
   - formalizar las colas de espera y las respuestas duplicadas o tardías.

2. **Completar la API colectiva**
   - implementar `dsm_free`;
   - implementar locks y unlocks distribuidos;
   - implementar barreras por generación;
   - añadir pruebas de orden colectivo y concurrencia.

3. **Robustecer el directorio**
   - manejar explícitamente cambios de home;
   - completar el movimiento de páginas;
   - incorporar recuperación ante nodo caído;
   - verificar que no se mantengan locks locales mientras se espera red.

4. **Ampliar la validación**
   - pruebas con más de dos nodos;
   - lecturas y escrituras concurrentes;
   - mensajes corruptos, timeouts y desconexiones;
   - ejecución de las pruebas bajo ASan y TSan, además de la compilación;
   - medición de latencia de fallos y volumen de mensajes.

5. **Documentar la API**
   - especificar qué operaciones son colectivas;
   - documentar la obligación de `dsm_thread_attach`;
   - documentar los límites actuales y códigos de error observables.

## 5. Dificultades encontradas

### 5.1 Integración de capas de red

La principal dificultad fue consolidar una capa de red previamente separada
sin conservar implementaciones duplicadas. Fue necesario separar el transporte
(`net.c`), el formato (`wire.c`) y las operaciones comunes (`util.c`), y
mantener compatibilidad entre descubrimiento, membresía, workers y MSI.

### 5.2 Formato de mensajes

La estructura de cabecera en memoria tiene alineación natural distinta del
formato transmitido. Por eso no se puede enviar la estructura C directamente:
el empaquetado campo por campo y el CRC32 son necesarios para evitar
dependencia de padding, endianess y corrupción silenciosa.

### 5.3 Fallos de página y señales

El acceso a una página `PROT_NONE` ocurre dentro de un handler de `SIGSEGV`,
pero la resolución requiere locks, memoria, sockets y esperas con timeout.
Separar el registro del fallo de su resolución mediante `faultq`, futex,
dispatcher y workers fue necesario para no realizar operaciones no seguras
dentro del handler.

### 5.4 Coordinación MSI

El flujo de lectura y escritura combina estados pendientes, directorio,
invalidaciones, downgrade y respuestas RPC. La implementación actual permite
demostrar el caso mínimo, pero todavía requiere ampliar la validación de
versiones, duplicados, fallos y concurrencia antes de considerarse completa.

### 5.5 Prueba de integración

El arnés de dos nodos tuvo inicialmente un problema de lectura de la salida:
`fgets()` podía conservar líneas en el buffer de `FILE *` mientras el
`poll()` observaba el descriptor del pipe. Se corrigió el lector del arnés
configurando la salida como no bufferizada. La corrección fue localizada en la
prueba y no modificó el código del núcleo DSM.

## 6. Conclusión

El Prototipo 1 valida la arquitectura actualizada y demuestra un flujo mínimo
de memoria DSM entre dos nodos con coherencia MSI. La sustitución de
`network.c`/`protocol.h` por `net.c`, `wire.c`, `util.c` y `discovery.c`
produjo una organización más clara y reutilizable. Las pruebas unitarias se ejecutan con AddressSanitizer y UBSan (make test-unit),
sin errores reportados. El perfil ThreadSanitizer (make tsan) compiló
correctamente, pero no se ejecutó la suite bajo TSan; queda como tarea del
Prototipo 2.

El siguiente prototipo debe concentrarse en completar las operaciones
colectivas, writeback, recuperación ante fallos y pruebas de concurrencia,
manteniendo explícitos los límites que todavía son stubs.
