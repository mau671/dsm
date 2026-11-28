# Guía de redacción de la entrega final

Este archivo no forma parte del PDF. Sirve para que todas las secciones compartan
criterio, nombres, cifras y estilo.

## Documentos

- Informe técnico: `SO-P1-EntregaFinal-MauricioGonzalezPrendas-SamuelDuranMaroto.tex`
- Manual de usuario: `ManualDeUsuario.tex`
- Secciones del informe: `secciones/*.tex`
- Figuras: `figuras/*.png` y diagramas TikZ dentro de cada sección
- No modificar nada de `docs/Entregable01`, `docs/Entregable02` ni `docs/Entregable03`.

## Tono y estilo

- Español de Costa Rica, sentence case en títulos.
- Presente descriptivo: el sistema *hace*, *mantiene*, *resuelve*, *garantiza*.
- No escribir “se implementó”, “se cambió”, “se corrigió”, “antes/ahora”, “en esta entrega”.
- No escribir capítulos de cambios respecto a prototipos anteriores.
- No crear tablas de “resumen”, “comparación”, “ventajas y desventajas” ni similares.
  La síntesis va en prosa. Solo se permiten tablas de catálogo técnico
  (parámetros, mensajes, API) y de mediciones puntuales.
- No usar “limitaciones” como capítulo. Las condiciones de operación se explican
  dentro del mecanismo correspondiente. Las ampliaciones posibles van en una
  subsección breve al final.
- Evitar muletillas repetidas: “asimismo”, “además”, “por otro lado”, “cabe destacar”.
- Evitar oraciones iniciales muy cortas. Cada párrafo desarrolla una idea.
- Las figuras llevan pie descriptivo, no “así se ve”. Ejemplo:
  “Reporte global de páginas del clúster.” y el texto explica qué muestra.

## Nombres y rutas vigentes

- Binarios: `dsm`, `dsmctl`, `demo`, `matmul`, `juego_vida`, `filtro_video`, `video_pack`.
- Rutas: `./build/bin/release/<binario>`.
- Datos: `data/video_128x96.rgb`, `data/tablero_192.gray`.
- Aplicaciones: `apps/demo.c`, `apps/matmul.c`, `apps/juego_vida.c`,
  `apps/filtro_video.c`; herramientas: `tools/dsmctl.c`, `tools/video_pack.c`.
- No usar nombres viejos: `dsm_dist_demo`, `dsm_raid_demo`, `matmul_dsm`,
  `test_basico`, `datos/`.
- `demo` tiene los roles `dist coord`, `dist worker`, `raid hold`,
  `raid produce`, `msi a`, `msi b`.

## Arquitectura y conceptos

- La vista virtual se mapea con `mmap` sobre `memfd` y se protege con `mprotect`.
- Los fallos de página llegan por `SIGSEGV` con `SA_SIGINFO`; el manejador obtiene
  `siginfo_t.si_addr`, identifica la página y encola el fallo.
- Grupos de hilos: hilo de señal, despachador(es) y trabajadores.
- Coherencia MSI: `M`, `S`, `I`, más los estados transitorios `IS_PEND`, `IM_PEND`
  y `SM_PEND`.
- Directorio en el nodo custodio (`home`), transacciones por página, `tseq`,
  acuses de invalidación y respuestas huérfanas.
- Anillo de dispersión consistente para asignar custodios.
- Asignador global: coordinador en el nodo vivo de menor ranura; mapa global
  replicado por `WELCOME`; huecos y marca de agua.
- Redundancia: `REDUNDANCY=true` exige al menos tres nodos vivos; `K` se deriva
  de la membresía como `nodos vivos - 1`, acotado entre 2 y 8. Franjas con paridad
  XOR, deltas y reconstrucción.
- Ejecución distribuida: registro de kernels por proceso, `DIST_RUN` y `DIST_DONE`,
  particiones descritas por direcciones de la vista.
- Descubrimiento: multicast UDP en `239.255.42.99:27699` con broadcast de respaldo;
  nodos escuchan en `27600-27698`.
- Configuración: once claves compartidas (`CLUSTER_ID`, `PAGE_SIZE`, `BASE_ADDR`,
  `VIEW_SIZE`, `PORT_MIN`, `PORT_MAX`, `DISCOVERY_GROUP`, `DISCOVERY_PORT`,
  `VNODES_PER_MB`, `HOME_SLACK`, `REDUNDANCY`). `NODE_MEM` es local y puede variar.
  El hash compartido viaja en el saludo y rechaza configuraciones incompatibles.
- Roles: nodo (aporta memoria) y cliente (no aporta memoria).
- `hb_missed` cuenta transiciones a SUSPECT y DEAD, no cada intervalo de retraso.

## Estructura del informe

Cada archivo de `secciones/` empieza directamente con `\section{...}` y usa
`\subsection`, `\subsubsection`, `\label`. No incluir preámbulo ni
`\begin{document}`. El archivo principal ya carga paquetes, portada, índices y
encabezados.

Etiquetas únicas por sección, por ejemplo `\label{sec:msi}`,
`\label{fig:msi-estados}`, `\label{tab:mensajes}`.

Para código usar `\texttt{}` o `lstlisting` con estilo ya definido. Escapar
`_`, `%`, `&`, `#` fuera de `\texttt` cuando corresponda.

## Cifras verificadas

- Compilación: `make release`, `make debug`, `make asan` y `make tsan` sin avisos.
- `make test-e2e`: tres escenarios, todos PASS.
  - Distribuida: `DIST PASS remote=4 total=8 verify=ok lectura=ok`.
  - RAID: 6 franjas reconstruidas, 5 páginas verificadas tras la caída.
  - MSI: flujo entre dos procesos, PASS.
- Clúster local de cuatro nodos con 64 MiB por nodo, 256 MiB totales:
  `hb_missed=0` en las cuatro ranuras; `matmul 256 --hilos 4` con checksum
  `41107839280` igual a la referencia, 0 de 65536 elementos distintos,
  `dsm_sync` en 0.519 s, cálculo en 0.253 s y referencia secuencial en 0.048 s.
- Clúster físico del laboratorio: cuatro equipos, 640 MiB aportados
  (256 MiB + 128 MiB + 128 MiB + 128 MiB); `matmul 256` verificó checksum y
  0 de 65536 elementos distintos; el reporte HTML se generó para los cuatro nodos.
- Reporte HTML de evidencia: cinco procesos locales (cuatro nodos con 64 MiB y
  un proceso de aplicación). Figuras `figuras/html-global.png` y
  `figuras/html-nodo.png`.
- No inventar tiempos, direcciones IP ni resultados que no estén en los archivos
  del repositorio.

## Figuras sugeridas

- Arquitectura de componentes y despliegue.
- Ciclo de vida de un nodo y secuencia de descubrimiento/JOIN.
- Mapa de la vista virtual, páginas y marcos.
- Máquina de estados MSI y secuencias de lectura/escritura.
- Modelo de hilos y flujo de un fallo de página.
- Franjas, paridad y reconstrucción.
- Flujo de ejecución distribuida.
- Cabecera del protocolo.
- Capturas HTML global y por nodo.
- Diagrama del clúster de validación.

Los diagramas TikZ deben ser legibles en A4, con colores suaves ya definidos en
la plantilla (`softblue`, `softgreen`, `softorange`, `softpurple`, `softteal`,
`softyellow`) y flechas `-{Latex}`. No saturar: máximo 12 nodos por figura.
