# Proyecto 01 — Sistema de Memoria Compartida Distribuida (DSM)

TEC / Sistemas Operativos — Prototipo 1.

## Estructura

- `include/`, `src/` — núcleo DSM (config, membresía, red, wire, memoria, MSI, sincronización).
- `apps/` — `dsmd` (demonio de nodo).
- `tools/` — `dsmctl` (CLI de diagnóstico).
- `tests/` — pruebas unitarias e integración.
- `dsm.conf` — configuración del clúster (bloque de puertos 27600–27699).

## Compilación

    make release   # -O2 -DNDEBUG
    make debug     # -g3 -O0
    make asan      # AddressSanitizer + UBSan
    make tsan      # ThreadSanitizer

## Pruebas

    make test-unit  # unitarias + integración (ASan)
    make test-int   # integración
