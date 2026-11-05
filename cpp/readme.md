# Núcleo de Jacobi por bloques (C++23)

Facultad de Ingeniería del Ejército — Sistemas Operativos 2 — Trabajo Práctico Integrador
(docentes: Ing. Repetto, Ing. Olivieri). Versión original 2019, modernizada en 2026.

Parte numérica del TP: resuelve con el método de Jacobi un bloque de filas de un
sistema disperso `A·x = b` en formato CSR. La parte distribuida (coordinador y
esclavos con ZeroMQ) está en
[comp-distribuida-python](https://github.com/carlosmgc2003/comp-distribuida-python),
que usa este núcleo como módulo de Python (`import jacobi_nativo`).

➡️ **[REVISION.md](REVISION.md)**: bugs de la versión 2019 (reproducidos) y
decisiones de diseño de la actual.

## Requisitos

- CMake ≥ 3.25 y Ninja
- Compilador con `std::expected` (C++23): GCC ≥ 13, Clang ≥ 19 (con libstdc++) o
  Clang ≥ 17 con libc++, MSVC ≥ 19.36
- Para los tests se descarga Catch2 v3 automáticamente (o se usa el del sistema)

## Compilar y testear

```bash
cmake --preset release && cmake --build --preset release && ctest --preset release
```

Presets: `debug`, `release` y `asan` (AddressSanitizer + UBSan, warnings como errores).

## CLI

Resuelve el sistema completo leyendo el formato de texto del TP (matriz densa y
vector, separados por espacios):

```bash
./build/release/jacobi_cli matriz.txt vector.txt --tol 1e-10 -o x.txt
```

## Módulo de Python

```bash
pip install .        # o: uv pip install .
```

```python
import numpy as np, scipy.sparse as sp, jacobi_nativo

A = sp.csr_array(np.array([[4.0, -1, 0], [-1, 4, -1], [0, -1, 4]]))
b = np.array([3.0, 2, 3])
x, iteraciones, residuo, convergio = jacobi_nativo.resolver_bloque(
    A.indptr, A.indices, A.data, 0, b, np.zeros(3), tolerancia=1e-12)
# x ≈ [1, 1, 1]
```

## API

```cpp
#include "jacobi/jacobi.hpp"

jacobi::VistaCSR bloque{indptr, indices, datos, /*fila_inicial=*/400};
auto r = jacobi::resolver_bloque(bloque, b_local, x_global, {.tolerancia = 1e-10});
if (!r) std::println("{}", jacobi::describir(r.error()));   // diagonal nula, divergencia, ...
else    usar(r->x, r->iteraciones, r->residuo, r->convergio);
```

`resolver_bloque` resuelve `A_BB·x_B = b_B − A_B,ext·x_ext` (las columnas de
otros bloques quedan fijas en `x_global`). El residuo informado es el exacto de
la `x` devuelta y se calcula gratis dentro del mismo barrido.

## Estructura

```
include/jacobi/jacobi.hpp   API pública (VistaCSR, Opciones, Resultado, Error)
src/jacobi.cpp              implementación
src/main.cpp                CLI jacobi_cli
python/bindings.cpp         módulo jacobi_nativo (nanobind, sin copias, libera el GIL)
tests/test_jacobi.cpp       tests (Catch2)
```
