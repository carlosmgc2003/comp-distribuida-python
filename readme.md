# Computación distribuida: Jacobi por bloques con ZeroMQ y C++

Facultad de Ingeniería del Ejército — Sistemas Operativos 2 — Trabajo Práctico Integrador
(docentes: Ing. Repetto, Ing. Olivieri). Versión original 2019, modernizada en 2026.

Un **coordinador** carga un sistema lineal `A·x = b`, lo divide en bloques de
filas y los reparte por la red a **esclavos**, que resuelven cada bloque con el
método de Jacobi usando un núcleo en **C++23**. El coordinador itera hasta que el
residuo global `‖b − A·x‖ / ‖b‖` es menor que la tolerancia.

➡️ **[REVISION.md](REVISION.md)**: análisis de los bugs de la versión 2019
(reproducidos) y de las decisiones de la versión actual.

➡️ **[docs/matematica.md](docs/matematica.md)**: por qué converge el método (con una
[infografía](docs/infografia-jacobi.svg)), falencias numéricas encontradas y cómo
acelerarlo, con números reproducibles (`python docs/experimentos_convergencia.py`).

> Este repositorio une los dos originales (`comp-distribuida-python` y
> `comp-distribuida-cpp`) conservando el historial de ambos.

## Instalación

Requiere Python ≥ 3.11 y un compilador con C++23 (GCC ≥ 13, Clang ≥ 19 o MSVC ≥ 19.36).
`pip`/`uv` compilan el núcleo C++ automáticamente (CMake y nanobind se descargan
si hace falta). Con [uv](https://docs.astral.sh/uv/):

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[grafico]"
```

## Uso

Generar el problema de ejemplo (ecuación de Laplace en una placa con el borde
superior a 100°) en el formato de texto original:

```bash
comp-distribuida generar --lado 40 --directorio ejercicios
```

En la máquina coordinadora:

```bash
comp-distribuida -v coordinador ejercicios/matriz.txt ejercicios/vector.txt --tam-bloque 400 --graficar placa.png
```

En cada máquina esclava (tantas como se quiera, en cualquier momento):

```bash
comp-distribuida esclavo --conectar tcp://IP_DEL_COORDINADOR:5555
```

Todo en un solo proceso (coordinador + 4 esclavos en hilos), para probar:

```bash
comp-distribuida local ejercicios/matriz.txt ejercicios/vector.txt --tam-bloque 400 --esclavos 4
```

Si `A` es simétrica definida positiva (como la placa), `--metodo pcg` usa gradiente
conjugado precondicionado con Jacobi por bloques: en la placa 40×40 converge en 26
rondas en lugar de 284 (0,08 s contra 0,72 s), con los mismos esclavos.

Opciones útiles del coordinador: `--tol` (residuo global), `--max-internas`
(barridos de Jacobi por trabajo; con `1` es Jacobi clásico distribuido),
`--plazo` (segundos antes de reasignar un trabajo), `--guardar x.npy`.
La matriz puede ser texto denso (formato del TP), Matrix Market (`.mtx`) o `.npz`.
Los esclavos pueden usar `--motor numpy` para comparar con la implementación de
referencia en NumPy (~13× más lenta).

## Estructura

```
src/comp_distribuida/      paquete Python
  problema.py              sistema A·x=b en CSR, lectura de archivos, partición en bloques
  solver.py                Jacobi sobre un bloque (motor C++ o NumPy, misma semántica)
  protocolo.py             mensajes ZeroMQ multiparte (cabecera JSON + arrays binarios)
  coordinador.py           socket ROUTER, reparto, reasignación, criterio de parada
  esclavo.py               socket REQ con reintentos (Lazy Pirate)
  cli.py                   comando `comp-distribuida`
  grafico.py               superficie 3D de la solución
  _nativo.pyi              firma del módulo compilado desde cpp/
cpp/
  include/jacobi/jacobi.hpp  API C++ (VistaCSR, Opciones, Resultado, Error)
  src/jacobi.cpp             implementación
  src/main.cpp               CLI jacobi_cli (sin la parte distribuida)
  bindings.cpp               módulo comp_distribuida._nativo (nanobind)
  tests/test_jacobi.cpp      tests C++ (Catch2)
tests/                     tests Python: unitarios, protocolo e integración con esclavos reales
docs/                      teoría del método, infografía y experimentos de convergencia
CMakeLists.txt             build C++ (lo usa pip vía scikit-build-core, o CMake directo)
pyproject.toml             paquete Python
```

## Desarrollo

Python:

```bash
uv pip install -e . --group dev
pytest
ruff check src tests && ruff format --check src tests
mypy src
```

C++ solo (CLI y tests Catch2; presets `debug`, `release` y `asan`):

```bash
cmake --preset asan && cmake --build --preset asan && ctest --preset asan
```

```bash
./build/asan/jacobi_cli ejercicios/matriz.txt ejercicios/vector.txt --tol 1e-10 -o x.txt
```
