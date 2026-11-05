# Computación distribuida: Jacobi por bloques con ZeroMQ

Facultad de Ingeniería del Ejército — Sistemas Operativos 2 — Trabajo Práctico Integrador
(docentes: Ing. Repetto, Ing. Olivieri). Versión original 2019, modernizada en 2026.

Un **coordinador** carga un sistema lineal `A·x = b`, lo divide en bloques de
filas y los reparte por la red a **esclavos**, que resuelven cada bloque con el
método de Jacobi usando el núcleo en C++ de
[comp-distribuida-cpp](https://github.com/carlosmgc2003/comp-distribuida-cpp)
(o NumPy, si no está compilado). El coordinador itera hasta que el residuo
global `‖b − A·x‖ / ‖b‖` es menor que la tolerancia.

➡️ **[REVISION.md](REVISION.md)**: análisis de los bugs de la versión 2019
(reproducidos) y de las decisiones de la versión actual.

## Instalación

Requiere Python ≥ 3.11. Con [uv](https://docs.astral.sh/uv/):

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[grafico]"
```

Núcleo en C++ (opcional, ~13× más rápido; requiere CMake ≥ 3.25 y GCC ≥ 13 o Clang ≥ 19):

```bash
uv pip install ../comp-distribuida-cpp
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

Opciones útiles del coordinador: `--tol` (residuo global), `--max-internas`
(barridos de Jacobi por trabajo; con `1` es Jacobi clásico distribuido),
`--plazo` (segundos antes de reasignar un trabajo), `--guardar x.npy`.
La matriz puede ser texto denso (formato del TP), Matrix Market (`.mtx`) o `.npz`.

## Estructura

```
src/comp_distribuida/
  problema.py     sistema A·x=b en CSR, lectura de archivos, partición en bloques
  solver.py       Jacobi sobre un bloque (motor C++ o NumPy, misma semántica)
  protocolo.py    mensajes ZeroMQ multiparte (cabecera JSON + arrays binarios)
  coordinador.py  socket ROUTER, reparto, reasignación, criterio de parada
  esclavo.py      socket REQ con reintentos (Lazy Pirate)
  cli.py          comando `comp-distribuida`
  grafico.py      superficie 3D de la solución
tests/            unitarios, protocolo e integración con esclavos reales
```

## Desarrollo

```bash
uv pip install -e . --group dev
pytest              # 43 tests (7 se saltean si no está el núcleo C++)
ruff check src tests && ruff format --check src tests
mypy src
```
