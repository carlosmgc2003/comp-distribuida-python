# Revisión del TP (2019) y modernización (2026)

En 2019 el trabajo estaba en dos repositorios: `comp-distribuida-python`
(coordinador y esclavos con ZeroMQ) y `comp-distribuida-cpp` (núcleo numérico).
Hoy está todo en este repositorio, con el historial de ambos: el código Python
en `src/` y el C++ en `cpp/` (los commits de 2019 del C++ aparecen con sus rutas
originales, antes de moverse a `cpp/`).

El objetivo del TP era resolver `A·x = b` repartiendo bloques de filas entre
varias máquinas: un **coordinador** en Python reparte trabajos, cada **esclavo**
recibe sus filas por la red y un programa en **C++** las resuelve con el método
de Jacobi.

Primero va un diagnóstico de la versión original, con **cada bug reproducido
ejecutando el código de 2019**, y después las decisiones de la versión nueva.

---

## 1. Resumen ejecutivo

| # | Problema | Dónde | Efecto | Severidad |
|---|----------|-------|--------|-----------|
| 1 | No se divide por la diagonal `a_ii` | `calculoJacobi.cpp` | Sólo funciona si `a_ii = 1`; con la placa real (`a_ii = 4`) **no termina nunca** | 🔴 crítico |
| 2 | Las semillas se indexan con la columna global sobre un vector local | `insertar_nueva_semilla` | Con más de un lote la solución es **incorrecta** (error ~20–40 en una placa de 100°) | 🔴 crítico |
| 3 | El C++ lee la columna `resultado` como semilla (`SELECT *` + índice 4) | `persistencia.cpp` | Las semillas que manda el coordinador **se ignoran**: varias "pasadas" no mejoran nada | 🔴 crítico |
| 4 | El JOIN de semillas usa `fila` en lugar de `pos_elemento`, y `actualizar_semilla` suma `+1` | `coordinador.py`, `analizador.py` | Aunque el C++ las leyera, serían las de otra variable | 🔴 crítico |
| 5 | Socket `REP` con varios esclavos y sin responder cuando no hay trabajo | `coordinador.py` | `ZMQError: Operation cannot be accomplished in current state` con **2+ esclavos** | 🔴 crítico |
| 6 | `cant_filas // tam_batch` descarta el resto | `Lista_Trabajos` | Filas sin resolver, el coordinador queda esperando / se cae | 🟠 alto |
| 7 | Bucle de Jacobi sin máximo de iteraciones ni detección de divergencia | `main.cpp` | Cuelgue infinito si no converge | 🟠 alto |
| 8 | `std::to_string(double)` al escribir resultados | `persistencia.cpp` | Sólo **6 decimales**: valores < 5e-7 se guardan como 0 | 🟠 alto |
| 9 | `NameError: filePath` en el primer arranque | `esclavo.py` | El esclavo no arranca si no existe un `.db` previo | 🟠 alto |
| 10 | Un mensaje de red **por cada coeficiente**, con ACK | protocolo | ~9 500 idas y vueltas por trabajo de 1 600 filas | 🟡 rendimiento |
| 11 | `-Wall -Wextra` nunca se aplicaban (`CMAKE_CXX_FLAGS_ALL` no existe) | `CMakeLists.txt` | Warnings ocultos | 🟡 calidad |
| 12 | Binario ELF commiteado en el repo de Python | `comp_distribuida` | No portable, no reproducible | 🟡 calidad |

Por qué "funcionaba" en la presentación: el `__main__` usaba
`tam_batch=1600` para una placa de 40×40 = 1 600 incógnitas, o sea **un único
lote y un único esclavo**, y (probablemente) una matriz ya normalizada con diagonal
1. Ese es justamente el único caso donde los bugs 1–6 no se manifiestan.

---

## 2. Reproducciones

Se armó una placa de Laplace de 6×6 (36 incógnitas, la misma estructura que la
de 40×40 del TP), se cargó con el `analizador.py` original, se armaron los lotes
con la misma consulta SQL del coordinador y se ejecutó el binario C++ original
(compilado del commit `bb5e0a4`). Se comparó contra `numpy.linalg.solve`.

```text
diag=1 tam_batch=36: error_max=2.357e-05 filas_sin_resolver=0   ← único caso correcto
diag=1 tam_batch=18: error_max=1.920e+01 filas_sin_resolver=0   ← bug 2/3
diag=1 tam_batch=12: error_max=3.080e+01 filas_sin_resolver=0
diag=1 tam_batch=10: error_max=4.102e+01 filas_sin_resolver=6   ← bug 6
diag=4 tam_batch=36: el binario NO TERMINA (diverge, sin max_iter) ← bug 1 + 7
```

Parte distribuida (coordinador y esclavos originales, procesos reales sobre TCP):

```text
1 trabajo,  1 esclavo  → COORDINADOR TERMINO OK
1 trabajo,  2 esclavos → zmq.error.ZMQError: Operation cannot be accomplished in current state
3 trabajos, 2 esclavos → zmq.error.ZMQError: Operation cannot be accomplished in current state
36 filas, tam_batch=10 → zmq.error.ZMQError: ... (llega un 'disponible' y no hay trabajo)
primer arranque del esclavo → NameError: name 'filePath' is not defined
```

---

## 3. Explicación de cada bug (para aprender de ellos)

### 3.1 Numérica (C++)

**Bug 1 — falta dividir por `a_ii`.** Jacobi despeja cada incógnita de su fila:

```
x_i^(k+1) = ( b_i − Σ_{j≠i} a_ij · x_j^(k) ) / a_ii
```

`calcular_fila` hacía `resultado_fila - acumulador`, sin dividir: la iteración
queda `x ← b − (A − D)·x`. Para la placa, `−(A − D)` es la matriz de vecinos de
la grilla, con radio espectral `4·cos(π/(k+1)) ≈ 4` (> 1): diverge. Como además (bug 7) no había límite de
iteraciones, el programa no terminaba. El commit "Corregido Bug que causaba
iteracion indefinida" atacó un síntoma, no la causa.

**Bug 2 — índices globales sobre un vector local.** `resultados` tiene una
entrada por fila **del lote** (posiciones 0..lote−1), pero se accedía con
`resultados.at(item.pos)`, donde `pos` es la columna **global**. Para el lote
que empieza en la fila 1600, `pos=1600..3199` → `out_of_range`, que se atrapaba
con `catch` y se ignoraba (commit "Corregido error de vector out of bonds"),
y para columnas de otro lote se leía el valor **de otra variable**. Lección:
un `try/catch` que silencia un `out_of_range` casi siempre esconde un bug de
lógica; la excepción estaba avisando.

**Bug 3 — `SELECT *` con índices fijos.** La tabla del esclavo tiene columnas
`triplaId, fila, pos_elemento, valor, resultado, semilla` (0..5); el código leía
`semilla = sqlite3_column_double(cursor, 4)`, que es `resultado`. Nunca usar
`SELECT *` con acceso posicional: nombrar las columnas.

**Bug 7 — criterio de parada.** `satisface` compara dos iterados sucesivos
(`‖xᵏ⁺¹ − xᵏ‖ ≤ 1e-5`, absoluto). Problemas: (a) no hay `max_iter`; (b) un paso
chico no garantiza estar cerca de la solución cuando el método converge lento
(radio espectral ≈ 0,997 en la placa 40×40); (c) cada vuelta del `do/while`
calculaba **dos** barridos de Jacobi para avanzar uno. La versión nueva usa el
**residuo relativo** `‖b − A·x‖ / ‖b‖`, que se obtiene gratis en el mismo barrido
(`r_i = s_i − a_ii·x_i`), y siempre tiene un tope de iteraciones.

**Bug 8 — precisión.** `std::to_string(1.23456789e-7)` produce `"0.000000"`.
Para persistir números usar parámetros enlazados (`sqlite3_bind_double`) o
`std::format("{:.17g}")`, que conserva los 17 dígitos significativos de un
`double`.

**Otros detalles C++:** `sqlite3_close(db)` antes de `sqlite3_errmsg(db)`
(uso después de liberar); `lecturaDatos` devuelve 0 (= éxito) cuando falla
abrir la base; con la tabla vacía `minmax_element` devuelve `end()` y se
desreferencia (UB); `pila_datos.top()` sobre una pila vacía si la última fila
sólo tenía diagonal; una fila con sólo diagonal nunca generaba solución;
`sqlite3_prepare_v2(db, sql, 128, ...)` con un string de 21 bytes; `INSERT`
uno por uno sin transacción (un `fsync` por fila); `catch` por valor;
`typedef struct` (estilo C); `CMAKE_BUILD_TYPE` forzado dentro del
`CMakeLists.txt`; nombres en MAYÚSCULAS reservados por convención a macros.

### 3.2 Distribuida (Python + ZeroMQ)

**Bug 5 — la máquina de estados de `REP`.** Un socket `REP` exige alternar
estrictamente `recv → send → recv → send`. Dos problemas:

1. Cuando llegaba `disponible` y no quedaban trabajos, no se respondía nada y
   el siguiente `recv_json()` lanzaba `EFSM`.
2. `REP` reparte los pedidos de **todos** los esclavos conectados. Mientras se
   le mandaban los datos a A (`recv`/`send` intercalados por cada coeficiente),
   el siguiente `recv` podía traer el `disponible` de B; el código esperaba
   `'RX'`, no respondía, y de nuevo `EFSM`.

O sea: el diseño sólo funciona con un esclavo a la vez. La solución estándar es
`ROUTER` en el servidor (cada mensaje viene con la identidad del cliente y se
puede responder a cualquiera en cualquier orden) y **un mensaje por trabajo**.

**Bug 4 — semillas cruzadas.** En la consulta del coordinador,
`LEFT JOIN semilla ON triplas.fila = semilla.variable` trae la semilla de la
**fila** (`x_i`) cuando para el coeficiente `a_ij` hace falta la de la
**columna** (`x_j`). Y `actualizar_semilla` actualizaba `variable = semilla[0] + 1`
(corrimiento de 1). Además el JOIN repetía para cada coeficiente datos que son
por fila (`b_i`).

**Bug 6 — el resto de la división.** `cant_filas // tam_batch` genera
`⌊n/t⌋` trabajos; las `n mod t` filas restantes nunca se envían.

**Bug 9 — `NameError`.** `except: print(..., filePath)` usa una variable
inexistente; además el `except:` desnudo atrapa todo (incluso `KeyboardInterrupt`).

**Otros detalles Python:**

- `if e == zmq.ZMQError` compara una instancia con una clase: siempre `False`,
  así que ante cualquier error de red el esclavo "se cierra elegantemente".
- `iniciar_polled_socket()` se llama en `__init__` y otra vez en cada
  `recibir_datos`/`enviar_solucion` sin cerrar el anterior: se acumulan sockets
  abiertos y registrados en el `Poller`.
- `run("./comp_distribuida", stdout=False)`: `False == 0`, así que la salida del
  hijo se redirige al descriptor 0 (stdin). Se quería `subprocess.DEVNULL`. Y no
  se revisaba el código de retorno (`check=True`).
- `Trabajo.__eq__` devuelve `None` e imprime un mensaje si se compara con algo
  que no sea `str`; definir `__eq__` además vuelve la clase no-hasheable.
- El constructor de `Servidor` ejecuta **todo** el cálculo (`__init__ →
  mensaje_inicial → escuchar`): efectos secundarios en un constructor.
- `while … else` y `for … else` sin `break`: el `else` siempre se ejecuta;
  confunde al lector.
- Un trabajo se marca `'listo'` antes de enviarse y si el esclavo muere nunca se
  reintenta: un esclavo caído cuelga al coordinador para siempre.
- Con `veces > 1`, los trabajos de la pasada 2 se pueden repartir antes de que
  termine la 1 (no hay barrera) y `len(self.solucion)` cuenta duplicados.
- `csv.reader(delimiter=' ')`: con dos espacios seguidos aparece un campo vacío
  que **corre las columnas** sin error.
- `cantidad_filas()` (un `COUNT(DISTINCT)` en SQLite) se ejecuta en cada mensaje.
- `actualizar_semilla` reescribe todas las soluciones acumuladas cada vez que
  llega una parcial (O(n²) en total).
- `np.save(f'respuesta-{datetime.now()}')` genera nombres con espacios y `:`
  (inválidos en Windows).
- Puerto 5000, rutas e IP fijas en el código; `print` en vez de `logging`.
- Archivos en el `.gitignore` como `__pycache__/analizador.cpython-37.pyc`
  (debería ignorarse el directorio entero) y `comp_distribuida`, que en la
  versión nueva **ocultaba el paquete Python completo** (lo descubrimos al
  empaquetar: la wheel salía vacía).

### 3.3 Algorítmica

El método que se quería implementar es **Jacobi por bloques**: cada esclavo
resuelve `A_BB · x_B = b_B − A_B,ext · x_ext` con los valores externos fijos, y
el coordinador repite hasta converger. Converge para la placa porque `A` es una
M-matriz: diagonal dominante (débil: en las filas interiores `4 = 1+1+1+1`) **e
irreducible** (la grilla es conexa), y entonces el splitting por bloques es regular
y `ρ < 1` para cualquier partición. La explicación completa, con las falencias y
optimizaciones encontradas en una revisión posterior, está en
[docs/matematica.md](docs/matematica.md). Números reales (calculados en esta
revisión; las iteraciones son estimaciones asintóticas `ln(tol)/ln ρ`, con `x⁰ = 0` y
el criterio de residuo se miden algo menos: 4 849 y 284 para la placa 40×40):

| Placa | Bloques | Radio espectral Jacobi | Iter. para 1e-8 | Radio Jacobi por bloques | Iter. externas |
|-------|---------|------------------------|-----------------|--------------------------|----------------|
| 20×20 | 4 | 0,98883 | ~1 640 | 0,90206 | ~180 |
| 40×40 | 4 | 0,99707 | ~6 270 | 0,94780 | ~345 |
| 40×40 | 8 | 0,99707 | ~6 270 | 0,97171 | ~640 |

Más bloques = más paralelismo, pero más iteraciones externas (más
comunicación). Es el compromiso central de la descomposición de dominio.

---

## 4. La versión nueva

### Arquitectura

```
                 ┌───────────────────────────── coordinador (Python) ─┐
 matriz.txt ──▶  │ Problema (scipy CSR) → particionar → bloques       │
 vector.txt      │ ROUTER tcp://*:5555                                │
                 │ por iteración externa k:                           │
                 │   repartir bloques a esclavos ociosos              │
                 │   reasignar si vence el plazo                      │
                 │   juntar x_B, residuo global ‖b−Ax‖/‖b‖            │
                 └──────────▲───────────────────────┬─────────────────┘
          RESULTADO + pedido│         TRABAJO (1 msg│multiparte: CSR del
                            │                       ▼ bloque, b_B, x)
                 ┌──────────┴──────── esclavo (Python, REQ) ──────────┐
                 │ Lazy Pirate (timeout + reintento con socket nuevo) │
                 │ _nativo.resolver_bloque(...)  ← C++23 (cpp/)     │
                 │   (o NumPy si no está compilado)                   │
                 └────────────────────────────────────────────────────┘
```

### Decisiones y por qué

| Antes | Ahora | Por qué |
|-------|-------|---------|
| SQLite como formato intermedio (coordinador, esclavo y C++) | CSR en memoria (`scipy.sparse`, `std::span`) | Es el formato estándar para matrices dispersas; sin E/S a disco |
| `subprocess` + archivo `.db` entre Python y C++ | Módulo nativo con **nanobind** + **scikit-build-core** (`pip install .` compila todo), sin copias, liberando el GIL | Una llamada a función en lugar de 2 bases de datos y un proceso por trabajo |
| Un mensaje JSON por coeficiente | Un mensaje multiparte por trabajo, arrays binarios | De ~9 500 idas y vueltas a 1; sin pérdida de precisión |
| `REP` + estado implícito | `ROUTER` + patrón *load balancing* con pedidos retenidos | Muchos esclavos a la vez, sin `EFSM`, sin esperas activas |
| Sin tolerancia a fallos | Plazo por trabajo, reasignación, resultados idempotentes | Un esclavo caído ya no cuelga el sistema |
| N "pasadas" fijas | Iterar hasta `‖b−Ax‖/‖b‖ ≤ tol` | Criterio de parada con significado |
| Errores silenciosos / UB | `std::expected<Resultado, Error>` en C++, `ErrorCalculo` en Python | Los errores de datos (diagonal nula, divergencia) se informan |
| C++14, `printf`, `typedef struct` | C++23: `std::expected`, `std::span`, `std::format`, inicializadores designados | Más seguro y expresivo |
| CMake con flags que no se aplicaban | Target de interfaz con warnings estrictos, presets (`debug`, `release`, `asan`), Catch2, CTest | Compila limpio con `-Werror` y sanitizers |
| Dos repositorios, un binario copiado a mano junto al esclavo | Un solo repositorio y un solo build (CMake invocado por `pip`) | Python y C++ no pueden quedar desincronizados |
| Scripts sueltos, `input()` para la IP | Paquete instalable (`pyproject.toml`), CLI `comp-distribuida`, `logging` | Reproducible y configurable |
| Sin tests | 7 tests C++ + 50 tests Python (unitarios, protocolo, integración con esclavos reales) | Cada bug de arriba tiene un test de regresión |
| `pickle` no, pero JSON por campo | JSON sólo para la cabecera; dtypes validados (sin `pickle`) | Deserializar `pickle` de la red permite ejecutar código |

### Un hallazgo durante la modernización

La primera versión del coordinador nuevo respondía `ESPERA` (dormir 50 ms) a
los esclavos sin trabajo. Funcionaba, pero cada iteración externa pagaba esa
espera: la suite de integración tardaba **73 s**. Reteniendo el pedido del
esclavo ocioso y respondiéndole apenas hay trabajo (lo que permite `ROUTER`),
bajó a **6 s**. En algoritmos con barrera por iteración, la **latencia** pesa
más que el ancho de banda.

### Resultados (placa 40×40, n = 1 600, 4 bloques, 4 esclavos en procesos separados)

```text
Convergió: 284 iteraciones externas, residuo 9.886e-09, 1136 trabajos (0 reasignados), 4 esclavos, 2.15 s
error máx vs scipy.sparse.linalg.spsolve: 9.2e-06
motor numpy:  29.5 s     motor nativo (C++): 2.3 s
```

(El error **relativo** es `1,2e-7`, unas 12 veces el residuo relativo `9,9e-9`:
la cota es `‖e‖/‖x‖ ≤ κ(A)·‖r‖/‖b‖` con `κ₂(A) ≈ 681` para la placa 40×40, así que
residuo chico no implica error igual de chico. El `9,2e-6` de arriba es un error
absoluto, en grados, y no se compara directamente con un residuo relativo.)

---

## 5. Próximos pasos posibles

1. **Cachear la matriz en los esclavos**: hoy cada trabajo reenvía su porción
   CSR, que no cambia entre iteraciones; sólo haría falta mandar `x`.
2. **Mandar sólo el "halo"**: cada bloque necesita únicamente las `x_j` de las
   columnas externas que aparecen en sus filas, no el vector entero.
3. **Asincronía** (Jacobi/Gauss-Seidel asíncrono): usar cada resultado apenas
   llega, sin barrera por iteración. Es lo que el diseño original insinuaba.
4. **Mejor método**: Jacobi por bloques como *precondicionador* de Gradiente
   Conjugado converge en decenas de iteraciones en lugar de cientos; o Schwarz
   aditivo con solapamiento, o multigrid.
5. **Paralelismo dentro del esclavo**: el barrido de Jacobi es trivialmente
   paralelo (OpenMP o `std::execution::par`).
6. **Herramientas "industriales"** para comparar: `mpi4py` (MPI), Dask o Ray
   resuelven la parte de reparto y tolerancia a fallos que aquí se hace a mano.
7. Seguridad de red: hoy cualquiera que llegue al puerto puede pedir trabajos o
   enviar resultados falsos; ZeroMQ trae CURVE para autenticar y cifrar.
