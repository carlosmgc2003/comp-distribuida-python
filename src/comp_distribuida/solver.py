"""Resolución local de un bloque con Jacobi.

Hay dos motores con la MISMA semántica:

* ``nativo``: el módulo ``comp_distribuida._nativo``, compilado desde ``cpp/``
  (C++23 + nanobind) al instalar el paquete. Se llama en el mismo proceso, con
  arrays de NumPy y sin copias.
* ``numpy``: implementación vectorizada de referencia, para no depender de un
  compilador (y para testear que ambos coinciden).

En el original, el esclavo escribía un .db de SQLite, ejecutaba el binario con
``subprocess.run`` y leía otro .db: E/S a disco en cada trabajo, pérdida de
precisión (``std::to_string`` deja 6 decimales) y errores silenciosos.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

import numpy as np
import numpy.typing as npt
import scipy.sparse as sp

from comp_distribuida.problema import Bloque, Problema, VectorF

try:
    import comp_distribuida._nativo as jacobi_nativo
except ImportError:  # pragma: no cover - p.ej. corriendo desde el código fuente sin compilar
    jacobi_nativo = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

Motor = Literal["auto", "nativo", "numpy"]
VectorI = npt.NDArray[np.int64]


class ErrorCalculo(ValueError):
    """Datos inválidos para Jacobi (diagonal nula, divergencia, dimensiones...)."""


@dataclass(frozen=True, slots=True)
class Opciones:
    tolerancia: float = 1e-10
    max_iteraciones: int = 10_000


@dataclass(frozen=True, slots=True)
class DatosBloque:
    """Todo lo que un esclavo necesita de la matriz para resolver un bloque."""

    indptr: VectorI  # puede no empezar en 0 (es una porción de la CSR global)
    indices: VectorI  # columnas GLOBALES
    datos: VectorF
    fila_inicial: int
    b_local: VectorF

    @property
    def n_filas(self) -> int:
        return len(self.indptr) - 1

    @classmethod
    def desde_problema(cls, problema: Problema, bloque: Bloque) -> DatosBloque:
        A = problema.A
        ini, fin = int(A.indptr[bloque.inicio]), int(A.indptr[bloque.fin])
        return cls(
            indptr=np.ascontiguousarray(A.indptr[bloque.inicio : bloque.fin + 1], dtype=np.int64),
            indices=np.ascontiguousarray(A.indices[ini:fin], dtype=np.int64),
            datos=np.ascontiguousarray(A.data[ini:fin], dtype=np.float64),
            fila_inicial=bloque.inicio,
            b_local=np.ascontiguousarray(problema.b[bloque.rango]),
        )


@dataclass(frozen=True, slots=True)
class ResultadoBloque:
    x: VectorF
    iteraciones: int
    residuo: float
    convergio: bool


def motor_disponible(motor: Motor = "auto") -> Literal["nativo", "numpy"]:
    if motor == "numpy":
        return "numpy"
    if jacobi_nativo is not None:
        return "nativo"
    if motor == "nativo":
        raise RuntimeError(
            "El núcleo C++ (comp_distribuida._nativo) no está compilado. "
            "Instalar el paquete con: pip install ."
        )
    return "numpy"


def resolver_bloque(
    bloque: DatosBloque, x_global: VectorF, opciones: Opciones = Opciones(), motor: Motor = "auto"
) -> ResultadoBloque:
    """Resuelve A_BB·x_B = b_B - A_B,ext·x_ext partiendo de x_global[B]."""
    if motor_disponible(motor) == "nativo":
        try:
            x, iteraciones, residuo, convergio = jacobi_nativo.resolver_bloque(
                bloque.indptr,
                bloque.indices,
                bloque.datos,
                bloque.fila_inicial,
                bloque.b_local,
                x_global,
                opciones.tolerancia,
                opciones.max_iteraciones,
            )
        except ValueError as e:
            raise ErrorCalculo(str(e)) from e
        return ResultadoBloque(x, int(iteraciones), float(residuo), bool(convergio))
    return _resolver_numpy(bloque, x_global, opciones)


def _resolver_numpy(bloque: DatosBloque, x_global: VectorF, opciones: Opciones) -> ResultadoBloque:
    if opciones.tolerancia < 0 or opciones.max_iteraciones < 0:
        raise ErrorCalculo("opciones inválidas")
    m, n, f0 = bloque.n_filas, len(x_global), bloque.fila_inicial
    if m < 0 or len(bloque.b_local) != m or f0 < 0 or f0 + m > n:
        raise ErrorCalculo("dimensiones inválidas en la CSR o en los vectores")
    if np.any((bloque.indices < 0) | (bloque.indices >= n)):
        raise ErrorCalculo("índice de columna fuera de rango")

    A = sp.csr_array((bloque.datos, bloque.indices, bloque.indptr - bloque.indptr[0]), shape=(m, n))
    # Separar columnas internas (del bloque) y externas (fijas durante la resolución)
    internas = (A.indices >= f0) & (A.indices < f0 + m)
    A_ext = A.copy()
    A_ext.data = np.where(internas, 0.0, A.data)
    A_int = A[:, f0 : f0 + m]
    diagonal = A_int.diagonal()
    if np.any(diagonal == 0):
        raise ErrorCalculo("diagonal nula: Jacobi requiere a_ii != 0")
    fuera_diagonal = A_int - sp.diags_array(diagonal)

    b_efectivo = bloque.b_local - A_ext @ x_global
    norma_b = float(np.linalg.norm(b_efectivo)) or 1.0
    x = x_global[f0 : f0 + m].copy()
    iteracion = 0
    while True:
        with np.errstate(over="ignore", invalid="ignore"):  # la divergencia se informa abajo
            s = b_efectivo - fuera_diagonal @ x
            residuo = float(np.linalg.norm(s - diagonal * x)) / norma_b  # residuo EXACTO de x
        if not np.isfinite(residuo):
            raise ErrorCalculo("el método divergió (NaN/Inf); ¿la matriz es diagonal dominante?")
        convergio = residuo <= opciones.tolerancia
        if convergio or iteracion == opciones.max_iteraciones:
            return ResultadoBloque(x, iteracion, residuo, convergio)
        x = s / diagonal
        iteracion += 1
