"""Representación del sistema A·x = b, lectura de archivos y partición en bloques.

En el original la matriz se guardaba en SQLite como "triplas" (fila, columna, valor)
y se consultaba fila por fila. Eso es, en esencia, el formato COO; aquí se usa
CSR (Compressed Sparse Row) de SciPy, que es el formato estándar para recorrer
filas de una matriz dispersa y el que entiende el núcleo en C++.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
import scipy.io
import scipy.sparse as sp

VectorF = npt.NDArray[np.float64]


@dataclass(frozen=True, slots=True)
class Bloque:
    """Rango de filas consecutivas [inicio, fin) que resuelve un esclavo."""

    indice: int
    inicio: int
    fin: int

    @property
    def n_filas(self) -> int:
        return self.fin - self.inicio

    @property
    def rango(self) -> slice:
        return slice(self.inicio, self.fin)


def particionar(n: int, tam_bloque: int) -> list[Bloque]:
    """Divide n filas en bloques de `tam_bloque` filas.

    El último bloque se queda con el resto. (El original hacía `n // tam_batch`
    y las filas sobrantes nunca se enviaban: el coordinador esperaba para siempre.)
    """
    if n <= 0:
        raise ValueError("el sistema no tiene filas")
    if tam_bloque <= 0:
        raise ValueError("tam_bloque debe ser positivo")
    return [
        Bloque(indice=i, inicio=inicio, fin=min(inicio + tam_bloque, n))
        for i, inicio in enumerate(range(0, n, tam_bloque))
    ]


@dataclass(frozen=True)
class Problema:
    """Sistema lineal cuadrado A·x = b con A en CSR."""

    A: sp.csr_array
    b: VectorF

    def __post_init__(self) -> None:
        filas, columnas = self.A.shape
        if filas != columnas:
            raise ValueError(f"A debe ser cuadrada y es {filas}x{columnas}")
        if self.b.shape != (filas,):
            raise ValueError(f"b debe tener {filas} elementos y tiene forma {self.b.shape}")
        if not np.all(np.isfinite(self.A.data)) or not np.all(np.isfinite(self.b)):
            raise ValueError("A y b no pueden contener NaN/Inf")

    @classmethod
    def crear(cls, A: npt.ArrayLike | sp.sparray | sp.spmatrix, b: npt.ArrayLike) -> Problema:
        matriz = sp.csr_array(A, dtype=np.float64)
        matriz.sum_duplicates()  # deja la CSR canónica (índices ordenados, sin repetidos)
        return cls(A=matriz, b=np.asarray(b, dtype=np.float64).ravel())

    @property
    def n(self) -> int:
        return int(self.A.shape[0])

    def residuo_relativo(self, x: VectorF) -> float:
        """||b - A·x||₂ / ||b||₂: el criterio de parada global (el original no tenía)."""
        norma_b = float(np.linalg.norm(self.b)) or 1.0
        return float(np.linalg.norm(self.b - self.A @ x)) / norma_b

    def es_diagonal_dominante(self) -> bool:
        """Condición suficiente (no necesaria) para que Jacobi converja."""
        diagonal = np.abs(self.A.diagonal())
        suma_fila = np.asarray(abs(self.A).sum(axis=1)).ravel() - diagonal
        return bool(np.all(diagonal >= suma_fila) and np.any(diagonal > suma_fila))


# --- Lectura / escritura ----------------------------------------------------------


def cargar(ruta_matriz: str | Path, ruta_vector: str | Path) -> Problema:
    """Carga A y b. Formatos de A: texto denso (el del TP), Matrix Market (.mtx) o .npz."""
    ruta_matriz = Path(ruta_matriz)
    match ruta_matriz.suffix.lower():
        case ".mtx":
            A = scipy.io.mmread(ruta_matriz)
        case ".npz":
            A = sp.load_npz(ruta_matriz)
        case _:
            # np.loadtxt tolera varios espacios seguidos. El original usaba
            # csv.reader(delimiter=' '): con dos espacios aparecía un campo vacío
            # que corría las columnas de lugar sin avisar.
            A = np.loadtxt(ruta_matriz, ndmin=2)
    b = np.loadtxt(ruta_vector, ndmin=1)
    return Problema.crear(A, b)


def guardar_texto(problema: Problema, ruta_matriz: str | Path, ruta_vector: str | Path) -> None:
    """Guarda en el formato de texto denso original (sólo razonable para n chico)."""
    np.savetxt(ruta_matriz, problema.A.toarray(), fmt="%.17g")
    np.savetxt(ruta_vector, problema.b, fmt="%.17g")


# --- Problema de ejemplo ------------------------------------------------------------


def generar_placa(lado: int, temperatura_borde: float = 100.0) -> Problema:
    """Ecuación de Laplace en una placa `lado`x`lado` (diferencias finitas de 5 puntos).

    El borde superior está a `temperatura_borde` y el resto a 0. Es el tipo de
    problema que se graficaba como superficie en el TP (de ahí el `sqrt(n)`).
    """
    if lado < 1:
        raise ValueError("lado debe ser >= 1")
    unos = np.ones(lado - 1)
    T = sp.diags_array([-unos, np.full(lado, 4.0), -unos], offsets=[-1, 0, 1])
    vecinos = sp.diags_array([-unos, -unos], offsets=[-1, 1], shape=(lado, lado))
    identidad = sp.eye_array(lado)
    A = sp.kron(identidad, T) + sp.kron(vecinos, identidad)
    b = np.zeros(lado * lado)
    b[:lado] = temperatura_borde  # primera fila de la grilla: toca el borde caliente
    return Problema.crear(A, b)
