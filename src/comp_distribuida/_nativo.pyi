"""Firma del módulo compilado desde cpp/bindings.cpp (nanobind)."""

import numpy as np
import numpy.typing as npt

def resolver_bloque(
    indptr: npt.ArrayLike,
    indices: npt.ArrayLike,
    datos: npt.ArrayLike,
    fila_inicial: int,
    b_local: npt.ArrayLike,
    x_global: npt.ArrayLike,
    tolerancia: float = 1e-10,
    max_iteraciones: int = 10000,
) -> tuple[npt.NDArray[np.float64], int, float, bool]: ...
