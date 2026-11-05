"""Gráfico 3D de la solución de la placa (requiere el extra `grafico`: matplotlib)."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np

from comp_distribuida.problema import VectorF


def graficar_placa(x: VectorF, titulo: str = "", archivo: str | Path | None = None) -> None:
    """Dibuja la solución como superficie. Si `archivo` es None, abre una ventana."""
    import matplotlib.pyplot as plt  # noqa: PLC0415 - dependencia opcional

    lado = math.isqrt(len(x))
    if lado * lado != len(x):
        raise ValueError(f"n={len(x)} no es un cuadrado perfecto: no es una placa")
    z = np.asarray(x).reshape(lado, lado)
    columnas, filas = np.meshgrid(np.arange(lado), np.arange(lado))

    ax: Any
    fig, ax = plt.subplots(subplot_kw={"projection": "3d"}, figsize=(8, 6))
    ax.plot_surface(columnas, filas, z, cmap="coolwarm", linewidth=0, antialiased=True)
    ax.set_xlabel("columna")
    ax.set_ylabel("fila")
    ax.set_zlabel("temperatura")
    ax.set_title(titulo)
    if archivo is None:
        plt.show()
    else:
        fig.savefig(archivo, dpi=150, bbox_inches="tight")
    plt.close(fig)
