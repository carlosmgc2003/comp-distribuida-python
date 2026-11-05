"""Reproduce los números de docs/matematica.md (placa 40x40, n = 1 600, 4 bloques).

    python docs/experimentos_convergencia.py            # ~1 minuto con el núcleo C++

Simula el algoritmo del coordinador en un solo proceso (sin red) y mide dos
costos por separado:

* **rondas**: iteraciones externas = viajes de ida y vuelta coordinador ↔ esclavos.
  Con la red (o incluso en localhost) es lo que domina el tiempo.
* **barridos**: barridos de Jacobi en el camino crítico (el esclavo más lento de
  cada ronda), incluido el barrido extra que el núcleo hace para medir el
  residuo exacto del x que devuelve.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import scipy.sparse.linalg as spla

from comp_distribuida.problema import Problema, generar_placa, particionar
from comp_distribuida.solver import DatosBloque, Opciones, resolver_bloque

TOL = 1e-8
LADO, TAM = 40, 400


def radio_espectral(A: np.ndarray, tam: int) -> float:
    """ρ(M⁻¹N) con M = diagonal por bloques de A (bloques de `tam` filas)."""
    M = np.zeros_like(A)
    for s in range(0, len(A), tam):
        M[s : s + tam, s : s + tam] = A[s : s + tam, s : s + tam]
    return float(max(abs(np.linalg.eigvals(np.linalg.solve(M, M - A)))))


def jacobi_dos_etapas(
    p: Problema, tam: int, opciones: Callable[[float, float], Opciones]
) -> tuple[int, int]:
    """El algoritmo del coordinador. `opciones(res_externo, res_bloque_inicial)`."""
    bloques = particionar(p.n, tam)
    datos = [DatosBloque.desde_problema(p, b) for b in bloques]
    x, barridos, res = np.zeros(p.n), 0, 1.0
    for k in range(1, 20_000):
        x_nuevo, peor = np.empty_like(x), 0
        for b, d in zip(bloques, datos, strict=True):
            r0 = resolver_bloque(d, x, Opciones(0.0, 0)).residuo  # 0 barridos: sólo mide
            r = resolver_bloque(d, x, opciones(res, r0))
            x_nuevo[b.rango] = r.x
            peor = max(peor, r.iteraciones + 1)
        x, barridos = x_nuevo, barridos + peor
        res = p.residuo_relativo(x)
        if res <= TOL:
            return k, barridos
    raise RuntimeError("no convergió")


def main() -> None:  # noqa: PLR0915 (script lineal, a propósito)
    p = generar_placa(LADO)
    A = p.A.toarray()
    print(f"# Placa {LADO}x{LADO}, n={p.n}, tolerancia global {TOL:g}\n")

    print("## Radios espectrales (convergencia asintótica)\n")
    print("| partición | ρ | rondas ≈ ln(tol)/ln(ρ) |\n|---|---|---|")
    for nb in (1600, 16, 8, 4, 2):
        tam = p.n // nb
        rho = radio_espectral(A, tam)
        nombre = "Jacobi puntual" if tam == 1 else f"{nb} bloques de {tam}"
        print(f"| {nombre} | {rho:.5f} | {np.log(TOL) / np.log(rho):.0f} |")
    ev = np.linalg.eigvalsh(A)
    print(f"\nκ₂(A) = {ev[-1] / ev[0]:.1f}\n")

    print("## Cuántos barridos internos hacer (4 bloques)\n")
    print("| criterio interno | rondas | barridos (camino crítico) |\n|---|---|---|")
    casos: list[tuple[str, Callable[[float, float], Opciones]]] = [
        ("actual: tol 1e-10, máx 500", lambda _, __: Opciones(1e-10, 500)),
        ("exacto (sin tope)", lambda _, __: Opciones(1e-10, 100_000)),
        *[(f"m = {m} fijos", lambda _, __, m=m: Opciones(0.0, m)) for m in (1, 2, 5, 10, 20, 100)],
        *[
            (f"relativo: ‖r_B‖ ≤ {t}·‖r_B⁰‖", lambda _, r0, t=t: Opciones(t * r0, 100_000))
            for t in (0.3, 0.1, 0.03)
        ],
    ]
    for nombre, op in casos:
        k, barridos = jacobi_dos_etapas(p, TAM, op)
        print(f"| {nombre} | {k} | {barridos} |")

    print("\n## Otros métodos externos (bloques resueltos exactamente, LU cacheada)\n")
    Acsr = p.A.tocsr()
    bloques = particionar(p.n, TAM)
    lus = [spla.splu(Acsr[b.rango, b.rango].tocsc()) for b in bloques]
    rho = radio_espectral(A, TAM)

    def precond(r: np.ndarray, color: int | None = None, w: float = 1.0) -> np.ndarray:
        z = np.zeros_like(r)
        for i, (b, lu) in enumerate(zip(bloques, lus, strict=True)):
            if color is None or i % 2 == color:
                z[b.rango] = w * lu.solve(r[b.rango])
        return z

    print("| método | rondas |\n|---|---|")
    for nombre, w in (
        ("Gauss-Seidel por bloques rojo-negro", 1.0),
        ("SOR por bloques rojo-negro, ω óptimo", 2 / (1 + np.sqrt(1 - rho**2))),
    ):
        x, rondas = np.zeros(p.n), 0
        while p.residuo_relativo(x) > TOL:
            for color in (0, 1):
                x += precond(p.b - Acsr @ x, color, w)
                rondas += 1
        print(f"| {nombre} (ω = {w:.3f}) | {rondas} |")

    datos = [DatosBloque.desde_problema(p, b) for b in bloques]

    def barridos_desde_cero(m: int) -> Callable[[np.ndarray], np.ndarray]:
        """m barridos de Jacobi sobre A_BB·z_B = r_B desde z = 0: un precondicionador
        lineal y simétrico que se calcula con el MISMO núcleo de los esclavos."""

        def aplicar(r: np.ndarray) -> np.ndarray:
            z, cero = np.empty_like(r), np.zeros_like(r)
            for b, d in zip(bloques, datos, strict=True):
                local = DatosBloque(d.indptr, d.indices, d.datos, d.fila_inicial, r[b.rango])
                z[b.rango] = resolver_bloque(local, cero, Opciones(0.0, m)).x
            return z

        return aplicar

    def pcg(aplicar_M: Callable[[np.ndarray], np.ndarray]) -> int:
        x, r = np.zeros(p.n), p.b.copy()
        z = aplicar_M(r)
        d, rz, k = z.copy(), r @ z, 0
        while np.linalg.norm(r) / np.linalg.norm(p.b) > TOL:
            q = Acsr @ d
            a = rz / (d @ q)
            x, r, k = x + a * d, r - a * q, k + 1
            z = aplicar_M(r)
            rz, rz_viejo = r @ z, rz
            d = z + (rz / rz_viejo) * d
        return k

    print(f"| Gradiente conjugado + Jacobi por bloques exacto | {pcg(precond)} |")
    for m in (1, 2, 10, 20):
        k = pcg(barridos_desde_cero(m))
        print(f"| Gradiente conjugado + {m} barridos de Jacobi por bloque | {k} |")

    x = spla.spsolve(Acsr.tocsc(), p.b)
    print(f"\n(verificación: residuo de spsolve {p.residuo_relativo(x):.1e})")


if __name__ == "__main__":
    main()
