import numpy as np
import pytest
import scipy.sparse.linalg as spla

from comp_distribuida import solver
from comp_distribuida.problema import Problema, generar_placa, particionar
from comp_distribuida.solver import DatosBloque, ErrorCalculo, Opciones, resolver_bloque

MOTORES = [
    "numpy",
    pytest.param(
        "nativo",
        marks=pytest.mark.skipif(solver.jacobi_nativo is None, reason="núcleo C++ no compilado"),
    ),
]


@pytest.mark.parametrize("motor", MOTORES)
def test_un_bloque_resuelve_todo(motor: str) -> None:
    p = generar_placa(6)
    (b,) = particionar(p.n, p.n)
    r = resolver_bloque(
        DatosBloque.desde_problema(p, b), np.zeros(p.n), Opciones(1e-12, 100_000), motor=motor
    )  # type: ignore[arg-type]
    assert r.convergio
    assert p.residuo_relativo(r.x) == pytest.approx(r.residuo, abs=1e-14)
    np.testing.assert_allclose(r.x, spla.spsolve(p.A.tocsc(), p.b), atol=1e-9)


@pytest.mark.parametrize("motor", MOTORES)
def test_divide_por_la_diagonal(motor: str) -> None:
    # El C++ original hacía x_i = b_i - Σ a_ij x_j sin dividir por a_ii.
    p = Problema.crear([[4.0, -1.0], [-1.0, 4.0]], [3.0, 3.0])
    (b,) = particionar(2, 2)
    r = resolver_bloque(DatosBloque.desde_problema(p, b), np.zeros(2), Opciones(1e-14), motor=motor)  # type: ignore[arg-type]
    np.testing.assert_allclose(r.x, [1.0, 1.0], atol=1e-13)


@pytest.mark.parametrize("motor", MOTORES)
@pytest.mark.parametrize("tam", [10, 25, 64])
def test_jacobi_por_bloques_converge(motor: str, tam: int) -> None:
    p = generar_placa(8)
    bloques = particionar(p.n, tam)
    datos = [DatosBloque.desde_problema(p, b) for b in bloques]
    x = np.zeros(p.n)
    for _ in range(3000):
        x_nuevo = np.empty_like(x)
        for b, d in zip(bloques, datos, strict=True):
            x_nuevo[b.rango] = resolver_bloque(d, x, Opciones(1e-12), motor=motor).x  # type: ignore[arg-type]
        x = x_nuevo
        if p.residuo_relativo(x) < 1e-10:
            break
    np.testing.assert_allclose(x, spla.spsolve(p.A.tocsc(), p.b), atol=1e-7)


@pytest.mark.skipif(solver.jacobi_nativo is None, reason="núcleo C++ no compilado")
def test_nativo_y_numpy_coinciden() -> None:
    p = generar_placa(10)
    rng = np.random.default_rng(0)
    x0 = rng.normal(size=p.n)
    for b in particionar(p.n, 17):
        d = DatosBloque.desde_problema(p, b)
        a = resolver_bloque(d, x0, Opciones(1e-9, 50), motor="numpy")
        n = resolver_bloque(d, x0, Opciones(1e-9, 50), motor="nativo")
        assert a.iteraciones == n.iteraciones
        np.testing.assert_allclose(a.x, n.x, rtol=1e-12, atol=1e-12)
        assert a.residuo == pytest.approx(n.residuo, rel=1e-9)


@pytest.mark.parametrize("motor", MOTORES)
def test_errores(motor: str) -> None:
    nula = Problema.crear([[0.0, 1.0], [1.0, 0.0]], [1.0, 1.0])
    (b,) = particionar(2, 2)
    with pytest.raises(ErrorCalculo, match="diagonal"):
        resolver_bloque(DatosBloque.desde_problema(nula, b), np.zeros(2), motor=motor)  # type: ignore[arg-type]
    diverge = Problema.crear([[1.0, 3.0], [3.0, 1.0]], [1.0, 1.0])
    with pytest.raises(ErrorCalculo, match="divergió"):
        resolver_bloque(
            DatosBloque.desde_problema(diverge, b),
            np.zeros(2),
            Opciones(max_iteraciones=100_000),
            motor=motor,
        )  # type: ignore[arg-type]
