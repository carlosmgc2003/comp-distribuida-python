from pathlib import Path

import numpy as np
import pytest
import scipy.sparse.linalg as spla

from comp_distribuida.problema import Problema, cargar, generar_placa, guardar_texto, particionar


@pytest.mark.parametrize(("n", "tam"), [(36, 36), (36, 12), (36, 10), (5, 100), (1, 1)])
def test_particionar_cubre_todas_las_filas(n: int, tam: int) -> None:
    bloques = particionar(n, tam)
    filas = [f for b in bloques for f in range(b.inicio, b.fin)]
    assert filas == list(range(n))  # el original perdía las filas sobrantes
    assert all(0 < b.n_filas <= tam for b in bloques)


@pytest.mark.parametrize(("n", "tam"), [(0, 1), (10, 0), (10, -3)])
def test_particionar_valida(n: int, tam: int) -> None:
    with pytest.raises(ValueError, match=r"filas|positivo"):
        particionar(n, tam)


def test_placa_es_diagonal_dominante_y_coincide_con_spsolve() -> None:
    p = generar_placa(5)
    assert p.n == 25
    assert p.es_diagonal_dominante()
    x = spla.spsolve(p.A.tocsc(), p.b)
    assert p.residuo_relativo(x) < 1e-12
    assert np.all(p.A.diagonal() == 4)


def test_cargar_tolera_espacios_multiples(tmp_path: Path) -> None:
    # Con csv.reader(delimiter=' ') la segunda fila quedaba corrida de columna.
    (tmp_path / "A.txt").write_text("4 -1  0\n-1  4 -1\n 0 -1   4\n")
    (tmp_path / "b.txt").write_text("1\n2  \n3\n")
    p = cargar(tmp_path / "A.txt", tmp_path / "b.txt")
    np.testing.assert_array_equal(p.A.toarray(), [[4, -1, 0], [-1, 4, -1], [0, -1, 4]])
    np.testing.assert_array_equal(p.b, [1, 2, 3])


def test_ida_y_vuelta_texto(tmp_path: Path) -> None:
    p = generar_placa(4)
    guardar_texto(p, tmp_path / "m.txt", tmp_path / "v.txt")
    q = cargar(tmp_path / "m.txt", tmp_path / "v.txt")
    assert (p.A != q.A).nnz == 0
    np.testing.assert_array_equal(p.b, q.b)


def test_problema_valida_dimensiones() -> None:
    with pytest.raises(ValueError, match="cuadrada"):
        Problema.crear(np.ones((2, 3)), np.ones(2))
    with pytest.raises(ValueError, match="elementos"):
        Problema.crear(np.eye(3), np.ones(2))
    with pytest.raises(ValueError, match="NaN"):
        Problema.crear(np.eye(2), [1.0, np.nan])
