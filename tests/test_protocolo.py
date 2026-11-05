import json

import numpy as np
import pytest

from comp_distribuida.problema import generar_placa, particionar
from comp_distribuida.protocolo import (
    VERSION,
    ErrorProtocolo,
    Espera,
    Fin,
    Pedir,
    Resultado,
    Trabajo,
    codificar,
    decodificar,
)
from comp_distribuida.solver import DatosBloque, Opciones


def ida_y_vuelta(m):  # type: ignore[no-untyped-def]
    return decodificar([bytes(f) for f in codificar(m)])


def test_mensajes_simples() -> None:
    assert ida_y_vuelta(Pedir("e1")) == Pedir("e1")
    assert ida_y_vuelta(Espera(50)) == Espera(50)
    assert ida_y_vuelta(Fin("listo")) == Fin("listo")


def test_trabajo_y_resultado_preservan_bits() -> None:
    p = generar_placa(5)
    b = particionar(p.n, 7)[1]
    x = np.random.default_rng(1).normal(size=p.n)  # valores con muchos decimales
    t = ida_y_vuelta(Trabajo(3, b.indice, DatosBloque.desde_problema(p, b), x, Opciones(1e-9, 7)))
    assert isinstance(t, Trabajo)
    assert (t.iteracion, t.bloque, t.opciones) == (3, 1, Opciones(1e-9, 7))
    np.testing.assert_array_equal(t.x_global, x)  # exacto, no "6 decimales"
    assert t.datos.fila_inicial == b.inicio
    assert t.datos.indices.dtype == np.int64

    r = ida_y_vuelta(Resultado("e", 3, 1, x[:7], 12, 1e-11, True))
    assert isinstance(r, Resultado)
    np.testing.assert_array_equal(r.x, x[:7])
    assert r.error is None


def test_rechaza_otra_version() -> None:
    frames = [bytes(f) for f in codificar(Pedir("x"))]
    frames[0] = b"comp-distribuida/1"
    with pytest.raises(ErrorProtocolo, match="versión"):
        decodificar(frames)


def test_rechaza_tamano_inconsistente() -> None:
    frames = [bytes(f) for f in codificar(Resultado("e", 1, 0, np.ones(4), 1, 0.0, True))]
    frames[-1] = frames[-1][:-8]
    with pytest.raises(ErrorProtocolo, match="bytes"):
        decodificar(frames)


def test_no_acepta_dtypes_arbitrarios() -> None:
    cabecera = {
        "esclavo": "e",
        "iteracion": 1,
        "bloque": 0,
        "iteraciones": 1,
        "residuo": 0.0,
        "convergio": True,
        "arrays": [{"dtype": "|O", "n": 1}],
    }
    with pytest.raises(ErrorProtocolo):
        decodificar([VERSION, b"resultado", json.dumps(cabecera).encode(), b"12345678"])


@pytest.mark.parametrize(
    "frames",
    [
        [],
        [VERSION, b"pedir"],
        [VERSION, b"nada", b"{}"],
        [VERSION, b"pedir", b"no es json"],
        [VERSION, b"pedir", b'{"arrays": []}'],
    ],
)
def test_basura(frames: list[bytes]) -> None:
    with pytest.raises(ErrorProtocolo):
        decodificar(frames)
