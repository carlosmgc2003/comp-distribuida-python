"""Coordinador + esclavos reales sobre TCP local (escenarios que rompían el original)."""

import threading

import numpy as np
import pytest
import scipy.sparse.linalg as spla
import zmq

from comp_distribuida import (
    ConfigCoordinador,
    Coordinador,
    ErrorCalculo,
    Opciones,
    Problema,
    generar_placa,
)
from comp_distribuida.protocolo import Pedir, Trabajo, codificar, decodificar

from .conftest import Esclavos


def config(**kw):  # type: ignore[no-untyped-def]
    base = dict(
        direccion="tcp://127.0.0.1:*",
        tam_bloque=16,
        tolerancia=1e-9,
        opciones_internas=Opciones(1e-10, 200),
        despedida_s=0.3,
        timeout_total_s=60,
    )
    base.update(kw)
    return ConfigCoordinador(**base)  # type: ignore[arg-type]


def verificar(p: Problema, x: np.ndarray) -> None:
    np.testing.assert_allclose(x, spla.spsolve(p.A.tocsc(), p.b), atol=1e-6)


@pytest.mark.parametrize(
    ("tam", "n_esclavos"),
    [
        (64, 1),  # caso que sí andaba en el original: un único trabajo
        (64, 3),  # más esclavos que trabajos -> EFSM en el original
        (16, 3),  # varios trabajos y varios esclavos -> EFSM en el original
        (10, 2),  # 64 no es múltiplo de 10 -> filas perdidas en el original
    ],
)
def test_resuelve_la_placa(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos, tam: int, n_esclavos: int
) -> None:
    with Coordinador(placa, config(tam_bloque=tam), contexto) as c:
        esclavos.lanzar(c.endpoint, n_esclavos)
        r = c.ejecutar()
    assert r.convergio
    assert r.residuo <= 1e-9
    verificar(placa, r.x)
    assert len(r.esclavos) == n_esclavos or tam == 64  # con 1 bloque puede trabajar uno solo


def test_un_barrido_por_trabajo_es_jacobi_clasico(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos
) -> None:
    with Coordinador(
        placa,
        config(opciones_internas=Opciones(0.0, 1), max_iteraciones_externas=5000, tolerancia=1e-8),
        contexto,
    ) as c:
        esclavos.lanzar(c.endpoint, 2)
        r = c.ejecutar()
    assert r.convergio
    assert r.iteraciones_externas > 100  # Jacobi puro necesita muchas más iteraciones


@pytest.mark.parametrize("metodo", ["jacobi", "pcg"])
def test_esclavo_que_muere_se_reasigna(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos, metodo: str
) -> None:
    """Un 'esclavo' toma un trabajo y desaparece: el bloque debe reasignarse."""
    with Coordinador(placa, config(plazo_trabajo_s=0.5, metodo=metodo), contexto) as c:
        tomado = threading.Event()

        def esclavo_que_muere() -> None:
            s = contexto.socket(zmq.REQ)
            s.connect(c.endpoint)
            s.send_multipart(codificar(Pedir("zombi")))
            assert isinstance(decodificar(s.recv_multipart()), Trabajo)
            tomado.set()
            s.close(linger=0)  # muere sin devolver nada

        zombi = threading.Thread(target=esclavo_que_muere)
        zombi.start()
        hilo_coordinador_resultado: list = []
        h = threading.Thread(target=lambda: hilo_coordinador_resultado.append(c.ejecutar()))
        h.start()
        assert tomado.wait(10)
        zombi.join()
        esclavos.lanzar(c.endpoint, 2)
        h.join(timeout=60)
    (r,) = hilo_coordinador_resultado
    assert r.convergio
    assert r.trabajos_reasignados >= 1
    verificar(placa, r.x)


def test_error_de_calculo_se_propaga(contexto: zmq.Context, esclavos: Esclavos) -> None:
    p = Problema.crear([[0.0, 1.0], [1.0, 0.0]], [1.0, 1.0])
    with Coordinador(p, config(tam_bloque=2), contexto) as c:
        esclavos.lanzar(c.endpoint, 1)
        with pytest.raises(ErrorCalculo, match="diagonal"):
            c.ejecutar()


def test_mensaje_basura_no_tumba_al_coordinador(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos
) -> None:
    with Coordinador(placa, config(), contexto) as c:
        intruso = contexto.socket(zmq.REQ)
        intruso.connect(c.endpoint)
        intruso.send_multipart([b"hola", b"mundo"])
        esclavos.lanzar(c.endpoint, 2)
        r = c.ejecutar()
        assert intruso.poll(2000)
        intruso.recv_multipart()  # recibió una respuesta (FIN), no quedó colgado
        intruso.close(linger=0)
    assert r.convergio


# --- gradiente conjugado precondicionado ---------------------------------------------


@pytest.mark.parametrize(("tam", "n_esclavos"), [(64, 1), (32, 2), (16, 3), (10, 2)])
def test_pcg_resuelve_la_placa_con_menos_rondas(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos, tam: int, n_esclavos: int
) -> None:
    resultados = {}
    for metodo in ("jacobi", "pcg"):
        cfg = config(tam_bloque=tam, metodo=metodo, max_iteraciones_externas=5000)
        with Coordinador(placa, cfg, contexto) as c:
            esclavos.lanzar(c.endpoint, n_esclavos)
            resultados[metodo] = c.ejecutar()
    r = resultados["pcg"]
    assert r.convergio
    assert r.metodo == "pcg"
    assert r.residuo == pytest.approx(placa.residuo_relativo(r.x))  # residuo verdadero
    assert r.residuo <= 1e-9
    verificar(placa, r.x)
    # Una ronda por iteración (la primera aplica M⁻¹ a r⁰ = b, la última no hace falta).
    assert r.rondas == r.iteraciones_externas
    assert r.trabajos_enviados >= r.rondas * len(range(0, placa.n, tam))
    # 64 incógnitas: CG termina en a lo sumo n pasos y tiene que ganarle a Jacobi.
    assert r.rondas <= placa.n
    if tam < placa.n:  # con un único bloque los dos resuelven el sistema exacto
        assert r.rondas < resultados["jacobi"].rondas
    else:
        assert r.rondas <= resultados["jacobi"].rondas


@pytest.mark.parametrize("opciones", [Opciones(0.1, 200), Opciones(0.0, 3), Opciones(0.0, 1)])
def test_pcg_con_precondicionador_inexacto(
    placa: Problema, contexto: zmq.Context, esclavos: Esclavos, opciones: Opciones
) -> None:
    """Con pocos barridos (o cortando por tolerancia) M⁻¹ es aproximado; CG flexible
    igual converge. Con 1 barrido M = diag(A): es gradiente conjugado con Jacobi puntual."""
    with Coordinador(placa, config(metodo="pcg", opciones_internas=opciones), contexto) as c:
        esclavos.lanzar(c.endpoint, 2)
        r = c.ejecutar()
    assert r.convergio
    verificar(placa, r.x)


def test_pcg_rechaza_lo_que_no_puede_resolver(contexto: zmq.Context) -> None:
    no_simetrica = Problema.crear([[4.0, -1.0], [-2.0, 4.0]], [1.0, 1.0])
    with pytest.raises(ValueError, match="simétrica"):
        Coordinador(no_simetrica, config(metodo="pcg"), contexto)
    con_diagonal_negativa = Problema.crear([[-4.0, 1.0], [1.0, -4.0]], [1.0, 1.0])
    with pytest.raises(ValueError, match="definida positiva"):
        Coordinador(con_diagonal_negativa, config(metodo="pcg"), contexto)
    with pytest.raises(ValueError, match="barrido"):
        Coordinador(
            generar_placa(3), config(metodo="pcg", opciones_internas=Opciones(0, 0)), contexto
        )
    with pytest.raises(ValueError, match="método"):
        Coordinador(generar_placa(3), config(metodo="gauss"), contexto)


def test_pcg_detecta_matriz_indefinida(contexto: zmq.Context, esclavos: Esclavos) -> None:
    # Simétrica, diagonal positiva, pero autovalores -1 y 3: d·A·d < 0 para d = (1, -1).
    p = Problema.crear([[1.0, 2.0], [2.0, 1.0]], [1.0, -1.0])
    with Coordinador(p, config(tam_bloque=1, metodo="pcg"), contexto) as c:
        esclavos.lanzar(c.endpoint, 1)
        with pytest.raises(ErrorCalculo, match="definida positiva"):
            c.ejecutar()
