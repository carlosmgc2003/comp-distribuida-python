from __future__ import annotations

import threading
from collections.abc import Iterator

import pytest
import zmq

from comp_distribuida import ConfigEsclavo, Esclavo, Problema, generar_placa


@pytest.fixture
def placa() -> Problema:
    return generar_placa(8)


@pytest.fixture
def contexto() -> Iterator[zmq.Context[zmq.Socket[bytes]]]:
    ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    yield ctx
    ctx.term()


class Esclavos:
    """Lanza esclavos en hilos y los detiene al final del test."""

    def __init__(self, contexto: zmq.Context[zmq.Socket[bytes]]) -> None:
        self.contexto = contexto
        self.detener = threading.Event()
        self.hilos: list[threading.Thread] = []
        self.esclavos: list[Esclavo] = []

    def lanzar(self, endpoint: str, cantidad: int, motor: str = "auto") -> None:
        for i in range(cantidad):
            config = ConfigEsclavo(
                direccion=endpoint,
                nombre=f"test-{len(self.hilos)}",
                timeout_respuesta_s=2,
                reintentos=2,
                motor=motor,
            )  # type: ignore[arg-type]
            e = Esclavo(config, self.contexto, self.detener)
            h = threading.Thread(target=e.ejecutar, daemon=True, name=f"esclavo-{i}")
            h.start()
            self.esclavos.append(e)
            self.hilos.append(h)

    def cerrar(self) -> None:
        self.detener.set()
        for h in self.hilos:
            h.join(timeout=10)
            assert not h.is_alive(), "un esclavo no terminó"


@pytest.fixture
def esclavos(contexto: zmq.Context[zmq.Socket[bytes]]) -> Iterator[Esclavos]:
    e = Esclavos(contexto)
    yield e
    e.cerrar()
