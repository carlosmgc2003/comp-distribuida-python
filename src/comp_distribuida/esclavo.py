"""Esclavo: pide trabajos, los resuelve con el núcleo C++ (o NumPy) y devuelve el resultado.

Usa un socket REQ con el patrón "Lazy Pirate" de la guía de ZeroMQ: si el
coordinador no contesta a tiempo, se cierra el socket (LINGER=0), se abre uno
nuevo y se reintenta. El original hacía algo parecido pero:

* nunca cerraba los sockets viejos (quedaban registrados en el Poller),
* en el primer arranque fallaba con ``NameError: filePath`` al no existir el .db,
* comparaba ``e == zmq.ZMQError`` (instancia contra clase: siempre False).
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from types import TracebackType

import zmq

from comp_distribuida.protocolo import (
    ErrorProtocolo,
    Espera,
    Fin,
    Mensaje,
    Pedir,
    Resultado,
    Trabajo,
    codificar,
    decodificar,
)
from comp_distribuida.solver import ErrorCalculo, Motor, motor_disponible, resolver_bloque

log = logging.getLogger(__name__)


class SinCoordinador(ConnectionError):
    """El coordinador no respondió después de todos los reintentos."""


@dataclass(frozen=True, slots=True)
class ConfigEsclavo:
    direccion: str = "tcp://localhost:5555"
    nombre: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    timeout_respuesta_s: float = 10.0
    reintentos: int = 3
    motor: Motor = "auto"


@dataclass(slots=True)
class Estadisticas:
    trabajos: int = 0
    segundos_calculo: float = 0.0


class Esclavo:
    def __init__(
        self,
        config: ConfigEsclavo = ConfigEsclavo(),
        contexto: zmq.Context[zmq.Socket[bytes]] | None = None,
        detener: threading.Event | None = None,
    ) -> None:
        self.config = config
        self.motor = motor_disponible(config.motor)
        self.estadisticas = Estadisticas()
        self._contexto = contexto or zmq.Context.instance()
        self._detener = detener or threading.Event()
        self._socket: zmq.Socket[bytes] | None = None

    # --- ciclo de vida ------------------------------------------------------------

    def _abrir(self) -> zmq.Socket[bytes]:
        self._cerrar()
        s: zmq.Socket[bytes] = self._contexto.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(self.config.direccion)
        self._socket = s
        return s

    def _cerrar(self) -> None:
        if self._socket is not None:
            self._socket.close(linger=0)
            self._socket = None

    def __enter__(self) -> Esclavo:
        return self

    def __exit__(
        self, tipo: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self._cerrar()

    # --- bucle principal ----------------------------------------------------------

    def ejecutar(self) -> Estadisticas:
        """Trabaja hasta recibir FIN, hasta que se active `detener` o se pierda el coordinador."""
        nombre = self.config.nombre
        log.info(
            "Esclavo %s conectando a %s (motor: %s)", nombre, self.config.direccion, self.motor
        )
        mensaje: Mensaje = Pedir(nombre)
        try:
            while not self._detener.is_set():
                respuesta = self._solicitar(mensaje)
                match respuesta:
                    case Trabajo():
                        mensaje = self._resolver(respuesta)
                    case Espera(ms):
                        self._detener.wait(ms / 1000)
                        mensaje = Pedir(nombre)
                    case Fin(motivo):
                        log.info(
                            "Fin recibido (%s). Trabajos realizados: %d",
                            motivo,
                            self.estadisticas.trabajos,
                        )
                        break
                    case _:
                        log.warning("respuesta inesperada: %r", respuesta)
                        mensaje = Pedir(nombre)
        finally:
            self._cerrar()
        return self.estadisticas

    def _resolver(self, t: Trabajo) -> Resultado:
        inicio = time.perf_counter()
        try:
            r = resolver_bloque(t.datos, t.x_global, t.opciones, motor=self.motor)
        except ErrorCalculo as e:
            log.error("no se pudo resolver el bloque %d: %s", t.bloque, e)
            return Resultado(
                self.config.nombre,
                t.iteracion,
                t.bloque,
                t.datos.b_local[:0],
                0,
                float("nan"),
                False,
                error=str(e),
            )
        segundos = time.perf_counter() - inicio
        self.estadisticas.trabajos += 1
        self.estadisticas.segundos_calculo += segundos
        log.debug(
            "iteración %d bloque %d: %d barridos, residuo %.2e, %.4f s",
            t.iteracion,
            t.bloque,
            r.iteraciones,
            r.residuo,
            segundos,
        )
        return Resultado(
            self.config.nombre, t.iteracion, t.bloque, r.x, r.iteraciones, r.residuo, r.convergio
        )

    def _solicitar(self, mensaje: Mensaje) -> Mensaje:
        """Envía y espera respuesta, reintentando con un socket nuevo si hay timeout."""
        frames = codificar(mensaje)
        for intento in range(1, self.config.reintentos + 1):
            s = self._socket or self._abrir()
            s.send_multipart(frames, copy=False)
            if s.poll(int(self.config.timeout_respuesta_s * 1000), zmq.POLLIN):
                try:
                    return decodificar(s.recv_multipart())
                except ErrorProtocolo as e:
                    log.error("respuesta inválida del coordinador: %s", e)
                    raise
            if self._detener.is_set():
                break
            log.warning(
                "sin respuesta del coordinador (intento %d/%d)", intento, self.config.reintentos
            )
            self._abrir()  # un REQ sin respuesta queda bloqueado: hay que recrearlo
        raise SinCoordinador(f"el coordinador en {self.config.direccion} no responde")
