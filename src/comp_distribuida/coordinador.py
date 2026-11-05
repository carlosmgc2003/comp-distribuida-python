"""Coordinador: reparte bloques entre esclavos y ejecuta Jacobi por bloques.

Algoritmo (Jacobi por bloques síncrono):

    x⁰ = 0
    repetir k = 1, 2, ...
        para cada bloque B (en paralelo, en los esclavos):
            x_B^k = resolver  A_BB·x_B = b_B - A_B,ext·x_ext^(k-1)
        si ||b - A·x^k|| / ||b|| <= tolerancia: terminar

Robustez frente al original:

* Socket ROUTER (no REP): atiende a muchos esclavos intercalados sin la máquina
  de estados estricta de REP, que provocaba "Operation cannot be accomplished in
  current state" en cuanto había 2 esclavos o más esclavos que trabajos.
* Siempre se responde a cada pedido (TRABAJO, ESPERA o FIN). Si no hay trabajo,
  el pedido queda "retenido" y se contesta apenas aparece uno (patrón broker de
  la guía de ZeroMQ): no hay esperas activas ni latencia agregada.
* Cada trabajo en curso tiene un plazo: si el esclavo muere, el bloque vuelve a
  la cola y lo toma otro. Los resultados repetidos o viejos se descartan.
* Criterio de parada por residuo global (antes: una cantidad fija de "veces").
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from types import TracebackType

import numpy as np
import zmq

from comp_distribuida.problema import Bloque, Problema, VectorF, particionar
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
from comp_distribuida.solver import DatosBloque, ErrorCalculo, Opciones

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConfigCoordinador:
    direccion: str = "tcp://*:5555"
    tam_bloque: int = 100
    tolerancia: float = 1e-8
    max_iteraciones_externas: int = 2_000
    opciones_internas: Opciones = field(
        default_factory=lambda: Opciones(tolerancia=1e-10, max_iteraciones=500)
    )
    plazo_trabajo_s: float = 30.0
    """Si un esclavo no devuelve un trabajo en este tiempo, se reasigna."""
    retencion_maxima_s: float = 1.0
    """Máximo tiempo que se retiene un pedido sin trabajo antes de responder ESPERA."""
    despedida_s: float = 1.0
    """Tiempo que se sigue respondiendo FIN para que los esclavos terminen limpio."""
    timeout_total_s: float | None = None


@dataclass(slots=True)
class ResultadoGlobal:
    x: VectorF
    convergio: bool
    iteraciones_externas: int
    residuo: float
    historial_residuos: list[float] = field(default_factory=list)
    trabajos_enviados: int = 0
    trabajos_reasignados: int = 0
    esclavos: set[str] = field(default_factory=set)
    segundos: float = 0.0


class Coordinador:
    def __init__(
        self,
        problema: Problema,
        config: ConfigCoordinador = ConfigCoordinador(),
        contexto: zmq.Context[zmq.Socket[bytes]] | None = None,
    ) -> None:
        self.problema = problema
        self.config = config
        self.bloques: list[Bloque] = particionar(problema.n, config.tam_bloque)
        # Las porciones CSR de cada bloque no cambian: se calculan una sola vez.
        self._datos = [DatosBloque.desde_problema(problema, b) for b in self.bloques]
        self._contexto = contexto or zmq.Context.instance()
        self._socket: zmq.Socket[bytes] = self._contexto.socket(zmq.ROUTER)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.bind(config.direccion)
        self.endpoint: str = self._socket.getsockopt_string(zmq.LAST_ENDPOINT)
        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
        # Esclavos esperando trabajo: (identidad ZeroMQ, momento en que pidieron)
        self._ociosos: deque[tuple[bytes, float]] = deque()
        log.info(
            "Coordinador en %s: n=%d, nnz=%d, %d bloques de hasta %d filas",
            self.endpoint,
            problema.n,
            problema.A.nnz,
            len(self.bloques),
            config.tam_bloque,
        )
        if not problema.es_diagonal_dominante():
            log.warning("A no es diagonal dominante: Jacobi podría no converger")

    # --- ciclo de vida ------------------------------------------------------------

    def cerrar(self) -> None:
        self._socket.close(linger=0)

    def __enter__(self) -> Coordinador:
        return self

    def __exit__(
        self, tipo: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.cerrar()

    # --- algoritmo ----------------------------------------------------------------

    def ejecutar(self) -> ResultadoGlobal:
        cfg = self.config
        inicio = time.perf_counter()
        x = np.zeros(self.problema.n)
        resultado = ResultadoGlobal(
            x=x, convergio=False, iteraciones_externas=0, residuo=self.problema.residuo_relativo(x)
        )
        try:
            for k in range(1, cfg.max_iteraciones_externas + 1):
                x = self._iteracion_externa(k, x, resultado, inicio)
                residuo = self.problema.residuo_relativo(x)
                resultado.x, resultado.residuo, resultado.iteraciones_externas = x, residuo, k
                resultado.historial_residuos.append(residuo)
                log.debug("iteración externa %d: residuo relativo %.3e", k, residuo)
                if residuo <= cfg.tolerancia:
                    resultado.convergio = True
                    break
            nivel = logging.INFO if resultado.convergio else logging.WARNING
            log.log(
                nivel,
                "%s en %d iteraciones externas, residuo %.3e",
                "Convergió" if resultado.convergio else "NO convergió",
                resultado.iteraciones_externas,
                resultado.residuo,
            )
            self._despedir("problema resuelto")
        except BaseException:
            self._despedir("el coordinador se detuvo")
            raise
        resultado.segundos = time.perf_counter() - inicio
        return resultado

    def _iteracion_externa(
        self, k: int, x: VectorF, resultado: ResultadoGlobal, inicio: float
    ) -> VectorF:
        cfg = self.config
        x_nuevo = np.empty_like(x)
        pendientes: deque[int] = deque(range(len(self.bloques)))
        en_curso: dict[int, float] = {}  # bloque -> plazo (time.monotonic)
        recibidos: set[int] = set()
        ultimo_aviso = time.monotonic()

        while len(recibidos) < len(self.bloques):
            if (
                cfg.timeout_total_s is not None
                and time.perf_counter() - inicio > cfg.timeout_total_s
            ):
                raise TimeoutError(f"se superaron {cfg.timeout_total_s} s")
            # Primero repartir: al empezar la iteración puede haber esclavos retenidos.
            self._repartir(k, x, pendientes, en_curso, resultado)

            if self._poller.poll(timeout=100):
                identidad, mensaje = self._recibir()
                if isinstance(mensaje, Resultado) and mensaje.error is not None:
                    self._enviar(identidad, Fin("error de cálculo"))
                    raise ErrorCalculo(f"el esclavo {mensaje.esclavo} informó: {mensaje.error}")
                if isinstance(mensaje, Resultado):
                    self._aceptar(k, mensaje, x_nuevo, recibidos, en_curso, pendientes)
                if isinstance(mensaje, Pedir | Resultado):
                    resultado.esclavos.add(mensaje.esclavo)
                    self._ociosos.append((identidad, time.monotonic()))
                else:  # None (no se pudo decodificar) u otro tipo que un esclavo no envía
                    self._enviar(identidad, Fin("mensaje inválido"))

            ahora = time.monotonic()
            for bloque, plazo in list(en_curso.items()):
                if ahora > plazo:
                    log.warning(
                        "bloque %d sin respuesta en %.1f s: se reasigna",
                        bloque,
                        cfg.plazo_trabajo_s,
                    )
                    del en_curso[bloque]
                    pendientes.append(bloque)
                    resultado.trabajos_reasignados += 1
            # Un pedido retenido demasiado tiempo vencería el timeout del esclavo.
            while self._ociosos and ahora - self._ociosos[0][1] > cfg.retencion_maxima_s:
                self._enviar(self._ociosos.popleft()[0], Espera(0))
            if not resultado.esclavos and ahora - ultimo_aviso > 5:
                log.info("Esperando esclavos en %s ...", self.endpoint)
                ultimo_aviso = ahora
        return x_nuevo

    def _aceptar(
        self,
        k: int,
        r: Resultado,
        x_nuevo: VectorF,
        recibidos: set[int],
        en_curso: dict[int, float],
        pendientes: deque[int],
    ) -> None:
        """Guarda un resultado si corresponde a esta iteración y aún no se tenía."""
        if r.iteracion != k or r.bloque in recibidos or not 0 <= r.bloque < len(self.bloques):
            log.debug(
                "descarto resultado viejo/duplicado: iteración %d bloque %d", r.iteracion, r.bloque
            )
            return
        bloque = self.bloques[r.bloque]
        if r.x.shape != (bloque.n_filas,):
            log.warning("resultado con n_filas incorrecto de %s: se descarta", r.esclavo)
            return
        if not r.convergio:
            log.debug(
                "bloque %d no alcanzó la tolerancia interna (residuo %.2e)", r.bloque, r.residuo
            )
        x_nuevo[bloque.rango] = r.x
        recibidos.add(r.bloque)
        en_curso.pop(r.bloque, None)
        if r.bloque in pendientes:  # había sido reasignado y llegó tarde: igual sirve
            pendientes.remove(r.bloque)

    def _repartir(
        self,
        k: int,
        x: VectorF,
        pendientes: deque[int],
        en_curso: dict[int, float],
        resultado: ResultadoGlobal,
    ) -> None:
        """Asigna bloques pendientes a esclavos ociosos (el que más espera, primero)."""
        while pendientes and self._ociosos:
            identidad, _ = self._ociosos.popleft()
            indice = pendientes.popleft()
            en_curso[indice] = time.monotonic() + self.config.plazo_trabajo_s
            trabajo = Trabajo(
                iteracion=k,
                bloque=indice,
                datos=self._datos[indice],
                x_global=x,
                opciones=self.config.opciones_internas,
            )
            self._enviar(identidad, trabajo)
            resultado.trabajos_enviados += 1

    def _despedir(self, motivo: str) -> None:
        """Durante un rato, responde FIN a todo el que pregunte."""
        while self._ociosos:
            self._enviar(self._ociosos.popleft()[0], Fin(motivo))
        limite = time.monotonic() + self.config.despedida_s
        while (restante := limite - time.monotonic()) > 0:
            if self._poller.poll(timeout=max(1, int(restante * 1000))):
                identidad, _ = self._recibir()
                self._enviar(identidad, Fin(motivo))

    # --- E/S ----------------------------------------------------------------------

    def _recibir(self) -> tuple[bytes, Mensaje | None]:
        # ROUTER antepone la identidad del par; REQ agrega un frame vacío.
        identidad, vacio, *frames = self._socket.recv_multipart(copy=False)
        try:
            if vacio.bytes != b"":
                raise ErrorProtocolo("falta el delimitador vacío (¿el cliente no es REQ?)")
            return identidad.bytes, decodificar([f.buffer for f in frames])
        except ErrorProtocolo as e:
            log.warning("mensaje descartado: %s", e)
            return identidad.bytes, None

    def _enviar(self, identidad: bytes, mensaje: Mensaje) -> None:
        self._socket.send_multipart([identidad, b"", *codificar(mensaje)], copy=False)
