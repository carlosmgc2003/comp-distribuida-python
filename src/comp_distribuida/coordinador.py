"""Coordinador: reparte bloques entre esclavos y resuelve A·x = b.

Hay dos métodos (``ConfigCoordinador.metodo``). En los dos, una **ronda** es lo
mismo para los esclavos: cada uno resuelve (con m barridos de Jacobi) el sistema
local de un bloque de filas. Cambia qué hace el coordinador con los resultados.

``"jacobi"`` — Jacobi por bloques síncrono (una ronda por iteración):

    x⁰ = 0
    repetir k = 1, 2, ...
        para cada bloque B (en paralelo, en los esclavos):
            x_B^k = resolver  A_BB·x_B = b_B - A_B,ext·x_ext^(k-1)
        si ||b - A·x^k|| / ||b|| <= tolerancia: terminar

``"pcg"`` — gradiente conjugado precondicionado con Jacobi por bloques. Exige A
simétrica definida positiva. Los esclavos aplican z = M⁻¹·r (M = diagonal por
bloques de A) resolviendo A_BB·z_B = r_B con x_ext = 0; el coordinador hace el
producto A·d y los productos escalares. Una ronda por iteración, pero muchas
menos iteraciones: κ(M⁻¹A) es mucho menor que κ(A) (ver docs/matematica.md).

    x⁰ = 0,  r⁰ = b,  z⁰ = M⁻¹r⁰,  d⁰ = z⁰
    repetir k = 0, 1, ...
        α = (rᵏ·zᵏ) / (dᵏ·A·dᵏ)
        xᵏ⁺¹ = xᵏ + α·dᵏ,   rᵏ⁺¹ = rᵏ - α·A·dᵏ
        si ||rᵏ⁺¹|| / ||b|| <= tolerancia: terminar
        zᵏ⁺¹ = M⁻¹rᵏ⁺¹                                  (ronda en los esclavos)
        β = rᵏ⁺¹·(zᵏ⁺¹ - zᵏ) / (rᵏ·zᵏ)                    (Polak-Ribière)
        dᵏ⁺¹ = zᵏ⁺¹ + β·dᵏ

Se usa el β de Polak-Ribière (CG "flexible"): si los esclavos cortan por
tolerancia, M⁻¹ deja de ser un operador lineal fijo y el β clásico pierde sus
garantías; con un M⁻¹ lineal los dos β coinciden en aritmética exacta.

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
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Literal

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

Metodo = Literal["jacobi", "pcg"]
METODOS: tuple[Metodo, ...] = ("jacobi", "pcg")


@dataclass(frozen=True, slots=True)
class ConfigCoordinador:
    direccion: str = "tcp://*:5555"
    tam_bloque: int = 100
    tolerancia: float = 1e-8
    metodo: Metodo = "jacobi"
    """"jacobi" (Jacobi por bloques) o "pcg" (gradiente conjugado precondicionado)."""
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
    metodo: Metodo = "jacobi"
    rondas: int = 0
    """Rondas de trabajo repartidas a los esclavos (idas y vueltas por la red)."""
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
        if config.metodo not in METODOS:
            raise ValueError(f"método desconocido: {config.metodo!r} (opciones: {METODOS})")
        if config.metodo == "pcg":
            _validar_para_pcg(problema, config.opciones_internas)
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
            "Coordinador en %s (%s): n=%d, nnz=%d, %d bloques de hasta %d filas",
            self.endpoint,
            config.metodo,
            problema.n,
            problema.A.nnz,
            len(self.bloques),
            config.tam_bloque,
        )
        if not problema.es_diagonal_dominante():
            log.warning(
                "A no es diagonal dominante (encadenada): no está garantizado que Jacobi converja"
            )

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
            x=x,
            convergio=False,
            iteraciones_externas=0,
            residuo=self.problema.residuo_relativo(x),
            metodo=cfg.metodo,
        )
        try:
            if cfg.metodo == "pcg":
                self._gradiente_conjugado(resultado, inicio)
            else:
                self._jacobi(resultado, inicio)
            nivel = logging.INFO if resultado.convergio else logging.WARNING
            log.log(
                nivel,
                "%s en %d iteraciones (%s, %d rondas), residuo %.3e",
                "Convergió" if resultado.convergio else "NO convergió",
                resultado.iteraciones_externas,
                cfg.metodo,
                resultado.rondas,
                resultado.residuo,
            )
            # El tiempo de resolución no incluye la despedida (un plazo fijo de
            # `despedida_s` respondiendo FIN), que antes inflaba la medición.
            resultado.segundos = time.perf_counter() - inicio
            self._despedir("problema resuelto")
        except BaseException:
            self._despedir("el coordinador se detuvo")
            raise
        return resultado

    def _jacobi(self, resultado: ResultadoGlobal, inicio: float) -> None:
        """Jacobi por bloques: cada ronda devuelve directamente el nuevo x."""
        cfg = self.config
        x = resultado.x
        for k in range(1, cfg.max_iteraciones_externas + 1):
            x = self._ronda(k, self._datos, x, resultado, inicio)
            residuo = self.problema.residuo_relativo(x)
            resultado.x, resultado.residuo, resultado.iteraciones_externas = x, residuo, k
            resultado.historial_residuos.append(residuo)
            log.debug("iteración externa %d: residuo relativo %.3e", k, residuo)
            if residuo <= cfg.tolerancia:
                resultado.convergio = True
                return

    def _gradiente_conjugado(self, resultado: ResultadoGlobal, inicio: float) -> None:
        """Gradiente conjugado precondicionado (flexible); M⁻¹·r lo calculan los esclavos."""
        cfg = self.config
        A, b = self.problema.A, self.problema.b
        norma_b = float(np.linalg.norm(b)) or 1.0
        x = resultado.x.copy()
        r = b.copy()  # r⁰ = b - A·0
        if resultado.residuo <= cfg.tolerancia:  # b = 0: x = 0 ya es la solución
            resultado.convergio = True
            return
        z = self._precondicionar(1, r, resultado, inicio)
        rz = float(r @ z)
        d = z.copy()
        for k in range(1, cfg.max_iteraciones_externas + 1):
            if not np.isfinite(rz) or rz <= 0:
                raise ErrorCalculo(
                    f"el precondicionador no es definido positivo (r·z = {rz:.3e}); "
                    "¿A es simétrica definida positiva?"
                )
            q = A @ d
            dq = float(d @ q)
            if not np.isfinite(dq) or dq <= 0:
                raise ErrorCalculo(
                    f"A no es definida positiva (d·A·d = {dq:.3e}): usar --metodo jacobi"
                )
            alfa = rz / dq
            x = x + alfa * d
            r = r - alfa * q
            residuo = float(np.linalg.norm(r)) / norma_b
            if residuo <= cfg.tolerancia:
                # El residuo actualizado por recurrencia se aparta del verdadero por
                # redondeo: antes de terminar se confirma con b - A·x (y si no
                # alcanza, se sigue desde el residuo verdadero).
                r = b - A @ x
                residuo = float(np.linalg.norm(r)) / norma_b
            resultado.x, resultado.residuo, resultado.iteraciones_externas = x, residuo, k
            resultado.historial_residuos.append(residuo)
            log.debug("iteración PCG %d: residuo relativo %.3e", k, residuo)
            if residuo <= cfg.tolerancia:
                resultado.convergio = True
                return
            z_nuevo = self._precondicionar(k + 1, r, resultado, inicio)
            beta = float(r @ (z_nuevo - z)) / rz  # Polak-Ribière (CG flexible)
            z, rz = z_nuevo, float(r @ z_nuevo)
            d = z + beta * d

    def _precondicionar(
        self, k: int, r: VectorF, resultado: ResultadoGlobal, inicio: float
    ) -> VectorF:
        """z = M⁻¹·r en los esclavos: cada uno resuelve A_BB·z_B = r_B con x_ext = 0.

        Es el mismo trabajo que en Jacobi, con b_B reemplazado por r_B y x_global = 0
        (las columnas externas no aportan y la iteración interna arranca de 0).
        """
        datos = [
            replace(d, b_local=np.ascontiguousarray(r[b.rango]))
            for b, d in zip(self.bloques, self._datos, strict=True)
        ]
        return self._ronda(k, datos, np.zeros_like(r), resultado, inicio)

    def _ronda(
        self,
        k: int,
        datos: list[DatosBloque],
        x: VectorF,
        resultado: ResultadoGlobal,
        inicio: float,
    ) -> VectorF:
        """Reparte un trabajo por bloque (datos[i], x) y junta las soluciones locales."""
        resultado.rondas = k
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
            self._repartir(k, datos, x, pendientes, en_curso, resultado)

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
        datos: list[DatosBloque],
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
                datos=datos[indice],
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


def _validar_para_pcg(problema: Problema, opciones: Opciones) -> None:
    """El gradiente conjugado exige A simétrica (y definida positiva, que se detecta
    durante la iteración) y un precondicionador que haga al menos un barrido."""
    A = problema.A
    escala = float(abs(A).max()) or 1.0
    asimetria = float(abs(A - A.T).max()) if A.nnz else 0.0
    if asimetria > 1e-12 * escala:
        raise ValueError(
            f"el gradiente conjugado requiere A simétrica (max |a_ij - a_ji| = {asimetria:.3e}); "
            "usar el método jacobi"
        )
    if np.any(A.diagonal() <= 0):
        raise ValueError("el gradiente conjugado requiere A definida positiva (diagonal <= 0)")
    if opciones.max_iteraciones < 1:
        raise ValueError("con pcg los esclavos deben hacer al menos un barrido (max_iteraciones)")
