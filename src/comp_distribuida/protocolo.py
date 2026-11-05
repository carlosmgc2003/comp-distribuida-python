"""Protocolo de mensajes entre coordinador y esclavos (ZeroMQ multipart).

Cada mensaje es UNA sola ida y vuelta, con los arrays como bytes crudos:

    [VERSION, tipo, cabecera JSON, array_1, array_2, ...]

Comparación con el original:

* Antes se mandaba un mensaje JSON por cada coeficiente distinto de cero, cada
  uno con su respuesta 'RX' (decenas de miles de viajes de red por trabajo).
  Ahora un trabajo completo viaja en un único mensaje multiparte.
* Los arrays van como bytes binarios (sin pasar por texto): no se pierde
  precisión y se reconstruyen con ``np.frombuffer`` sin copiar.
* No se usa ``pickle`` (``send_pyobj``): deserializar pickle de la red permite
  ejecutar código arbitrario. La cabecera es JSON y los dtypes se validan.

Flujo (patrón "load balancing" de la guía de ZeroMQ, con el resultado
"a caballito" del pedido siguiente):

    esclavo (REQ)                       coordinador (ROUTER)
      PEDIR ─────────────────────────────▶
           ◀───────────────────────────── TRABAJO | ESPERA | FIN
      RESULTADO (y pide otro) ───────────▶
           ◀───────────────────────────── TRABAJO | ESPERA | FIN
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import numpy as np
import numpy.typing as npt

from comp_distribuida.solver import DatosBloque, Opciones

VERSION = b"comp-distribuida/2"

_DTYPES_PERMITIDOS = {"<f8": np.float64, "<i8": np.int64}

Frame = bytes | memoryview


class ErrorProtocolo(ValueError):
    """Mensaje malformado, de otra versión o con datos inconsistentes."""


class Tipo(StrEnum):
    PEDIR = "pedir"
    RESULTADO = "resultado"
    TRABAJO = "trabajo"
    ESPERA = "espera"
    FIN = "fin"


# --- Mensajes ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Pedir:
    esclavo: str


@dataclass(frozen=True, slots=True)
class Resultado:
    esclavo: str
    iteracion: int
    bloque: int
    x: npt.NDArray[np.float64]
    iteraciones: int
    residuo: float
    convergio: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class Trabajo:
    iteracion: int
    bloque: int
    datos: DatosBloque
    x_global: npt.NDArray[np.float64]
    opciones: Opciones


@dataclass(frozen=True, slots=True)
class Espera:
    milisegundos: int


@dataclass(frozen=True, slots=True)
class Fin:
    motivo: str = ""


Mensaje = Pedir | Resultado | Trabajo | Espera | Fin


# --- Serialización -----------------------------------------------------------------


def _array_a_frame(a: npt.NDArray[Any]) -> tuple[dict[str, Any], memoryview]:
    a = np.ascontiguousarray(a)
    dtype = a.dtype.newbyteorder("<").str  # orden de bytes explícito (little endian)
    if dtype not in _DTYPES_PERMITIDOS:
        raise ErrorProtocolo(f"dtype no soportado: {a.dtype}")
    a = a.astype(dtype, copy=False)
    return {"dtype": dtype, "n": int(a.size)}, memoryview(a).cast("B")


def _frame_a_array(meta: dict[str, Any], frame: Frame) -> npt.NDArray[Any]:
    try:
        dtype = np.dtype(_DTYPES_PERMITIDOS[meta["dtype"]]).newbyteorder("<")
        n = int(meta["n"])
    except (KeyError, TypeError, ValueError) as e:
        raise ErrorProtocolo(f"descriptor de array inválido: {meta!r}") from e
    if len(frame) != n * dtype.itemsize:
        raise ErrorProtocolo(f"el array declara {n} elementos pero llegaron {len(frame)} bytes")
    return np.frombuffer(frame, dtype=dtype)  # sin copia, sólo lectura


def codificar(mensaje: Mensaje) -> list[Frame]:
    arrays: list[npt.NDArray[Any]] = []
    cabecera: dict[str, Any]
    match mensaje:
        case Pedir(esclavo):
            tipo, cabecera = Tipo.PEDIR, {"esclavo": esclavo}
        case Resultado():
            tipo = Tipo.RESULTADO
            cabecera = {
                "esclavo": mensaje.esclavo,
                "iteracion": mensaje.iteracion,
                "bloque": mensaje.bloque,
                "iteraciones": mensaje.iteraciones,
                "residuo": mensaje.residuo,
                "convergio": mensaje.convergio,
                "error": mensaje.error,
            }
            arrays = [mensaje.x]
        case Trabajo():
            tipo = Tipo.TRABAJO
            d = mensaje.datos
            cabecera = {
                "iteracion": mensaje.iteracion,
                "bloque": mensaje.bloque,
                "fila_inicial": d.fila_inicial,
                "tolerancia": mensaje.opciones.tolerancia,
                "max_iteraciones": mensaje.opciones.max_iteraciones,
            }
            arrays = [d.indptr, d.indices, d.datos, d.b_local, mensaje.x_global]
        case Espera(ms):
            tipo, cabecera = Tipo.ESPERA, {"milisegundos": ms}
        case Fin(motivo):
            tipo, cabecera = Tipo.FIN, {"motivo": motivo}
        case _:
            raise ErrorProtocolo(f"mensaje desconocido: {mensaje!r}")

    descriptores, frames = [], []
    for a in arrays:
        meta, frame = _array_a_frame(a)
        descriptores.append(meta)
        frames.append(frame)
    cabecera["arrays"] = descriptores
    return [VERSION, tipo.encode(), json.dumps(cabecera).encode(), *frames]


def decodificar(frames: Sequence[Frame]) -> Mensaje:
    if len(frames) < 3:
        raise ErrorProtocolo(f"se esperaban al menos 3 frames y llegaron {len(frames)}")
    version, tipo_bytes, cabecera_bytes, *datos = frames
    if bytes(version) != VERSION:
        raise ErrorProtocolo(f"versión de protocolo incompatible: {bytes(version)!r}")
    try:
        tipo = Tipo(bytes(tipo_bytes).decode())
        c = json.loads(bytes(cabecera_bytes))
        descriptores = c.pop("arrays")
    except (ValueError, KeyError, UnicodeDecodeError) as e:
        raise ErrorProtocolo("cabecera inválida") from e
    if not isinstance(descriptores, list) or len(descriptores) != len(datos):
        raise ErrorProtocolo("la cantidad de arrays no coincide con la cabecera")
    arrays = [_frame_a_array(m, f) for m, f in zip(descriptores, datos, strict=True)]

    try:
        match tipo:
            case Tipo.PEDIR:
                return Pedir(esclavo=str(c["esclavo"]))
            case Tipo.RESULTADO:
                (x,) = arrays
                return Resultado(
                    esclavo=str(c["esclavo"]),
                    iteracion=int(c["iteracion"]),
                    bloque=int(c["bloque"]),
                    x=x,
                    iteraciones=int(c["iteraciones"]),
                    residuo=float(c["residuo"]),
                    convergio=bool(c["convergio"]),
                    error=c.get("error"),
                )
            case Tipo.TRABAJO:
                indptr, indices, datos_csr, b_local, x_global = arrays
                return Trabajo(
                    iteracion=int(c["iteracion"]),
                    bloque=int(c["bloque"]),
                    datos=DatosBloque(
                        indptr=indptr,
                        indices=indices,
                        datos=datos_csr,
                        fila_inicial=int(c["fila_inicial"]),
                        b_local=b_local,
                    ),
                    x_global=x_global,
                    opciones=Opciones(
                        tolerancia=float(c["tolerancia"]),
                        max_iteraciones=int(c["max_iteraciones"]),
                    ),
                )
            case Tipo.ESPERA:
                return Espera(milisegundos=int(c["milisegundos"]))
            case Tipo.FIN:
                return Fin(motivo=str(c.get("motivo", "")))
    except (KeyError, TypeError, ValueError) as e:
        raise ErrorProtocolo(f"mensaje {tipo} inválido") from e
    raise ErrorProtocolo(f"tipo no manejado: {tipo}")  # pragma: no cover
