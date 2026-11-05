"""Línea de comandos: `comp-distribuida {generar,coordinador,esclavo,local}`.

Reemplaza los valores fijos del original (puerto 5000, rutas ./ejercicios/...,
`input()` para la IP) por argumentos.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
from pathlib import Path

import numpy as np
import zmq

from comp_distribuida.coordinador import ConfigCoordinador, Coordinador, ResultadoGlobal
from comp_distribuida.esclavo import ConfigEsclavo, Esclavo, SinCoordinador
from comp_distribuida.problema import cargar, generar_placa, guardar_texto
from comp_distribuida.solver import ErrorCalculo, Opciones, motor_disponible

log = logging.getLogger("comp_distribuida")


def _argumentos_coordinador(p: argparse.ArgumentParser) -> None:
    p.add_argument("matriz", type=Path, help="matriz A (texto denso, .mtx o .npz)")
    p.add_argument("vector", type=Path, help="vector b (texto)")
    p.add_argument("--tam-bloque", type=int, default=100, help="filas por trabajo (def. 100)")
    p.add_argument("--tol", type=float, default=1e-8, help="residuo relativo global (def. 1e-8)")
    p.add_argument("--max-externas", type=int, default=2000)
    p.add_argument("--tol-interna", type=float, default=1e-10)
    p.add_argument(
        "--max-internas",
        type=int,
        default=500,
        help="barridos de Jacobi por trabajo (1 = Jacobi clásico distribuido)",
    )
    p.add_argument(
        "--plazo", type=float, default=30.0, help="segundos antes de reasignar un trabajo"
    )
    p.add_argument("--guardar", type=Path, help="guardar la solución (.npy o .txt)")
    p.add_argument(
        "--graficar",
        nargs="?",
        const="",
        metavar="ARCHIVO.png",
        help="graficar la placa (en ventana, o en un archivo si se indica)",
    )


def _config_coordinador(a: argparse.Namespace, direccion: str) -> ConfigCoordinador:
    return ConfigCoordinador(
        direccion=direccion,
        tam_bloque=a.tam_bloque,
        tolerancia=a.tol,
        max_iteraciones_externas=a.max_externas,
        opciones_internas=Opciones(a.tol_interna, a.max_internas),
        plazo_trabajo_s=a.plazo,
    )


def _informar(a: argparse.Namespace, r: ResultadoGlobal) -> int:
    print(
        f"{'Convergió' if r.convergio else 'NO convergió'}: {r.iteraciones_externas} iteraciones "
        f"externas, residuo {r.residuo:.3e}, {r.trabajos_enviados} trabajos "
        f"({r.trabajos_reasignados} reasignados), {len(r.esclavos)} esclavos, {r.segundos:.2f} s"
    )
    if a.guardar:
        if a.guardar.suffix == ".npy":
            np.save(a.guardar, r.x)
        else:
            np.savetxt(a.guardar, r.x, fmt="%.17g")
        print(f"Solución guardada en {a.guardar}")
    if a.graficar is not None:
        from comp_distribuida.grafico import graficar_placa  # noqa: PLC0415 (opcional)

        graficar_placa(
            r.x,
            f"Placa: {r.iteraciones_externas} iteraciones, bloques de {a.tam_bloque} filas",
            a.graficar or None,
        )
    return 0 if r.convergio else 2


def cmd_generar(a: argparse.Namespace) -> int:
    a.directorio.mkdir(parents=True, exist_ok=True)
    problema = generar_placa(a.lado, a.temperatura)
    guardar_texto(problema, a.directorio / "matriz.txt", a.directorio / "vector.txt")
    print(f"Placa {a.lado}x{a.lado} (n={problema.n}) guardada en {a.directorio}/")
    return 0


def cmd_coordinador(a: argparse.Namespace) -> int:
    problema = cargar(a.matriz, a.vector)
    with Coordinador(problema, _config_coordinador(a, a.bind)) as coordinador:
        return _informar(a, coordinador.ejecutar())


def cmd_esclavo(a: argparse.Namespace) -> int:
    config = ConfigEsclavo(
        direccion=a.conectar, motor=a.motor, reintentos=a.reintentos, timeout_respuesta_s=a.timeout
    )
    with Esclavo(config) as esclavo:
        try:
            esclavo.ejecutar()
        except SinCoordinador as e:
            log.error("%s", e)
            return 1
    return 0


def cmd_local(a: argparse.Namespace) -> int:
    """Coordinador + N esclavos en hilos del mismo proceso (demo / pruebas)."""
    problema = cargar(a.matriz, a.vector)
    contexto: zmq.Context[zmq.Socket[bytes]] = zmq.Context.instance()
    detener = threading.Event()
    with Coordinador(problema, _config_coordinador(a, "tcp://127.0.0.1:*"), contexto) as c:
        hilos = [
            threading.Thread(
                target=Esclavo(
                    ConfigEsclavo(direccion=c.endpoint, nombre=f"hilo-{i}", motor=a.motor),
                    contexto,
                    detener,
                ).ejecutar,
                name=f"esclavo-{i}",
                daemon=True,
            )
            for i in range(a.esclavos)
        ]
        for h in hilos:
            h.start()
        try:
            resultado = c.ejecutar()
        finally:
            detener.set()
            for h in hilos:
                h.join(timeout=5)
    return _informar(a, resultado)


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="comp-distribuida", description=__doc__)
    parser.add_argument("-v", "--verbose", action="count", default=0)
    sub = parser.add_subparsers(dest="comando", required=True)

    g = sub.add_parser("generar", help="generar la placa de Laplace de ejemplo")
    g.add_argument("--lado", type=int, default=20)
    g.add_argument("--temperatura", type=float, default=100.0)
    g.add_argument("--directorio", type=Path, default=Path("ejercicios"))
    g.set_defaults(func=cmd_generar)

    c = sub.add_parser("coordinador", help="repartir el problema entre esclavos remotos")
    _argumentos_coordinador(c)
    c.add_argument("--bind", default="tcp://*:5555", help="dirección ZeroMQ (def. tcp://*:5555)")
    c.set_defaults(func=cmd_coordinador)

    e = sub.add_parser("esclavo", help="resolver trabajos de un coordinador")
    e.add_argument("--conectar", default="tcp://localhost:5555")
    e.add_argument("--motor", choices=["auto", "nativo", "numpy"], default="auto")
    e.add_argument("--timeout", type=float, default=10.0)
    e.add_argument("--reintentos", type=int, default=3)
    e.set_defaults(func=cmd_esclavo)

    loc = sub.add_parser("local", help="coordinador y esclavos en un solo proceso")
    _argumentos_coordinador(loc)
    loc.add_argument("--esclavos", type=int, default=4)
    loc.add_argument("--motor", choices=["auto", "nativo", "numpy"], default="auto")
    loc.set_defaults(func=cmd_local)
    return parser


def main(argv: list[str] | None = None) -> int:
    a = construir_parser().parse_args(argv)
    logging.basicConfig(
        level=[logging.WARNING, logging.INFO, logging.DEBUG][min(a.verbose, 2)],
        format="%(asctime)s %(levelname)-7s %(threadName)s %(name)s: %(message)s",
    )
    log.info("motor de cálculo disponible: %s", motor_disponible())
    try:
        return int(a.func(a))
    except KeyboardInterrupt:
        print("\nInterrumpido.", file=sys.stderr)
        return 130
    except (ErrorCalculo, OSError, ValueError) as e:
        log.error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
