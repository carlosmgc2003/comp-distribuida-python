"""Resolución distribuida de sistemas lineales con Jacobi por bloques (ZeroMQ + C++)."""

from comp_distribuida.coordinador import ConfigCoordinador, Coordinador, ResultadoGlobal
from comp_distribuida.esclavo import ConfigEsclavo, Esclavo, SinCoordinador
from comp_distribuida.problema import Bloque, Problema, cargar, generar_placa, particionar
from comp_distribuida.solver import ErrorCalculo, Opciones, motor_disponible, resolver_bloque

__all__ = [
    "Bloque",
    "ConfigCoordinador",
    "ConfigEsclavo",
    "Coordinador",
    "ErrorCalculo",
    "Esclavo",
    "Opciones",
    "Problema",
    "ResultadoGlobal",
    "SinCoordinador",
    "cargar",
    "generar_placa",
    "motor_disponible",
    "particionar",
    "resolver_bloque",
]

__version__ = "2.0.0"
