from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "factlecturas_extended",
    "raw_dimensions",
    "sensor_expected_values",
    "maquinas_nieve",
    "maquinas_soda",
    "luces",
    "factReadingsMeasurement_device",
    "termostatos",
    "cuartos_congelacion",
    "cuartos_refrigeracion",
]

_MODULE_BY_EXPORT = {
    "factlecturas_extended": ".factlecturas_extended",
    "raw_dimensions": ".raw_dimensions",
    "sensor_expected_values": ".sensor_expected_values",
    "maquinas_nieve": ".maquinas_nieve",
    "maquinas_soda": ".maquinas_soda",
    "luces": ".luces",
    "factReadingsMeasurement_device": ".factReadingsMeasurement_device",
    "termostatos": ".termostatos",
    "cuartos_congelacion": ".cuartos_congelacion",
    "cuartos_refrigeracion": ".cuartos_refrigeracion",
}


def __getattr__(name: str) -> Any:
    module_path = _MODULE_BY_EXPORT.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_path, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
