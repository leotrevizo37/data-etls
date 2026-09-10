from dagster import Definitions

from .sensors.asset_row_count_trend_sensor import asset_row_count_trend_sensor
from .sensors.operational_alerts_sensor import operational_alerts_sensor


def _build_defs() -> Definitions:
    return Definitions(
        sensors=[
            asset_row_count_trend_sensor,
            operational_alerts_sensor,
        ],
    )


def __getattr__(name: str):
    if name == "defs":
        return _build_defs()
    raise AttributeError(name)
