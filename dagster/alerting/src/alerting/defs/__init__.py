from dagster import Definitions

from .resources.alert_state_store import alert_state_store_resource
from .resources.email_notifier import email_notifier_resource
from .resources.mssql_reader import mssql_reader_resource
from .sensors.asset_row_count_trend_sensor import asset_row_count_trend_sensor
from .sensors.operational_alerts_sensor import operational_alerts_sensor


def _build_defs() -> Definitions:
    return Definitions(
        sensors=[asset_row_count_trend_sensor, operational_alerts_sensor],
        resources={
            "mssql_reader": mssql_reader_resource,
            "alert_state": alert_state_store_resource,
            "email_notifier": email_notifier_resource,
        },
    )


def __getattr__(name: str):
    if name == "defs":
        return _build_defs()
    raise AttributeError(name)
