from __future__ import annotations

from functools import lru_cache
from typing import Any


__all__ = ["defs"]


@lru_cache(maxsize=1)
def _build_defs() -> Any:
    from dagster import Definitions

    from .resources import (
        dim_sqlserver_resource,
        manager_access_api_resource,
        spark_df_io_manager,
        spark_resource,
        sqlserver_resource,
    )
    from .sensors import (
        asset_row_count_trend_sensor,
        operational_alerts_sensor,
    )

    return Definitions(
        resources={
            "spark": spark_resource,
            "spark_df_io_manager": spark_df_io_manager,
            "sqlserver": sqlserver_resource,
            "dim_sqlserver": dim_sqlserver_resource,
            "manager_access_api": manager_access_api_resource,
        },
        sensors=[
            asset_row_count_trend_sensor,
            operational_alerts_sensor,
        ],
    )


def __getattr__(name: str) -> Any:
    if name == "defs":
        return _build_defs()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"defs"})
