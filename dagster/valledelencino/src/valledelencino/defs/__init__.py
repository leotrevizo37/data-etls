from __future__ import annotations

from functools import lru_cache
from typing import Any

__all__ = ["defs"]


@lru_cache(maxsize=1)
def _build_defs() -> Any:
    from dagster import Definitions

    from .asset_checks import quality_asset_checks
    from .assets import (
        cleanup_old_runs,
        dbo_devices,
        dbo_sensor_tys,
        dbo_sensors,
        dbo_device_tys,
        dbo_sublocations,
        dbo_sublocation_tys,
        dwh_dimsidonproddimensions,
        dwh_factlecturas,
        sense_operational_assets,
    )
    from .jobs import (
        cleanup_job,
        fact_lecturas_backfill_daily_job,
        fact_lecturas_ingestion_job,
        fact_lecturas_sensor_backfill_job,
        index_maintenance_job,
        refresh_dimensions_job,
        sense_operational_job,
        spark_df_io_manager_healthcheck_job,
    )
    from .resources import (
        dim_sqlserver_resource,
        spark_df_io_manager,
        spark_resource,
        sqlserver_resource,
        transformation_ops,
    )
    from .schedules import (
        cleanup_schedule,
        fact_lecturas_sched_every_1h,
        fact_lecturas_sched_every_6h,
        fact_lecturas_sched_every_w,
        factlecturas_cleanup_schedule,
        index_maintenance_schedule,
        sense_operational_sched_every_6h,
        sense_operational_sched_every_w,
        sense_operational_sched_every_1h_w,
    )
    from .sensors import (
        dwh_dimsidonproddimensions_sensor,
    )

    return Definitions(
        jobs=[
            fact_lecturas_ingestion_job,
            fact_lecturas_backfill_daily_job,
            fact_lecturas_sensor_backfill_job,
            sense_operational_job,
            refresh_dimensions_job,
            spark_df_io_manager_healthcheck_job,
            index_maintenance_job,
            cleanup_job,
        ],
        resources={
            "sqlserver": sqlserver_resource,
            "spark": spark_resource,
            "dim_sqlserver": dim_sqlserver_resource,
            "transformation_ops": transformation_ops,
            "spark_df_io_manager": spark_df_io_manager,
        },
        schedules=[
            fact_lecturas_sched_every_6h,
            fact_lecturas_sched_every_w,
            fact_lecturas_sched_every_1h,
            factlecturas_cleanup_schedule,
            sense_operational_sched_every_1h_w,
            sense_operational_sched_every_w,
            sense_operational_sched_every_6h,
            cleanup_schedule,
            index_maintenance_schedule,
        ],
        sensors=[
            dwh_dimsidonproddimensions_sensor,
        ],
        assets=[
            dbo_sensors,
            dbo_sensor_tys,
            dbo_devices,
            dbo_device_tys,
            dbo_sublocations,
            dbo_sublocation_tys,
            dwh_factlecturas,
            sense_operational_assets,
            dwh_dimsidonproddimensions,
            cleanup_old_runs,
        ],
        asset_checks=quality_asset_checks,
    )


def __getattr__(name: str) -> Any:
    if name == "defs":
        return _build_defs()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"defs"})
