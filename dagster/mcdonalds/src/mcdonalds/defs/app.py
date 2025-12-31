from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job
from .jobs.factoperations_ingestion import fact_operaciones_ingestion_job
from .schedules.factlecturas_scheduler import fact_lecturas_sched_every_2h
from .schedules.factoperations_scheduler import fact_operaciones_sched_every_4h
from .assets.dwh_factReadingsMeasurement import dwh_factReadingsMeasurement
from .assets.source_assets import (dbo_sensors,
                                   dbo_sensor_tys,
                                   dbo_devices,
                                   dbo_device_tys,
                                   dbo_sublocations,
                                   dwh_factlecturas)

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
        fact_operaciones_ingestion_job
    ],
    schedules=[
        fact_lecturas_sched_every_2h,
        fact_operaciones_sched_every_4h,
    ],
    assets=[
        dwh_factReadingsMeasurement,
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        dwh_factlecturas
    ],
)
