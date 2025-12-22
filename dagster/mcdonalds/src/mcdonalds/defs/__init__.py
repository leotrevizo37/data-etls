from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job
from .jobs.factoperations_ingestion import fact_operaciones_ingestion_job
from .resources.sql_server import sqlserver_resource
from .resources.spark import spark_resource
from .resources.sql_server_dimensions import dim_sqlserver_resource
from .schedules.factlecturas_scheduler import fact_lecturas_sched_every_2h
from .schedules.factoperations_scheduler import fact_operaciones_sched_every_4h
from .assets.factoperations import factoperations

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
    resources={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
        "dim_sqlserver": dim_sqlserver_resource
    },
    schedules=[
        fact_lecturas_sched_every_2h,
        fact_operaciones_sched_every_4h
    ],
    assets=[
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        factoperations,
        dwh_factlecturas
    ],
)
