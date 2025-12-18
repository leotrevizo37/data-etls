from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job
from .resources.sql_server import sqlserver_resource
from .resources.spark import spark_resource
from .resources.sql_server_dimensions import dim_sqlserver_resource
from .schedules.factlecturas_scheduler import fact_lecturas_sched_every_2h
from .assets.factoperations import factoperations

defs = Definitions(
    jobs=[fact_lecturas_ingestion_job],
    resources={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
        "dim_sqlserver": dim_sqlserver_resource
    },
    schedules=[fact_lecturas_sched_every_2h],
    assets=[factoperations],
)
