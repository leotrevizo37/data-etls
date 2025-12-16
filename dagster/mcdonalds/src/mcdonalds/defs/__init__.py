from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job
from .resources.sql_server import sqlserver_resource
from .resources.spark import spark_resource

defs = Definitions(
    jobs=[fact_lecturas_ingestion_job],
    resources={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
    },
)
