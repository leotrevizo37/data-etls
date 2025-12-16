from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
    ],
)
