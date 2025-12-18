from dagster import Definitions
from .jobs.factlecturas_ingestion import fact_lecturas_ingestion_job
from .schedules.factlecturas_scheduler import fact_lecturas_sched_every_2h
from .assets.factoperations import factoperations

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
    ],
    schedules=[fact_lecturas_sched_every_2h],
    assets=[factoperations],

)
