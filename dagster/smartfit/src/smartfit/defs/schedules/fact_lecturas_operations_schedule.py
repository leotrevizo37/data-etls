from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import fact_lecturas_operations_job


fact_lecturas_operations_sched_every_1h_w = ScheduleDefinition(
    name="fact_lecturas_operations_every_1h_w",
    job=fact_lecturas_operations_job,
    cron_schedule="30 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
