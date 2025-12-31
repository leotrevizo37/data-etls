from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_operaciones_ingestion_job

fact_operaciones_sched_every_4h = ScheduleDefinition(
    name="fact_operaciones_sched_every_4h",
    job=fact_operaciones_ingestion_job,
    cron_schedule="30 */4 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,

)

fact_operaciones_sched_every_6h = ScheduleDefinition(
    name="fact_operaciones_sched_every_6h",
    job=fact_operaciones_ingestion_job,
    cron_schedule="30 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,

)

fact_operaciones_sched_every_2h = ScheduleDefinition(
    name="fact_operaciones_sched_every_2h",
    job=fact_operaciones_ingestion_job,
    cron_schedule="20 */2 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,

)
