from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import fact_mantiz_tickets_job


mantiz_tickets_sched_every_6h = ScheduleDefinition(
    name="mantiz_tickets_sched_every_6h",
    job=fact_mantiz_tickets_job,
    cron_schedule="40 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
