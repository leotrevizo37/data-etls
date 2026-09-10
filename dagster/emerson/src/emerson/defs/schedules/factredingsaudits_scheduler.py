from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import fact_redings_audits_job


redings_audits_sched_every_10m = ScheduleDefinition(
    name="022_sense_redings_audits_every_10m",
    job=fact_redings_audits_job,
    cron_schedule="*/10 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
