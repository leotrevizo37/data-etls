from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import chirpstack_metrics_job

chirpstack_metrics_sched_every_30m = ScheduleDefinition(
    name="023_sense_chirpstack_metrics_every_30m",
    job=chirpstack_metrics_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
