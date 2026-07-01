from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.index_maintenance_job import index_maintenance_job


index_maintenance_schedule = ScheduleDefinition(
    name="081_maintenance_index_weekly",
    job=index_maintenance_job,
    cron_schedule="0 4 * * 0",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
