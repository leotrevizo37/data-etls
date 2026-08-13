from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs.observability_dagster_asset_execution_job import (
    observability_dagster_asset_execution_job,
)


observability_dagster_asset_execution_every_2h = ScheduleDefinition(
    name="123_observability_dagster_asset_execution_every_2h",
    job=observability_dagster_asset_execution_job,
    cron_schedule="15 1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
