import os

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import bridge_tables_job

bridge_tables_schedule = ScheduleDefinition(
    name="bridge_tables_schedule",
    job=bridge_tables_job,
    cron_schedule=os.getenv("BRIDGE_SYNC_CRON", "0 */8 * * *"),
    execution_timezone=os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez"),
    default_status=DefaultScheduleStatus.STOPPED,
)
