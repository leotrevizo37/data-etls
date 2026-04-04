import os

from dagster import DefaultScheduleStatus, ScheduleDefinition

def build_bridge_schedules(bridge_jobs):
    schedules = []
    cron_schedule = os.getenv("BRIDGE_SYNC_CRON", "0 */8 * * *")
    execution_timezone = os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez")

    for bridge_job in bridge_jobs:
        schedules.append(
            ScheduleDefinition(
                name=bridge_job.name.replace("_job", "_schedule"),
                job=bridge_job,
                cron_schedule=cron_schedule,
                execution_timezone=execution_timezone,
                default_status=DefaultScheduleStatus.STOPPED,
            )
        )

    return schedules
