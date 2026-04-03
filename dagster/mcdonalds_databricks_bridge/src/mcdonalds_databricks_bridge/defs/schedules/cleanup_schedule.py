import os

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import bridge_cleanup_job

bridge_cleanup_schedule = ScheduleDefinition(
    name="bridge_cleanup_schedule",
    job=bridge_cleanup_job,
    cron_schedule=os.getenv("BRIDGE_CLEANUP_CRON", "0 1 * * 1-5"),
    run_config={
        "ops": {
            "bridge_cleanup_old_runs": {
                "config": {"keep_days": 10, "batch_size": 200}
            }
        }
    },
    execution_timezone=os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez"),
    description=(
        "Runs bridge maintenance to remove old Dagster run history on a regular cadence."
    ),
    default_status=DefaultScheduleStatus.RUNNING,
)
