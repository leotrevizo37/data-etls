from dagster import DefaultScheduleStatus, ScheduleDefinition


manager_access_daily_schedule = ScheduleDefinition(
    name="024_bafar_manager_access_daily_every_1d",
    job_name="024_bafar_manager_access_daily",
    cron_schedule="40 21 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
