from dagster import DefaultScheduleStatus, ScheduleDefinition


store_schedules_schedule = ScheduleDefinition(
    name="010_bafar_store_schedules_every_1d",
    job_name="010_bafar_store_schedules",
    cron_schedule="30 2 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
