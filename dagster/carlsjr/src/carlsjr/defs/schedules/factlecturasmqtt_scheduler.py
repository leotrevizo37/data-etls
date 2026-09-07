from dagster import DefaultScheduleStatus, ScheduleDefinition


factlecturasmqtt_daily_schedule = ScheduleDefinition(
    name="023_observability_factlecturasmqtt_every_1d",
    job_name="023_observability_factlecturasmqtt",
    cron_schedule="30 23 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
