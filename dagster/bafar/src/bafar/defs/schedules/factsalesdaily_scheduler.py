from dagster import DefaultScheduleStatus, ScheduleDefinition


fact_store_sales_daily_sched_every_1d = ScheduleDefinition(
    name="021_bafar_store_sales_daily_every_1d",
    job_name="021_bafar_store_sales_daily",
    cron_schedule="10 3 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
