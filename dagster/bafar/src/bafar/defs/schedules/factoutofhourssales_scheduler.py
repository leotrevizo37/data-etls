from dagster import DefaultScheduleStatus, ScheduleDefinition

fact_out_of_hours_sales_sched_every_1h = ScheduleDefinition(
    name="020_bafar_out_of_hours_sales_every_1h",
    job_name="020_bafar_out_of_hours_sales",
    cron_schedule="0 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_out_of_hours_sales_sched_every_6h = ScheduleDefinition(
    name="020_bafar_out_of_hours_sales_every_6h",
    job_name="020_bafar_out_of_hours_sales",
    cron_schedule="0 4-17/6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
