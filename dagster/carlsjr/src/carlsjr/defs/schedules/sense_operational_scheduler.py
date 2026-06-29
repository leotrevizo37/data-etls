from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import sense_operational_job


sense_operational_sched_every_6h = ScheduleDefinition(
    name="020_sense_operations_sensor_events_every_6h",
    job=sense_operational_job,
    cron_schedule="40 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

sense_operational_sched_every_w = ScheduleDefinition(
    name="020_sense_operations_sensor_events_every_w",
    job=sense_operational_job,
    cron_schedule="40 5-16/3 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

sense_operational_sched_every_1h_w = ScheduleDefinition(
    name="020_sense_operations_sensor_events_every_1h_w",
    job=sense_operational_job,
    cron_schedule="40 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)
