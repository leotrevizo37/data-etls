from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import (
    device_operational_intelligence_job,
    device_operational_review_promotion_job,
    device_operational_training_job,
)


device_operational_intelligence_sched_every_6h = ScheduleDefinition(
    name="022_sense_device_operational_intelligence_every_6h",
    job=device_operational_intelligence_job,
    cron_schedule="45 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

device_operational_training_weekly = ScheduleDefinition(
    name="120_sense_device_operational_training_weekly",
    job=device_operational_training_job,
    cron_schedule="15 2 * * 0",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

device_operational_review_promotion_daily = ScheduleDefinition(
    name="122_sense_device_insight_review_promotion_daily",
    job=device_operational_review_promotion_job,
    cron_schedule="10 3 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)
