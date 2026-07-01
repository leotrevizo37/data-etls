from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..jobs import fact_smartaudits_categories_job, smartaudits_classifier_training_job


smartaudits_categories_sched_every_6h = ScheduleDefinition(
    name="110_smartaudits_categories_every_6h",
    job=fact_smartaudits_categories_job,
    cron_schedule="25 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

smartaudits_classifier_training_daily = ScheduleDefinition(
    name="111_smartaudits_classifier_training_daily",
    job=smartaudits_classifier_training_job,
    cron_schedule="10 2 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

