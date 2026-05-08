from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import fact_lecturas_ingestion_job

fact_lecturas_sched_every_3h = ScheduleDefinition(
    name="ingest_fact_lecturas_every_3h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */3 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_2h = ScheduleDefinition(
    name="ingest_fact_lecturas_every_2h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */2 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_6h = ScheduleDefinition(
    name="ingest_fact_lecturas_every_6h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */6 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_1h = ScheduleDefinition(
    name="ingest_fact_lecturas_every_1h",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 */1 * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_30m = ScheduleDefinition(
    name="ingest_fact_lecturas_every_30m",
    job=fact_lecturas_ingestion_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)

fact_lecturas_sched_every_w = ScheduleDefinition(
    name="ingest_fact_lecturas_every_w",
    job=fact_lecturas_ingestion_job,
    cron_schedule="0 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.RUNNING,
)

fact_lecturas_sched_every_30m_w = ScheduleDefinition(
    name="ingest_fact_lecturas_every_30m_w",
    job=fact_lecturas_ingestion_job,
    cron_schedule="*/30 5-16 * * 1-5",
    execution_timezone="America/Ciudad_Juarez",
    default_status=DefaultScheduleStatus.STOPPED,
)