from datetime import datetime, timedelta
from time import perf_counter

from dagster import AssetExecutionContext, MaterializeResult, RetryPolicy, asset

from ...operations_window import OPERATIONS_TIMEZONE
from ..resources import LecturasTenantApiResource, SqlServerResource


FACTLECTURAS_MQTT_INGESTION_NAME = "FactLecturasMqtt"
FACTLECTURAS_MQTT_START_DATE = datetime(2026, 1, 1)
FACTLECTURAS_MQTT_OVERLAP = timedelta(days=1)
FACTLECTURAS_MQTT_REQUEST_WINDOW = timedelta(hours=1)
FACTLECTURAS_MQTT_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@asset(
    name="observability_factlecturasmqtt",
    group_name="emerson",
    key_prefix=["emerson"],
    required_resource_keys={"lecturas_tenant_api", "sqlserver"},
    retry_policy=FACTLECTURAS_MQTT_RETRY_POLICY,
)
def observability_factlecturasmqtt(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    api: LecturasTenantApiResource = context.resources.lecturas_tenant_api
    sqlserver: SqlServerResource = context.resources.sqlserver
    run_id = context.run_id
    last_loaded_at = sqlserver.get_watermark(FACTLECTURAS_MQTT_INGESTION_NAME)
    if last_loaded_at is not None and last_loaded_at.tzinfo is not None:
        last_loaded_at = (
            last_loaded_at
            .astimezone(OPERATIONS_TIMEZONE)
            .replace(tzinfo=None)
        )
    from_local = (
        FACTLECTURAS_MQTT_START_DATE
        if last_loaded_at is None
        else max(
            FACTLECTURAS_MQTT_START_DATE,
            last_loaded_at - FACTLECTURAS_MQTT_OVERLAP,
        )
    )
    to_local = datetime.now(OPERATIONS_TIMEZONE).replace(
        tzinfo=None,
        microsecond=0,
    )
    request_count_before = api.requests_made
    raw_messages = 0
    measurement_values = 0
    staged_rows = 0
    merged_rows = 0
    windows_processed = 0
    run_started = False

    try:
        sqlserver.mark_run_started(FACTLECTURAS_MQTT_INGESTION_NAME)
        run_started = True
        window_start = from_local
        while window_start < to_local:
            window_end = min(
                window_start + FACTLECTURAS_MQTT_REQUEST_WINDOW,
                to_local,
            )
            sqlserver.clear_fact_lecturas_mqtt_stage(run_id)
            rows = api.fetch_range(window_start, window_end)
            if rows:
                staged_rows += sqlserver.write_fact_lecturas_mqtt_stage(
                    run_id,
                    rows,
                )
                merged_rows += sqlserver.merge_fact_lecturas_mqtt(run_id)
                sqlserver.clear_fact_lecturas_mqtt_stage(run_id)
            raw_messages += len(rows)
            measurement_values += sum(
                len(row["mediciones"] or {}) for row in rows
            )
            windows_processed += 1
            window_start = window_end
        sqlserver.mark_run_completed(
            ingestion_name=FACTLECTURAS_MQTT_INGESTION_NAME,
            new_last_loaded_at=max(last_loaded_at or from_local, to_local),
            succeeded=True,
            error_message=None,
        )
    except Exception as exc:
        try:
            sqlserver.clear_fact_lecturas_mqtt_stage(run_id)
        except Exception:
            pass
        if run_started:
            sqlserver.mark_run_completed(
                ingestion_name=FACTLECTURAS_MQTT_INGESTION_NAME,
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(exc)[:4000],
            )
        raise

    return MaterializeResult(
        metadata={
            "row_count": raw_messages,
            "measurement_values": measurement_values,
            "staged_rows": staged_rows,
            "merged_rows": merged_rows,
            "api_requests": api.requests_made - request_count_before,
            "windows_processed": windows_processed,
            "from_local": str(from_local),
            "to_local": str(to_local),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        }
    )
