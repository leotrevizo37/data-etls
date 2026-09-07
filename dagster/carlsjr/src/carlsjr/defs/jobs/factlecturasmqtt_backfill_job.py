from datetime import datetime, timedelta
from typing import Any, Dict

from dagster import RetryPolicy, daily_partitioned_config, job, op

from ...operations_window import OPERATIONS_TIMEZONE_NAME
from ..assets.observability_factLecturasMqtt import (
    FACTLECTURAS_MQTT_REQUEST_WINDOW,
    FACTLECTURAS_MQTT_START_DATE,
)
from ..resources import LecturasTenantApiResource, SqlServerResource


FACTLECTURAS_MQTT_BACKFILL_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=60,
)


@daily_partitioned_config(
    start_date=FACTLECTURAS_MQTT_START_DATE,
    timezone=OPERATIONS_TIMEZONE_NAME,
)
def factlecturasmqtt_backfill_daily_config(start, _end):
    return {
        "ops": {
            "backfill_factlecturasmqtt_daily_op": {
                "config": {
                    "from_local": start.replace(tzinfo=None).isoformat(),
                    "to_local": (
                        start.replace(tzinfo=None) + timedelta(days=1)
                    ).isoformat(),
                }
            }
        }
    }


@op(
    required_resource_keys={"lecturas_tenant_api", "sqlserver"},
    config_schema={"from_local": str, "to_local": str},
    retry_policy=FACTLECTURAS_MQTT_BACKFILL_RETRY_POLICY,
)
def backfill_factlecturasmqtt_daily_op(context) -> Dict[str, Any]:
    api: LecturasTenantApiResource = context.resources.lecturas_tenant_api
    sqlserver: SqlServerResource = context.resources.sqlserver
    from_local = datetime.fromisoformat(context.op_config["from_local"])
    to_local = datetime.fromisoformat(context.op_config["to_local"])
    if to_local - from_local != timedelta(days=1):
        raise ValueError("FactLecturas MQTT backfill requires one local day.")

    run_id = context.run_id
    raw_messages = 0
    measurement_values = 0
    staged_rows = 0
    merged_rows = 0
    windows_processed = 0
    try:
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
    finally:
        sqlserver.clear_fact_lecturas_mqtt_stage(run_id)

    return {
        "from_local": from_local,
        "to_local": to_local,
        "raw_messages": raw_messages,
        "measurement_values": measurement_values,
        "staged_rows": staged_rows,
        "merged_rows": merged_rows,
        "windows_processed": windows_processed,
    }


@job(
    name="023_observability_factlecturasmqtt_backfill_daily",
    config=factlecturasmqtt_backfill_daily_config,
    tags={
        "job_type": "observability",
        "run_lane": "observability",
        "user_code_run_lane": "carlsjr_observability",
        "api_rate_limit": "carlsjr_lecturas_tenant",
        "user_code": "carlsjr",
        "backfill": "factlecturasmqtt_daily",
    },
)
def factlecturasmqtt_backfill_daily_job():
    backfill_factlecturasmqtt_daily_op()
