from datetime import datetime
from typing import Any, Dict

from dagster import RetryPolicy, daily_partitioned_config, job, op

from ..assets.dwh_factManagerAccessDaily import (
    FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
    FACT_MANAGER_ACCESS_DAILY_HISTORY_DAYS,
)
from ..resources import ManagerAccessApiResource, SqlServerResource


FACT_MANAGER_ACCESS_BACKFILL_START_DATE = datetime(2026, 1, 1)
FACT_MANAGER_ACCESS_BACKFILL_TIMEZONE = "America/Ciudad_Juarez"
FACT_MANAGER_ACCESS_BACKFILL_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)


@daily_partitioned_config(
    start_date=FACT_MANAGER_ACCESS_BACKFILL_START_DATE,
    timezone=FACT_MANAGER_ACCESS_BACKFILL_TIMEZONE,
)
def fact_manager_access_backfill_daily_config(
    start: datetime,
    _end: datetime,
):
    return {
        "ops": {
            "backfill_fact_manager_access_daily_op": {
                "config": {"fecha": start.strftime("%Y-%m-%d")}
            }
        }
    }


@op(
    required_resource_keys={"manager_access_api", "sqlserver"},
    config_schema={"fecha": str},
    retry_policy=FACT_MANAGER_ACCESS_BACKFILL_RETRY_POLICY,
)
def backfill_fact_manager_access_daily_op(context) -> Dict[str, Any]:
    api: ManagerAccessApiResource = context.resources.manager_access_api
    sqlserver: SqlServerResource = context.resources.sqlserver
    reference_date = datetime.strptime(context.op_config["fecha"], "%Y-%m-%d").date()
    run_id = context.run_id

    try:
        payload = api.fetch(
            reference_date,
            FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
            FACT_MANAGER_ACCESS_DAILY_HISTORY_DAYS,
        )
        sqlserver.clear_fact_manager_access_daily_stage(run_id)
        evaluated_rows = sqlserver.write_fact_manager_access_daily_stage(
            run_id,
            reference_date,
            payload,
        )
        affected_rows = sqlserver.merge_fact_manager_access_daily(run_id)
        sqlserver.clear_fact_manager_access_daily_stage(run_id)
    except Exception:
        try:
            sqlserver.clear_fact_manager_access_daily_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean factManagerAccessDaily staging rows before retry: %s",
                cleanup_exc,
            )
        raise

    return {
        "fecha": reference_date.isoformat(),
        "fecha_corte": payload["fechaCorte"],
        "history_days": payload["diasHistorial"],
        "evaluated_rows": evaluated_rows,
        "affected_rows": affected_rows,
        "total_records": payload["totalRegistros"],
    }


@job(
    name="025_bafar_manager_access_backfill_daily",
    config=fact_manager_access_backfill_daily_config,
    tags={
        "job_type": "api",
        "run_lane": "operational",
        "user_code_run_lane": "bafar_operational",
        "user_code": "bafar",
        "backfill": "fact_manager_access_daily",
    },
)
def fact_manager_access_backfill_daily_job():
    backfill_fact_manager_access_daily_op()
