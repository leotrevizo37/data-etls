from datetime import datetime
from typing import Any, Dict
from zoneinfo import ZoneInfo

from dagster import MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op

from ..resources import ManagerAccessApiResource, SqlServerResource


FACT_MANAGER_ACCESS_DAILY_TIMEZONE = ZoneInfo("America/Ciudad_Juarez")
FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR = 23
FACT_MANAGER_ACCESS_DAILY_MAX_RETRIES = 3
FACT_MANAGER_ACCESS_DAILY_RETRY_DELAY_SECONDS = 60
FACT_MANAGER_ACCESS_DAILY_RETRY_POLICY = RetryPolicy(
    max_retries=FACT_MANAGER_ACCESS_DAILY_MAX_RETRIES,
    delay=FACT_MANAGER_ACCESS_DAILY_RETRY_DELAY_SECONDS,
)


@op(
    required_resource_keys={"manager_access_api", "sqlserver"},
    ins={},
    out=Out(),
    retry_policy=FACT_MANAGER_ACCESS_DAILY_RETRY_POLICY,
)
def ingest_fact_manager_access_daily_op(context):
    api: ManagerAccessApiResource = context.resources.manager_access_api
    sqlserver: SqlServerResource = context.resources.sqlserver
    reference_date = datetime.now(FACT_MANAGER_ACCESS_DAILY_TIMEZONE).date()
    last_loaded_at = sqlserver.get_watermark("FactManagerAccessDaily")
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)
    run_started = False

    try:
        sqlserver.mark_run_started("FactManagerAccessDaily")
        run_started = True
        payload: Dict[str, Any] = api.fetch(
            reference_date,
            FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
        )
        sqlserver.clear_fact_manager_access_daily_stage(run_id)
        evaluated_rows = sqlserver.write_fact_manager_access_daily_stage(
            run_id,
            reference_date,
            payload,
        )
        affected_rows = sqlserver.merge_fact_manager_access_daily(run_id)
        sqlserver.clear_fact_manager_access_daily_stage(run_id)
        cutoff_at = datetime.fromisoformat(payload["fechaCorte"])
        new_last_loaded_at = (
            max(last_loaded_at, cutoff_at)
            if last_loaded_at is not None
            else cutoff_at
        )
        sqlserver.mark_run_completed(
            ingestion_name="FactManagerAccessDaily",
            new_last_loaded_at=new_last_loaded_at,
            succeeded=True,
            error_message=None,
        )
    except Exception as exc:
        try:
            sqlserver.clear_fact_manager_access_daily_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean factManagerAccessDaily staging rows before retry: %s",
                cleanup_exc,
            )
        if retry_number < FACT_MANAGER_ACCESS_DAILY_MAX_RETRIES:
            raise RetryRequested(
                max_retries=FACT_MANAGER_ACCESS_DAILY_MAX_RETRIES,
                seconds_to_wait=FACT_MANAGER_ACCESS_DAILY_RETRY_DELAY_SECONDS,
            ) from exc
        if run_started:
            sqlserver.mark_run_completed(
                ingestion_name="FactManagerAccessDaily",
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(exc)[:4000],
            )
        raise

    return MaterializeResult(
        metadata={
            "fecha": reference_date.isoformat(),
            "fecha_corte": payload["fechaCorte"],
            "evaluated_rows": evaluated_rows,
            "affected_rows": affected_rows,
            "total_managers": payload["totalGerentes"],
            "entered_before_cutoff": payload["totalIngresaronAntesDeCorte"],
            "without_entry_at_cutoff": payload["totalSinIngresoEnCorte"],
            "compliance_percentage": payload["porcentajeCumplimiento"],
            "rows_without_store_id": sum(
                row["idTienda"] is None for row in payload["data"]
            ),
        }
    )


@graph_asset(
    name="dwh_factmanageraccessdaily",
    key_prefix=["bafar"],
    group_name="bafar",
)
def dwh_factmanageraccessdaily():
    return ingest_fact_manager_access_daily_op()
