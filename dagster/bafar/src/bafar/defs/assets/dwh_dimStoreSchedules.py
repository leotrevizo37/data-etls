import json
from datetime import datetime
from typing import Any, Dict

from dagster import MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op

from ..resources import SqlServerResource, StoreSchedulesApiResource


DIM_STORE_SCHEDULES_MAX_RETRIES = 3
DIM_STORE_SCHEDULES_RETRY_DELAY_SECONDS = 60
DIM_STORE_SCHEDULES_RETRY_POLICY = RetryPolicy(
    max_retries=DIM_STORE_SCHEDULES_MAX_RETRIES,
    delay=DIM_STORE_SCHEDULES_RETRY_DELAY_SECONDS,
)


@op(
    required_resource_keys={"store_schedules_api", "sqlserver"},
    ins={},
    out=Out(),
    retry_policy=DIM_STORE_SCHEDULES_RETRY_POLICY,
)
def ingest_dim_store_schedules_op(context):
    api: StoreSchedulesApiResource = context.resources.store_schedules_api
    sqlserver: SqlServerResource = context.resources.sqlserver
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)
    last_loaded_at = sqlserver.get_watermark("DimStoreSchedules")
    run_started = False

    try:
        sqlserver.mark_run_started("DimStoreSchedules")
        run_started = True
        sqlserver.clear_dim_store_schedules_stage(run_id)
        stores = []
        store_ids = set()
        seen_cursors = set()
        cursor = None
        page_number = 1

        while True:
            payload: Dict[str, Any] = api.fetch_page(cursor)
            sqlserver.write_dim_store_schedules_raw_page(
                run_id,
                retry_number,
                page_number,
                cursor,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            )
            response = api.validate_page(payload)
            for store in response["STORES"]:
                store_id = store["STORE_ID"].casefold()
                if store_id in store_ids:
                    raise ValueError(
                        "Store schedules API contains duplicate stores across pages."
                    )
                store_ids.add(store_id)
                stores.append(store)
            if not response["HAS_MORE"]:
                break
            next_cursor = response["NEXT_CURSOR"]
            if next_cursor.casefold() in seen_cursors:
                raise ValueError("Store schedules API pagination cursor was repeated.")
            seen_cursors.add(next_cursor.casefold())
            cursor = next_cursor
            page_number += 1

        rows = api.normalize_stores(stores)
        staged_rows = sqlserver.write_dim_store_schedules_stage(run_id, rows)
        affected_rows = sqlserver.merge_dim_store_schedules(run_id)
        sqlserver.clear_dim_store_schedules_stage(run_id)
        loaded_at = datetime.utcnow()
        sqlserver.mark_run_completed(
            ingestion_name="DimStoreSchedules",
            new_last_loaded_at=loaded_at,
            succeeded=True,
            error_message=None,
        )
    except Exception as exc:
        try:
            sqlserver.clear_dim_store_schedules_stage(run_id)
        except Exception as cleanup_exc:
            context.log.warning(
                "Could not clean dimStoreSchedules staging rows before retry: %s",
                cleanup_exc,
            )
        if retry_number < DIM_STORE_SCHEDULES_MAX_RETRIES:
            raise RetryRequested(
                max_retries=DIM_STORE_SCHEDULES_MAX_RETRIES,
                seconds_to_wait=DIM_STORE_SCHEDULES_RETRY_DELAY_SECONDS,
            ) from exc
        if run_started:
            sqlserver.mark_run_completed(
                ingestion_name="DimStoreSchedules",
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(exc)[:4000],
            )
        raise

    return MaterializeResult(
        metadata={
            "pages": page_number,
            "stores": len(stores),
            "staged_rows": staged_rows,
            "affected_rows": affected_rows,
            "loaded_at": loaded_at.isoformat(),
        }
    )


@graph_asset(
    name="dwh_dimstoreschedules",
    key_prefix=["bafar"],
    group_name="bafar",
)
def dwh_dimstoreschedules():
    return ingest_dim_store_schedules_op()
