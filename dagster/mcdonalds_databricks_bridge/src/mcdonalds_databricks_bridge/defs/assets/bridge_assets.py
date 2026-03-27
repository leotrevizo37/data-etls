from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import os
import uuid

from dagster import AssetKey, MaterializeResult, asset

from ..resources import (
    BridgeStateSqlServerResource,
    DatabricksSqlResource,
    SourceSqlServerResource,
    load_table_config,
)


def _sync_table(context, slot: str) -> MaterializeResult:
    table_config = load_table_config(slot)
    source_sql: SourceSqlServerResource = context.resources.source_sqlserver
    bridge_state: BridgeStateSqlServerResource = context.resources.bridge_state_sqlserver
    databricks_sql: DatabricksSqlResource = context.resources.databricks_sql

    batch_id = (
        table_config.target_table
        + "_"
        + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8]
    )
    overlap_minutes = int(os.getenv("BRIDGE_WATERMARK_OVERLAP_MINUTES", "15"))

    last_loaded_at = bridge_state.get_watermark(table_config.ingestion_name)
    if last_loaded_at is None:
        from_updated_at: Optional[datetime] = None
        context.log.info(
            "No watermark found for %s. A full initial load will be attempted.",
            table_config.ingestion_name,
        )
    else:
        from_updated_at = last_loaded_at - timedelta(minutes=overlap_minutes)
        context.log.info(
            "Using incremental extraction for %s from %s with overlap_minutes=%s.",
            table_config.ingestion_name,
            from_updated_at.isoformat(),
            overlap_minutes,
        )

    bridge_state.mark_run_started(table_config.ingestion_name)

    staging_root = Path(databricks_sql.staging_allowed_local_path)
    local_output_path = (
        staging_root
        / table_config.target_table
        / batch_id
        / (table_config.target_table + ".parquet")
    )

    try:
        columns = source_sql.get_table_schema(table_config)
        extract_result = source_sql.extract_incremental_to_parquet(
            table_config=table_config,
            columns=columns,
            from_updated_at=from_updated_at,
            output_path=local_output_path,
        )

        if extract_result.row_count == 0:
            bridge_state.mark_run_completed(
                ingestion_name=table_config.ingestion_name,
                new_last_loaded_at=last_loaded_at,
                succeeded=True,
                error_message=None,
            )
            return MaterializeResult(
                metadata={
                    "source_table": table_config.source_fqn,
                    "target_table": table_config.target_table,
                    "row_count": 0,
                    "watermark": last_loaded_at.isoformat() if last_loaded_at else "NULL",
                    "batch_id": batch_id,
                }
            )

        remote_batch_dir = databricks_sql.build_remote_batch_dir(table_config, batch_id)
        remote_file_path = databricks_sql.build_remote_file_path(
            table_config=table_config,
            batch_id=batch_id,
            local_path=extract_result.output_path or local_output_path.as_posix(),
        )

        with databricks_sql.connect() as connection:
            with connection.cursor() as cursor:
                databricks_sql.ensure_objects(
                    cursor=cursor,
                    table_config=table_config,
                    columns=extract_result.columns,
                )
                databricks_sql.truncate_stage(cursor=cursor, table_config=table_config)
                databricks_sql.upload_file(
                    cursor=cursor,
                    local_path=extract_result.output_path or local_output_path.as_posix(),
                    remote_path=remote_file_path,
                )
                databricks_sql.copy_into_stage(
                    cursor=cursor,
                    table_config=table_config,
                    remote_batch_dir=remote_batch_dir,
                )
                databricks_sql.merge_stage_into_target(
                    cursor=cursor,
                    table_config=table_config,
                    columns=extract_result.columns,
                )
                databricks_sql.truncate_stage(cursor=cursor, table_config=table_config)
                databricks_sql.remove_remote_file(cursor=cursor, remote_path=remote_file_path)

        if local_output_path.exists():
            local_output_path.unlink()

        bridge_state.mark_run_completed(
            ingestion_name=table_config.ingestion_name,
            new_last_loaded_at=extract_result.max_updated_at or last_loaded_at,
            succeeded=True,
            error_message=None,
        )

        return MaterializeResult(
            metadata={
                "source_table": table_config.source_fqn,
                "target_table": table_config.target_table,
                "row_count": extract_result.row_count,
                "max_updated_at": (
                    extract_result.max_updated_at.isoformat()
                    if extract_result.max_updated_at
                    else "NULL"
                ),
                "batch_id": batch_id,
                "local_output_path": extract_result.output_path or "NULL",
                "remote_batch_dir": remote_batch_dir,
            }
        )
    except Exception as ex:
        try:
            with databricks_sql.connect() as connection:
                with connection.cursor() as cursor:
                    databricks_sql.truncate_stage(cursor=cursor, table_config=table_config)
        except Exception:
            pass

        bridge_state.mark_run_completed(
            ingestion_name=table_config.ingestion_name,
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(ex),
        )
        raise


@asset(
    name="dbx_tablaA_bridge",
    non_argument_deps={AssetKey("src_tablaA")},
    required_resource_keys={"source_sqlserver", "bridge_state_sqlserver", "databricks_sql"},
)
def dbx_tablaA_bridge(context) -> MaterializeResult:
    return _sync_table(context, "A")


@asset(
    name="dbx_tablaB_bridge",
    non_argument_deps={AssetKey("src_tablaB")},
    required_resource_keys={"source_sqlserver", "bridge_state_sqlserver", "databricks_sql"},
)
def dbx_tablaB_bridge(context) -> MaterializeResult:
    return _sync_table(context, "B")


@asset(
    name="dbx_tablaC_bridge",
    non_argument_deps={AssetKey("src_tablaC")},
    required_resource_keys={"source_sqlserver", "bridge_state_sqlserver", "databricks_sql"},
)
def dbx_tablaC_bridge(context) -> MaterializeResult:
    return _sync_table(context, "C")
