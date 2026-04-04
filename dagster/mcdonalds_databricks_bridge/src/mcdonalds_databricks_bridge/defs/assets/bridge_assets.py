from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional
import os
import uuid

from dagster import AssetKey, MaterializeResult, asset

from ..resources import (
    BridgeStateSqlServerResource,
    BridgeTableConfig,
    DatabricksSqlResource,
    SourceMySqlResource,
)


def _sync_table(context, table_config: BridgeTableConfig) -> MaterializeResult:
    source_sql: SourceMySqlResource = context.resources.source_mysql
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
    from_updated_at: Optional[datetime] = None

    bridge_state.mark_run_started(table_config.ingestion_name)

    staging_root = Path(databricks_sql.staging_allowed_local_path)
    local_output_path = (
        staging_root
        / table_config.target_table
        / batch_id
        / (table_config.target_table + ".parquet")
    )
    remote_batch_dir: Optional[str] = None
    remote_file_path: Optional[str] = None

    try:
        columns = source_sql.get_table_schema(table_config)
        cursor_column = source_sql.resolve_incremental_cursor_column(
            table_config=table_config,
            columns=columns,
        )

        if cursor_column is None:
            configured_candidates = table_config.incremental_cursor_candidates
            if configured_candidates:
                context.log.warning(
                    "Bridge table %s does not expose any configured incremental cursor columns %s. "
                    "A full refresh will be executed against the target table.",
                    table_config.source_fqn,
                    configured_candidates,
                )
            else:
                context.log.info(
                    "Bridge table %s does not define updated/created cursor columns. "
                    "A full refresh will be executed against the target table.",
                    table_config.source_fqn,
                )
        elif last_loaded_at is None:
            context.log.info(
                "No watermark found for %s. An initial load will be extracted using cursor column %s.",
                table_config.ingestion_name,
                cursor_column,
            )
        else:
            from_updated_at = last_loaded_at - timedelta(minutes=overlap_minutes)
            context.log.info(
                "Using incremental extraction for %s from %s with cursor_column=%s and overlap_minutes=%s.",
                table_config.ingestion_name,
                from_updated_at.isoformat(),
                cursor_column,
                overlap_minutes,
            )

        extract_result = source_sql.extract_incremental_to_parquet(
            table_config=table_config,
            columns=columns,
            from_updated_at=from_updated_at,
            output_path=local_output_path,
            cursor_column=cursor_column,
        )

        if extract_result.row_count == 0:
            if extract_result.load_mode == "full_refresh":
                with databricks_sql.connect() as connection:
                    with connection.cursor() as cursor:
                        databricks_sql.ensure_objects(
                            cursor=cursor,
                            table_config=table_config,
                            columns=extract_result.columns,
                        )
                        databricks_sql.truncate_target(
                            cursor=cursor,
                            table_config=table_config,
                        )

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
                    "load_mode": extract_result.load_mode,
                    "cursor_column": extract_result.cursor_column or "NULL",
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
                databricks_sql.upload_file(
                    cursor=cursor,
                    local_path=extract_result.output_path or local_output_path.as_posix(),
                    remote_path=remote_file_path,
                )
                if extract_result.load_mode == "full_refresh":
                    databricks_sql.replace_target_from_remote(
                        cursor=cursor,
                        table_config=table_config,
                        columns=extract_result.columns,
                        remote_batch_dir=remote_batch_dir,
                    )
                else:
                    databricks_sql.copy_into_target(
                        cursor=cursor,
                        table_config=table_config,
                        remote_batch_dir=remote_batch_dir,
                    )
                databricks_sql.remove_remote_file(cursor=cursor, remote_path=remote_file_path)
                remote_file_path = None

        bridge_state.mark_run_completed(
            ingestion_name=table_config.ingestion_name,
            new_last_loaded_at=extract_result.max_cursor_at or last_loaded_at,
            succeeded=True,
            error_message=None,
        )

        return MaterializeResult(
            metadata={
                "source_table": table_config.source_fqn,
                "target_table": table_config.target_table,
                "row_count": extract_result.row_count,
                "load_mode": extract_result.load_mode,
                "write_strategy": (
                    "replace_table_contents"
                    if extract_result.load_mode == "full_refresh"
                    else "append_only_copy_into"
                ),
                "cursor_column": extract_result.cursor_column or "NULL",
                "max_cursor_at": (
                    extract_result.max_cursor_at.isoformat()
                    if extract_result.max_cursor_at
                    else "NULL"
                ),
                "batch_id": batch_id,
                "local_output_path": extract_result.output_path or "NULL",
                "remote_batch_dir": remote_batch_dir,
            }
        )
    except Exception as ex:
        bridge_state.mark_run_completed(
            ingestion_name=table_config.ingestion_name,
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(ex),
        )
        raise
    finally:
        if remote_file_path:
            try:
                with databricks_sql.connect() as connection:
                    with connection.cursor() as cursor:
                        databricks_sql.remove_remote_file(cursor=cursor, remote_path=remote_file_path)
            except Exception:
                pass

        if local_output_path.exists():
            try:
                local_output_path.unlink()
            except Exception:
                pass


def build_bridge_asset(table_config: BridgeTableConfig):
    @asset(
        name=table_config.bridge_asset_name,
        non_argument_deps={AssetKey(table_config.source_asset_name)},
        required_resource_keys={"source_mysql", "bridge_state_sqlserver", "databricks_sql"},
        group_name=table_config.asset_group_name,
        description=(
            "Appends rows from the source table `"
            + table_config.source_fqn
            + "` into Databricks bronze target `"
            + table_config.target_table
            + "`. The bridge preserves row history in bronze and leaves "
            + "deduplication or latest-state logic to downstream Databricks "
            + "transformations."
        ),
        tags={
            "job_type": "databricks_bridge",
            "bridge_group": table_config.job_group,
            "bridge_target_table": table_config.target_table,
        },
    )
    def _bridge_asset(context) -> MaterializeResult:
        return _sync_table(context, table_config)

    return _bridge_asset


def build_bridge_assets(table_configs: Iterable[BridgeTableConfig]) -> List:
    return [build_bridge_asset(table_config) for table_config in table_configs]
