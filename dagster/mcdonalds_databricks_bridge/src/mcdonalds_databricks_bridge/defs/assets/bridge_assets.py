from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional
import os
import uuid

from dagster import AssetKey, MaterializeResult, MetadataValue, asset

from ..resources import (
    BridgeStateSqlServerResource,
    BridgeTableConfig,
    DatabricksSqlResource,
    SourceMySqlResource,
)


def _elapsed_seconds(started_at: datetime) -> float:
    return round((datetime.now(timezone.utc) - started_at).total_seconds(), 3)


def _build_metadata(
    table_config: BridgeTableConfig,
    batch_id: str,
    row_count: int,
    overlap_minutes: int,
    started_at: datetime,
    column_count: int,
    last_loaded_at: Optional[datetime],
    load_mode: str,
    local_output_path: Optional[str] = None,
    remote_batch_dir: Optional[str] = None,
    max_updated_at: Optional[datetime] = None,
    remote_files_uploaded: int = 0,
) -> dict:
    metadata = {
        "source_table": table_config.source_fqn,
        "target_table": table_config.target_table,
        "job_group": table_config.job_group,
        "row_count": row_count,
        "rows_loaded": row_count,
        "column_count": column_count,
        "primary_key_count": len(table_config.primary_keys),
        "remote_files_uploaded": remote_files_uploaded,
        "watermark_overlap_minutes": overlap_minutes,
        "batch_duration_seconds": _elapsed_seconds(started_at),
        "batch_id": batch_id,
        "load_mode": load_mode,
        "last_loaded_at": (
            MetadataValue.timestamp(last_loaded_at) if last_loaded_at else "NULL"
        ),
        "cursor_column": table_config.updated_at_column,
    }

    if max_updated_at:
        metadata["max_updated_at"] = MetadataValue.timestamp(max_updated_at)

    if local_output_path:
        metadata["local_output_path"] = local_output_path

    if remote_batch_dir:
        metadata["remote_batch_dir"] = remote_batch_dir

    metadata["load_summary"] = MetadataValue.md(
        "Append-only bridge load from `"
        + table_config.source_fqn
        + "` into Databricks bronze `"
        + table_config.target_table
        + "`."
    )

    return metadata


def _sync_table(context, table_config: BridgeTableConfig) -> MaterializeResult:
    source_sql: SourceMySqlResource = context.resources.source_mysql
    bridge_state: BridgeStateSqlServerResource = context.resources.bridge_state_sqlserver
    databricks_sql: DatabricksSqlResource = context.resources.databricks_sql
    started_at = datetime.now(timezone.utc)

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
                metadata=_build_metadata(
                    table_config=table_config,
                    batch_id=batch_id,
                    row_count=0,
                    overlap_minutes=overlap_minutes,
                    started_at=started_at,
                    column_count=len(columns),
                    last_loaded_at=last_loaded_at,
                    load_mode="append_only_copy_into",
                )
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
            metadata=_build_metadata(
                table_config=table_config,
                batch_id=batch_id,
                row_count=extract_result.row_count,
                overlap_minutes=overlap_minutes,
                started_at=started_at,
                column_count=len(extract_result.columns),
                last_loaded_at=last_loaded_at,
                load_mode="append_only_copy_into",
                local_output_path=extract_result.output_path,
                remote_batch_dir=remote_batch_dir,
                max_updated_at=extract_result.max_updated_at,
                remote_files_uploaded=1,
            )
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
            "deduplication or latest-state logic to downstream Databricks transformations."
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
