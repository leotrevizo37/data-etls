from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from mcdonalds_databricks_bridge.defs.assets.bridge_assets import _sync_table
from mcdonalds_databricks_bridge.defs.jobs.bridge_tables_job import build_bridge_jobs
from mcdonalds_databricks_bridge.defs.resources.bridge_config import (
    BridgeTableConfig,
    _build_config_from_manifest_entry,
)
from mcdonalds_databricks_bridge.defs.resources.databricks_sql import DatabricksSqlResource
from mcdonalds_databricks_bridge.defs.resources.source_mysql import (
    ExtractResult,
    SourceColumn,
    resolve_incremental_cursor_column,
)
from mcdonalds_databricks_bridge.defs.schedules.bridge_tables_schedule import build_bridge_schedules


class _RecordingCursor:
    def __init__(self) -> None:
        self.commands = []

    def execute(self, sql_text: str) -> None:
        self.commands.append(sql_text)


class _FakeLog:
    def __init__(self) -> None:
        self.messages = []

    def info(self, message, *args) -> None:
        self.messages.append(("info", message % args if args else message))

    def warning(self, message, *args) -> None:
        self.messages.append(("warning", message % args if args else message))


class _FakeBridgeState:
    def __init__(self, watermark: datetime | None) -> None:
        self.watermark = watermark
        self.started = []
        self.completed = []

    def get_watermark(self, ingestion_name: str):
        return self.watermark

    def mark_run_started(self, ingestion_name: str) -> None:
        self.started.append(ingestion_name)

    def mark_run_completed(
        self,
        ingestion_name: str,
        new_last_loaded_at,
        succeeded: bool,
        error_message,
    ) -> None:
        self.completed.append(
            {
                "ingestion_name": ingestion_name,
                "new_last_loaded_at": new_last_loaded_at,
                "succeeded": succeeded,
                "error_message": error_message,
            }
        )


class _FakeSourceMySql:
    def __init__(self, extract_result: ExtractResult, columns: list[SourceColumn]) -> None:
        self.extract_result = extract_result
        self.columns = columns
        self.extract_calls = []

    def get_table_schema(self, table_config: BridgeTableConfig):
        return self.columns

    def resolve_incremental_cursor_column(self, table_config: BridgeTableConfig, columns):
        return None

    def extract_incremental_to_parquet(
        self,
        table_config: BridgeTableConfig,
        columns,
        from_updated_at,
        output_path,
        cursor_column,
    ) -> ExtractResult:
        self.extract_calls.append(
            {
                "from_updated_at": from_updated_at,
                "cursor_column": cursor_column,
                "output_path": output_path.as_posix(),
            }
        )
        return self.extract_result


class _FakeDatabricksSql:
    def __init__(self, staging_path: str) -> None:
        self.staging_allowed_local_path = staging_path
        self.ensure_calls = []
        self.truncate_calls = []

    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def cursor(self):
        yield self

    def ensure_objects(self, cursor, table_config, columns) -> None:
        self.ensure_calls.append(table_config.target_table)

    def truncate_target(self, cursor, table_config) -> None:
        self.truncate_calls.append(table_config.target_table)


class BridgeBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table_config = BridgeTableConfig(
            key="categories",
            ingestion_name="DatabricksBridge_categories",
            source_schema="",
            source_table="categories",
            target_table="sa_categories",
            primary_keys=["Id"],
            updated_at_column="",
            created_at_column="",
            job_group="dims",
        )

    def test_manifest_entry_preserves_blank_cursor_columns(self) -> None:
        config = _build_config_from_manifest_entry(
            {
                "key": "categories",
                "source_schema": "",
                "source_table": "categories",
                "target_table": "sa_categories",
                "primary_keys": ["Id"],
                "updated_at_column": "",
                "created_at_column": "",
                "job_group": "dims",
            },
            index=1,
        )

        self.assertEqual(config.updated_at_column, "")
        self.assertEqual(config.created_at_column, "")
        self.assertEqual(config.incremental_cursor_candidates, [])

    def test_resolve_incremental_cursor_column_uses_created_when_updated_is_missing(self) -> None:
        columns = [
            SourceColumn(name="Id", sql_type="int", nullable=False, ordinal_position=1),
            SourceColumn(
                name="CreatedAt",
                sql_type="datetime",
                nullable=False,
                ordinal_position=2,
            ),
        ]
        table_config = BridgeTableConfig(
            key="regions",
            ingestion_name="DatabricksBridge_regions",
            source_schema="",
            source_table="regions",
            target_table="sa_regions",
            primary_keys=["Id"],
            updated_at_column="updated_at",
            created_at_column="CreatedAt",
            job_group="dims",
        )

        self.assertEqual(
            resolve_incremental_cursor_column(table_config, columns),
            "CreatedAt",
        )

    def test_full_refresh_with_empty_source_truncates_target(self) -> None:
        columns = [
            SourceColumn(name="Id", sql_type="int", nullable=False, ordinal_position=1),
            SourceColumn(name="Name", sql_type="varchar", nullable=True, ordinal_position=2),
        ]
        extract_result = ExtractResult(
            row_count=0,
            max_cursor_at=None,
            columns=columns,
            output_path=None,
            cursor_column=None,
            load_mode="full_refresh",
        )

        with tempfile.TemporaryDirectory() as staging_dir:
            context = SimpleNamespace(
                resources=SimpleNamespace(
                    source_mysql=_FakeSourceMySql(extract_result=extract_result, columns=columns),
                    bridge_state_sqlserver=_FakeBridgeState(watermark=None),
                    databricks_sql=_FakeDatabricksSql(staging_path=staging_dir),
                ),
                log=_FakeLog(),
            )

            result = _sync_table(context, self.table_config)

        self.assertEqual(result.metadata["load_mode"], "full_refresh")
        self.assertEqual(result.metadata["row_count"], 0)
        self.assertEqual(
            context.resources.databricks_sql.ensure_calls,
            ["sa_categories"],
        )
        self.assertEqual(
            context.resources.databricks_sql.truncate_calls,
            ["sa_categories"],
        )
        self.assertTrue(context.resources.bridge_state_sqlserver.completed[0]["succeeded"])

    def test_replace_target_from_remote_uses_insert_overwrite(self) -> None:
        resource = DatabricksSqlResource(
            server_hostname="server",
            http_path="/sql/path",
            auth_mode="pat",
            access_token="token",
            client_id="",
            client_secret="",
            catalog="main",
            target_schema="bronze",
            staging_schema="staging",
            volume_catalog="main",
            volume_schema="files",
            volume_name="bridge",
            staging_allowed_local_path="/tmp/databricks_bridge",
        )
        cursor = _RecordingCursor()
        columns = [
            SourceColumn(name="Id", sql_type="int", nullable=False, ordinal_position=1),
            SourceColumn(name="Name", sql_type="varchar", nullable=True, ordinal_position=2),
        ]

        resource.replace_target_from_remote(
            cursor=cursor,
            table_config=self.table_config,
            columns=columns,
            remote_batch_dir="/Volumes/main/files/bridge/sa_categories/batch_1",
        )

        self.assertEqual(cursor.commands[0], "DROP TABLE IF EXISTS `main`.`staging`.`sa_categories__bridge_full_refresh_stage`")
        self.assertIn("CREATE OR REPLACE TABLE `main`.`staging`.`sa_categories__bridge_full_refresh_stage`", cursor.commands[1])
        self.assertIn("COPY INTO `main`.`staging`.`sa_categories__bridge_full_refresh_stage`", cursor.commands[2])
        self.assertEqual(
            cursor.commands[3],
            "INSERT OVERWRITE TABLE `main`.`bronze`.`sa_categories` SELECT * FROM `main`.`staging`.`sa_categories__bridge_full_refresh_stage`",
        )
        self.assertEqual(cursor.commands[4], "DROP TABLE IF EXISTS `main`.`staging`.`sa_categories__bridge_full_refresh_stage`")

    def test_dims_group_schedule_defaults_to_weekly(self) -> None:
        facts_config = BridgeTableConfig(
            key="work_plan",
            ingestion_name="DatabricksBridge_work_plan",
            source_schema="",
            source_table="work_plan",
            target_table="sa_work_plan",
            primary_keys=["Id"],
            updated_at_column="timeLocal",
            created_at_column="",
            job_group="facts",
        )
        bridge_jobs = build_bridge_jobs([self.table_config, facts_config])

        with patch.dict(os.environ, {}, clear=False):
            schedules = build_bridge_schedules(bridge_jobs, [self.table_config, facts_config])

        schedules_by_name = {schedule.name: schedule.cron_schedule for schedule in schedules}

        self.assertEqual(schedules_by_name["bridge_dims_schedule"], "0 11 * * 1")
        self.assertEqual(schedules_by_name["bridge_facts_schedule"], "0 */8 * * *")
        self.assertEqual(schedules_by_name["bridge_tables_schedule"], "0 */8 * * *")


if __name__ == "__main__":
    unittest.main()
