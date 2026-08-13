import importlib
import json
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [
    str(PROJECT_SRC),
    *[
        path
        for path in original_sys_path
        if Path(path or ".").resolve() != REPOSITORY_ROOT
    ],
]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    importlib.import_module("dagster")
    asset_module = importlib.import_module(
        "mcdonalds_cdp.defs.assets.observability_factDagsterAssetExecutionDaily"
    )
    job_module = importlib.import_module(
        "mcdonalds_cdp.defs.jobs.observability_dagster_asset_execution_job"
    )
    schedule_module = importlib.import_module(
        "mcdonalds_cdp.defs.schedules.observability_dagster_asset_execution_schedule"
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


class SourceResultStub:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class SourceConnectionStub:
    def __init__(self, storage):
        self.storage = storage

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, parameters):
        self.storage.query = str(query)
        self.storage.parameters = parameters
        if self.storage.error is not None:
            raise self.storage.error
        return SourceResultStub(self.storage.rows)


class RunStorageStub:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.query = None
        self.parameters = None

    def connect(self):
        return SourceConnectionStub(self)


class SqlServerStub:
    def __init__(self, watermark=None, merge_result=1):
        self.watermark = watermark
        self.merge_result = merge_result
        self.started = []
        self.completed = []
        self.queries = []

    def get_watermark(self, ingestion_name):
        return self.watermark

    def mark_run_started(self, ingestion_name):
        self.started.append(ingestion_name)

    def execute_query(self, query, *args):
        self.queries.append((query, *args))
        return self.merge_result

    def mark_run_completed(self, **kwargs):
        self.completed.append(kwargs)


class DagsterAssetExecutionDailyTests(unittest.TestCase):
    def test_upserts_daily_metrics_and_json_details(self) -> None:
        step_details = json.dumps(
            [
                {
                    "run_id": "run-success",
                    "step_key": "asset_step",
                    "status": "MATERIALIZED",
                    "duration_seconds": 120.0,
                    "queued_seconds": 30.0,
                },
                {
                    "run_id": "run-failure",
                    "step_key": "asset_step",
                    "status": "FAILED",
                    "duration_seconds": 60.0,
                    "queued_seconds": 10.0,
                },
            ]
        )
        asset_check_details = json.dumps(
            [
                {
                    "run_id": "run-success",
                    "check_name": "row_count_gt_0",
                    "status": "FAILED",
                }
            ]
        )
        rows = [
            {
                "user_code": "mcdonalds_cdp",
                "asset_key": '["mcdonalds_cdp", "dwh_factlecturas"]',
                "metric_date": date(2026, 8, 10),
                "execution_count": 2,
                "materialization_count": 1,
                "inserted_row_count": 4500,
                "merged_row_count": 4300,
                "execution_failure_count": 1,
                "asset_check_count": 1,
                "asset_check_failure_count": 1,
                "average_time_to_complete_seconds": Decimal("120.000"),
                "has_failed": True,
                "number_of_failures": 2,
                "average_queued_time_seconds": Decimal("20.000"),
                "minutes_on_job": Decimal("3.000"),
                "time_in_job_percentage": Decimal("0.4167"),
                "step_details_json": step_details,
                "asset_check_details_json": asset_check_details,
            }
        ]
        storage = RunStorageStub(rows=rows)
        sqlserver = SqlServerStub(
            watermark=datetime(2026, 8, 10, 9, 30)
        )
        context = SimpleNamespace(
            instance=SimpleNamespace(run_storage=storage),
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        with patch.object(asset_module, "datetime", FixedDateTime):
            asset_module.observability_factdagsterassetexecutiondaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(
            sqlserver.started,
            ["factDagsterAssetExecutionDaily"],
        )
        self.assertEqual(
            storage.parameters,
            {
                "user_code": "mcdonalds_cdp",
                "from_utc": datetime(2026, 8, 9),
                "to_utc": datetime(2026, 8, 10, 12),
            },
        )
        self.assertEqual(
            sqlserver.queries[0][1:],
            (
                "mcdonalds_cdp",
                "mcdonalds_cdp/dwh_factlecturas",
                date(2026, 8, 10),
                2,
                1,
                4500,
                4300,
                1,
                1,
                1,
                Decimal("120.000"),
                True,
                2,
                Decimal("20.000"),
                Decimal("3.000"),
                Decimal("0.4167"),
                step_details,
                asset_check_details,
            ),
        )
        self.assertEqual(
            json.loads(sqlserver.queries[0][-2])[1]["status"],
            "FAILED",
        )
        self.assertEqual(
            json.loads(sqlserver.queries[0][-1])[0]["check_name"],
            "row_count_gt_0",
        )
        self.assertTrue(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            datetime(2026, 8, 10, 12),
        )

    def test_first_run_reads_all_available_history(self) -> None:
        storage = RunStorageStub()
        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            instance=SimpleNamespace(run_storage=storage),
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        with patch.object(asset_module, "datetime", FixedDateTime):
            asset_module.observability_factdagsterassetexecutiondaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertIsNone(storage.parameters["from_utc"])
        self.assertEqual(sqlserver.queries, [])
        self.assertTrue(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            datetime(2026, 8, 10, 12),
        )

    def test_source_failure_preserves_watermark(self) -> None:
        watermark = datetime(2026, 8, 9, 8)
        storage = RunStorageStub(error=RuntimeError("source unavailable"))
        sqlserver = SqlServerStub(watermark=watermark)
        context = SimpleNamespace(
            instance=SimpleNamespace(run_storage=storage),
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        with (
            patch.object(asset_module, "datetime", FixedDateTime),
            self.assertRaisesRegex(RuntimeError, "source unavailable"),
        ):
            asset_module.observability_factdagsterassetexecutiondaily.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(sqlserver.queries, [])
        self.assertFalse(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            watermark,
        )

    def test_source_query_contract_covers_metrics_and_details(self) -> None:
        source_sql = asset_module.DAGSTER_ASSET_EXECUTION_DAILY_SQL

        for table_name in (
            "FROM event_logs",
            "INNER JOIN runs",
            "FROM asset_check_executions",
        ):
            self.assertIn(table_name, source_sql)
        for event_type in (
            "ASSET_MATERIALIZATION_PLANNED",
            "ASSET_MATERIALIZATION",
            "STEP_START",
            "STEP_RESTARTED",
            "STEP_SUCCESS",
            "STEP_FAILURE",
        ):
            self.assertIn(event_type, source_sql)
        self.assertIn("asset_key::jsonb ->> 0 = :user_code", source_sql)
        self.assertIn("runs.start_time", source_sql)
        self.assertIn("runs.end_time", source_sql)
        self.assertIn("runs.create_timestamp", source_sql)
        self.assertIn("'EXECUTION_FAILED'", source_sql)
        self.assertIn("ELSE 1440.0", source_sql)
        self.assertIn("run_asset_weights AS", source_sql)
        self.assertIn("run_node_costs AS", source_sql)
        self.assertIn(
            "evaluation_event::jsonb ->> 'step_key'",
            source_sql,
        )
        self.assertIn("run_interval_boundaries AS", source_sql)
        self.assertIn("LEAD(boundary) OVER", source_sql)
        self.assertIn("active_run_segments AS", source_sql)
        self.assertIn(
            "COALESCE(asset_busy_aggregates.minutes_on_job, 0.0)",
            source_sql,
        )
        self.assertEqual(source_sql.count("jsonb_agg("), 2)
        self.assertIn("'duration_seconds'", source_sql)
        self.assertIn("'queued_seconds'", source_sql)
        self.assertIn("'evaluation_event_storage_id'", source_sql)
        self.assertIn("'materialization_event_storage_id'", source_sql)
        self.assertIn("materialization_metadata_values AS", source_sql)
        self.assertIn("asset_check_row_metadata AS", source_sql)
        self.assertIn("check_metadata.key = 'row_count'", source_sql)
        self.assertIn("'staged_row_count'", source_sql)
        self.assertIn("'merged_rows'", source_sql)
        self.assertIn("'evaluated_rows'", source_sql)
        self.assertIn("'affected_rows'", source_sql)
        self.assertIn("'review_queue_rows'", source_sql)
        self.assertIn("'review_queue_merged_rows'", source_sql)
        self.assertIn("'detail_row_count'", source_sql)
        self.assertIn("AS inserted_row_count", source_sql)
        self.assertIn("AS merged_row_count", source_sql)

    def test_global_ddl_job_and_schedule_cover_all_code_locations(self) -> None:
        sql_name = "023_observability_factDagsterAssetExecutionDaily.sql"
        sql_path = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
            / sql_name
        )
        ddl = sql_path.read_text(encoding="utf-8")

        self.assertIn(
            "CREATE TABLE observability.factDagsterAssetExecutionDaily",
            ddl,
        )
        self.assertIn("StepDetailsJson NVARCHAR(MAX) NOT NULL", ddl)
        self.assertIn("AssetCheckDetailsJson NVARCHAR(MAX) NOT NULL", ddl)
        self.assertIn("InsertedRowCount BIGINT NULL", ddl)
        self.assertIn("MergedRowCount BIGINT NULL", ddl)
        self.assertIn("N'InsertedRowCount'", ddl)
        self.assertIn("N'MergedRowCount'", ddl)
        self.assertIn("LastLoadedAt = NULL", ddl)
        self.assertEqual(ddl.count("CHECK (ISJSON("), 2)
        self.assertIn(
            "PRIMARY KEY CLUSTERED ([Date], UserCode, AssetKey)",
            ddl,
        )
        self.assertIn("N'factDagsterAssetExecutionDaily'", ddl)

        user_codes_root = REPOSITORY_ROOT / "sql" / "user_codes"
        for user_code_path in user_codes_root.iterdir():
            if user_code_path.is_dir():
                self.assertFalse(
                    (user_code_path / "init" / sql_name).exists()
                )

        for user_code in (
            "bafar",
            "bafar_poc_gabinete",
            "carlsjr",
            "emerson",
            "mcdonalds",
            "mcdonalds_cdp",
            "smartfit",
            "valledelencino",
        ):
            definitions_root = (
                REPOSITORY_ROOT
                / "dagster"
                / user_code
                / "src"
                / user_code
                / "defs"
            )
            asset_source = (
                definitions_root
                / "assets"
                / "observability_factDagsterAssetExecutionDaily.py"
            ).read_text(encoding="utf-8")
            job_source = (
                definitions_root
                / "jobs"
                / "observability_dagster_asset_execution_job.py"
            ).read_text(encoding="utf-8")
            schedule_source = (
                definitions_root
                / "schedules"
                / "observability_dagster_asset_execution_schedule.py"
            ).read_text(encoding="utf-8")
            self.assertIn(f'key_prefix=["{user_code}"]', asset_source)
            self.assertIn(f'"user_code": "{user_code}"', asset_source)
            self.assertIn(
                f'["{user_code}", '
                '"observability_factdagsterassetexecutiondaily"]',
                job_source,
            )
            self.assertIn(
                f'"user_code_run_lane": "{user_code}_observability"',
                job_source,
            )
            self.assertIn(f'"user_code": "{user_code}"', job_source)
            self.assertIn('cron_schedule="15 1 * * *"', schedule_source)
            self.assertIn(
                "default_status=DefaultScheduleStatus.RUNNING",
                schedule_source,
            )

        job = job_module.observability_dagster_asset_execution_job
        self.assertEqual(
            job.name,
            "123_observability_dagster_asset_execution",
        )
        self.assertEqual(job.tags["job_type"], "observability")
        self.assertEqual(job.tags["run_lane"], "observability")
        self.assertEqual(
            job.tags["user_code_run_lane"],
            "mcdonalds_cdp_observability",
        )
        self.assertEqual(job.tags["user_code"], "mcdonalds_cdp")

        schedule = (
            schedule_module.observability_dagster_asset_execution_every_2h
        )
        self.assertEqual(
            schedule.name,
            "123_observability_dagster_asset_execution_every_2h",
        )
        self.assertEqual(schedule.cron_schedule, "15 1 * * *")
        self.assertEqual(
            schedule.execution_timezone,
            "America/Ciudad_Juarez",
        )
        self.assertEqual(schedule.default_status.value, "RUNNING")


if __name__ == "__main__":
    unittest.main()
