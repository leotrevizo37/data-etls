import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch
from zoneinfo import ZoneInfo


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
    backfill_module = importlib.import_module(
        "bafar.defs.jobs.sales_backfill_jobs",
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


class SalesBackfillConfigTests(unittest.TestCase):
    def test_daily_configs_use_complete_local_days(self) -> None:
        cases = (
            (
                backfill_module.fact_out_of_hours_sales_backfill_daily_config,
                "backfill_fact_out_of_hours_sales_daily_op",
            ),
            (
                backfill_module.fact_store_sales_daily_backfill_daily_config,
                "backfill_fact_store_sales_daily_op",
            ),
        )

        for partitioned_config, op_name in cases:
            with self.subTest(op_name=op_name):
                run_config = partitioned_config.get_run_config_for_partition_key(
                    "2026-05-01",
                )

                self.assertEqual(
                    run_config["ops"][op_name]["config"],
                    {
                        "from_value": "2026-05-01T00:00:00.000000",
                        "to_value": "2026-05-02T00:00:00.000000",
                    },
                )
                self.assertEqual(
                    partitioned_config.partitions_def.timezone,
                    "America/Ciudad_Juarez",
                )
                self.assertEqual(
                    partitioned_config.partitions_def.get_partition_keys(
                        current_time=datetime(
                            2026,
                            5,
                            3,
                            12,
                            tzinfo=ZoneInfo("America/Ciudad_Juarez"),
                        ),
                    ),
                    ["2026-05-01", "2026-05-02"],
                )

    def test_backfill_jobs_keep_sales_concurrency_tags(self) -> None:
        cases = (
            (
                backfill_module.fact_out_of_hours_sales_backfill_daily_job,
                "022_bafar_out_of_hours_sales_backfill_daily",
                "fact_out_of_hours_sales_daily",
            ),
            (
                backfill_module.fact_store_sales_daily_backfill_daily_job,
                "023_bafar_store_sales_daily_backfill_daily",
                "fact_store_sales_daily",
            ),
        )

        for job_def, job_name, backfill_tag in cases:
            with self.subTest(job_name=job_name):
                self.assertEqual(job_def.name, job_name)
                self.assertEqual(job_def.tags["job_type"], "ventas")
                self.assertEqual(job_def.tags["user_code"], "bafar")
                self.assertEqual(job_def.tags["backfill"], backfill_tag)


class SalesBackfillOpTests(unittest.TestCase):
    def test_out_of_hours_backfill_uses_bounded_window_and_merge(self) -> None:
        spark_resource = MagicMock()
        sqlserver = MagicMock()
        source_sqlserver = MagicMock()
        sales_df = object()
        schedules_df = object()
        result_df = object()
        spark_resource.read_fact_out_of_hours_sales_window.return_value = sales_df
        spark_resource.read_store_schedules.return_value = schedules_df
        spark_resource.write_fact_out_of_hours_sales_stage.return_value = 8
        sqlserver.merge_fact_out_of_hours_sales.return_value = 5
        context = SimpleNamespace(
            resources=SimpleNamespace(
                spark=spark_resource,
                sqlserver=sqlserver,
                dim_sqlserver=source_sqlserver,
            ),
            op_config={
                "from_value": "2026-05-01T00:00:00.000000",
                "to_value": "2026-05-02T00:00:00.000000",
            },
            run_id="run-1",
            log=MagicMock(),
        )

        with patch.object(
            backfill_module,
            "transform_fact_out_of_hours_sales",
            return_value=result_df,
        ) as transform:
            result = (
                backfill_module.backfill_fact_out_of_hours_sales_daily_op
                .compute_fn.decorated_fn(context)
            )

        spark_resource.read_fact_out_of_hours_sales_window.assert_called_once_with(
            source_sqlserver,
            "2026-05-01T00:00:00.000000",
            "2026-05-02T00:00:00.000000",
        )
        spark_resource.read_store_schedules.assert_called_once_with(sqlserver)
        transform.assert_called_once_with(sales_df, schedules_df)
        spark_resource.write_fact_out_of_hours_sales_stage.assert_called_once_with(
            result_df,
            "run-1",
        )
        sqlserver.merge_fact_out_of_hours_sales.assert_called_once_with("run-1")
        sqlserver.clear_fact_out_of_hours_sales_stage.assert_has_calls(
            [call("run-1"), call("run-1")],
        )
        sqlserver.get_watermark.assert_not_called()
        sqlserver.mark_run_started.assert_not_called()
        sqlserver.mark_run_completed.assert_not_called()
        self.assertEqual(result["evaluated_rows"], 8)
        self.assertEqual(result["affected_rows"], 5)

    def test_store_sales_daily_backfill_uses_bounded_window_and_merge(self) -> None:
        spark_resource = MagicMock()
        sqlserver = MagicMock()
        source_sqlserver = MagicMock()
        sales_df = object()
        result_df = object()
        spark_resource.read_fact_store_sales_daily_window.return_value = sales_df
        spark_resource.write_fact_store_sales_daily_stage.return_value = 6
        sqlserver.merge_fact_store_sales_daily.return_value = 4
        context = SimpleNamespace(
            resources=SimpleNamespace(
                spark=spark_resource,
                sqlserver=sqlserver,
                dim_sqlserver=source_sqlserver,
            ),
            op_config={
                "from_value": "2026-05-01T00:00:00.000000",
                "to_value": "2026-05-02T00:00:00.000000",
            },
            run_id="run-2",
            log=MagicMock(),
        )

        with patch.object(
            backfill_module,
            "transform_fact_store_sales_daily",
            return_value=result_df,
        ) as transform:
            result = (
                backfill_module.backfill_fact_store_sales_daily_op
                .compute_fn.decorated_fn(context)
            )

        spark_resource.read_fact_store_sales_daily_window.assert_called_once_with(
            source_sqlserver,
            "2026-05-01T00:00:00.000000",
            "2026-05-02T00:00:00.000000",
        )
        transform.assert_called_once_with(sales_df)
        spark_resource.write_fact_store_sales_daily_stage.assert_called_once_with(
            result_df,
            "run-2",
        )
        sqlserver.merge_fact_store_sales_daily.assert_called_once_with("run-2")
        sqlserver.clear_fact_store_sales_daily_stage.assert_has_calls(
            [call("run-2"), call("run-2")],
        )
        sqlserver.get_watermark.assert_not_called()
        sqlserver.mark_run_started.assert_not_called()
        sqlserver.mark_run_completed.assert_not_called()
        self.assertEqual(result["evaluated_rows"], 6)
        self.assertEqual(result["affected_rows"], 4)

    def test_failed_backfill_cleans_its_staging_rows(self) -> None:
        spark_resource = MagicMock()
        sqlserver = MagicMock()
        source_sqlserver = MagicMock()
        spark_resource.write_fact_store_sales_daily_stage.side_effect = RuntimeError(
            "write failed",
        )
        context = SimpleNamespace(
            resources=SimpleNamespace(
                spark=spark_resource,
                sqlserver=sqlserver,
                dim_sqlserver=source_sqlserver,
            ),
            op_config={
                "from_value": "2026-05-01T00:00:00.000000",
                "to_value": "2026-05-02T00:00:00.000000",
            },
            run_id="run-3",
            log=MagicMock(),
        )

        with (
            patch.object(
                backfill_module,
                "transform_fact_store_sales_daily",
                return_value=object(),
            ),
            self.assertRaisesRegex(RuntimeError, "write failed"),
        ):
            (
                backfill_module.backfill_fact_store_sales_daily_op
                .compute_fn.decorated_fn(context)
            )

        sqlserver.clear_fact_store_sales_daily_stage.assert_has_calls(
            [call("run-3"), call("run-3")],
        )
        sqlserver.merge_fact_store_sales_daily.assert_not_called()


if __name__ == "__main__":
    unittest.main()
