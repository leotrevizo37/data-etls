import importlib
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


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
sys.modules.pop("dagster", None)
try:
    asset_module = importlib.import_module(
        "bafar.defs.assets.dwh_factOutOfHoursSales",
    )
    daily_asset_module = importlib.import_module(
        "bafar.defs.assets.dwh_factStoreSalesDaily",
    )
finally:
    sys.path[:] = original_sys_path


class FactOutOfHoursSalesWindowTests(unittest.TestCase):
    def test_window_applies_overlap_without_changing_watermark(self) -> None:
        last_loaded_at = datetime(2026, 7, 22, 14, 0)
        sqlserver = MagicMock()
        sqlserver.get_watermark.return_value = last_loaded_at
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        window = asset_module.get_fact_out_of_hours_sales_window_op.compute_fn.decorated_fn(
            context,
        )

        self.assertEqual(window["last_loaded_at"], last_loaded_at)
        self.assertEqual(window["from_value"], "2026-07-22T13:45:00.000000")

    def test_overlap_rows_do_not_move_watermark_backwards(self) -> None:
        last_loaded_at = datetime(2026, 7, 22, 14, 0)
        cases = (
            (datetime(2026, 7, 22, 13, 50), last_loaded_at),
            (datetime(2026, 7, 22, 14, 10), datetime(2026, 7, 22, 14, 10)),
            (None, last_loaded_at),
        )

        for max_sale_date, expected_watermark in cases:
            with self.subTest(max_sale_date=max_sale_date):
                sqlserver = MagicMock()
                sqlserver.merge_fact_out_of_hours_sales.return_value = 1
                spark_resource = MagicMock()
                spark_resource.write_fact_out_of_hours_sales_stage.return_value = 1
                context = SimpleNamespace(
                    resources=SimpleNamespace(
                        spark=spark_resource,
                        sqlserver=sqlserver,
                    ),
                    run_id="run-1",
                    retry_number=0,
                    log=MagicMock(),
                )
                sales_df = MagicMock()
                sales_df.agg.return_value.first.return_value = SimpleNamespace(
                    MaxFecha=max_sale_date,
                )

                with (
                    patch.object(asset_module, "spark_max") as spark_max,
                    patch.object(asset_module, "_read_store_schedules"),
                    patch.object(
                        asset_module,
                        "transform_fact_out_of_hours_sales",
                    ) as transform,
                ):
                    spark_max.return_value.alias.return_value = object()
                    transform.return_value = object()
                    result = asset_module.process_fact_out_of_hours_sales_op.compute_fn.decorated_fn(
                        context,
                        {
                            "last_loaded_at": last_loaded_at,
                            "from_value": "2026-07-22T13:45:00.000000",
                        },
                        sales_df,
                    )

                self.assertEqual(
                    result["new_last_loaded_at"],
                    expected_watermark,
                )


class FactStoreSalesDailyWindowTests(unittest.TestCase):
    def test_window_uses_complete_days(self) -> None:
        last_loaded_at = datetime(2026, 7, 20, 16, 35)
        sqlserver = MagicMock()
        sqlserver.get_watermark.return_value = last_loaded_at
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        with patch.object(daily_asset_module, "datetime", wraps=datetime) as current_datetime:
            current_datetime.now.return_value = datetime(2026, 7, 22, 1, 0)
            window = daily_asset_module.get_fact_store_sales_daily_window_op.compute_fn.decorated_fn(
                context,
            )

        self.assertEqual(window["last_loaded_at"], last_loaded_at)
        self.assertEqual(window["from_value"], "2026-07-20T00:00:00.000000")
        self.assertEqual(window["to_value"], "2026-07-22T00:00:00.000000")

    def test_second_run_on_same_day_reprocesses_watermark_day(self) -> None:
        last_loaded_at = datetime(2026, 7, 21)
        sqlserver = MagicMock()
        sqlserver.get_watermark.return_value = last_loaded_at
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        with patch.object(daily_asset_module, "datetime", wraps=datetime) as current_datetime:
            current_datetime.now.return_value = datetime(2026, 7, 22, 18, 30)
            window = daily_asset_module.get_fact_store_sales_daily_window_op.compute_fn.decorated_fn(
                context,
            )

        self.assertEqual(window["from_value"], "2026-07-21T00:00:00.000000")
        self.assertEqual(window["to_value"], "2026-07-22T00:00:00.000000")

    def test_sales_max_date_sets_truncated_watermark(self) -> None:
        last_loaded_at = datetime(2026, 7, 20)
        sqlserver = MagicMock()
        sqlserver.merge_fact_store_sales_daily.return_value = 2
        spark_resource = MagicMock()
        spark_resource.write_fact_store_sales_daily_stage.return_value = 2
        context = SimpleNamespace(
            resources=SimpleNamespace(
                spark=spark_resource,
                sqlserver=sqlserver,
            ),
            run_id="run-1",
            retry_number=0,
            log=MagicMock(),
        )
        sales_df = MagicMock()
        sales_df.agg.return_value.first.return_value = SimpleNamespace(
            MaxFecha=datetime(2026, 7, 21, 23, 45),
        )

        with (
            patch.object(daily_asset_module, "spark_max") as spark_max,
            patch.object(
                daily_asset_module,
                "transform_fact_store_sales_daily",
            ) as transform,
        ):
            spark_max.return_value.alias.return_value = object()
            transform.return_value = object()
            result = daily_asset_module.process_fact_store_sales_daily_op.compute_fn.decorated_fn(
                context,
                {
                    "last_loaded_at": last_loaded_at,
                    "from_value": "2026-07-20T00:00:00.000000",
                    "to_value": "2026-07-22T00:00:00.000000",
                },
                sales_df,
            )

        self.assertEqual(result["new_last_loaded_at"], datetime(2026, 7, 21))

    def test_finalize_persists_sales_day_watermark(self) -> None:
        sales_day = datetime(2026, 7, 21)
        sqlserver = MagicMock()
        context = SimpleNamespace(
            resources=SimpleNamespace(sqlserver=sqlserver),
        )

        daily_asset_module.finalize_fact_store_sales_daily_run_op.compute_fn.decorated_fn(
            context,
            {
                "new_last_loaded_at": sales_day,
                "evaluated_rows": 2,
                "affected_rows": 2,
                "from_value": "2026-07-21T00:00:00.000000",
                "to_value": "2026-07-22T00:00:00.000000",
            },
        )

        sqlserver.mark_run_completed.assert_called_once_with(
            ingestion_name="FactStoreSalesDaily",
            new_last_loaded_at=sales_day,
            succeeded=True,
            error_message=None,
        )


if __name__ == "__main__":
    unittest.main()
