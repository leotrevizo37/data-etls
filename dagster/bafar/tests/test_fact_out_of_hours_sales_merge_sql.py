import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _install_stubs() -> None:
    if "pyodbc" not in sys.modules:
        pyodbc = types.ModuleType("pyodbc")
        pyodbc.Connection = object
        pyodbc.Error = Exception
        sys.modules["pyodbc"] = pyodbc

    dagster = sys.modules.get("dagster")
    if dagster is None:
        dagster = types.ModuleType("dagster")
        sys.modules["dagster"] = dagster
    if not hasattr(dagster, "resource"):
        dagster.resource = lambda fn=None, **_kwargs: fn


def _load_sql_server_module():
    _install_stubs()
    path = PROJECT_ROOT / "src" / "bafar" / "defs" / "resources" / "sql_server.py"
    spec = importlib.util.spec_from_file_location("bafar_sql_server_resource", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FactOutOfHoursSalesMergeSqlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_sql_server_module()

    def setUp(self) -> None:
        self.resource = self.module.SqlServerResource(
            host="localhost",
            port=1433,
            database="test_database",
            user="test_user",
            password="",
            connect_timeout=30,
            command_timeout=60,
            driver="ODBC Driver 18 for SQL Server",
            retries=1,
            retry_interval=1,
        )
        self.cursor = MagicMock()
        self.cursor.__enter__.return_value = self.cursor
        self.connection = MagicMock()
        self.connection.__enter__.return_value = self.connection
        self.connection.cursor.return_value = self.cursor
        self.resource._get_connection = MagicMock(return_value=self.connection)

    def test_merge_sql_is_idempotent_by_sale_id(self) -> None:
        sql = self.module.build_fact_out_of_hours_sales_merge_sql()

        self.assertIn("MERGE dwh.factOutOfHoursSales", sql)
        self.assertIn("FROM stg.factOutOfHoursSales", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.SaleId = source.SaleId", sql)
        self.assertIn("target.OutOfHoursType = source.OutOfHoursType", sql)
        self.assertIn("target.DeviationMinutes = source.DeviationMinutes", sql)
        self.assertIn("source.OutOfHoursType", sql)
        self.assertIn("source.DeviationMinutes", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertIn("WHEN MATCHED AND source.OutOfHoursType IS NULL THEN", sql)
        self.assertIn("DELETE", sql)
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn(
            "WHEN NOT MATCHED BY TARGET AND source.OutOfHoursType IS NOT NULL THEN",
            sql,
        )

    def test_clear_stage_deletes_only_the_requested_run(self) -> None:
        self.cursor.rowcount = 4

        deleted = self.resource.clear_fact_out_of_hours_sales_stage("run-1")

        self.assertEqual(deleted, 4)
        self.cursor.execute.assert_called_once_with(
            "DELETE FROM stg.factOutOfHoursSales WHERE RunId = ?",
            "run-1",
        )
        self.connection.commit.assert_called_once_with()

    def test_merge_returns_affected_rows(self) -> None:
        self.cursor.fetchone.return_value = types.SimpleNamespace(AffectedRows=3)

        affected = self.resource.merge_fact_out_of_hours_sales("run-2")

        self.assertEqual(affected, 3)
        self.cursor.execute.assert_called_once_with(
            self.module.build_fact_out_of_hours_sales_merge_sql(),
            "run-2",
        )
        self.connection.commit.assert_called_once_with()

    def test_watermark_query_errors_are_not_treated_as_an_initial_load(self) -> None:
        self.cursor.execute.side_effect = self.module.pyodbc.Error("query failed")

        with self.assertRaises(self.module.pyodbc.Error):
            self.resource.get_watermark("FactOutOfHoursSales")


if __name__ == "__main__":
    unittest.main()
