import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


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
    with patch.dict(sys.modules):
        sys.modules.pop("dagster", None)
        sys.modules.pop("pyodbc", None)
        _install_stubs()
        path = PROJECT_ROOT / "src" / "bafar" / "defs" / "resources" / "sql_server.py"
        spec = importlib.util.spec_from_file_location("bafar_sql_server_resource", path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
    return module


class DimStoreSchedulesMergeSqlTests(unittest.TestCase):
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

    def test_merge_sql_is_idempotent_by_store_and_day(self) -> None:
        sql = self.module.build_dim_store_schedules_merge_sql()

        self.assertIn("MERGE dwh.dimStoreSchedules", sql)
        self.assertIn("FROM stg.dimStoreSchedules", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.StoreId = source.StoreId", sql)
        self.assertIn("target.DayNo = source.DayNo", sql)
        self.assertIn("target.ModifiedAt = SYSUTCDATETIME()", sql)
        self.assertIn("ClosingDate < CONVERT(DATE, SYSUTCDATETIME())", sql)
        self.assertNotIn("target.CreatedAt =", sql)
        self.assertNotIn("WHEN NOT MATCHED BY SOURCE", sql)
        self.assertIn("WHEN MATCHED AND EXISTS", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)

    def test_clear_stage_deletes_only_the_requested_run(self) -> None:
        self.cursor.rowcount = 4

        deleted = self.resource.clear_dim_store_schedules_stage("run-1")

        self.assertEqual(deleted, 4)
        self.cursor.execute.assert_called_once_with(
            "DELETE FROM stg.dimStoreSchedules WHERE RunId = ?",
            "run-1",
        )
        self.connection.commit.assert_called_once_with()

    def test_merge_returns_affected_rows(self) -> None:
        self.cursor.fetchone.return_value = types.SimpleNamespace(AffectedRows=3)

        affected = self.resource.merge_dim_store_schedules("run-2")

        self.assertEqual(affected, 3)
        self.cursor.execute.assert_called_once_with(
            self.module.build_dim_store_schedules_merge_sql(),
            "run-2",
        )
        self.connection.commit.assert_called_once_with()

    def test_stage_writer_uses_normalized_store_day_rows(self) -> None:
        rows = [
            {
                "StoreId": "M017",
                "CustomerNo": "M017",
                "StoreName": "Tienda de prueba",
                "OpeningDate": "2016-01-01",
                "ClosingDate": "9999-12-31",
                "IsBlocked": False,
                "BlockedFrom": None,
                "BlockedTo": None,
                "ReceivingId": "003",
                "DayCode": "MON",
                "DayNo": 1,
                "IsClosed": False,
                "OpeningTime": "06:30:00",
                "LunchStartTime": None,
                "LunchEndTime": None,
                "ClosingTime": "14:30:00",
            }
        ]

        written = self.resource.write_dim_store_schedules_stage("run-3", rows)

        self.assertEqual(written, 1)
        self.cursor.executemany.assert_called_once()
        self.assertIn(
            "INSERT INTO stg.dimStoreSchedules",
            self.cursor.executemany.call_args.args[0],
        )
        self.assertEqual(self.cursor.executemany.call_args.args[1][0][0], "run-3")
        self.connection.commit.assert_called_once_with()

if __name__ == "__main__":
    unittest.main()
