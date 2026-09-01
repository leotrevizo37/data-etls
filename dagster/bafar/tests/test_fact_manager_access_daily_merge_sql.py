import importlib.util
import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


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


def build_payload():
    return {
        "fechaCorte": "2026-08-31T11:00:00.0000000",
        "corteHora": 11,
        "diasHistorial": 180,
        "data": [
            {
                "idUsuario": 105,
                "noEmpleado": "10022421",
                "nombreGerente": "Gerente Uno",
                "nombreUsuario": "gerente.uno",
                "email": "gerente.uno@example.com",
                "puesto": "Gerente de Sucursal",
                "rol": "Gestor",
                "activo": True,
                "idTienda": 318,
                "claveTienda": "M377",
                "nombreTienda": "Tienda Uno",
                "zona": "NORTE",
                "ultimoInicioSesion": "2026-08-31T10:30:00.1234567",
                "ingresoAntesDelCorte": True,
                "diasSinIngresar": 0,
                "estatusCorte": "Ingresó a tiempo",
            },
            {
                "idUsuario": 265,
                "noEmpleado": "94120",
                "nombreGerente": "Gerente Dos",
                "nombreUsuario": "gerente.dos",
                "email": "gerente.dos@example.com",
                "puesto": "Gerente tienda",
                "rol": "Operacion",
                "activo": True,
                "idTienda": None,
                "claveTienda": "M541",
                "nombreTienda": "Tienda Dos",
                "zona": "SUR",
                "ultimoInicioSesion": None,
                "ingresoAntesDelCorte": False,
                "diasSinIngresar": None,
                "estatusCorte": "Sin ingreso hoy",
            },
        ],
    }


class FactManagerAccessDailySqlTests(unittest.TestCase):
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

    def test_merge_upserts_by_date_manager_and_store(self) -> None:
        sql = self.module.build_fact_manager_access_daily_merge_sql()

        self.assertIn("MERGE dwh.factManagerAccessDaily", sql)
        self.assertIn("FROM stg.factManagerAccessDaily", sql)
        self.assertIn("WHERE RunId = ?", sql)
        self.assertIn("target.Fecha = source.Fecha", sql)
        self.assertIn("target.IdUsuario = source.IdUsuario", sql)
        self.assertIn("target.ClaveTienda = source.ClaveTienda", sql)
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)

    def test_stage_write_preserves_nullable_store_and_access_values(self) -> None:
        written = self.resource.write_fact_manager_access_daily_stage(
            "run-1",
            date(2026, 8, 31),
            build_payload(),
        )

        rows = self.cursor.executemany.call_args.args[1]
        self.assertEqual(written, 2)
        self.assertEqual(rows[0][1], date(2026, 8, 31))
        self.assertEqual(rows[0][11], 318)
        self.assertIsNone(rows[1][11])
        self.assertIsNone(rows[1][15])
        self.assertIsNone(rows[1][17])
        self.connection.commit.assert_called_once_with()

    def test_merge_returns_affected_rows(self) -> None:
        self.cursor.fetchone.return_value = types.SimpleNamespace(AffectedRows=2)

        affected = self.resource.merge_fact_manager_access_daily("run-2")

        self.assertEqual(affected, 2)
        self.cursor.execute.assert_called_once_with(
            self.module.build_fact_manager_access_daily_merge_sql(),
            "run-2",
        )
        self.connection.commit.assert_called_once_with()

    def test_ddl_contains_history_keys_control_row_and_query_indexes(self) -> None:
        ddl_path = (
            REPOSITORY_ROOT
            / "sql"
            / "user_codes"
            / "bafar"
            / "init"
            / "012_dwh_stg_factManagerAccessDaily.sql"
        )
        if not ddl_path.is_file():
            self.skipTest("Repository SQL tree is not mounted in this runtime.")
        ddl = ddl_path.read_text(encoding="utf-8")

        self.assertIn("Fecha DATE NOT NULL", ddl)
        self.assertIn("FechaCorte DATETIME2(0) NOT NULL", ddl)
        self.assertIn("DiasHistorial SMALLINT NOT NULL", ddl)
        self.assertIn("NombreUsuario NVARCHAR(250) NOT NULL", ddl)
        self.assertIn("UltimoInicioSesion DATETIME2(0) NULL", ddl)
        self.assertIn(
            "COL_LENGTH(N'dwh.factManagerAccessDaily', N'DiasHistorial')",
            ddl,
        )
        self.assertIn(
            "COL_LENGTH(N'dwh.factManagerAccessDaily', N'NombreUsuario')",
            ddl,
        )
        self.assertIn(
            "COL_LENGTH(N'stg.factManagerAccessDaily', N'DiasHistorial')",
            ddl,
        )
        self.assertIn(
            "COL_LENGTH(N'stg.factManagerAccessDaily', N'NombreUsuario')",
            ddl,
        )
        self.assertIn(
            "PRIMARY KEY CLUSTERED (Fecha, IdUsuario, ClaveTienda)",
            ddl,
        )
        self.assertIn("ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte", ddl)
        self.assertIn("ix_factManagerAccessDaily_claveTienda_fecha", ddl)
        self.assertIn("IngestionName = N'FactManagerAccessDaily'", ddl)


if __name__ == "__main__":
    unittest.main()
