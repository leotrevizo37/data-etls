import importlib
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch


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
    api_module = importlib.import_module(
        "bafar.defs.resources.manager_access_api",
    )
    asset_module = importlib.import_module(
        "bafar.defs.assets.dwh_factManagerAccessDaily",
    )
    backfill_module = importlib.import_module(
        "bafar.defs.jobs.manager_access_backfill_job",
    )
    job_module = importlib.import_module(
        "bafar.defs.jobs.manager_access_daily_job",
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


def build_payload(fecha="2026-08-31", corte_hora=11, dias_historial=180):
    return {
        "ok": True,
        "fechaCorte": f"{fecha}T{corte_hora:02d}:00:00.0000000",
        "corteHora": corte_hora,
        "diasHistorial": dias_historial,
        "totalGerentes": 2,
        "totalIngresaronAntesDeCorte": 1,
        "totalSinIngresoEnCorte": 1,
        "porcentajeCumplimiento": 50.0,
        "totalRegistros": 3,
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
                "ultimoInicioSesion": f"{fecha}T10:30:00.0715",
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
                "estatusCorte": f"Sin ingreso en los últimos {dias_historial} días",
            },
            {
                "idUsuario": 105,
                "noEmpleado": "10022421",
                "nombreGerente": "Gerente Uno",
                "nombreUsuario": "gerente.uno",
                "email": "gerente.uno@example.com",
                "puesto": "Gerente de Sucursal",
                "rol": "Gestor",
                "activo": True,
                "idTienda": 319,
                "claveTienda": "M378",
                "nombreTienda": "Tienda Tres",
                "zona": "NORTE",
                "ultimoInicioSesion": f"{fecha}T10:30:00.1234567",
                "ingresoAntesDelCorte": True,
                "diasSinIngresar": 0,
                "estatusCorte": "Ingresó a tiempo",
            },
        ],
    }


class FakeHeaders:
    def get_content_type(self):
        return "application/json"


class FakeResponse:
    status = 200
    headers = FakeHeaders()

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class ManagerAccessApiContractTests(unittest.TestCase):
    def setUp(self):
        self.resource = api_module.ManagerAccessApiResource(
            url="http://api.example.test/api/v1/gerentes/ultimo-acceso",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
        )

    def test_valid_contract_preserves_rows_without_store_id(self):
        payload = build_payload()

        result = self.resource.validate_payload(
            payload,
            datetime(2026, 8, 31).date(),
            11,
            180,
        )

        self.assertIs(result, payload)
        self.assertIsNone(result["data"][1]["idTienda"])
        self.assertEqual(result["totalRegistros"], 3)

    def test_valid_contract_accepts_prior_access_inside_history_window(self):
        payload = build_payload()
        payload["data"][1]["ultimoInicioSesion"] = "2026-08-27T16:10:00.7654321"
        payload["data"][1]["diasSinIngresar"] = 4
        payload["data"][1]["estatusCorte"] = "Sin ingreso al corte"

        result = self.resource.validate_payload(
            payload,
            datetime(2026, 8, 31).date(),
            11,
            180,
        )

        self.assertIs(result, payload)

    def test_empty_snapshot_preserves_the_response_contract(self):
        payload = build_payload()
        payload["data"] = []
        payload["totalGerentes"] = 0
        payload["totalIngresaronAntesDeCorte"] = 0
        payload["totalSinIngresoEnCorte"] = 0
        payload["porcentajeCumplimiento"] = 0
        payload["totalRegistros"] = 0

        result = self.resource.validate_payload(
            payload,
            datetime(2026, 8, 31).date(),
            11,
            180,
        )

        self.assertIs(result, payload)

    def test_contract_drift_fails_before_loading(self):
        cases = []
        unknown_field = build_payload()
        unknown_field["newField"] = "unexpected"
        cases.append(unknown_field)
        inconsistent_total = build_payload()
        inconsistent_total["totalGerentes"] = 3
        cases.append(inconsistent_total)
        duplicate_key = build_payload()
        duplicate_key["data"][1]["idUsuario"] = 105
        duplicate_key["data"][1]["claveTienda"] = "M377"
        cases.append(duplicate_key)
        invalid_history_type = build_payload()
        invalid_history_type["diasHistorial"] = "180"
        cases.append(invalid_history_type)
        invalid_datetime = build_payload()
        invalid_datetime["data"][2]["ultimoInicioSesion"] = (
            "2026-08-31 10:30:00.1234567"
        )
        cases.append(invalid_datetime)
        inconsistent_record_total = build_payload()
        inconsistent_record_total["totalRegistros"] = 2
        cases.append(inconsistent_record_total)

        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_payload(
                    payload,
                    datetime(2026, 8, 31).date(),
                    11,
                    180,
                )

    def test_fetch_sends_explicit_date_and_cutoff(self):
        payload = build_payload()

        with patch.object(
            api_module,
            "urlopen",
            return_value=FakeResponse(payload),
        ) as urlopen:
            result = self.resource.fetch(datetime(2026, 8, 31).date(), 11, 180)

        request = urlopen.call_args.args[0]
        self.assertIn("fecha=2026-08-31", request.full_url)
        self.assertIn("corteHora=11", request.full_url)
        self.assertIn("diasHistorial=180", request.full_url)
        self.assertNotIn("soloSinIngreso", request.full_url)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))
        self.assertEqual(result, payload)


class ManagerAccessDailyAssetTests(unittest.TestCase):
    def test_asset_loads_snapshot_and_advances_control_watermark(self):
        payload = build_payload(
            corte_hora=asset_module.FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
        )
        api = MagicMock()
        api.fetch.return_value = payload
        sqlserver = MagicMock()
        sqlserver.get_watermark.return_value = datetime(2026, 8, 30, 11)
        sqlserver.write_fact_manager_access_daily_stage.return_value = 3
        sqlserver.merge_fact_manager_access_daily.return_value = 3
        context = SimpleNamespace(
            resources=SimpleNamespace(
                manager_access_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-1",
            retry_number=0,
            log=MagicMock(),
        )

        with patch.object(asset_module, "datetime", wraps=datetime) as current_datetime:
            current_datetime.now.return_value = datetime(2026, 8, 31, 11, 10)
            asset_module.ingest_fact_manager_access_daily_op.compute_fn.decorated_fn(
                context,
            )

        reference_date = datetime(2026, 8, 31).date()
        api.fetch.assert_called_once_with(
            reference_date,
            asset_module.FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
            asset_module.FACT_MANAGER_ACCESS_DAILY_HISTORY_DAYS,
        )
        sqlserver.mark_run_started.assert_called_once_with("FactManagerAccessDaily")
        sqlserver.write_fact_manager_access_daily_stage.assert_called_once_with(
            "run-1",
            reference_date,
            payload,
        )
        sqlserver.merge_fact_manager_access_daily.assert_called_once_with("run-1")
        sqlserver.clear_fact_manager_access_daily_stage.assert_has_calls(
            [call("run-1"), call("run-1")],
        )
        sqlserver.mark_run_completed.assert_called_once_with(
            ingestion_name="FactManagerAccessDaily",
            new_last_loaded_at=datetime(
                2026,
                8,
                31,
                asset_module.FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
            ),
            succeeded=True,
            error_message=None,
        )

    def test_final_failure_preserves_watermark_and_marks_control_failed(self):
        api = MagicMock()
        api.fetch.side_effect = ValueError("contract drift")
        sqlserver = MagicMock()
        last_loaded_at = datetime(2026, 8, 30, 11)
        sqlserver.get_watermark.return_value = last_loaded_at
        context = SimpleNamespace(
            resources=SimpleNamespace(
                manager_access_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-2",
            retry_number=3,
            log=MagicMock(),
        )

        with (
            patch.object(asset_module, "datetime", wraps=datetime) as current_datetime,
            self.assertRaisesRegex(ValueError, "contract drift"),
        ):
            current_datetime.now.return_value = datetime(2026, 8, 31, 11, 10)
            asset_module.ingest_fact_manager_access_daily_op.compute_fn.decorated_fn(
                context,
            )

        sqlserver.write_fact_manager_access_daily_stage.assert_not_called()
        sqlserver.merge_fact_manager_access_daily.assert_not_called()
        sqlserver.mark_run_completed.assert_called_once_with(
            ingestion_name="FactManagerAccessDaily",
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message="contract drift",
        )


class ManagerAccessBackfillTests(unittest.TestCase):
    def test_backfill_partition_uses_one_local_date(self):
        run_config = (
            backfill_module.fact_manager_access_backfill_daily_config
            .get_run_config_for_partition_key("2026-08-27")
        )

        self.assertEqual(
            run_config["ops"]["backfill_fact_manager_access_daily_op"]["config"],
            {"fecha": "2026-08-27"},
        )
        self.assertEqual(
            backfill_module.fact_manager_access_backfill_daily_config
            .partitions_def.timezone,
            "America/Ciudad_Juarez",
        )

    def test_backfill_upserts_without_changing_control_watermark(self):
        payload = build_payload(
            fecha="2026-08-27",
            corte_hora=asset_module.FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
        )
        payload["data"][0]["ultimoInicioSesion"] = "2026-08-27T10:30:00"
        api = MagicMock()
        api.fetch.return_value = payload
        sqlserver = MagicMock()
        sqlserver.write_fact_manager_access_daily_stage.return_value = 3
        sqlserver.merge_fact_manager_access_daily.return_value = 3
        context = SimpleNamespace(
            resources=SimpleNamespace(
                manager_access_api=api,
                sqlserver=sqlserver,
            ),
            op_config={"fecha": "2026-08-27"},
            run_id="run-3",
            log=MagicMock(),
        )

        result = (
            backfill_module.backfill_fact_manager_access_daily_op
            .compute_fn.decorated_fn(context)
        )

        reference_date = datetime(2026, 8, 27).date()
        api.fetch.assert_called_once_with(
            reference_date,
            asset_module.FACT_MANAGER_ACCESS_DAILY_CUTOFF_HOUR,
            asset_module.FACT_MANAGER_ACCESS_DAILY_HISTORY_DAYS,
        )
        sqlserver.write_fact_manager_access_daily_stage.assert_called_once_with(
            "run-3",
            reference_date,
            payload,
        )
        sqlserver.merge_fact_manager_access_daily.assert_called_once_with("run-3")
        sqlserver.get_watermark.assert_not_called()
        sqlserver.mark_run_started.assert_not_called()
        sqlserver.mark_run_completed.assert_not_called()
        self.assertEqual(result["fecha"], "2026-08-27")

    def test_job_contracts(self):
        self.assertEqual(
            job_module.manager_access_daily_job.name,
            "024_bafar_manager_access_daily",
        )
        self.assertEqual(
            backfill_module.fact_manager_access_backfill_daily_job.name,
            "025_bafar_manager_access_backfill_daily",
        )
        self.assertEqual(
            backfill_module.fact_manager_access_backfill_daily_job.tags["backfill"],
            "fact_manager_access_daily",
        )


if __name__ == "__main__":
    unittest.main()
