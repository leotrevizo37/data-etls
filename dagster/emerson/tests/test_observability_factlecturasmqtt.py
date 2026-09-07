import sys
import unittest
from datetime import datetime
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_SRC = PROJECT_ROOT / "src"
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
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
    import_module("dagster")
    asset_module = import_module(
        "emerson.defs.assets.observability_factLecturasMqtt"
    )
    backfill_module = import_module(
        "emerson.defs.jobs.factlecturasmqtt_backfill_job"
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


class FixedDateTime(datetime):
    @classmethod
    def now(cls, timezone=None):
        return cls(2026, 1, 3, 12, tzinfo=timezone)


class ApiStub:
    def __init__(self, error=None):
        self.error = error
        self.requests_made = 0
        self.calls = []

    def fetch_range(self, from_local, to_local):
        self.requests_made += 1
        self.calls.append((from_local, to_local))
        if self.error is not None:
            raise self.error
        return [{"mediciones": {"temperature": 4.0}}]


class SqlServerStub:
    def __init__(self, watermark=None):
        self.watermark = watermark
        self.started = []
        self.completed = []
        self.cleared = []
        self.writes = []

    def get_watermark(self, _ingestion_name):
        return self.watermark

    def mark_run_started(self, ingestion_name):
        self.started.append(ingestion_name)

    def mark_run_completed(self, **kwargs):
        self.completed.append(kwargs)

    def clear_fact_lecturas_mqtt_stage(self, run_id):
        self.cleared.append(run_id)
        return 0

    def write_fact_lecturas_mqtt_stage(self, run_id, rows):
        self.writes.append((run_id, rows))
        return len(rows)

    def merge_fact_lecturas_mqtt(self, _run_id):
        return len(self.writes[-1][1])


class ObservabilityFactLecturasMqttTests(unittest.TestCase):
    def test_incremental_asset_uses_hourly_windows_and_advances_after_success(self):
        api = ApiStub()
        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            run_id="run-1",
            resources=SimpleNamespace(
                lecturas_tenant_api=api,
                sqlserver=sqlserver,
            ),
        )

        with patch.object(asset_module, "datetime", FixedDateTime):
            asset_module.observability_factlecturasmqtt.op.compute_fn.decorated_fn(
                context
            )

        self.assertEqual(len(api.calls), 60)
        self.assertEqual(api.calls[0][0], datetime(2026, 1, 1))
        self.assertEqual(api.calls[-1][1], datetime(2026, 1, 3, 12))
        self.assertTrue(
            all(
                end - start == asset_module.FACTLECTURAS_MQTT_REQUEST_WINDOW
                for start, end in api.calls
            )
        )
        self.assertEqual(sqlserver.started, ["FactLecturasMqtt"])
        self.assertTrue(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            datetime(2026, 1, 3, 12),
        )

    def test_incremental_asset_preserves_watermark_after_failure(self):
        watermark = datetime(2026, 1, 2)
        api = ApiStub(RuntimeError("api failed"))
        sqlserver = SqlServerStub(watermark)
        context = SimpleNamespace(
            run_id="run-2",
            resources=SimpleNamespace(
                lecturas_tenant_api=api,
                sqlserver=sqlserver,
            ),
        )

        with (
            patch.object(asset_module, "datetime", FixedDateTime),
            self.assertRaisesRegex(RuntimeError, "api failed"),
        ):
            asset_module.observability_factlecturasmqtt.op.compute_fn.decorated_fn(
                context
            )

        self.assertFalse(sqlserver.completed[-1]["succeeded"])
        self.assertEqual(
            sqlserver.completed[-1]["new_last_loaded_at"],
            watermark,
        )

    def test_daily_backfill_uses_one_local_day_without_watermark(self):
        api = ApiStub()
        sqlserver = SqlServerStub()
        context = SimpleNamespace(
            run_id="run-3",
            op_config={
                "from_local": "2026-05-01T00:00:00",
                "to_local": "2026-05-02T00:00:00",
            },
            resources=SimpleNamespace(
                lecturas_tenant_api=api,
                sqlserver=sqlserver,
            ),
        )

        backfill_module.backfill_factlecturasmqtt_daily_op.compute_fn.decorated_fn(
            context
        )

        self.assertEqual(len(api.calls), 24)
        self.assertEqual(api.calls[0][0], datetime(2026, 5, 1))
        self.assertEqual(api.calls[-1][1], datetime(2026, 5, 2))
        self.assertTrue(
            all(
                end - start == asset_module.FACTLECTURAS_MQTT_REQUEST_WINDOW
                for start, end in api.calls
            )
        )
        self.assertEqual(sqlserver.started, [])
        self.assertEqual(sqlserver.completed, [])

    def test_stage_write_preserves_column_types_and_json_values(self):
        resource = asset_module.SqlServerResource(
            host="localhost",
            port=1433,
            database="test",
            user="test",
            password="",
            connect_timeout=30,
            command_timeout=60,
            driver="ODBC Driver 18 for SQL Server",
            retries=1,
            retry_interval=1,
        )
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value = cursor
        resource._get_connection = MagicMock(return_value=connection)
        rows = [
            {
                "mqtt_id": "11111111-1111-1111-1111-111111111111",
                "fecha_local": datetime(2026, 8, 30, 0, 0, 0, 570080),
                "time_span_utc": datetime(2026, 8, 30, 6, 0, 0, 123000),
                "deveui": "device-1",
                "tenant": "emerson",
                "dispositivo": "Dispositivo ñ",
                "aplicacion": "Aplicación",
                "sucursal": "Sucursal",
                "topic": "tenant/emerson/device",
                "variable": "temperature",
                "valor": {"number": 4.25, "boolean": True},
                "valor_texto": "4.25",
                "mediciones": {"temperature": 4.25, "state": "active"},
                "fecha_recepcion": "2026-08-30T06:00:00.123Z",
                "mensaje_raw": '{"time":"2026-08-30T06:00:00.123Z"}',
            },
            {
                "mqtt_id": "22222222-2222-2222-2222-222222222222",
                "fecha_local": datetime(2026, 8, 30, 0, 0, 1),
                "time_span_utc": datetime(2026, 8, 30, 6, 0, 1),
                "deveui": "device-2",
                "tenant": "emerson",
                "dispositivo": None,
                "aplicacion": None,
                "sucursal": None,
                "topic": None,
                "variable": None,
                "valor": None,
                "valor_texto": None,
                "mediciones": None,
                "fecha_recepcion": None,
                "mensaje_raw": '{"time":"2026-08-30T06:00:01Z"}',
            },
        ]

        written = resource.write_fact_lecturas_mqtt_stage("run-4", rows)

        staged = cursor.executemany.call_args.args[1]
        self.assertEqual(written, 2)
        self.assertTrue(all(len(row) == 16 for row in staged))
        self.assertIsInstance(staged[0][1], str)
        self.assertIsInstance(staged[0][2], datetime)
        self.assertIsInstance(staged[0][3], datetime)
        self.assertEqual(staged[0][6], "Dispositivo ñ")
        self.assertEqual(staged[0][7], "Aplicación")
        self.assertEqual(staged[0][11], '{"number":4.25,"boolean":true}')
        self.assertEqual(
            staged[0][13],
            '{"temperature":4.25,"state":"active"}',
        )
        self.assertIsNone(staged[1][11])
        self.assertEqual(staged[1][13], "null")
        self.assertIs(cursor.fast_executemany, True)
        connection.commit.assert_called_once_with()

    def test_incremental_job_and_daily_schedule_contract(self):
        job_source = (
            PROJECT_SRC
            / "emerson"
            / "defs"
            / "jobs"
            / "factlecturasmqtt_job.py"
        ).read_text()
        schedule_source = (
            PROJECT_SRC
            / "emerson"
            / "defs"
            / "schedules"
            / "factlecturasmqtt_scheduler.py"
        ).read_text()
        dagster_config = (
            REPOSITORY_ROOT / "dagster" / "dagster_home" / "dagster.yaml"
        ).read_text()

        self.assertIn(
            'name="023_observability_factlecturasmqtt"',
            job_source,
        )
        self.assertIn(
            'AssetKey(["emerson", "observability_factlecturasmqtt"])',
            job_source,
        )
        self.assertIn(
            '"api_rate_limit": "emerson_lecturas_tenant"',
            job_source,
        )
        self.assertIn(
            'name="023_observability_factlecturasmqtt_every_1d"',
            schedule_source,
        )
        self.assertIn(
            'job_name="023_observability_factlecturasmqtt"',
            schedule_source,
        )
        self.assertIn('cron_schedule="30 23 * * *"', schedule_source)
        self.assertIn(
            'execution_timezone="America/Ciudad_Juarez"',
            schedule_source,
        )
        self.assertIn(
            "default_status=DefaultScheduleStatus.RUNNING",
            schedule_source,
        )
        self.assertIn('key: "api_rate_limit"', dagster_config)
        self.assertIn('value: "emerson_lecturas_tenant"', dagster_config)
        self.assertIn("        limit: 1", dagster_config)

    def test_sql_and_emerson_configuration_contract(self):
        sql = (
            REPOSITORY_ROOT
            / "sql"
            / "user_codes"
            / "emerson"
            / "init"
            / "018_observability_factLecturasMqtt.sql"
        ).read_text()
        compose = (
            REPOSITORY_ROOT / "compose-data_etls" / "docker-compose.yml"
        ).read_text()
        example = (
            REPOSITORY_ROOT / "compose-data_etls" / ".env.example"
        ).read_text()
        sql_resource = (
            PROJECT_SRC
            / "emerson"
            / "defs"
            / "resources"
            / "sql_server.py"
        ).read_text()

        self.assertIn("observability.factLecturasMqtt", sql)
        self.assertIn("stg.factLecturasMqtt", sql)
        self.assertIn("PRIMARY KEY CLUSTERED (MqttId)", sql)
        self.assertIn("PRIMARY KEY CLUSTERED (RunId, MqttId)", sql)
        self.assertIn("TimeSpan DATETIME2(6) NOT NULL", sql)
        self.assertIn("FechaLocal DATETIME2(6) NOT NULL", sql)
        self.assertEqual(sql.count("Dispositivo NVARCHAR(255) NULL"), 2)
        self.assertEqual(sql.count("Aplicacion NVARCHAR(255) NULL"), 2)
        self.assertIn(
            "(FechaLocal, Aplicacion, Dispositivo)",
            sql,
        )
        self.assertIn(
            "(Aplicacion, FechaLocal, Dispositivo)",
            sql,
        )
        self.assertIn(
            "(Dispositivo, FechaLocal, Aplicacion)",
            sql,
        )
        self.assertIn("(TimeSpan, DevEui)", sql)
        self.assertIn("(DevEui, TimeSpan)", sql)
        self.assertIn("FactLecturasMqtt", sql)
        self.assertIn(
            "MERGE observability.factLecturasMqtt WITH (HOLDLOCK)",
            sql_resource,
        )
        self.assertIn("FROM stg.factLecturasMqtt", sql_resource)
        self.assertIn("ON target.MqttId = source.MqttId", sql_resource)
        self.assertIn("batch_size: int = 500", sql_resource)
        self.assertIn("EMERSON_LECTURAS_TENANT_API_URL:", compose)
        self.assertIn("EMERSON_LECTURAS_TENANT_API_URL=", example)
        self.assertEqual(
            asset_module.FACTLECTURAS_MQTT_START_DATE,
            datetime(2026, 1, 1),
        )
        self.assertEqual(
            backfill_module.factlecturasmqtt_backfill_daily_job.name,
            "023_observability_factlecturasmqtt_backfill_daily",
        )
        self.assertEqual(
            backfill_module.factlecturasmqtt_backfill_daily_job.tags[
                "user_code_run_lane"
            ],
            "emerson_observability",
        )
        self.assertEqual(
            backfill_module.factlecturasmqtt_backfill_daily_job.tags[
                "api_rate_limit"
            ],
            "emerson_lecturas_tenant",
        )


if __name__ == "__main__":
    unittest.main()
