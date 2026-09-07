import io
import json
import sys
import unittest
from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse


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
    import_module("dagster")
    api_module = import_module("carlsjr.defs.resources.lecturas_tenant_api")
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path
LecturasTenantApiResource = api_module.LecturasTenantApiResource
LecturasTenantWindowTooLarge = api_module.LecturasTenantWindowTooLarge


class JsonHeaders(dict):
    def get_content_type(self):
        return "application/json"


class JsonResponse(io.BytesIO):
    status = 200
    headers = JsonHeaders()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class LecturasTenantApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.from_local = datetime(2026, 8, 30)
        self.to_local = datetime(2026, 8, 30, 0, 1)
        self.resource = LecturasTenantApiResource(
            url="http://api.test/api/lecturas/tenant",
            user="readonly",
            password="secret",
            timeout_seconds=30,
            limit=2,
            requests_per_minute=95,
        )

    def payload(self):
        return {
            "ok": True,
            "tenant": "carlsjr",
            "fecha_inicio": "2026-08-30T00:00:00Z",
            "fecha_fin": "2026-08-30T00:01:00Z",
            "total": 9,
            "data": [
                {
                    "id": "11111111-1111-1111-1111-111111111111",
                    "fecha": "2026-08-30T00:00:00.100000",
                    "deveui": "device-1",
                    "tenant": "carlsjr",
                    "dispositivo": "device",
                    "aplicacion": "application",
                    "sucursal": None,
                    "topic": None,
                    "variable": None,
                    "valor": None,
                    "valor_texto": None,
                    "mediciones": {"temperature": 4.0},
                    "fecha_recepcion": None,
                    "mensaje_raw": json.dumps(
                        {
                            "time": "2026-08-30T06:00:00.500000Z",
                            "rxInfo": [
                                {
                                    "gwTime": "2026-08-30T06:00:00.500500Z",
                                    "nsTime": "2026-08-30T06:00:00.560000Z",
                                }
                            ],
                        }
                    ),
                }
            ],
        }

    def test_uses_raw_message_time_as_utc_time_span(self) -> None:
        payload = self.payload()
        payload["data"][0]["mensaje_raw"] = json.dumps(
            {"time": "2026-08-30T06:00:00.500000789Z"}
        )
        rows = self.resource.validate_payload(
            payload,
            self.from_local,
            self.to_local,
        )

        self.assertEqual(
            rows[0]["time_span_utc"],
            datetime(2026, 8, 30, 6, 0, 0, 500001),
        )
        self.assertEqual(
            rows[0]["fecha_local"],
            datetime(2026, 8, 30, 0, 0, 0, 100000),
        )
        self.assertEqual(payload["total"], 9)
        self.assertEqual(len(rows), 1)

    def test_accepts_reported_five_digit_local_fraction(self) -> None:
        payload = self.payload()
        payload["fecha_inicio"] = "2026-08-11T00:00:00Z"
        payload["fecha_fin"] = "2026-08-11T01:00:00Z"
        payload["data"][0]["fecha"] = "2026-08-11T00:54:31.57008"
        payload["data"][0]["mensaje_raw"] = json.dumps(
            {"time": "2026-08-11T06:54:31.570+00:00"}
        )

        rows = self.resource.validate_payload(
            payload,
            datetime(2026, 8, 11),
            datetime(2026, 8, 11, 1),
        )

        self.assertEqual(
            rows[0]["fecha_local"],
            datetime(2026, 8, 11, 0, 54, 31, 570080),
        )

    def test_accepts_iso_fractional_precision_and_utc_forms(self) -> None:
        cases = (
            ("2026-08-30T00:00:00", datetime(2026, 8, 30)),
            ("2026-08-30T00:00:00.1", datetime(2026, 8, 30, 0, 0, 0, 100000)),
            ("2026-08-30T00:00:00.123", datetime(2026, 8, 30, 0, 0, 0, 123000)),
            ("2026-08-30T00:00:00.12345", datetime(2026, 8, 30, 0, 0, 0, 123450)),
            ("2026-08-30T00:00:00.123456", datetime(2026, 8, 30, 0, 0, 0, 123456)),
            ("2026-08-30T00:00:00.1234567", datetime(2026, 8, 30, 0, 0, 0, 123457)),
            ("2026-08-30T00:00:00.12345678", datetime(2026, 8, 30, 0, 0, 0, 123457)),
            ("2026-08-30T00:00:00.123456789", datetime(2026, 8, 30, 0, 0, 0, 123457)),
            ("2026-08-30T00:00:00.999999999", datetime(2026, 8, 30, 0, 0, 1)),
        )
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(api_module._parse_api_datetime(value), expected)
        self.assertEqual(
            api_module._parse_api_datetime("2026-08-30T06:00:00.123Z"),
            api_module._parse_api_datetime(
                "2026-08-30T06:00:00.123+00:00"
            ),
        )

    def test_preserves_supported_values_for_every_row_field(self) -> None:
        payload = self.payload()
        row = payload["data"][0]
        row["fecha"] = "2026-08-30T00:00:00.57008"
        row["dispositivo"] = "Dispositivo ñ"
        row["aplicacion"] = "Aplicación"
        row["sucursal"] = "Sucursal"
        row["topic"] = "tenant/carlsjr/device"
        row["variable"] = "temperature"
        row["valor"] = {
            "number": 4.25,
            "integer": 4,
            "boolean": True,
            "text": "activo",
            "null": None,
            "array": [1, "dos"],
        }
        row["valor_texto"] = "4.25"
        row["mediciones"] = {
            "number": 4.25,
            "integer": 4,
            "boolean": False,
            "text": "cerrada",
            "null": None,
            "object": {"nested": 1},
            "array": [1, 2],
        }
        row["fecha_recepcion"] = "2026-08-30T06:00:00.500Z"

        rows = self.resource.validate_payload(
            payload,
            self.from_local,
            self.to_local,
        )

        for field in api_module.ROW_FIELDS:
            self.assertEqual(rows[0][field], row[field])
        self.assertEqual(
            rows[0]["fecha_local"],
            datetime(2026, 8, 30, 0, 0, 0, 570080),
        )
        self.assertIsInstance(rows[0]["time_span_utc"], datetime)

    def test_accepts_registered_device_without_measurements(self) -> None:
        payload = self.payload()
        nullable_fields = (
            "dispositivo",
            "aplicacion",
            "sucursal",
            "topic",
            "variable",
            "valor",
            "valor_texto",
            "mediciones",
            "fecha_recepcion",
        )
        for field in nullable_fields:
            payload["data"][0][field] = None

        rows = self.resource.validate_payload(
            payload,
            self.from_local,
            self.to_local,
        )

        self.assertTrue(all(rows[0][field] is None for field in nullable_fields))

    def test_rejects_invalid_response_field_types_and_formats(self) -> None:
        cases = (
            ("ok", 1),
            ("tenant", 1),
            ("tenant", "other"),
            ("fecha_inicio", "2026-08-30T00:00:00"),
            ("fecha_fin", "2026-08-30T01:01:00+01:00"),
            ("total", True),
            ("total", -1),
            ("data", {}),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value):
                payload = self.payload()
                payload[field] = value
                with self.assertRaises(ValueError):
                    self.resource.validate_payload(
                        payload,
                        self.from_local,
                        self.to_local,
                    )

        payload = self.payload()
        payload["unexpected"] = None
        with self.assertRaisesRegex(ValueError, "contract"):
            self.resource.validate_payload(
                payload,
                self.from_local,
                self.to_local,
            )

    def test_rejects_invalid_row_field_types_and_formats(self) -> None:
        cases = (
            ("id", 11111111111111111111111111111111),
            ("fecha", 1),
            ("fecha", "2026-08-30 00:00:00"),
            ("fecha", "2026-08-30T00:00:00.1234567890"),
            ("fecha", "2026-08-30T00:00:00+00:00"),
            ("deveui", None),
            ("deveui", ""),
            ("deveui", "d" * 65),
            ("tenant", 1),
            ("tenant", "other"),
            ("dispositivo", 1),
            ("dispositivo", "d" * 256),
            ("aplicacion", 1),
            ("aplicacion", "a" * 256),
            ("sucursal", 1),
            ("topic", 1),
            ("variable", 1),
            ("valor", object()),
            ("valor", float("nan")),
            ("valor_texto", 1),
            ("mediciones", []),
            ("mediciones", {"temperature": float("nan")}),
            ("fecha_recepcion", 1),
            ("fecha_recepcion", "f" * 101),
            ("mensaje_raw", 1),
        )
        for field, value in cases:
            with self.subTest(field=field, value=repr(value)):
                payload = self.payload()
                payload["data"][0][field] = value
                with self.assertRaises(ValueError):
                    self.resource.validate_payload(
                        payload,
                        self.from_local,
                        self.to_local,
                    )

        payload = self.payload()
        payload["data"][0].pop("topic")
        with self.assertRaisesRegex(ValueError, "contract"):
            self.resource.validate_payload(
                payload,
                self.from_local,
                self.to_local,
            )

    def test_rejects_invalid_raw_message_time_types_and_formats(self) -> None:
        invalid_raw_messages = (
            "not-json",
            json.dumps([]),
            json.dumps({}),
            json.dumps({"time": 1}),
            json.dumps({"time": "2026-08-30T06:00:00.123"}),
            json.dumps({"time": "2026-08-30T06:00:00.123+01:00"}),
            json.dumps({"time": "2026-08-30T06:00:00.1234567890Z"}),
            json.dumps(
                {
                    "time": "2026-08-30T06:00:00.123Z",
                    "value": float("nan"),
                }
            ),
        )
        for raw_message in invalid_raw_messages:
            with self.subTest(raw_message=raw_message):
                payload = self.payload()
                payload["data"][0]["mensaje_raw"] = raw_message
                with self.assertRaises(ValueError):
                    self.resource.validate_payload(
                        payload,
                        self.from_local,
                        self.to_local,
                    )

    def test_request_uses_half_open_local_window_and_limit(self) -> None:
        response = JsonResponse(json.dumps(self.payload()).encode("utf-8"))
        with patch.object(api_module, "urlopen", return_value=response) as mocked:
            rows = self.resource._request_window(
                self.from_local,
                self.to_local,
            )

        request = mocked.call_args.args[0]
        query = parse_qs(urlparse(request.full_url).query)
        self.assertEqual(query["tenant"], ["carlsjr"])
        self.assertEqual(query["fechaInicio"], ["2026-08-30T00:00:00.000000Z"])
        self.assertEqual(query["fechaFin"], ["2026-08-30T00:01:00.000000Z"])
        self.assertEqual(query["limit"], ["2"])
        self.assertTrue(request.headers["Authorization"].startswith("Basic "))
        self.assertEqual(len(rows), 1)

    def test_saturated_response_is_split_without_gaps(self) -> None:
        saturated = [{"mqtt_id": "one"}, {"mqtt_id": "two"}]
        left = [{"mqtt_id": "left"}]
        right = [{"mqtt_id": "right"}]
        with patch.object(
            self.resource,
            "_request_window",
            side_effect=[saturated, left, right],
        ) as mocked:
            rows = self.resource.fetch_range(self.from_local, self.to_local)

        midpoint = self.from_local + (self.to_local - self.from_local) / 2
        self.assertEqual(mocked.call_args_list[1].args, (self.from_local, midpoint))
        self.assertEqual(mocked.call_args_list[2].args, (midpoint, self.to_local))
        self.assertEqual(rows, left + right)

    def test_504_is_split_and_minimum_saturation_fails_closed(self) -> None:
        with patch.object(
            self.resource,
            "_request_window",
            side_effect=[
                LecturasTenantWindowTooLarge(),
                [{"mqtt_id": "left"}],
                [{"mqtt_id": "right"}],
            ],
        ):
            rows = self.resource.fetch_range(self.from_local, self.to_local)
        self.assertEqual(len(rows), 2)

        with patch.object(
            self.resource,
            "_request_window",
            return_value=[{"mqtt_id": "one"}, {"mqtt_id": "two"}],
        ):
            with self.assertRaisesRegex(RuntimeError, "minimum precision"):
                self.resource.fetch_range(
                    self.from_local,
                    self.from_local + timedelta(microseconds=1),
                )

    def test_429_is_retried(self) -> None:
        response = JsonResponse(json.dumps(self.payload()).encode("utf-8"))
        rate_limit_error = HTTPError(
            self.resource.url,
            429,
            "Too Many Requests",
            {"Retry-After": "0"},
            None,
        )
        with (
            patch.object(self.resource, "_wait_for_request_slot"),
            patch.object(api_module, "sleep"),
            patch.object(
                api_module,
                "urlopen",
                side_effect=[rate_limit_error, response],
            ) as mocked,
        ):
            rows = self.resource._request_window(
                self.from_local,
                self.to_local,
            )

        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
