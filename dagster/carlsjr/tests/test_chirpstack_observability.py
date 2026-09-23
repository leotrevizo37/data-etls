import importlib.util
import io
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
PROJECT_SRC = PROJECT_ROOT / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

chirpstack_metrics = importlib.import_module(
    "carlsjr.defs.transformations.chirpstack_metrics"
)
build_device_metric_row = chirpstack_metrics.build_device_metric_row
build_device_event_metric_rows = chirpstack_metrics.build_device_event_metric_rows
build_gateway_metric_row = chirpstack_metrics.build_gateway_metric_row
normalize_dev_eui = chirpstack_metrics.normalize_dev_eui


def _chart(label, value, timestamp="2026-09-17T10:00:00Z"):
    datasets = [] if label is None else [{"label": label, "data": [value]}]
    return {
        "timestamps": [timestamp],
        "datasets": datasets,
        "kind": "ABSOLUTE",
    }


def _device_payload():
    return {
        "device": {
            "devEui": "24E124136E090452",
            "applicationId": "32bf78f8-d347-4dc7-a7ea-f4336bd98290",
            "deviceProfileId": "9e74cac7-6ab3-4f38-ba35-ff923657e589",
        },
        "deviceStatus": {
            "batteryLevel": 42,
            "externalPowerSource": False,
        },
        "unknownFutureField": {"ignored": True},
    }


def _application_payload():
    return {
        "application": {
            "id": "32bf78f8-d347-4dc7-a7ea-f4336bd98290",
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
        }
    }


def _profile_payload():
    return {
        "deviceProfile": {
            "id": "9e74cac7-6ab3-4f38-ba35-ff923657e589",
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
            "uplinkInterval": 600,
            "measurements": {"battery": {"kind": "GAUGE"}},
        }
    }


class ChirpStackMetricTransformationTests(unittest.TestCase):
    def test_device_hour_preserves_zero_errors_and_metric_battery(self) -> None:
        time_span = datetime(2026, 9, 17, 10, tzinfo=timezone.utc)
        row = build_device_metric_row(
            dev_eui="24e124136e090452",
            device_payload=_device_payload(),
            application_payload=_application_payload(),
            device_profile_payload=_profile_payload(),
            link_metrics_payload={
                "rxPackets": _chart("rx_count", 6),
                "gwRssi": _chart("rssi", -81.125),
                "gwSnr": _chart("snr", 7.25),
                "errors": _chart(None, None),
            },
            device_metrics_payload={
                "metrics": {"battery": _chart("battery", 89.5)},
                "states": {},
            },
            time_span=time_span,
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(row["DevEui"], "24e124136e090452")
        self.assertEqual(row["ExpectedUplinkCount"], Decimal("6.000000"))
        self.assertEqual(row["ReceivedUplinkCount"], 6)
        self.assertEqual(row["AverageRssi"], Decimal("-81.125000"))
        self.assertEqual(row["AverageSnr"], Decimal("7.250000"))
        self.assertEqual(row["LinkErrorCount"], 0)
        self.assertEqual(row["LinkErrorCountsJson"], "{}")
        self.assertEqual(row["BatteryLevel"], Decimal("89.5000"))
        self.assertEqual(row["BatteryLevelSource"], "DEVICE_METRICS")
        self.assertEqual(row["BatteryObservedAt"], time_span.replace(tzinfo=None))
        self.assertTrue(row["MetricsAvailable"])

    def test_missing_hour_remains_null_instead_of_becoming_zero(self) -> None:
        row = build_device_metric_row(
            dev_eui="24e124136e090452",
            device_payload=_device_payload(),
            application_payload=_application_payload(),
            device_profile_payload=_profile_payload(),
            link_metrics_payload={
                "rxPackets": _chart("rx_count", 3, "2026-09-17T09:00:00Z"),
                "gwRssi": _chart("rssi", -80, "2026-09-17T09:00:00Z"),
                "gwSnr": _chart("snr", 8, "2026-09-17T09:00:00Z"),
                "errors": _chart(None, None, "2026-09-17T09:00:00Z"),
            },
            device_metrics_payload={"metrics": {}, "states": {}},
            time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertIsNone(row["ReceivedUplinkCount"])
        self.assertIsNone(row["AverageRssi"])
        self.assertIsNone(row["AverageSnr"])
        self.assertIsNone(row["LinkErrorCount"])
        self.assertEqual(row["BatteryLevel"], Decimal("42.0000"))
        self.assertEqual(row["BatteryLevelSource"], "DEVICE_STATUS")
        self.assertFalse(row["MetricsAvailable"])

    def test_rejects_misaligned_metric_series(self) -> None:
        malformed_chart = {
            "timestamps": ["2026-09-17T10:00:00Z"],
            "datasets": [{"label": "rx_count", "data": []}],
        }
        with self.assertRaisesRegex(ValueError, "align with timestamps"):
            build_device_metric_row(
                dev_eui="24e124136e090452",
                device_payload=_device_payload(),
                application_payload=_application_payload(),
                device_profile_payload=_profile_payload(),
                link_metrics_payload={
                    "rxPackets": malformed_chart,
                    "gwRssi": _chart("rssi", -80),
                    "gwSnr": _chart("snr", 8),
                    "errors": _chart(None, None),
                },
                device_metrics_payload=None,
                time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
            )

    def test_rejects_values_outside_sql_contract(self) -> None:
        for field, value, message in (
            ("rxPackets", 9223372036854775808, "SQL BIGINT contract"),
            ("gwRssi", 1000000000000, "decimal contract"),
        ):
            link_metrics = {
                "rxPackets": _chart("rx_count", 1),
                "gwRssi": _chart("rssi", -80),
                "gwSnr": _chart("snr", 8),
                "errors": _chart(None, None),
            }
            label = "rx_count" if field == "rxPackets" else "rssi"
            link_metrics[field] = _chart(label, value)
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, message):
                build_device_metric_row(
                    dev_eui="24e124136e090452",
                    device_payload=_device_payload(),
                    application_payload=_application_payload(),
                    device_profile_payload=_profile_payload(),
                    link_metrics_payload=link_metrics,
                    device_metrics_payload=None,
                    time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                    extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
                )

    def test_gateway_row_uses_official_state_and_packet_statuses(self) -> None:
        row = build_gateway_metric_row(
            gateway_list_item={
                "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                "gatewayId": "C0BA1FFFFE01F6D7",
                "state": "OFFLINE",
                "lastSeenAt": "2026-09-17T10:59:30Z",
            },
            gateway_payload={
                "gateway": {
                    "gatewayId": "c0ba1ffffe01f6d7",
                    "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
                    "name": "Carls-jr-juventud",
                    "statsInterval": 30,
                }
            },
            gateway_metrics_payload={
                "rxPackets": _chart("rx_count", 20),
                "txPackets": _chart("tx_count", 2),
                "txPacketsPerStatus": {
                    "timestamps": ["2026-09-17T10:00:00Z"],
                    "datasets": [
                        {"label": "OK", "data": [2]},
                        {"label": "TOO_LATE", "data": [1]},
                    ],
                },
            },
            expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
            time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
            extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(row["GatewayState"], "OFFLINE")
        self.assertEqual(row["ReceivedPacketCount"], 20)
        self.assertEqual(row["TransmittedPacketCount"], 2)
        self.assertEqual(row["TxOkCount"], 2)
        self.assertEqual(row["TxErrorCount"], 1)
        self.assertEqual(
            row["TxPacketStatusCountsJson"],
            '{"OK":2,"TOO_LATE":1}',
        )
        self.assertTrue(row["MetricsAvailable"])

    def test_event_rows_aggregate_uplinks_logs_and_gateway_receptions(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        gateway_one = "c0ba1ffffe01f6d7"
        gateway_two = "c0ba1ffffe01f6d8"
        device_info = {"devEui": dev_eui, "tenantId": tenant_id}
        event_rows = [
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 5, 0, 123456),
                "RawMessage": {
                    "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "time": "2026-09-17T10:05:00.123456Z",
                    "deviceInfo": device_info,
                    "rxInfo": [
                        {"gatewayId": gateway_one, "rssi": -80, "snr": 8},
                        {"gatewayId": gateway_two, "rssi": -90, "snr": 3},
                    ],
                },
            },
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 10),
                "RawMessage": {
                    "deduplicationId": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "time": "2026-09-17T10:10:00Z",
                    "deviceInfo": device_info,
                    "rxInfo": [
                        {"gatewayId": gateway_one, "rssi": -70, "snr": 10},
                    ],
                },
            },
            {
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 15),
                "RawMessage": {
                    "time": "2026-09-17T10:15:00Z",
                    "deviceInfo": device_info,
                    "level": "ERROR",
                    "code": "UPLINK_F_CNT_RETRANSMISSION",
                    "description": "retransmission",
                },
            },
        ]

        device_rows, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=event_rows,
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        self.assertEqual(device_rows[0]["CapturedUplinkCount"], 2)
        self.assertEqual(rejected_rows, [])
        self.assertEqual(device_rows[0]["ObservedGatewayCount"], 2)
        self.assertEqual(device_rows[0]["MultiGatewayUplinkCount"], 1)
        self.assertEqual(
            device_rows[0]["EventLogCountsJson"],
            '{"UPLINK_F_CNT_RETRANSMISSION":1}',
        )
        self.assertEqual(len(gateway_rows), 2)
        gateway_one_row = next(
            row for row in gateway_rows if row["GatewayId"] == gateway_one
        )
        self.assertEqual(gateway_one_row["GatewayReceptionCount"], 2)
        self.assertEqual(gateway_one_row["ReceptionShare"], Decimal("1.000000"))
        self.assertEqual(
            gateway_one_row["FirstReceivedAt"],
            datetime(2026, 9, 17, 10, 5, 0, 123456),
        )

    def test_event_reception_accepts_optional_rssi_snr_and_channel(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        gateways = [
            "c0ba1ffffe01f6d7",
            "c0ba1ffffe01f6d8",
            "c0ba1ffffe01f6d9",
        ]
        _, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=[
                {
                    "DevEui": dev_eui,
                    "EventTime": datetime(2026, 9, 17, 10, 5),
                    "RawMessage": {
                        "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                        "time": "2026-09-17T10:05:00Z",
                        "deviceInfo": {
                            "devEui": dev_eui,
                            "tenantId": tenant_id,
                        },
                        "rxInfo": [
                            {"gatewayId": gateways[0], "rssi": -86},
                            {"gatewayId": gateways[1], "snr": 4.5},
                            {"gatewayId": gateways[2]},
                        ],
                    },
                }
            ],
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        rows_by_gateway = {row["GatewayId"]: row for row in gateway_rows}
        self.assertEqual(len(rows_by_gateway), 3)
        self.assertEqual(rejected_rows, [])
        self.assertEqual(
            rows_by_gateway[gateways[0]]["AverageRssi"],
            Decimal("-86.000000"),
        )
        self.assertIsNone(rows_by_gateway[gateways[0]]["AverageSnr"])
        self.assertIsNone(rows_by_gateway[gateways[1]]["AverageRssi"])
        self.assertEqual(
            rows_by_gateway[gateways[1]]["AverageSnr"],
            Decimal("4.500000"),
        )
        self.assertIsNone(rows_by_gateway[gateways[2]]["AverageRssi"])
        self.assertIsNone(rows_by_gateway[gateways[2]]["AverageSnr"])
        self.assertTrue(all(row["GatewayReceptionCount"] == 1 for row in gateway_rows))

    def test_invalid_event_is_rejected_without_discarding_valid_event(self) -> None:
        tenant_id = "087175fa-5bf9-4180-a02e-7c57fb3543e8"
        dev_eui = "24e124136e090452"
        query_metadata = {
            "Tenant": "carls",
            "FechaInicio": "2026-09-17T04:00:00.000000Z",
            "FechaFin": "2026-09-17T05:00:00.000000Z",
            "RequestedLimit": 5000,
            "FetchedAt": datetime(2026, 9, 17, 11, 0),
        }
        event_rows = [
            {
                **query_metadata,
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 5),
                "SourceRowJson": '{"id":"valid"}',
                "RawMessage": {
                    "deduplicationId": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "time": "2026-09-17T10:05:00Z",
                    "deviceInfo": {
                        "devEui": dev_eui,
                        "tenantId": tenant_id,
                    },
                    "rxInfo": [{"gatewayId": "c0ba1ffffe01f6d7"}],
                },
            },
            {
                **query_metadata,
                "DevEui": dev_eui,
                "EventTime": datetime(2026, 9, 17, 10, 10),
                "SourceRowJson": '{"id":"invalid"}',
                "RawMessage": {
                    "deduplicationId": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "time": "2026-09-17T10:10:00Z",
                    "deviceInfo": {
                        "devEui": dev_eui,
                        "tenantId": tenant_id,
                    },
                    "rxInfo": None,
                },
            },
        ]

        device_rows, gateway_rows, rejected_rows = build_device_event_metric_rows(
            event_rows=event_rows,
            devices={dev_eui: tenant_id},
            from_utc=datetime(2026, 9, 17, 10),
            to_utc=datetime(2026, 9, 17, 11),
            extracted_at=datetime(2026, 9, 17, 11, 2),
        )

        self.assertEqual(device_rows[0]["CapturedUplinkCount"], 1)
        self.assertEqual(len(gateway_rows), 1)
        self.assertEqual(len(rejected_rows), 1)
        self.assertEqual(rejected_rows[0]["RowJson"], '{"id":"invalid"}')
        self.assertEqual(
            rejected_rows[0]["FechaInicio"],
            query_metadata["FechaInicio"],
        )
        self.assertIn("rxInfo must be an array", rejected_rows[0]["FailureReason"])

    def test_rejects_unknown_gateway_state_and_invalid_eui(self) -> None:
        with self.assertRaisesRegex(ValueError, "16-character hexadecimal"):
            normalize_dev_eui("not-an-eui")
        gateway_list_item = {
            "tenantId": "087175fa-5bf9-4180-a02e-7c57fb3543e8",
            "gatewayId": "c0ba1ffffe01f6d7",
            "state": "DEGRADED",
            "lastSeenAt": "2026-09-17T10:59:30Z",
        }
        with self.assertRaisesRegex(ValueError, "unknown gateway state"):
            build_gateway_metric_row(
                gateway_list_item=gateway_list_item,
                gateway_payload={},
                gateway_metrics_payload={},
                expected_tenant_id="087175fa-5bf9-4180-a02e-7c57fb3543e8",
                time_span=datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                extracted_at=datetime(2026, 9, 17, 11, 2, tzinfo=timezone.utc),
            )


class ChirpStackApiResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        dagster_module = importlib.import_module("dagster")
        original_resource = getattr(dagster_module, "resource", None)
        if original_resource is None:
            dagster_module.resource = lambda fn=None, **_kwargs: (
                fn if fn else lambda wrapped: wrapped
            )
        path = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "resources"
            / "chirpstack.py"
        )
        spec = importlib.util.spec_from_file_location(
            "carlsjr_chirpstack_resource",
            path,
        )
        cls.resource_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        try:
            spec.loader.exec_module(cls.resource_module)
        finally:
            if original_resource is None:
                del dagster_module.resource

    def test_gateway_pagination_and_authorization_header(self) -> None:
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            query = parse_qs(urlparse(request.full_url).query)
            offset = int(query["offset"][0])
            result = (
                [{"gatewayId": "0000000000000001"}, {"gatewayId": "0000000000000002"}]
                if offset == 0
                else [{"gatewayId": "0000000000000003"}]
            )
            return io.BytesIO(
                json.dumps({"totalCount": 3, "result": result}).encode()
            )

        resource = self.resource_module.ChirpStackApiResource(
            base_url="http://chirpstack.test",
            token="private-token",
            timeout_seconds=15,
            page_size=2,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            gateways = resource.list_gateways(
                "087175fa-5bf9-4180-a02e-7c57fb3543e8"
            )

        self.assertEqual(len(gateways), 3)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0][1], 15)
        headers = {
            key.lower(): value
            for key, value in requests[0][0].header_items()
        }
        self.assertEqual(
            headers["grpc-metadata-authorization"],
            "Bearer private-token",
        )
        self.assertNotIn("private-token", requests[0][0].full_url)

    def test_metric_request_uses_closed_hour_contract(self) -> None:
        captured_requests = []

        def fake_urlopen(request, timeout):
            captured_requests.append(request)
            return io.BytesIO(b"{}")

        resource = self.resource_module.ChirpStackApiResource(
            base_url="https://chirpstack.test/",
            token="private-token",
            timeout_seconds=15,
            page_size=100,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=fake_urlopen,
        ):
            resource.get_device_link_metrics(
                "24e124136e090452",
                datetime(2026, 9, 17, 10, tzinfo=timezone.utc),
                datetime(2026, 9, 17, 11, tzinfo=timezone.utc),
            )

        parsed = urlparse(captured_requests[0].full_url)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/api/devices/24e124136e090452/link-metrics")
        self.assertEqual(query["start"], ["2026-09-17T10:00:00Z"])
        self.assertEqual(query["end"], ["2026-09-17T11:00:00Z"])
        self.assertEqual(query["aggregation"], ["HOUR"])


class MqttEventHistoryApiResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        path = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "resources"
            / "mqtt_event_history.py"
        )
        spec = importlib.util.spec_from_file_location(
            "carlsjr_mqtt_event_history_resource",
            path,
        )
        cls.resource_module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(cls.resource_module)

    def test_request_uses_mqtt_history_contract_and_basic_auth(self) -> None:
        payload = {
            "ok": True,
            "tenant": "carls",
            "fecha_inicio": "2026-09-17T04:00:00Z",
            "fecha_fin": "2026-09-17T05:00:00Z",
            "total": 1,
            "data": [
                {
                    "id": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "fecha": "2026-09-17T04:30:00",
                    "deveui": "24e124136e090452",
                    "tenant": "carls",
                    "mensaje_raw": json.dumps(
                        {
                            "time": "2026-09-17T10:00:00.123456Z",
                            "deviceInfo": {
                                "devEui": "24e124136e090452",
                            },
                        }
                    ),
                }
            ],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return Response(json.dumps(payload).encode())

        with patch.object(self.resource_module, "urlopen", side_effect=fake_urlopen):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        parsed = urlparse(requests[0][0].full_url)
        query = parse_qs(parsed.query)
        headers = {
            key.lower(): value for key, value in requests[0][0].header_items()
        }
        self.assertEqual(query["tenant"], ["carls"])
        self.assertEqual(query["limit"], ["5000"])
        self.assertEqual(requests[0][1], 15)
        self.assertTrue(headers["authorization"].startswith("Basic "))
        self.assertNotIn("private-password", requests[0][0].full_url)
        self.assertEqual(rows[0]["DevEui"], "24e124136e090452")
        self.assertEqual(
            rows[0]["EventTime"],
            datetime(2026, 9, 17, 10, 0, 0, 123456),
        )

    def test_splits_a_saturated_window(self) -> None:
        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        first_id = "9232a3a7-79f5-4301-90f7-fa878659d322"
        second_id = "6fd801c3-7ee5-4be2-842b-d48475d34753"

        def fake_request(from_local, to_local):
            if to_local - from_local == timedelta(hours=1):
                raise self.resource_module.MqttEventHistoryWindowTooLarge()
            return [
                {
                    "MqttId": first_id if from_local.minute == 0 else second_id,
                }
            ]

        with patch.object(resource, "_request_window", side_effect=fake_request):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual([row["MqttId"] for row in rows], [first_id, second_id])

    def test_retries_a_transient_transport_failure(self) -> None:
        payload = {
            "ok": True,
            "tenant": "carls",
            "fecha_inicio": "2026-09-17T04:00:00Z",
            "fecha_fin": "2026-09-17T05:00:00Z",
            "total": 0,
            "data": [],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            side_effect=[URLError("temporary"), Response(json.dumps(payload).encode())],
        ) as urlopen_mock, patch.object(self.resource_module, "sleep"):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual(rows, [])
        self.assertEqual(urlopen_mock.call_count, 2)

    def test_invalid_api_row_is_truncated_and_does_not_discard_valid_row(self) -> None:
        valid_raw = {
            "time": "2026-09-17T10:00:00Z",
            "deviceInfo": {"devEui": "24e124136e090452"},
        }
        payload = {
            "ok": True,
            "tenant": "carls",
            "fecha_inicio": "2026-09-17T04:00:00Z",
            "fecha_fin": "2026-09-17T05:00:00Z",
            "total": 2,
            "data": [
                {
                    "id": "9232a3a7-79f5-4301-90f7-fa878659d322",
                    "fecha": "2026-09-17T04:30:00",
                    "deveui": "24e124136e090452",
                    "tenant": "carls",
                    "mensaje_raw": json.dumps(valid_raw),
                },
                {
                    "id": "6fd801c3-7ee5-4be2-842b-d48475d34753",
                    "fecha": "2026-09-17T04:31:00",
                    "deveui": "24e124136e090452",
                    "tenant": "carls",
                    "mensaje_raw": json.dumps({"payload": "x" * 5000}),
                },
            ],
        }

        class Headers:
            @staticmethod
            def get_content_type():
                return "application/json"

        class Response(io.BytesIO):
            status = 200
            headers = Headers()

        resource = self.resource_module.MqttEventHistoryApiResource(
            url="http://mqtt-history.test/api/lecturas/tenant",
            user="api-user",
            password="private-password",
            timeout_seconds=15,
        )
        with patch.object(
            self.resource_module,
            "urlopen",
            return_value=Response(json.dumps(payload).encode()),
        ):
            rows = resource.fetch_range(
                datetime(2026, 9, 17, 4),
                datetime(2026, 9, 17, 5),
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(resource.rejected_rows), 1)
        rejected = resource.rejected_rows[0]
        self.assertEqual(rejected["FechaInicio"], "2026-09-17T04:00:00.000000Z")
        self.assertEqual(rejected["FechaFin"], "2026-09-17T05:00:00.000000Z")
        self.assertEqual(rejected["RequestedLimit"], 5000)
        self.assertEqual(len(rejected["RowJson"]), 4000)
        self.assertIn("'time'", rejected["FailureReason"])


class ChirpStackContractTests(unittest.TestCase):
    def test_sql_contract_has_hourly_keys_and_official_gateway_state(self) -> None:
        sql_directory = (
            REPOSITORY_ROOT
            / "sql"
            / "init"
        )
        ddl_paths = [
            sql_directory / "023_observability_serving_vwSenseSensorHealth.sql",
            sql_directory / "024_observability_factChirpStackDeviceMetrics.sql",
            sql_directory / "025_observability_factChirpStackGatewayMetrics.sql",
            sql_directory / "026_observability_chirpstackGatewayViews.sql",
            sql_directory / "027_serving_sensorCommunicationHealthViews.sql",
            sql_directory / "034_observability_vwSenseDevEuiUplinkReadingsDaily.sql",
            sql_directory / "035_observability_factChirpStackDeviceEventMetrics.sql",
            sql_directory / "036_observability_factChirpStackDeviceGatewayMetrics.sql",
            sql_directory / "037_raw_MqttEventHistoryRejectedRow.sql",
        ]
        sql = "\n".join(path.read_text(encoding="utf-8") for path in ddl_paths)

        self.assertTrue(all(path.is_file() for path in ddl_paths))
        carlsjr_sql_directory = (
            REPOSITORY_ROOT / "sql" / "user_codes" / "carlsjr" / "init"
        )
        self.assertFalse(any(
            carlsjr_sql_directory.glob("*ChirpStack*.sql")
        ))
        self.assertIn("PRIMARY KEY CLUSTERED (DevEui, TimeSpan)", sql)
        self.assertIn("PRIMARY KEY CLUSTERED (GatewayId, TimeSpan)", sql)
        self.assertIn(
            "PRIMARY KEY CLUSTERED (DevEui, GatewayId, TimeSpan)",
            sql,
        )
        self.assertIn("LinkErrorCountsJson NVARCHAR(MAX) NULL", sql)
        self.assertIn("TxPacketStatusCountsJson NVARCHAR(MAX) NULL", sql)
        self.assertIn("raw.MqttEventHistoryRejectedRow", sql)
        self.assertIn("RowJson NVARCHAR(4000) NOT NULL", sql)
        self.assertIn("GatewayState NVARCHAR(20) NOT NULL", sql)
        self.assertIn("BatteryLevel DECIMAL(7, 4) NULL", sql)
        self.assertIn("serving.vwSenseSensorCommunicationHealthHourly", sql)
        self.assertIn("serving.vwSenseSensorCoverageHourly", sql)
        self.assertIn("serving.vwSenseSensorCoverageDaily", sql)
        self.assertIn("serving.vwSenseSensorCoverageWeekly", sql)
        self.assertIn("observability.vwSenseDevEuiUplinkReadingsDaily", sql)
        self.assertIn("COUNT_BIG(*) AS SensorIdCount", sql)
        self.assertNotIn("CREATE OR ALTER VIEW serving.vwSenseSensorHealth", sql)
        self.assertIn("serving.vwSenseGatewayHealthHourly", sql)
        self.assertIn("observability.vwChirpStackTenantGatewayHealthHourly", sql)
        self.assertIn("N'DATA_INGESTION_PROBLEM'", sql)
        self.assertIn("N'DEVICE_NOT_COMMUNICATING'", sql)
        self.assertIn("N'GATEWAY_PROBLEM'", sql)
        self.assertLess(
            sql.index("WHEN device.ReceivedUplinkCount > 0"),
            sql.index("WHEN device.ExpectedUplinkCount IS NULL"),
        )
        self.assertNotIn("LastSeenAt DATETIME2(0)", sql.split(
            "CREATE TABLE observability.factChirpStackDeviceMetrics",
            1,
        )[1].split("CREATE TABLE stg.factChirpStackDeviceMetrics", 1)[0])
        bafar_cleanup = (
            REPOSITORY_ROOT
            / "sql"
            / "user_codes"
            / "bafar"
            / "init"
            / "000_stg_ctl_dwh_dropUnusedTables.sql"
        ).read_text(encoding="utf-8")
        for object_name in (
            "observability.vwSenseDevEuiUplinkReadingsDaily",
            "serving.vwSenseSensorCommunicationHealthWeekly",
            "serving.vwSenseSensorCommunicationHealthDaily",
            "serving.vwSenseSensorCommunicationHealthHourly",
            "serving.vwSenseSensorCoverageWeekly",
            "serving.vwSenseSensorCoverageDaily",
            "serving.vwSenseSensorCoverageHourly",
            "serving.vwSenseGatewayHealthHourly",
            "observability.vwChirpStackTenantGatewayHealthHourly",
            "stg.factChirpStackDeviceMetrics",
            "stg.factChirpStackGatewayMetrics",
            "observability.factChirpStackDeviceMetrics",
            "observability.factChirpStackGatewayMetrics",
            "stg.factChirpStackDeviceEventMetrics",
            "stg.factChirpStackDeviceGatewayMetrics",
            "observability.factChirpStackDeviceEventMetrics",
            "observability.factChirpStackDeviceGatewayMetrics",
            "raw.MqttEventHistoryRejectedRow",
        ):
            self.assertIn(object_name, bafar_cleanup)
        self.assertIn(
            "WHERE IngestionName = N'factChirpStackMetrics'",
            bafar_cleanup,
        )

    def test_registration_and_configuration_are_scoped_to_carlsjr(self) -> None:
        compose = (
            REPOSITORY_ROOT / "compose-data_etls" / "docker-compose.yml"
        ).read_text(encoding="utf-8")
        defs_source = (
            PROJECT_SRC / "carlsjr" / "defs" / "__init__.py"
        ).read_text(encoding="utf-8")
        asset = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "assets"
            / "observability_chirpstack_metrics.py"
        ).read_text(encoding="utf-8")
        schedule = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "schedules"
            / "chirpstack_metrics_scheduler.py"
        ).read_text(encoding="utf-8")

        self.assertIn('"chirpstack_api": chirpstack_api_resource', defs_source)
        self.assertIn(
            '"mqtt_event_history_api": mqtt_event_history_api_resource',
            defs_source,
        )
        self.assertIn("observability_chirpstackmetrics", defs_source)
        self.assertIn("CARLSJR_CHIRPSTACK_API_URL", compose)
        self.assertIn("CARLSJR_MQTT_EVENT_HISTORY_API_URL", compose)
        self.assertIn(
            "CARLSJR_MQTT_EVENT_HISTORY_REJECTED_ROW_RETENTION_DAYS",
            compose,
        )
        self.assertIn("to_utc = extracted_at.replace", asset)
        self.assertIn("from_utc = to_utc - timedelta(hours=1)", asset)
        self.assertIn("except (TypeError, ValueError):", asset)
        self.assertIn('"valid_dev_eui_count"', asset)
        self.assertIn('"invalid_physical_identifier_count"', asset)
        self.assertIn('"invalid_physical_identifiers"', asset)
        self.assertIn('group_name="carlsjr"', asset)
        self.assertIn('key_prefix=["carlsjr"]', asset)
        self.assertNotIn("overlap", asset.lower())
        self.assertNotIn("backfill", asset.lower())
        self.assertIn('name="023_sense_chirpstack_metrics_hourly_at_30"', schedule)
        self.assertIn('cron_schedule="30 * * * *"', schedule)
        self.assertIn('execution_timezone="America/Ciudad_Juarez"', schedule)
        self.assertIn(
            "default_status=DefaultScheduleStatus.RUNNING",
            schedule,
        )
        self.assertNotIn("BAFAR_CHIRPSTACK_API_URL", compose)

    def test_staging_and_merges_are_run_scoped(self) -> None:
        sql_resource = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "resources"
            / "sql_server.py"
        ).read_text(encoding="utf-8")
        spark_resource = (
            PROJECT_SRC
            / "carlsjr"
            / "defs"
            / "resources"
            / "spark.py"
        ).read_text(encoding="utf-8")

        self.assertIn("FROM stg.factChirpStackDeviceMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackGatewayMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackDeviceEventMetrics", sql_resource)
        self.assertIn("FROM stg.factChirpStackDeviceGatewayMetrics", sql_resource)
        self.assertIn("def write_mqtt_event_history_rejected_rows", sql_resource)
        self.assertIn("DATEADD(DAY, -?, SYSUTCDATETIME())", sql_resource)
        self.assertGreaterEqual(sql_resource.count("WHERE RunId = ?"), 2)
        self.assertIn("def merge_chirpstack_metrics", sql_resource)
        self.assertIn("conn.rollback()", sql_resource)
        self.assertEqual(
            sql_resource.count(
                "target.MetricsAvailable = 0\n"
                "    OR source.MetricsAvailable = 1"
            ),
            2,
        )
        self.assertIn(
            '.dropDuplicates(["DevEui", "TimeSpan"])',
            spark_resource,
        )
        self.assertIn(
            '.dropDuplicates(["GatewayId", "TimeSpan"])',
            spark_resource,
        )


if __name__ == "__main__":
    unittest.main()
