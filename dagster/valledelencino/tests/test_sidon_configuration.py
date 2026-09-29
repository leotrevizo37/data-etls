import copy
import importlib
import json
import sys
from datetime import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [str(PROJECT_SRC), *[
    path for path in original_sys_path
    if Path(path or ".").resolve() != REPOSITORY_ROOT
]]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    sensor_module = importlib.import_module("valledelencino.defs.sensors.sidon_configuration_sensor")
    ai_module = importlib.import_module("valledelencino.defs.resources.sidon_configuration_ai")
    operational_module = importlib.import_module("valledelencino.defs.sensors.operational_alerts_sensor")
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


@pytest.fixture
def snapshot():
    return {
        "devices": [{"DeviceId": "device-id", "DeviceTyId": "device-type-id", "Name": "Congelador", "Active": True}],
        "device_types": [{"DeviceTyId": "device-type-id", "Name": "Congelador", "Active": True}],
        "sensors": [{"SensorId": "sensor-id", "DeviceId": "device-id", "SensorTyId": "sensor-type-id", "Name": "Temperatura", "Active": True}],
        "sensor_types": [{"SensorTyId": "sensor-type-id", "Name": "Temperatura F", "UnitName": "Fahrenheit", "UnitAbbreviation": "F", "Active": True}],
        "alerts": [{"AlertId": "alert-id", "DeviceTyId": "device-type-id", "Active": True}],
        "conditions": [{"AlertConditionId": "condition-id", "AlertId": "alert-id", "SensorTyId": "sensor-type-id", "Operator": ">=", "Value": 10.0, "Active": True}],
        "stopped": [],
        "expected": [{"SensorId": "sensor-id", "DeviceId": "device-id", "min_expected_value": Decimal("0"), "max_expected_value": Decimal("10"), "StartTime": time(0), "EndTime": time(23, 59), "scheduledDays": "1,2,3,4,5,6,7"}],
    }


def test_payload_preserves_ranges_and_includes_entity_ids(snapshot):
    second = dict(snapshot["expected"][0], StartTime=time(12), min_expected_value=None, max_expected_value=Decimal("12"), scheduledDays="6,7")
    snapshot["expected"].append(second)
    candidates = sensor_module._candidates(snapshot)
    alert = candidates[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert alert["complete"] and not alert["problems"]
    assert candidates[(sensor_module._DEVICE_TYPE, "device:device-id")]["payload"]["device_id"] == "device-id"
    assert alert["payload"]["alert_id"] == "alert-id"
    item = alert["payload"]["devices"][0]["conditions"][0]
    assert item["alert_condition_id"] == "condition-id"
    assert item["threshold"] == 10 and item["operator"] == ">="
    ranges = item["sensors"][0]["expected_ranges"]
    assert ranges[0]["min"] == 0 and ranges[1]["min"] is None
    assert ranges[1]["start"] == "12:00:00" and ranges[1]["days"] == "6,7"
    for candidate in candidates.values():
        assert "source_ids" not in candidate["payload"]


@pytest.mark.parametrize("missing", ["expected", "device_types", "sensor_types", "conditions", "sensors"])
def test_missing_evidence_is_not_consistent(snapshot, missing):
    snapshot[missing] = []
    alert = sensor_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    if missing == "expected":
        assert alert["complete"] and not alert["problems"]
        assert alert["payload"]["devices"][0]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    else:
        assert alert["problems"] or not alert["complete"]


def test_stopped_devices_and_inactive_alerts_are_excluded(snapshot):
    snapshot["stopped"] = [{"AlertId": "alert-id", "DeviceId": "device-id"}]
    alert = sensor_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert not alert["complete"] and alert["payload"]["devices"] == []
    snapshot["alerts"][0]["Active"] = False
    assert all(key[0] != sensor_module._ALERT_TYPE for key in sensor_module._candidates(snapshot))


def test_orphans_invalid_operator_and_inverted_range(snapshot):
    snapshot["devices"] = []
    assert sensor_module._candidates(snapshot)[(sensor_module._DEVICE_TYPE, "device:device-id")]["problems"]
    snapshot["alerts"] = []
    assert sensor_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "condition:condition-id")]["problems"]


@pytest.mark.parametrize("value", [None, float("nan"), float("inf")])
def test_invalid_threshold_is_structural_and_serializable(snapshot, value):
    snapshot["conditions"][0]["Value"] = value
    alert = sensor_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert alert["problems"]
    assert sensor_module._fingerprint(alert)


def test_invalid_operator_and_range(snapshot):
    snapshot["conditions"][0]["Operator"] = None
    snapshot["expected"][0]["min_expected_value"] = Decimal("20")
    alert = sensor_module._candidates(snapshot)[(sensor_module._ALERT_TYPE, "alert:alert-id")]
    assert len(alert["problems"]) == 2


@pytest.fixture
def evaluation(monkeypatch, snapshot):
    connection = MagicMock()
    rule = {
        "alert_type": sensor_module._DEVICE_TYPE, "alert_type_id": "type-id",
        "is_enabled": True, "evaluation_interval_seconds": 3600,
        "rule_version": b"1", "alert_rule_id": "rule-id",
    }
    config = {"version_token": "1", "rules": {(sensor_module._DEVICE_TYPE, None): rule}}
    evaluator = SimpleNamespace(policy="policy-1", evaluate=Mock(return_value={
        "status": "suspicious", "confidence": 0.95,
        "reasons": ["Incongruencia semántica comprobable."], "model": "test-model",
    }))
    context = SimpleNamespace(
        cursor=None, update_cursor=Mock(), resources=SimpleNamespace(
            sqlserver=SimpleNamespace(_get_connection=lambda: connection),
            dim_sqlserver=Mock(), sidon_configuration_ai=evaluator,
        ),
    )
    monkeypatch.setattr(sensor_module, "_load_active_config", Mock(return_value=(config, None)))
    monkeypatch.setattr(sensor_module, "_snapshot", Mock(side_effect=lambda source, warehouse, watermark_only=False: (
        {"devices": [{"modified_at": "2026-09-28T12:00:00", "row_count": 1}]}
        if watermark_only else snapshot
    )))
    monkeypatch.setattr(sensor_module, "_rows", Mock(return_value=[]))
    detected = Mock()
    cleared = Mock()
    monkeypatch.setattr(sensor_module, "_record_detected", detected)
    monkeypatch.setattr(sensor_module, "_record_cleared", cleared)
    return context, evaluator, detected, cleared, config, connection


def test_detects_using_existing_signal_writer_and_honors_interval(evaluation):
    context, evaluator, detected, cleared, _, connection = evaluation
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert detected.call_count == 1 and not cleared.called
    assert detected.call_args.args[2] == "sensor_monitoring"
    assert detected.call_args.args[3] == "device:device-id"
    connection.__exit__.assert_called_once_with(None, None, None)
    context.cursor = context.update_cursor.call_args.args[0]
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_count == 1


@pytest.mark.parametrize("kind,entity_key,code", [
    ("sidon_device_configuration", "device:device-id", "configuracion-dispositivo-device-device-id"),
    ("sidon_alert_configuration", "alert:alert-id", "configuracion-alerta-alert-alert-id"),
])
def test_incident_lookup_and_signal_writer_are_tenant_scoped(monkeypatch, evaluation, kind, entity_key, code):
    context, _, _, _, config, connection = evaluation
    rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
    rule.update(alert_type=kind, severity_id="severity-id", alert_config_id="config-id")
    config["rules"][(kind, None)] = rule
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (1,)
    monkeypatch.setattr(sensor_module, "_record_detected", operational_module._record_detected)

    sensor_module.sidon_configuration_sensor._raw_fn(context)

    lookup, write = connection.cursor.return_value.__enter__.return_value.execute.call_args_list
    assert "WHERE user_code = N'valledelencino'" in lookup.args[0]
    assert lookup.args[1:] == ("type-id", entity_key)
    assert write.args[0].count("?") == len(write.args) - 1
    assert "valledelencino" in write.args[1:] and code in write.args[1:]
    assert "carlsjr" not in write.args[1:] and "emerson" not in write.args[1:]
    assert operational_module._SIGNAL_TYPE_IDS["DETECTED"] in write.args[1:]
    connection.__exit__.assert_called_once_with(None, None, None)


def test_active_configuration_is_tenant_scoped():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = []
    cursor.fetchall.return_value = []
    config, reason = operational_module._load_active_config(connection)
    assert config is None and reason
    assert cursor.execute.call_args.args[1:] == ("valledelencino",)


@pytest.mark.parametrize("status,confidence,error", [
    ("insufficient_evidence", 1, None), ("suspicious", 0.89, None),
    ("consistent", 0.95, TimeoutError()), ("consistent", 0.95, ValueError()),
])
def test_inconclusive_evaluation_emits_nothing(evaluation, status, confidence, error):
    context, evaluator, detected, cleared, _, _ = evaluation
    evaluator.evaluate.return_value.update(status=status, confidence=confidence)
    evaluator.evaluate.side_effect = error
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert not detected.called and not cleared.called


def test_structural_detection_does_not_need_azure(evaluation, snapshot):
    context, evaluator, detected, _, _, _ = evaluation
    snapshot["sensor_types"] = []
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert detected.called and not evaluator.evaluate.called


@pytest.mark.parametrize("status,detects", [("consistent", False), ("suspicious", True)])
def test_alert_without_expected_ranges_is_evaluated_without_unsafe_clear(evaluation, snapshot, status, detects):
    context, evaluator, detected, cleared, config, _ = evaluation
    snapshot["expected"] = []
    evaluator.evaluate.return_value["status"] = status
    rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
    rule["alert_type"] = sensor_module._ALERT_TYPE
    config["rules"][(sensor_module._ALERT_TYPE, None)] = rule
    sensor_module._rows.return_value = [{"metadata_json": json.dumps({
        "source_fingerprint": "previous-source", "policy": evaluator.policy,
        "assessment": {"model": "test-model"},
    })}]
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_args.args[0]["devices"][0]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    assert detected.called == detects and not cleared.called


def test_alert_with_mixed_expected_ranges_evaluates_all_devices(evaluation, snapshot):
    context, evaluator, detected, cleared, config, _ = evaluation
    snapshot["devices"].append(dict(snapshot["devices"][0], DeviceId="device-two", Name="Otro congelador"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], SensorId="sensor-two", DeviceId="device-two"))
    rule = config["rules"].pop((sensor_module._DEVICE_TYPE, None))
    rule["alert_type"] = sensor_module._ALERT_TYPE
    config["rules"][(sensor_module._ALERT_TYPE, None)] = rule
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    devices = evaluator.evaluate.call_args.args[0]["devices"]
    assert len(devices) == 2
    assert devices[0]["conditions"][0]["sensors"][0]["expected_ranges"]
    assert devices[1]["conditions"][0]["sensors"][0]["expected_ranges"] == []
    assert detected.called and not cleared.called


def test_disabled_entity_override_is_respected(evaluation):
    context, evaluator, detected, cleared, config, _ = evaluation
    rule = config["rules"][(sensor_module._DEVICE_TYPE, None)]
    config["rules"][(sensor_module._DEVICE_TYPE, "device:device-id")] = dict(rule, is_enabled=False)
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert not evaluator.evaluate.called and not detected.called and not cleared.called


def test_new_sensor_is_registered_stopped():
    definitions = importlib.import_module("valledelencino.defs")._build_defs()
    detector = sensor_module.sidon_configuration_sensor
    assert detector in definitions.sensors
    assert detector.default_status == sensor_module.DefaultSensorStatus.STOPPED
    assert detector.required_resource_keys <= definitions.resources.keys()
    assert definitions.resources["sidon_configuration_ai"] is ai_module.sidon_configuration_ai_resource


@pytest.mark.parametrize("change", ["source", "config"])
def test_changes_during_evaluation_emit_nothing(monkeypatch, evaluation, snapshot, change):
    context, _, detected, cleared, config, _ = evaluation
    if change == "source":
        changed = copy.deepcopy(snapshot)
        changed["devices"][0]["Name"] = "Otra configuración"
        sensor_module._snapshot.side_effect = [{"devices": []}, snapshot, changed]
    else:
        changed = dict(config, version_token="2")
        sensor_module._load_active_config.side_effect = [(config, None), (changed, None)]
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert not detected.called and not cleared.called


@pytest.mark.parametrize("same_source,policy,model,expected_clear", [
    (False, "policy-1", "test-model", True),
    (False, "policy-1", "structural", True),
    (True, "policy-1", "test-model", False),
    (False, "old-policy", "test-model", False),
    (False, "policy-1", "old-model", False),
])
def test_recovery_requires_changed_evidence_and_same_policy(monkeypatch, evaluation, snapshot, same_source, policy, model, expected_clear):
    context, evaluator, detected, cleared, _, _ = evaluation
    evaluator.evaluate.return_value["status"] = "consistent"
    candidate = sensor_module._candidates(snapshot)[(sensor_module._DEVICE_TYPE, "device:device-id")]
    previous = {
        "source_fingerprint": sensor_module._fingerprint(candidate) if same_source else "previous-source",
        "policy": policy, "assessment": {"model": model},
    }
    monkeypatch.setattr(sensor_module, "_rows", Mock(return_value=[{"alert_id": "incident-id", "metadata_json": json.dumps(previous)}]))
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert cleared.called == expected_clear and not detected.called


@pytest.fixture
def azure(monkeypatch):
    connection = MagicMock()
    response = connection.getresponse.return_value
    response.status = 200
    assessment = {"status": "consistent", "confidence": 0.95, "reasons": ["Tipos compatibles."]}
    result = {"model": "test-model", "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(assessment)}}]}
    response.read.side_effect = lambda _: json.dumps(result).encode()
    factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", factory)
    client = ai_module.SidonConfigurationAI("https://example.openai.azure.com", "deployment-name", "fake-test-key")
    return client, connection, result, assessment, factory


def test_azure_request_contract(azure):
    client, connection, _, _, _ = azure
    assert client.evaluate({"kind": "device_configuration"})["model"] == "test-model"
    args = connection.request.call_args.args
    assert args[:2] == ("POST", "/openai/v1/chat/completions")
    body = json.loads(args[2])
    assert body["model"] == "deployment-name" and body["store"] is False
    assert body["response_format"]["json_schema"]["strict"] is True
    assert "temperature" not in body
    connection.close.assert_called_once()


@pytest.mark.parametrize("invalid", ["truncated", "refusal", "redirect", "nan", "bool", "empty", "extra", "status", "model"])
def test_invalid_azure_response_is_rejected(azure, invalid):
    client, connection, result, assessment, _ = azure
    if invalid == "truncated":
        result["choices"][0]["finish_reason"] = "length"
    elif invalid == "refusal":
        result["choices"][0]["message"]["refusal"] = "refused"
    elif invalid == "redirect":
        connection.getresponse.return_value.status = 302
    elif invalid == "nan":
        assessment["confidence"] = float("nan")
    elif invalid == "bool":
        assessment["confidence"] = True
    elif invalid == "empty":
        assessment["reasons"] = []
    elif invalid == "extra":
        assessment["unrequested"] = "data"
    elif invalid == "model":
        result["model"] = None
    else:
        assessment["status"] = "unknown"
    result["choices"][0]["message"]["content"] = json.dumps(assessment)
    with pytest.raises(ValueError):
        client.evaluate({})
    connection.close.assert_called_once()


@pytest.mark.parametrize("endpoint", ["http://example.openai.azure.com", "https://untrusted.example", "https://example.openai.azure.com@untrusted.example", "https://example.openai.azure.com/path"])
def test_invalid_endpoint_is_not_contacted(azure, endpoint):
    client, _, _, _, factory = azure
    client.endpoint = endpoint
    with pytest.raises(ValueError):
        client.evaluate({})
    factory.assert_not_called()


def test_snapshot_reads_only_required_configuration(monkeypatch):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = MagicMock(spec_set=sensor_module.pyodbc.Cursor)
    connection.cursor.return_value.__enter__.return_value.description = []
    connection.cursor.return_value.__enter__.return_value.fetchall.return_value = []
    connect = Mock(return_value=connection)
    monkeypatch.setattr(sensor_module.pyodbc, "connect", connect)
    source = SimpleNamespace(host="localhost", port=1433, database="synthetic", user="test", password="fake", connect_timeout=10)
    warehouse = SimpleNamespace(_get_connection=lambda: connection)
    snapshot = sensor_module._snapshot(source, warehouse)
    assert len(snapshot) == 8 and all(rows == [] for rows in snapshot.values())
    assert connect.call_args.kwargs["driver"] == "{ODBC Driver 18 for SQL Server}"
    assert connection.close.call_count == 2
    assert connection.timeout == 20


def test_watermark_covers_all_tables_and_counts_inactive_rows(monkeypatch):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = MagicMock(spec_set=sensor_module.pyodbc.Cursor)
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.description = [("modified_at",), ("row_count",)]
    cursor.fetchall.side_effect = [[("2026-09-28T12:00:00.1234567", 3)]] * 7 + [[(None, 0)]]
    monkeypatch.setattr(sensor_module.pyodbc, "connect", Mock(return_value=connection))
    source = SimpleNamespace(host="localhost", port=1433, database="synthetic", user="test", password="fake", connect_timeout=10)
    warehouse = SimpleNamespace(_get_connection=lambda: connection)
    watermark = sensor_module._snapshot(source, warehouse, watermark_only=True)
    assert set(watermark) == {"devices", "device_types", "sensors", "sensor_types", "alerts", "conditions", "stopped", "expected"}
    assert watermark["devices"] == [{"modified_at": "2026-09-28T12:00:00.1234567", "row_count": 3}]
    assert watermark["expected"] == [{"modified_at": None, "row_count": 0}]
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert all("MAX(ModifiedAt)" in query and "COUNT_BIG(*)" in query and "WHERE" not in query for query in queries)
    assert "dbo.AlertStoppedOnDevices" in queries[6]
    assert "dwh.dimSensorExpectedVal" in queries[7]


def test_unchanged_watermark_skips_snapshot_and_ai_even_after_interval(evaluation):
    context, evaluator, _, _, _, _ = evaluation
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == []
    for check in state["checks"].values():
        check["at"] = 0
    context.cursor = json.dumps(state)
    sensor_module._snapshot.reset_mock()
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_count == 1
    assert sensor_module._snapshot.call_count == 1
    assert sensor_module._snapshot.call_args.kwargs == {"watermark_only": True}


@pytest.mark.parametrize("change", ["modified_at", "row_count", "config", "rule", "policy"])
def test_watermark_or_evaluation_changes_restart_processing(evaluation, snapshot, change):
    context, evaluator, detected, _, config, _ = evaluation
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    context.cursor = json.dumps(state)
    watermark = copy.deepcopy(state["watermark"])
    if change == "modified_at":
        watermark["devices"][0]["modified_at"] = "2026-09-28T13:00:00"
    elif change == "row_count":
        watermark["devices"][0]["row_count"] = 0
    elif change == "config":
        config["version_token"] = "2"
    elif change == "rule":
        config["rules"][(sensor_module._DEVICE_TYPE, None)]["rule_version"] = b"2"
    else:
        evaluator.policy = "policy-2"
    sensor_module._snapshot.side_effect = lambda source, warehouse, watermark_only=False: watermark if watermark_only else snapshot
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_count == 2 and detected.call_count == 2
    assert json.loads(context.update_cursor.call_args.args[0])["pending"] == []


def test_pending_entities_survive_between_ticks(evaluation, snapshot):
    context, evaluator, detected, _, _, _ = evaluation
    snapshot["devices"].append(dict(snapshot["devices"][0], DeviceId="device-two"))
    snapshot["sensors"].append(dict(snapshot["sensors"][0], DeviceId="device-two", SensorId="sensor-two"))
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-two"]]
    context.cursor = json.dumps(state)
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_count == 2 and detected.call_count == 2
    assert json.loads(context.update_cursor.call_args.args[0])["pending"] == []


def test_azure_failure_remains_pending_and_retries_after_interval(evaluation):
    context, evaluator, detected, _, _, _ = evaluation
    evaluator.evaluate.side_effect = TimeoutError()
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-id"]]
    assert not detected.called
    context.cursor = json.dumps(state)
    sensor_module._snapshot.reset_mock()
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert evaluator.evaluate.call_count == 1
    assert sensor_module._snapshot.call_count == 1
    for check in state["checks"].values():
        check["at"] = 0
    context.cursor = json.dumps(state)
    evaluator.evaluate.side_effect = None
    sensor_module.sidon_configuration_sensor._raw_fn(context)
    assert detected.call_count == 1
    assert json.loads(context.update_cursor.call_args.args[0])["pending"] == []


def test_persistence_failure_does_not_complete_entity(evaluation):
    context, _, detected, _, _, _ = evaluation
    detected.side_effect = RuntimeError("Synthetic persistence failure")
    with pytest.raises(RuntimeError):
        sensor_module.sidon_configuration_sensor._raw_fn(context)
    state = json.loads(context.update_cursor.call_args.args[0])
    assert state["pending"] == [[sensor_module._DEVICE_TYPE, "device:device-id"]]


@pytest.mark.parametrize("endpoint,host,port,tls", [
    ("http://localhost:1234", "localhost", 1234, False),
    ("http://127.0.0.1:8080", "127.0.0.1", 8080, False),
    ("http://[::1]:1234", "::1", 1234, False),
    ("http://host.docker.internal:1234", "host.docker.internal", 1234, False),
    ("https://localhost:8443", "localhost", 8443, True),
])
def test_local_request_without_azure_credentials(monkeypatch, azure, endpoint, host, port, tls):
    _, connection, _, _, _ = azure
    http_factory = Mock(return_value=connection)
    https_factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", https_factory)
    client = ai_module.SidonConfigurationAI(endpoint, "local-model", provider="local", timeout_seconds=40)
    assert client.evaluate({"kind": "device_configuration"})["status"] == "consistent"
    factory, unused = (https_factory, http_factory) if tls else (http_factory, https_factory)
    factory.assert_called_once_with(host, port=port, timeout=40)
    unused.assert_not_called()
    args = connection.request.call_args.args
    assert args[:2] == ("POST", "/v1/chat/completions")
    assert args[3] == {"Content-Type": "application/json"}
    body = json.loads(args[2])
    assert body["model"] == "local-model" and body["max_tokens"] == 4096
    assert body["stream"] is False
    assert "store" not in body and "max_completion_tokens" not in body
    assert body["response_format"]["json_schema"]["schema"] == ai_module._SCHEMA
    assert body["messages"][0]["content"] == ai_module._PROMPT
    connection.close.assert_called_once()


def test_local_optional_authentication(monkeypatch, azure):
    _, connection, _, _, _ = azure
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", Mock(return_value=connection))
    client = ai_module.SidonConfigurationAI("http://localhost:1234", "local-model", "fake-local-key", provider="local")
    client.evaluate({})
    headers = connection.request.call_args.args[3]
    assert headers["Authorization"] == "Bearer fake-local-key"
    assert "api-key" not in headers


@pytest.mark.parametrize("endpoint", [
    "http://untrusted.example:1234", "http://localhost.untrusted.example:1234",
    "http://localhost:1234/v1", "http://localhost:1234?token=example",
    "http://user@localhost:1234", "http://localhost:0", "ftp://localhost:1234",
])
def test_local_rejects_invalid_endpoints_without_request(monkeypatch, endpoint):
    http_factory = Mock()
    https_factory = Mock()
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    monkeypatch.setattr(ai_module.http.client, "HTTPSConnection", https_factory)
    client = ai_module.SidonConfigurationAI(endpoint, "local-model", provider="local")
    with pytest.raises(ValueError):
        client.evaluate({})
    http_factory.assert_not_called()
    https_factory.assert_not_called()


@pytest.mark.parametrize("status", [302, 500])
def test_local_http_error_closes_connection_without_fallback(monkeypatch, azure, status):
    _, connection, _, _, https_factory = azure
    connection.getresponse.return_value.status = status
    http_factory = Mock(return_value=connection)
    monkeypatch.setattr(ai_module.http.client, "HTTPConnection", http_factory)
    client = ai_module.SidonConfigurationAI("http://localhost:1234", "local-model", provider="local")
    with pytest.raises(ValueError):
        client.evaluate({})
    assert http_factory.call_count == 1
    https_factory.assert_not_called()
    connection.close.assert_called_once()


@pytest.mark.parametrize("provider", [None, "azure", "local"])
def test_resource_selects_only_configured_provider(monkeypatch, provider):
    values = {"VALLEDELENCINO_SIDON_AI_TIMEOUT_SECONDS": "40"}
    if provider is not None:
        values["VALLEDELENCINO_SIDON_AI_PROVIDER"] = provider
    if provider == "local":
        values["VALLEDELENCINO_SIDON_LOCAL_MODEL"] = "local-model"
    getenv = Mock(side_effect=values.get)
    monkeypatch.setattr(ai_module.os, "getenv", getenv)
    client = ai_module.sidon_configuration_ai_resource.resource_fn(None)
    assert client.provider == (provider or "azure")
    assert client.timeout_seconds == 40
    assert all(call.args[0].startswith("VALLEDELENCINO_SIDON_") for call in getenv.call_args_list)
    if provider == "local":
        assert client.endpoint == "http://localhost:1234" and client.api_key == ""
        assert client.deployment == "local-model"
        assert all("AZURE" not in call.args[0] for call in getenv.call_args_list)


@pytest.mark.parametrize("timeout", [0, -1, 301, float("inf"), True])
def test_invalid_local_timeout_is_rejected(timeout):
    with pytest.raises(ValueError):
        ai_module.SidonConfigurationAI("http://localhost:1234", "local-model", provider="local", timeout_seconds=timeout)


def test_unknown_provider_is_rejected():
    with pytest.raises(ValueError):
        ai_module.SidonConfigurationAI("http://localhost:1234", "local-model", provider="unknown")
