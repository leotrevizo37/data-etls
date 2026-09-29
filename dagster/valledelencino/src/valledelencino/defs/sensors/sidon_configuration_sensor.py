import hashlib
import http.client
import json
import math
from contextlib import closing
from datetime import datetime, timezone

import pyodbc
from dagster import DefaultSensorStatus, SkipReason, sensor

from .operational_alerts_sensor import (
    _is_due,
    _load_active_config,
    _mark_checked,
    _record_cleared,
    _record_detected,
    _rows,
    _select_rule,
)


_DEVICE_TYPE = "sidon_device_configuration"
_ALERT_TYPE = "sidon_alert_configuration"


def _fingerprint(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _snapshot(source, warehouse, watermark_only=False):
    tables = {
        "devices": "SELECT DeviceId, DeviceTyId, Name, Active FROM dbo.Devices ORDER BY DeviceId",
        "device_types": "SELECT DeviceTyId, Name, Active FROM dbo.DeviceTys ORDER BY DeviceTyId",
        "sensors": "SELECT SensorId, DeviceId, SensorTyId, Name, Active FROM dbo.Sensors ORDER BY SensorId",
        "sensor_types": "SELECT SensorTyId, Name, UnitName, UnitAbbreviation, Active FROM dbo.SensorTys ORDER BY SensorTyId",
        "alerts": "SELECT AlertId, DeviceTyId, Active FROM dbo.Alerts ORDER BY AlertId",
        "conditions": "SELECT AlertConditionId, AlertId, SensorTyId, Operator, Value, Active FROM dbo.AlertConditions ORDER BY AlertConditionId",
        "stopped": "SELECT AlertId, DeviceId FROM dbo.AlertStoppedOnDevices WHERE Active = 1 ORDER BY AlertId, DeviceId",
    }
    watermark_query = "SELECT CONVERT(VARCHAR(33), MAX(ModifiedAt), 126) AS modified_at, COUNT_BIG(*) AS row_count FROM {}"
    if watermark_only:
        tables = {
            name: watermark_query.format(table)
            for name, table in (
                ("devices", "dbo.Devices"),
                ("device_types", "dbo.DeviceTys"),
                ("sensors", "dbo.Sensors"),
                ("sensor_types", "dbo.SensorTys"),
                ("alerts", "dbo.Alerts"),
                ("conditions", "dbo.AlertConditions"),
                ("stopped", "dbo.AlertStoppedOnDevices"),
            )
        }
    snapshot = {}
    with closing(pyodbc.connect(
        driver="{ODBC Driver 18 for SQL Server}",
        server=f"{source.host},{source.port}", database=source.database,
        uid=source.user, pwd=source.password, timeout=source.connect_timeout,
        TrustServerCertificate="yes",
    )) as connection:
        connection.timeout = 20
        with connection.cursor() as cursor:
            for name, query in tables.items():
                cursor.execute(query)
                snapshot[name] = _rows(cursor)
    with closing(warehouse._get_connection()) as connection:
        connection.timeout = 20
        with connection.cursor() as cursor:
            cursor.execute(watermark_query.format("dwh.dimSensorExpectedVal") if watermark_only else """
                SELECT SensorId, DeviceId, min_expected_value, max_expected_value,
                    StartTime, EndTime, scheduledDays
                FROM dwh.dimSensorExpectedVal
                ORDER BY SensorId, DeviceId, StartTime, scheduledDays
            """)
            snapshot["expected"] = _rows(cursor)
    return snapshot


def _candidates(snapshot):
    devices = {str(row["DeviceId"]): row for row in snapshot["devices"]}
    device_types = {str(row["DeviceTyId"]): row for row in snapshot["device_types"]}
    sensor_types = {str(row["SensorTyId"]): row for row in snapshot["sensor_types"]}
    sensors = {}
    expected = {}
    for row in snapshot["sensors"]:
        if row["Active"]:
            sensors.setdefault(str(row["DeviceId"]), []).append(row)
    for row in snapshot["expected"]:
        expected.setdefault((str(row["DeviceId"]), str(row["SensorId"])), []).append(row)
    candidates = {}
    for device_id in sorted(set(devices) | set(sensors)):
        device = devices.get(device_id)
        attached = sensors.get(device_id, [])
        if device and not device["Active"] and not attached:
            continue
        device_type = device_types.get(str(device["DeviceTyId"])) if device else None
        problems = []
        if not device or not device["Active"]:
            problems.append("Sensores activos asociados a un dispositivo inexistente o inactivo.")
        if device and (not device_type or not device_type["Active"]):
            problems.append("El tipo del dispositivo no existe o está inactivo.")
        payload = {
            "kind": "device_configuration",
            "device_id": device_id,
            "device": device["Name"] if device else None,
            "device_type": device_type["Name"] if device_type else None,
            "sensors": [],
        }
        complete = bool(attached and device and device["Name"] and device_type and device_type["Name"])
        for row in attached:
            sensor_type = sensor_types.get(str(row["SensorTyId"]))
            if not sensor_type or not sensor_type["Active"]:
                problems.append("Un sensor activo tiene un tipo inexistente o inactivo.")
            payload["sensors"].append({
                "sensor": row["Name"],
                "sensor_type": sensor_type["Name"] if sensor_type else None,
                "unit": sensor_type["UnitAbbreviation"] if sensor_type else None,
                "unit_name": sensor_type["UnitName"] if sensor_type else None,
            })
            complete = complete and bool(row["Name"] and sensor_type and sensor_type["Name"])
        candidates[(_DEVICE_TYPE, f"device:{device_id}")] = {
            "payload": payload, "problems": problems, "complete": complete,
            "source_ids": [device_id, *[str(row["SensorId"]) for row in attached]],
        }
    alerts = {str(row["AlertId"]): row for row in snapshot["alerts"]}
    conditions = {}
    for row in snapshot["conditions"]:
        if row["Active"]:
            alert_id = str(row["AlertId"])
            conditions.setdefault(alert_id, []).append(row)
            if alert_id not in alerts:
                candidates[(_ALERT_TYPE, f"condition:{row['AlertConditionId']}")] = {
                    "payload": {"kind": "alert_configuration"},
                    "problems": ["Una condición activa referencia una alerta inexistente."],
                    "complete": False, "source_ids": [str(row["AlertConditionId"])],
                }
    stopped = {(str(row["AlertId"]), str(row["DeviceId"])) for row in snapshot["stopped"]}
    for alert_id, alert in alerts.items():
        if not alert["Active"]:
            continue
        device_type = device_types.get(str(alert["DeviceTyId"]))
        active_conditions = conditions.get(alert_id, [])
        problems = []
        if not device_type or not device_type["Active"]:
            problems.append("La alerta tiene un tipo de dispositivo inexistente o inactivo.")
        if not active_conditions:
            problems.append("La alerta activa no tiene condiciones activas.")
        payload = {
            "kind": "alert_configuration",
            "alert_id": alert_id,
            "device_type": device_type["Name"] if device_type else None,
            "devices": [],
        }
        source_ids = [alert_id, *[str(row["AlertConditionId"]) for row in active_conditions]]
        complete = bool(device_type and device_type["Name"] and active_conditions)
        for condition in active_conditions:
            sensor_type = sensor_types.get(str(condition["SensorTyId"]))
            if not sensor_type or not sensor_type["Active"]:
                problems.append("Una condición usa un tipo de sensor inexistente o inactivo.")
            if condition["Operator"] not in ("<", "<=", ">", ">=", "=", "==", "!=", "<>"):
                problems.append("Una condición tiene un operador ausente o no reconocido.")
            if condition["Value"] is None or not math.isfinite(float(condition["Value"])):
                problems.append("Una condición tiene un umbral ausente o no finito.")
        for device_id, device in devices.items():
            if (
                not device["Active"] or device["DeviceTyId"] != alert["DeviceTyId"]
                or (alert_id, device_id) in stopped
            ):
                continue
            source_ids.append(device_id)
            evaluation = {"device": device["Name"], "conditions": []}
            for condition in active_conditions:
                sensor_type = sensor_types.get(str(condition["SensorTyId"]))
                matching = [row for row in sensors.get(device_id, []) if row["SensorTyId"] == condition["SensorTyId"]]
                if not matching:
                    problems.append("Una condición no tiene sensores activos de su tipo en un dispositivo aplicable.")
                item = {
                    "alert_condition_id": str(condition["AlertConditionId"]),
                    "sensor_type": sensor_type["Name"] if sensor_type else None,
                    "unit": sensor_type["UnitAbbreviation"] if sensor_type else None,
                    "unit_name": sensor_type["UnitName"] if sensor_type else None,
                    "operator": condition["Operator"],
                    "threshold": (
                        condition["Value"] if condition["Value"] is not None
                        and math.isfinite(float(condition["Value"])) else None
                    ), "sensors": [],
                }
                complete = complete and bool(sensor_type and sensor_type["Name"] and sensor_type["UnitAbbreviation"])
                for row in matching:
                    sensor_id = str(row["SensorId"])
                    source_ids.append(sensor_id)
                    ranges = []
                    for reference in expected.get((device_id, sensor_id), []):
                        lower = reference["min_expected_value"]
                        upper = reference["max_expected_value"]
                        if lower is not None and upper is not None and lower > upper:
                            problems.append("Un rango esperado tiene mínimo mayor que máximo.")
                        ranges.append({
                            "min": float(lower) if lower is not None else None,
                            "max": float(upper) if upper is not None else None,
                            "start": str(reference["StartTime"]),
                            "end": str(reference["EndTime"]),
                            "days": reference["scheduledDays"],
                        })
                        complete = complete and bool(
                            (lower is not None or upper is not None)
                            and reference["StartTime"] is not None
                            and reference["EndTime"] is not None
                            and reference["scheduledDays"]
                        )
                    item["sensors"].append({"sensor": row["Name"], "expected_ranges": ranges})
                evaluation["conditions"].append(item)
            payload["devices"].append(evaluation)
        candidates[(_ALERT_TYPE, f"alert:{alert_id}")] = {
            "payload": payload, "problems": sorted(set(problems)),
            "complete": complete and bool(payload["devices"]), "source_ids": source_ids,
        }
    return candidates


@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    required_resource_keys={"sqlserver", "dim_sqlserver", "sidon_configuration_ai"},
)
def sidon_configuration_sensor(context):
    warehouse = context.resources.sqlserver
    source = context.resources.dim_sqlserver
    evaluator = context.resources.sidon_configuration_ai
    now = datetime.now(timezone.utc)
    state = json.loads(context.cursor) if context.cursor else {}
    checks = state.get("checks", {})
    with closing(warehouse._get_connection()) as connection:
        config, reason = _load_active_config(connection)
    if config is None:
        return SkipReason(reason)
    if not any((kind, None) in config["rules"] for kind in (_DEVICE_TYPE, _ALERT_TYPE)):
        return SkipReason("No hay reglas de configuración SIDON habilitadas.")
    watermark = _snapshot(source, warehouse, watermark_only=True)
    evaluation_version = _fingerprint({
        "config": config["version_token"], "policy": evaluator.policy,
        "rules": sorted(
            (kind, entity_key or "", bytes(rule["rule_version"]).hex())
            for (kind, entity_key), rule in config["rules"].items()
            if kind in (_DEVICE_TYPE, _ALERT_TYPE)
        ),
    })
    if state.get("watermark") != watermark or state.get("evaluation_version") != evaluation_version:
        state = {"watermark": watermark, "evaluation_version": evaluation_version}
        checks = {}
    if state.get("pending") == []:
        return SkipReason("No hay cambios en la configuración SIDON.")
    if "pending" in state and not any(
        _is_due(config, _select_rule(config, kind, entity_key), entity_key, checks, now)
        for kind, entity_key in state["pending"]
    ):
        return SkipReason("Las configuraciones SIDON pendientes esperan su intervalo de evaluación.")
    candidates = _candidates(_snapshot(source, warehouse))
    candidate_keys = {"::".join(item) for item in candidates}
    checks = {key: value for key, value in checks.items() if key in candidate_keys}
    state["checks"] = checks
    state["pending"] = [
        [kind, entity_key] for kind, entity_key in state.get("pending", candidates)
        if (kind, entity_key) in candidates and (kind, None) in config["rules"]
        and _select_rule(config, kind, entity_key)["is_enabled"]
    ]
    context.update_cursor(json.dumps(state))
    pending = sorted(state["pending"], key=lambda key: checks.get("::".join(key), {}).get("at", 0))
    for kind, entity_key in pending:
        if (kind, None) not in config["rules"]:
            continue
        rule = _select_rule(config, kind, entity_key)
        if not rule["is_enabled"] or not _is_due(config, rule, entity_key, checks, now):
            continue
        candidate = candidates[(kind, entity_key)]
        _mark_checked(config, rule, entity_key, checks, now)
        state["checks"] = checks
        context.update_cursor(json.dumps(state))
        if candidate["problems"]:
            assessment = {"status": "suspicious", "confidence": 1, "reasons": candidate["problems"], "model": "structural"}
        elif not candidate["complete"]:
            state["pending"].remove([kind, entity_key])
            context.update_cursor(json.dumps(state))
            return SkipReason("Evidencia insuficiente para evaluar la configuración SIDON.")
        else:
            try:
                assessment = evaluator.evaluate(candidate["payload"])
            except (ValueError, OSError, http.client.HTTPException):
                return SkipReason("Evaluación SIDON AI no disponible o inválida; no se emiten señales.")
        if assessment["status"] == "insufficient_evidence" or assessment["confidence"] < 0.9:
            state["pending"].remove([kind, entity_key])
            context.update_cursor(json.dumps(state))
            return SkipReason("Evaluación SIDON inconclusa; no se emiten señales.")
        fresh = _candidates(_snapshot(source, warehouse)).get((kind, entity_key))
        if fresh != candidate:
            return SkipReason("La configuración SIDON cambió durante la evaluación.")
        fingerprint = _fingerprint(candidate)
        metadata = {
            "source_fingerprint": fingerprint, "policy": evaluator.policy,
            "assessment": assessment, "evidence": candidate,
        }
        with closing(warehouse._get_connection()) as connection, connection:
            current, reason = _load_active_config(connection)
            if current is None or current["version_token"] != config["version_token"]:
                return SkipReason(reason or "La configuración del detector cambió.")
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT alert_id, detected_at, metadata_json
                    FROM alerting.alert_incident
                    WHERE user_code = N'valledelencino' AND alert_type_id = ?
                        AND entity_key = ? AND cleared_at IS NULL
                        AND is_point_in_time = 0
                """, rule["alert_type_id"], entity_key)
                incidents = _rows(cursor)
            if assessment["status"] == "suspicious":
                _record_detected(
                    connection, rule, "sensor_monitoring", entity_key, entity_key,
                    now, "; ".join(assessment["reasons"]), metadata,
                )
            elif candidate["complete"] and incidents and all(
                sensor["expected_ranges"] for device in candidate["payload"].get("devices", [])
                for condition in device["conditions"] for sensor in condition["sensors"]
            ):
                incident = incidents[0]
                previous = json.loads(incident["metadata_json"])
                if (
                    previous.get("source_fingerprint")
                    and previous["source_fingerprint"] != fingerprint
                    and previous.get("policy") == evaluator.policy
                    and previous.get("assessment", {}).get("model") in ("structural", assessment["model"])
                ):
                    _record_cleared(
                        connection, incident, now, "; ".join(assessment["reasons"]), metadata,
                    )
        state["pending"].remove([kind, entity_key])
        context.update_cursor(json.dumps(state))
        return SkipReason("Configuración SIDON evaluada; señales persistidas cuando corresponde.")
    return SkipReason("No hay configuraciones SIDON pendientes de evaluación.")
