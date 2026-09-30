import json
from contextlib import closing
from datetime import datetime, timedelta, timezone

from dagster import AssetCheckResult, AssetCheckSeverity, AssetCheckSpec, Failure, MaterializeResult, MetadataValue, asset

from ..sensors.operational_alerts_sensor import (
    _OPERATIONAL_TYPES, _clear_dagster_failures, _failed_run_assets, _ingestions,
    _is_due, _load_active_config, _load_active_incidents, _mark_checked,
    _record_cleared, _record_detected, _select_rule, _sensor_dimensions, _sensor_statuses, _utc, _version,
)


@asset(
    name="operational_alerts",
    group_name="bafar_poc_gabinete",
    key_prefix=["bafar_poc_gabinete"],
    required_resource_keys={"sqlserver"},
    config_schema={
        "alert_type": str, "entity_key": str, "evaluation_key": str,
        "source_run_id": str, "incident_id": str, "config_version": str, "rule_version": str,
    },
    check_specs=[AssetCheckSpec(name="evaluation_conclusive", asset=["bafar_poc_gabinete", "operational_alerts"])],
)
def operational_alerts(context):
    now = datetime.now(timezone.utc)
    requested = context.op_config
    kind = requested["alert_type"]
    checks = {}
    outcome = "no_evidence"
    run_metadata = {"evaluation_run_id": context.run_id, **requested}
    metadata = dict(run_metadata)
    with closing(context.resources.sqlserver._get_connection()) as connection, connection:
        config, reason = _load_active_config(connection)
        if config is None:
            raise Failure(reason, metadata=run_metadata)
        if kind not in (*_OPERATIONAL_TYPES, "dagster_run_failure") or (kind, None) not in config["rules"]:
            raise Failure("No hay una regla operativa aplicable.", metadata=run_metadata)
        requested_rule = _select_rule(config, kind, requested["entity_key"])
        if (
            not requested_rule["is_enabled"] or config["version_token"] != requested["config_version"]
            or _version(requested_rule["rule_version"]) != requested["rule_version"]
        ):
            raise Failure("La configuración del detector cambió antes de evaluar.", metadata=run_metadata)
        incidents = _load_active_incidents(connection)
        detected = 0
        cleared = 0
        statuses = [status for status in _sensor_statuses(connection, config["sensor_lookback_hours"])
                    if status["sensor_id"] == requested["entity_key"]] if kind in ("connection_lost", "late_readings") else []
        dimensions = _sensor_dimensions(
            connection,
            [status["sensor_id"] for status in statuses],
        )
        ingestions = [row for row in _ingestions(connection) if row["ingestion_name"] == requested["entity_key"]] if kind in ("pipeline_failure", "pipeline_stale") else []
        failures = _failed_run_assets(
            context, config["dagster_failure_lookback_minutes"], config["dagster_failure_batch_size"], now,
            run_id=requested["source_run_id"],
        ) if kind == "dagster_run_failure" and requested["source_run_id"] else []
        current, reason = _load_active_config(connection)
        if (
            current is None or current["version_token"] != config["version_token"]
            or (kind, None) not in current["rules"]
            or _select_rule(current, kind, requested["entity_key"]) != requested_rule
        ):
            raise Failure(reason or "La configuración del detector cambió antes de persistir.")
        for status in statuses:
            entity_key = status["sensor_id"]
            for alert_type in (kind,):
                rule = _select_rule(config, alert_type, entity_key)
                if not rule["is_enabled"] or not _is_due(
                    config,
                    rule,
                    entity_key,
                    checks,
                    now,
                ):
                    continue
                _mark_checked(config, rule, entity_key, checks, now)
                incident = incidents.get((alert_type, entity_key))
                threshold = (
                    incident["no_readings_minutes"]
                    if incident is not None
                    and incident["no_readings_minutes"] is not None
                    else rule["no_readings_minutes"]
                )
                source_age = (
                    incident["source_max_age_minutes"]
                    if incident is not None
                    and incident["source_max_age_minutes"] is not None
                    else rule["source_max_age_minutes"]
                )
                metadata = {**run_metadata, "source_status": status}
                values = (
                    status["observed_at"],
                    status["last_reading_at"],
                    status["minutes_without_readings"],
                    status["modified_at"],
                )
                if any(value is None for value in values):
                    outcome = "incomplete_evidence"
                    continue
                observed_at = _utc(status["observed_at"])
                last_reading_at = _utc(status["last_reading_at"])
                modified_at = _utc(status["modified_at"])
                if (
                    observed_at > now
                    or last_reading_at > now
                    or modified_at > now
                    or now - modified_at
                    > timedelta(minutes=source_age)
                ):
                    outcome = "stale_or_future_evidence"
                    continue
                metadata = {
                    **run_metadata,
                    "dimension": dimensions.get(entity_key),
                    "measurements": {
                        "observed_at": observed_at.isoformat(),
                        "last_reading_at": last_reading_at.isoformat(),
                        "minutes_without_readings": (
                            status["minutes_without_readings"]
                        ),
                        "source_modified_at": modified_at.isoformat(),
                    },
                    "thresholds": {
                        "no_readings_minutes": threshold,
                        "source_max_age_minutes": source_age,
                    },
                }
                outcome = "recovery_not_confirmed" if incident is not None else "condition_absent"
                if status["minutes_without_readings"] >= threshold:
                    outcome = "condition_active"
                    if incident is None:
                        occurred_at = last_reading_at + timedelta(
                            minutes=threshold
                        )
                        outcome = "deduplicated" if occurred_at <= now else "future_threshold"
                        if occurred_at <= now and _record_detected(
                            connection,
                            rule,
                            "sensor_monitoring",
                            entity_key,
                            entity_key,
                            occurred_at,
                            (
                                f"El sensor excedió {threshold} minutos "
                                "sin lecturas."
                            ),
                            metadata,
                        ):
                            detected += 1
                    continue
                if (
                    incident is not None
                    and last_reading_at > _utc(incident["detected_at"])
                    and _record_cleared(
                        connection,
                        incident,
                        last_reading_at,
                        "Una lectura posterior confirmó la recuperación.",
                        metadata,
                    )
                ):
                    cleared += 1

        for ingestion in ingestions:
            entity_key = ingestion["ingestion_name"]
            status_code = (ingestion["last_run_status"] or "").upper()
            finished = ingestion["last_run_finished_at"]
            for alert_type in (kind,):
                rule = _select_rule(config, alert_type, entity_key)
                if not rule["is_enabled"] or not _is_due(
                    config,
                    rule,
                    entity_key,
                    checks,
                    now,
                ):
                    continue
                _mark_checked(config, rule, entity_key, checks, now)
                incident = incidents.get((alert_type, entity_key))
                metadata = {
                    **run_metadata,
                    "evidence": {
                        "last_run_status": status_code,
                        "last_run_started_at": (
                            _utc(ingestion["last_run_started_at"]).isoformat()
                            if ingestion["last_run_started_at"] is not None
                            else None
                        ),
                        "last_run_finished_at": (
                            _utc(finished).isoformat()
                            if finished is not None
                            else None
                        ),
                        "last_run_error": ingestion["last_run_error"],
                    }
                }
                outcome = "recovery_not_confirmed" if incident is not None else "condition_absent"
                if finished is not None and _utc(finished) > now:
                    outcome = "future_evidence"
                    continue
                if alert_type == "pipeline_failure":
                    if status_code == "FAILED":
                        outcome = "condition_active" if incident is not None else "deduplicated"
                    if status_code not in ("SUCCESS", "FAILED") or finished is None:
                        outcome = "incomplete_evidence"
                    if (
                        incident is not None
                        and status_code == "SUCCESS"
                        and finished is not None
                        and _utc(finished) > _utc(incident["detected_at"])
                        and _record_cleared(
                            connection,
                            incident,
                            _utc(finished),
                            (
                                "Una ejecución exitosa posterior confirmó "
                                "la recuperación."
                            ),
                            metadata,
                        )
                    ):
                        cleared += 1
                    elif (
                        incident is None
                        and status_code == "FAILED"
                        and finished is not None
                        and _utc(finished) <= now
                        and _record_detected(
                            connection,
                            rule,
                            "dagster_etl",
                            entity_key,
                            entity_key,
                            _utc(finished),
                            "La última ejecución de la ingesta falló.",
                            metadata,
                        )
                    ):
                        detected += 1
                    continue

                if status_code != "SUCCESS" or finished is None:
                    outcome = "no_success_evidence"
                    continue
                finished_at = _utc(finished)
                if (
                    incident is not None
                    and finished_at > _utc(incident["detected_at"])
                ):
                    if _record_cleared(
                        connection,
                        incident,
                        finished_at,
                        (
                            "Una ejecución exitosa posterior confirmó "
                            "la recuperación."
                        ),
                        metadata,
                    ):
                        cleared += 1
                    incident = None
                if incident is None:
                    current_interval = rule["expected_interval_minutes"]
                    occurred_at = finished_at + timedelta(
                        minutes=current_interval
                    )
                    if occurred_at <= now:
                        outcome = "deduplicated"
                        metadata["thresholds"] = {
                            "expected_interval_minutes": current_interval
                        }
                        if _record_detected(
                            connection,
                            rule,
                            "dagster_etl",
                            entity_key,
                            entity_key,
                            occurred_at,
                            (
                                "La ingesta excedió su intervalo esperado "
                                "sin una nueva ejecución exitosa."
                            ),
                            metadata,
                        ):
                            detected += 1

        if kind == "dagster_run_failure":
            for failure in failures:
                if failure["asset_key"] != requested["entity_key"]:
                    continue
                entity_key = failure["asset_key"]
                failure_key = f"{failure['run_id']}::{entity_key}"
                rule = _select_rule(
                    config,
                    "dagster_run_failure",
                    entity_key,
                )
                if not rule["is_enabled"] or not _is_due(
                    config,
                    rule,
                    failure_key,
                    checks,
                    now,
                ):
                    continue
                _mark_checked(config, rule, failure_key, checks, now)
                if failure["occurred_at"] > now:
                    outcome = "future_evidence"
                    continue
                metadata = {
                    **run_metadata,
                    "evidence": {
                        "run_id": failure["run_id"],
                        "job_name": failure["job_name"],
                        "step_key": failure["step_key"],
                        "asset_key": entity_key,
                    }
                }
                outcome = "deduplicated"
                if _record_detected(
                    connection,
                    rule,
                    "dagster",
                    failure["run_id"],
                    entity_key,
                    failure["occurred_at"],
                    (
                        f"El fallo del paso {failure['step_key']} impidió "
                        f"materializar {entity_key}."
                    ),
                    metadata,
                ):
                    detected += 1
            if requested["incident_id"]:
                recoveries = []
                cleared += _clear_dagster_failures(
                    context, connection, config, checks, now,
                    incident_id=requested["incident_id"], results=recoveries,
                )
                metadata["recoveries"] = recoveries
                if recoveries:
                    outcome = recoveries[0]["outcome"]
    if detected and cleared:
        outcome = "cleared_and_detected"
    elif detected:
        outcome = "detected"
    elif cleared:
        outcome = "cleared"
    conclusive = outcome not in {"no_evidence", "incomplete_evidence", "stale_or_future_evidence", "future_evidence", "future_threshold", "no_success_evidence", "no_recovery_evidence"}
    return MaterializeResult(
        metadata={
            **run_metadata, "outcome": outcome, "detected_count": detected, "cleared_count": cleared,
            "severity": requested_rule["severity"], "alert_rule_id": str(requested_rule["alert_rule_id"]),
            "evaluation": MetadataValue.json(json.loads(json.dumps(metadata, ensure_ascii=False, default=str))),
        },
        check_results=[AssetCheckResult(
            check_name="evaluation_conclusive", passed=conclusive, severity=AssetCheckSeverity.WARN,
            metadata={"entity_key": requested["entity_key"], "outcome": outcome},
        )],
    )
