import json
import re
import unicodedata
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Any

from dagster import (
    AssetKey,
    AssetRecordsFilter,
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    Failure,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..jobs.operational_alerts_job import operational_alerts_job


_USER_CODE = "bafar"
_SIGNAL_TYPE_IDS = {
    "DETECTED": "20000000-0000-0000-0000-000000000001",
    "CLEARED": "20000000-0000-0000-0000-000000000002",
}
_TRIGGER_SOURCE_IDS = {
    "sensor_monitoring": "40000000-0000-0000-0000-000000000001",
    "dagster_etl": "40000000-0000-0000-0000-000000000002",
    "dagster": "40000000-0000-0000-0000-000000000003",
}
_OPERATIONAL_TYPES = (
    "connection_lost",
    "late_readings",
    "pipeline_failure",
    "pipeline_stale",
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _version(value: Any) -> str:
    return bytes(value).hex() if value is not None else ""


def _rows(cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _load_active_config(connection) -> tuple[dict[str, Any] | None, str | None]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                config.alert_config_id,
                config.version_number,
                config.version AS config_version,
                operational.sensor_lookback_hours,
                operational.dagster_failure_lookback_minutes,
                operational.dagster_failure_batch_size,
                trend.capture_interval_seconds,
                trend.metadata_key,
                trend.backfill_days,
                trend.batch_size
            FROM alerting.alert_config AS config
            INNER JOIN alerting.dim_config_status AS status
                ON status.config_status_id = config.config_status_id
            INNER JOIN alerting.alert_detector_operational_config AS operational
                ON operational.alert_config_id = config.alert_config_id
            INNER JOIN alerting.alert_detector_row_count_config AS trend
                ON trend.alert_config_id = config.alert_config_id
            WHERE
                config.user_code = ?
                AND status.code = N'ACTIVE'
            """,
            _USER_CODE,
        )
        rows = _rows(cursor)
        if len(rows) != 1:
            return None, "No existe una configuración activa única."
        config = rows[0]
        cursor.execute(
            """
            SELECT CodigoValidacion
            FROM serving.vwValidacionConfiguracionesAlerta
            WHERE
                ConfiguracionAlertaId = ?
                AND EsBloqueante = 1
            """,
            config["alert_config_id"],
        )
        problems = [row[0] for row in cursor.fetchall()]
        if problems:
            return None, "La configuración activa tiene errores bloqueantes."
        cursor.execute(
            """
            SELECT
                alert_rule.alert_rule_id,
                alert_rule.alert_config_id,
                alert_rule.alert_type_id,
                alert_type.code AS alert_type,
                alert_rule.entity_key,
                alert_rule.is_enabled,
                alert_rule.severity_id,
                severity.code AS severity,
                alert_rule.evaluation_interval_seconds,
                alert_rule.version AS rule_version,
                no_readings.no_readings_minutes,
                no_readings.source_max_age_minutes,
                pipeline_stale.expected_interval_minutes,
                trend.warning_drop_pct,
                trend.critical_drop_pct,
                trend.minimum_observed_days_7d,
                trend.minimum_observed_days_28d,
                trend.critical_severity_id
            FROM alerting.alert_rule AS alert_rule
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = alert_rule.alert_type_id
            INNER JOIN alerting.dim_severity AS severity
                ON severity.severity_id = alert_rule.severity_id
            LEFT JOIN alerting.alert_rule_no_readings AS no_readings
                ON no_readings.alert_rule_id = alert_rule.alert_rule_id
            LEFT JOIN alerting.alert_rule_pipeline_stale AS pipeline_stale
                ON pipeline_stale.alert_rule_id = alert_rule.alert_rule_id
            LEFT JOIN alerting.alert_rule_row_count_trend AS trend
                ON trend.alert_rule_id = alert_rule.alert_rule_id
            WHERE alert_rule.alert_config_id = ?
            """,
            config["alert_config_id"],
        )
        rules = _rows(cursor)
    config["rules"] = {
        (row["alert_type"], row["entity_key"]): row
        for row in rules
    }
    config["version_token"] = _version(config["config_version"])
    return config, None


def _select_rule(
    config: dict[str, Any],
    alert_type: str,
    entity_key: str,
) -> dict[str, Any]:
    rules = config["rules"]
    return rules.get((alert_type, entity_key), rules[(alert_type, None)])


def _is_due(
    config: dict[str, Any],
    rule: dict[str, Any],
    entity_key: str,
    checks: dict[str, Any],
    now: datetime,
) -> bool:
    key = f"{rule['alert_type']}::{entity_key}"
    previous = checks.get(key, {})
    token = f"{config['version_token']}:{_version(rule['rule_version'])}"
    return (
        previous.get("version") != token
        or now.timestamp() - previous.get("at", 0)
        >= rule["evaluation_interval_seconds"]
    )


def _mark_checked(
    config: dict[str, Any],
    rule: dict[str, Any],
    entity_key: str,
    checks: dict[str, Any],
    now: datetime,
) -> None:
    checks[f"{rule['alert_type']}::{entity_key}"] = {
        "version": (
            f"{config['version_token']}:{_version(rule['rule_version'])}"
        ),
        "at": now.timestamp(),
    }


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value).strip("-")
    return slug or "entidad"


def _alert_code(
    alert_type: str,
    entity_key: str,
    dimension: dict[str, Any] | None = None,
) -> str:
    if alert_type in {"connection_lost", "late_readings"}:
        dimension = dimension or {}
        sensor_type = dimension.get("sensor_type")
        sublocation = dimension.get("sublocation_name")
        if sensor_type is None and sublocation is None:
            suffix = _slug(entity_key)
        else:
            suffix = (
                f"{_slug(sensor_type or entity_key)}-"
                f"{_slug(sublocation or entity_key)}"
            )
        prefix = (
            "sin-senal"
            if alert_type == "connection_lost"
            else "lecturas-retrasadas"
        )
        return f"{prefix}-{suffix}"
    prefixes = {
        "pipeline_failure": "fallo-ingesta",
        "pipeline_stale": "ingesta-atrasada",
        "dagster_run_failure": "fallo-dagster",
        "asset_row_count_trend": "caida-volumen",
    }
    return f"{prefixes[alert_type]}-{_slug(entity_key)}"


def _load_active_incidents(connection) -> dict[tuple[str, str], dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                incident.alert_id,
                alert_type.code AS alert_type,
                incident.entity_id,
                incident.entity_key,
                incident.detected_at,
                no_readings.no_readings_minutes,
                no_readings.source_max_age_minutes,
                pipeline_stale.expected_interval_minutes
            FROM alerting.alert_incident AS incident
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = incident.alert_type_id
            LEFT JOIN alerting.alert_rule_no_readings AS no_readings
                ON no_readings.alert_rule_id = incident.alert_rule_id
            LEFT JOIN alerting.alert_rule_pipeline_stale AS pipeline_stale
                ON pipeline_stale.alert_rule_id = incident.alert_rule_id
            WHERE
                incident.user_code = ?
                AND incident.is_point_in_time = 0
                AND incident.cleared_at IS NULL
                AND alert_type.code IN (
                    N'connection_lost',
                    N'late_readings',
                    N'pipeline_failure',
                    N'pipeline_stale'
                )
            """,
            _USER_CODE,
        )
        return {
            (row["alert_type"], row["entity_key"]): row
            for row in _rows(cursor)
            if row["entity_key"] is not None
        }


def _record_detected(
    connection,
    rule: dict[str, Any],
    trigger_source: str,
    entity_id: str,
    entity_key: str,
    occurred_at: datetime,
    details: str,
    metadata: dict[str, Any],
    is_point_in_time: bool = False,
    severity_id: Any = None,
) -> bool:
    alert_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    is_occurrence = is_point_in_time or rule["alert_type"] == "dagster_run_failure"
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @inserted BIT = 0;

            IF (
                ? = 1
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_id = ?
                        AND entity_key = ?
                )
            )
            OR (
                ? = 0
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_key = ?
                        AND is_point_in_time = 0
                        AND cleared_at IS NULL
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                    WHERE
                        user_code = ?
                        AND alert_type_id = ?
                        AND entity_key = ?
                        AND is_point_in_time = 0
                        AND detected_at = ?
                )
            )
                BEGIN
                    INSERT INTO alerting.alert_incident (
                        alert_id,
                        alert_code,
                        user_code,
                        alert_type_id,
                        severity_id,
                        trigger_source_id,
                        entity_id,
                        entity_key,
                        alert_config_id,
                        alert_rule_id,
                        detected_at,
                        details,
                        metadata_json,
                        is_point_in_time,
                        is_legacy
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0);

                    INSERT INTO alerting.alert_signal_event (
                        event_id,
                        alert_id,
                        signal_type_id,
                        occurred_at,
                        details,
                        metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?);

                    SET @inserted = 1;
                END;

            COMMIT TRANSACTION;
            SELECT @inserted;
            """,
            int(is_occurrence),
            _USER_CODE,
            rule["alert_type_id"],
            entity_id,
            entity_key,
            int(is_occurrence),
            _USER_CODE,
            rule["alert_type_id"],
            entity_key,
            _USER_CODE,
            rule["alert_type_id"],
            entity_key,
            occurred_at,
            alert_id,
            _alert_code(rule["alert_type"], entity_key, metadata.get("dimension")),
            _USER_CODE,
            rule["alert_type_id"],
            severity_id or rule["severity_id"],
            _TRIGGER_SOURCE_IDS[trigger_source],
            entity_id,
            entity_key,
            rule["alert_config_id"],
            rule["alert_rule_id"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
            int(is_point_in_time),
            event_id,
            alert_id,
            _SIGNAL_TYPE_IDS["DETECTED"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
        )
        return bool(cursor.fetchone()[0])


def _record_cleared(
    connection,
    incident: dict[str, Any],
    occurred_at: datetime,
    details: str,
    metadata: dict[str, Any],
) -> bool:
    event_id = str(uuid.uuid4())
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            DECLARE @cleared BIT = 0;

            IF EXISTS (
                SELECT 1
                FROM alerting.alert_incident WITH (UPDLOCK, HOLDLOCK)
                WHERE
                    alert_id = ?
                    AND cleared_at IS NULL
                    AND detected_at < ?
            )
                BEGIN
                    UPDATE alerting.alert_incident
                    SET cleared_at = ?
                    WHERE alert_id = ?;

                    IF NOT EXISTS (
                        SELECT 1
                        FROM alerting.alert_signal_event WITH (UPDLOCK, HOLDLOCK)
                        WHERE
                            alert_id = ?
                            AND signal_type_id = ?
                    )
                        INSERT INTO alerting.alert_signal_event (
                            event_id,
                            alert_id,
                            signal_type_id,
                            occurred_at,
                            details,
                            metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?);

                    SET @cleared = 1;
                END;

            COMMIT TRANSACTION;
            SELECT @cleared;
            """,
            incident["alert_id"],
            occurred_at,
            occurred_at,
            incident["alert_id"],
            incident["alert_id"],
            _SIGNAL_TYPE_IDS["CLEARED"],
            event_id,
            incident["alert_id"],
            _SIGNAL_TYPE_IDS["CLEARED"],
            occurred_at,
            details,
            json.dumps(metadata, ensure_ascii=False, default=str),
        )
        return bool(cursor.fetchone()[0])


def _sensor_statuses(connection, lookback_hours: int) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            WITH UltimoEstado AS (
                SELECT
                    LOWER(CONVERT(VARCHAR(36), SensorId)) AS sensor_id,
                    TimeSpan AS observed_at,
                    LastReadingAt AS last_reading_at,
                    MinutesWithoutReadings AS minutes_without_readings,
                    ModifiedAt AS modified_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY SensorId
                        ORDER BY TimeSpan DESC
                    ) AS row_number
                FROM observability.factRedingsAudits
                WHERE TimeSpan >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            )
            SELECT
                sensor_id,
                observed_at,
                last_reading_at,
                minutes_without_readings,
                modified_at
            FROM UltimoEstado
            WHERE row_number = 1
            """,
            -lookback_hours,
        )
        return _rows(cursor)


def _sensor_dimensions(
    connection,
    sensor_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not sensor_ids:
        return {}
    placeholders = ",".join("?" for _ in sensor_ids)
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                LOWER(CONVERT(VARCHAR(36), SensorId)) AS sensor_id,
                location_name,
                sublocation_name,
                sublocation_type,
                device_name,
                device_type,
                sensor_name,
                sensor_type
            FROM dwh.dimSidonProdDimensions
            WHERE LOWER(CONVERT(VARCHAR(36), SensorId)) IN ({placeholders})
            """,
            sensor_ids,
        )
        return {row["sensor_id"]: row for row in _rows(cursor)}


def _ingestions(connection) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                IngestionName AS ingestion_name,
                LastRunStatus AS last_run_status,
                LastRunStartedAt AS last_run_started_at,
                LastRunFinishedAt AS last_run_finished_at,
                LastRunError AS last_run_error
            FROM ctl.IngestionControl
            WHERE IsActive = 1
            """
        )
        return _rows(cursor)


def _code_location(run: Any) -> str | None:
    origin = getattr(run, "remote_job_origin", None)
    if origin is None:
        return None
    repository_origin = getattr(origin, "repository_origin", None)
    code_location_origin = getattr(
        repository_origin,
        "code_location_origin",
        None,
    )
    return getattr(code_location_origin, "location_name", None)


def _failed_run_assets(
    context: SensorEvaluationContext,
    lookback_minutes: int,
    batch_size: int,
    now: datetime,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.FAILURE],
            updated_after=now - timedelta(minutes=lookback_minutes) if run_id is None else None,
            run_ids=[run_id] if run_id is not None else None,
        ),
        limit=batch_size,
    )
    failures: dict[tuple[str, str], dict[str, Any]] = {}
    for run_record in run_records:
        run = run_record.dagster_run
        if _code_location(run) != _USER_CODE or run.job_name == "operational_alerts_job":
            continue
        snapshot_id = getattr(run, "execution_plan_snapshot_id", None)
        if snapshot_id is None:
            continue
        snapshot = context.instance.get_execution_plan_snapshot(snapshot_id)
        if snapshot is None:
            continue
        step_keys = set(snapshot.step_keys_to_execute)
        steps = {
            step.key: step for step in snapshot.steps if step.key in step_keys
        }
        downstream = {step_key: set() for step_key in steps}
        for step_key, upstream_steps in snapshot.step_deps.items():
            if step_key not in steps:
                continue
            for upstream_step in upstream_steps:
                if upstream_step in downstream:
                    downstream[upstream_step].add(step_key)
        materialized = {
            record.asset_key
            for record in context.instance.get_records_for_run(
                run.run_id,
                of_type=DagsterEventType.ASSET_MATERIALIZATION,
            ).records
        }
        records = context.instance.get_records_for_run(
            run.run_id,
            of_type=DagsterEventType.STEP_FAILURE,
        ).records
        for record in records:
            entry = record.event_log_entry
            step = steps.get(entry.step_key)
            if step is None or entry.timestamp is None:
                continue
            occurred_at = datetime.fromtimestamp(entry.timestamp, timezone.utc)
            pending = [step.key]
            visited = set()
            while pending:
                step_key = pending.pop()
                if step_key in visited:
                    continue
                visited.add(step_key)
                pending.extend(sorted(downstream[step_key] - visited))
                for output in steps[step_key].outputs:
                    entity_key = output.entity_key
                    if (
                        not isinstance(entity_key, AssetKey)
                        or entity_key in materialized
                    ):
                        continue
                    asset_key = entity_key.to_user_string()
                    failures[(run.run_id, asset_key)] = {
                        "run_id": run.run_id,
                        "job_name": run.job_name,
                        "step_key": entry.step_key,
                        "asset_key": asset_key,
                        "occurred_at": occurred_at,
                    }
    return list(failures.values())


def _clear_dagster_failures(
    context: SensorEvaluationContext,
    connection,
    config: dict[str, Any],
    checks: dict[str, Any],
    now: datetime,
    incident_id: str | None = None,
    results: list[dict[str, Any]] | None = None,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                incident.alert_id,
                incident.entity_id,
                incident.entity_key,
                incident.detected_at
            FROM alerting.alert_incident AS incident
            INNER JOIN alerting.dim_alert_type AS alert_type
                ON alert_type.alert_type_id = incident.alert_type_id
            WHERE
                incident.user_code = ?
                AND alert_type.code = N'dagster_run_failure'
                AND incident.cleared_at IS NULL
                AND incident.entity_key IS NOT NULL
            """,
            _USER_CODE,
        )
        incidents = _rows(cursor)
    cleared = 0
    for incident in incidents:
        if incident_id is not None and str(incident["alert_id"]) != incident_id:
            continue
        entity_key = incident["entity_key"]
        if incident_id is not None and entity_key != context.op_config["entity_key"]:
            raise Failure("La incidencia no corresponde a la entidad solicitada.")
        rule = _select_rule(config, "dagster_run_failure", entity_key)
        recovery_key = f"recovery::{incident['alert_id']}"
        if not rule["is_enabled"] or not _is_due(
            config, rule, recovery_key, checks, now
        ):
            continue
        detected_at = _utc(incident["detected_at"])
        records_filter = AssetRecordsFilter(
            asset_key=AssetKey.from_user_string(entity_key),
            after_timestamp=detected_at.timestamp(),
            before_timestamp=now.timestamp(),
        )
        cursor = None
        recovered = False
        result = {"incident_id": str(incident["alert_id"]), "outcome": "no_recovery_evidence"}
        while not recovered:
            page = context.instance.fetch_materializations(
                records_filter,
                limit=config["dagster_failure_batch_size"],
                cursor=cursor,
                ascending=True,
            )
            for record in page.records:
                occurred_at = datetime.fromtimestamp(record.timestamp, timezone.utc)
                if (
                    record.run_id == incident["entity_id"]
                    or occurred_at <= detected_at
                    or occurred_at > now
                ):
                    continue
                run = context.instance.get_run_by_id(record.run_id)
                if _code_location(run) != _USER_CODE:
                    continue
                metadata = {
                    "evidence": {
                        "failed_run_id": incident["entity_id"],
                        "recovery_run_id": record.run_id,
                        "asset_key": entity_key,
                        "materialization_storage_id": record.storage_id,
                    }
                }
                if incident_id is not None:
                    current, reason = _load_active_config(connection)
                    if (
                        current is None or current["version_token"] != config["version_token"]
                        or _select_rule(current, "dagster_run_failure", entity_key) != rule
                    ):
                        raise Failure(reason or "La configuración del detector cambió antes de confirmar la recuperación.")
                    metadata["evaluation_run_id"] = context.run_id
                result.update(metadata, outcome="deduplicated")
                if _record_cleared(
                    connection,
                    incident,
                    occurred_at,
                    (
                        "Una materialización exitosa posterior confirmó "
                        f"la recuperación de {entity_key}."
                    ),
                    metadata,
                ):
                    cleared += 1
                    result["outcome"] = "cleared"
                recovered = True
                break
            if not page.has_more:
                break
            cursor = page.cursor
        _mark_checked(config, rule, recovery_key, checks, now)
        if results is not None:
            results.append(result)
    return cleared


@sensor(
    name="operational_alerts_sensor",
    job=operational_alerts_job,
    minimum_interval_seconds=30,
    required_resource_keys={"sqlserver"},
    default_status=DefaultSensorStatus.STOPPED,
)
def operational_alerts_sensor(
    context: SensorEvaluationContext,
) -> RunRequest | SkipReason:
    now = datetime.now(timezone.utc)
    try:
        state = json.loads(context.cursor or "{}")
    except (json.JSONDecodeError, TypeError):
        state = {}
    pending = state.setdefault("pending", {})
    checks = state.setdefault("checks", {})
    in_flight = state.get("in_flight")
    if in_flight:
        runs = context.instance.get_runs(filters=RunsFilter(
            job_name=operational_alerts_job.name,
            tags={"user_code": _USER_CODE, "dagster/sensor_name": "operational_alerts_sensor", "dagster/run_key": in_flight["run_key"]},
        ), limit=1)
        if not runs:
            return RunRequest(
                run_key=in_flight["run_key"],
                run_config={"ops": {"bafar__operational_alerts": {"config": in_flight["config"]}}},
                tags={"alert_type": in_flight["config"]["alert_type"], "entity_key": in_flight["config"]["entity_key"]},
            )
        if not runs[0].is_finished:
            return SkipReason("La evaluación operativa anterior sigue pendiente o en ejecución.")
        if runs[0].status == DagsterRunStatus.SUCCESS:
            del pending[in_flight["pending_key"]]
        del state["in_flight"]
        context.update_cursor(json.dumps(state))
    with closing(context.resources.sqlserver._get_connection()) as connection:
        config, reason = _load_active_config(connection)
        if config is None:
            return SkipReason(reason)
        candidates = []
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT LOWER(CONVERT(VARCHAR(36), SensorId)) AS entity_key
                FROM observability.factRedingsAudits
                WHERE TimeSpan >= DATEADD(HOUR, ?, SYSUTCDATETIME())
                ORDER BY entity_key
            """, -config["sensor_lookback_hours"])
            for row in _rows(cursor):
                candidates.extend((kind, row["entity_key"], "", "") for kind in ("connection_lost", "late_readings"))
            cursor.execute("""
                SELECT IngestionName AS entity_key
                FROM ctl.IngestionControl WHERE IsActive = 1
                ORDER BY IngestionName
            """)
            for row in _rows(cursor):
                candidates.extend((kind, row["entity_key"], "", "") for kind in ("pipeline_failure", "pipeline_stale"))
            if any(rule["alert_type"] == "dagster_run_failure" and rule["is_enabled"] for rule in config["rules"].values()):
                for failure in _failed_run_assets(
                    context, config["dagster_failure_lookback_minutes"], config["dagster_failure_batch_size"], now,
                ):
                    candidates.append(("dagster_run_failure", failure["asset_key"], failure["run_id"], ""))
                cursor.execute("""
                    SELECT incident.alert_id, incident.entity_key
                    FROM alerting.alert_incident AS incident
                    INNER JOIN alerting.dim_alert_type AS alert_type ON alert_type.alert_type_id = incident.alert_type_id
                    WHERE incident.user_code = ? AND alert_type.code = N'dagster_run_failure'
                        AND incident.cleared_at IS NULL AND incident.entity_key IS NOT NULL
                    ORDER BY incident.detected_at, incident.alert_id
                """, _USER_CODE)
                candidates.extend(("dagster_run_failure", row["entity_key"], "", str(row["alert_id"])) for row in _rows(cursor))
        for kind, entity_key, source_run_id, incident_id in candidates:
            evaluation_key = f"recovery::{incident_id}" if incident_id else f"{source_run_id}::{entity_key}" if source_run_id else entity_key
            pending[f"{kind}::{evaluation_key}"] = {
                "alert_type": kind, "entity_key": entity_key, "evaluation_key": evaluation_key,
                "source_run_id": source_run_id, "incident_id": incident_id,
            }
        for key, candidate in list(pending.items()):
            kind, entity_key = candidate["alert_type"], candidate["entity_key"]
            if (kind, None) not in config["rules"] or not _select_rule(config, kind, entity_key)["is_enabled"]:
                del pending[key]
        context.update_cursor(json.dumps(state))
        for key in sorted(pending, key=lambda key: checks.get(key, {}).get("at", 0)):
            candidate = pending[key]
            rule = _select_rule(config, candidate["alert_type"], candidate["entity_key"])
            if not _is_due(config, rule, candidate["evaluation_key"], checks, now):
                continue
            _mark_checked(config, rule, candidate["evaluation_key"], checks, now)
            state["attempt"] = state.get("attempt", 0) + 1
            run_key = str(uuid.uuid5(uuid.NAMESPACE_URL, json.dumps([
                _USER_CODE, "operational_alerts", key, state["attempt"], config["version_token"], _version(rule["rule_version"]),
            ])))
            run_config = {**candidate, "config_version": config["version_token"], "rule_version": _version(rule["rule_version"])}
            state["in_flight"] = {"pending_key": key, "run_key": run_key, "config": run_config}
            context.update_cursor(json.dumps(state))
            return RunRequest(
                run_key=run_key,
                run_config={"ops": {"bafar__operational_alerts": {"config": run_config}}},
                tags={"alert_type": candidate["alert_type"], "entity_key": candidate["entity_key"]},
            )
    return SkipReason("No hay entidades operativas pendientes de evaluación o esperan su intervalo.")
