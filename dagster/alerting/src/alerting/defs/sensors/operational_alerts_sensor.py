"""Detect operational alerts and persist them in each tenant SQL Server."""

import json
import os
import uuid
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import psycopg2
import yaml
from dagster import (
    DefaultSensorStatus,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from ..resources.alert_state_store import (
    ALERT_STATE_OPEN,
    NOTIFICATION_STATUS_SENT,
    AlertStateStoreResource,
)
from ..resources.mssql_reader import MssqlReaderResource


_ALERT_CONFIG_PATH = (
    Path(__file__).resolve().parent.parent.parent / "alert_config.yaml"
)

_SENSOR_ALERT_TYPES = frozenset({"connection_lost", "late_readings"})
_OPERATIONAL_ALERT_TYPES = _SENSOR_ALERT_TYPES | frozenset({
    "pipeline_failure",
    "pipeline_stale",
    "dagster_run_failure",
})
_TREND_ALERT_TYPE = "asset_row_count_trend"


def _load_config() -> Dict[str, Any]:
    """Load and return the alert_config.yaml contents."""
    with open(_ALERT_CONFIG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _get_recent_dagster_failures(
    hours: int = 2,
) -> List[Dict[str, Any]]:
    """Return recent failed Dagster runs with their code location."""
    dsn = (
        f"host={os.environ.get('DAGSTER_PG_HOSTNAME', 'dagster-postgres')} "
        f"port={os.environ.get('DAGSTER_PG_PORT', '5432')} "
        f"dbname={os.environ.get('DAGSTER_PG_DB', 'dagster')} "
        f"user={os.environ['DAGSTER_PG_USER']} "
        f"password={os.environ['DAGSTER_PG_PASSWORD']}"
    )
    query = """
        SELECT
            r.run_id,
            r.pipeline_name AS job_name,
            COALESCE(
                r.run_body::json->>'remote_job_origin',
                r.run_body::json->>'external_pipeline_origin'
            ) AS origin_json,
            r.status,
            r.create_timestamp,
            r.update_timestamp
        FROM runs r
        WHERE r.status = 'FAILURE'
          AND r.update_timestamp >= NOW() - (%s * INTERVAL '1 hour')
        ORDER BY r.update_timestamp DESC
        LIMIT 200
    """
    results: List[Dict[str, Any]] = []
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (hours,))
            for row in cur.fetchall():
                code_location = "unknown"
                try:
                    origin = json.loads(row[2]) if row[2] else {}
                    location_origin = (
                        origin.get("external_repository_origin", {}).get(
                            "repository_location_origin", {}
                        )
                        or origin.get("repository_origin", {}).get(
                            "code_location_origin", {}
                        )
                    )
                    code_location = location_origin.get(
                        "location_name", "unknown"
                    )
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
                results.append(
                    {
                        "run_id": row[0],
                        "job_name": row[1],
                        "code_location": code_location,
                        "status": row[3],
                        "created": row[4],
                        "updated": row[5],
                    }
                )
    return results


def _build_alert_key(
    user_code: str,
    alert_type: str,
    entity_id: str,
) -> str:
    return f"{user_code}::{alert_type}::{entity_id}"


def _resolve_user_code_databases(
    user_codes_cfg: Dict[str, Any],
) -> Dict[str, str]:
    return {
        user_code: os.environ.get(
            user_code_cfg.get("database_env", ""),
            user_code_cfg.get("database", user_code),
        )
        for user_code, user_code_cfg in user_codes_cfg.items()
    }


@sensor(
    name="operational_alerts_sensor",
    minimum_interval_seconds=300,
    required_resource_keys={"mssql_reader", "alert_state"},
    default_status=DefaultSensorStatus.RUNNING,
)
def operational_alerts_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    mssql: MssqlReaderResource = context.resources.mssql_reader
    state_store: AlertStateStoreResource = context.resources.alert_state

    config = _load_config()
    defaults = config.get("defaults", {})
    sensor_defaults = defaults.get("sensor_alerts", {})
    pipeline_defaults = defaults.get("pipeline_alerts", {})
    reminder_cfg = defaults.get("daily_reminders", {})
    quiet_cfg = defaults.get("quiet_hours", {})
    delivery_cfg = defaults.get("email_delivery", {})
    max_notification_attempts = max(
        1, int(delivery_cfg.get("max_notification_attempts", 3))
    )
    user_codes_cfg = config.get("user_codes", {})
    uc_databases = _resolve_user_code_databases(user_codes_cfg)

    reminder_timezone = ZoneInfo(
        reminder_cfg.get(
            "timezone",
            quiet_cfg.get("timezone", "America/Mexico_City"),
        )
    )
    reminder_now = datetime.now(reminder_timezone)
    reminders_silenced = False
    if quiet_cfg.get("enabled", False):
        quiet_start = time.fromisoformat(quiet_cfg["start"])
        quiet_end = time.fromisoformat(quiet_cfg["end"])
        current_time = datetime.now(
            ZoneInfo(quiet_cfg.get("timezone", "America/Mexico_City"))
        ).time().replace(tzinfo=None)
        if quiet_start < quiet_end:
            reminders_silenced = quiet_start <= current_time < quiet_end
        elif quiet_start > quiet_end:
            reminders_silenced = (
                current_time >= quiet_start or current_time < quiet_end
            )

    active_conditions: Dict[str, Dict[str, Any]] = {}
    for uc_name, uc_cfg in user_codes_cfg.items():
        database = uc_databases[uc_name]
        uc_sensor_cfg = {**sensor_defaults, **uc_cfg.get("sensor_alerts", {})}
        uc_pipeline_cfg = {
            **pipeline_defaults,
            **uc_cfg.get("pipeline_alerts", {}),
        }
        lookback = int(uc_sensor_cfg.get("lookback_hours", 2))
        ingestions_cfg = uc_cfg.get("ingestions", {}) or {}

        if uc_sensor_cfg.get("connection_lost", True):
            for alert in mssql.get_connection_lost_sensors(
                database, lookback, uc_name
            ):
                key = _build_alert_key(
                    uc_name, "connection_lost", alert.sensor_id
                )
                lost_at = (
                    alert.connection_lost_at.isoformat()
                    if alert.connection_lost_at is not None
                    else None
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "trigger_source": "sensor_monitoring",
                    "alert_type": "connection_lost",
                    "entity_id": alert.sensor_id,
                    "severity": "CRITICAL",
                    "details": (
                        "MinutesWithoutReadings: "
                        f"{alert.minutes_without_readings}, "
                        f"ConnectionLostAt: {alert.connection_lost_at}"
                    ),
                    "metadata": {
                        "reason": "El sensor reporta pérdida de comunicación.",
                        "measurements": {
                            "minutes_without_readings": (
                                alert.minutes_without_readings
                            ),
                            "connection_lost_at": lost_at,
                        },
                        "thresholds": {"lookback_hours": lookback},
                    },
                }

        if uc_sensor_cfg.get("late_readings", True):
            for alert in mssql.get_late_reading_sensors(
                database, lookback, uc_name
            ):
                key = _build_alert_key(
                    uc_name, "late_readings", alert.sensor_id
                )
                latest_time_span = (
                    alert.latest_time_span.isoformat()
                    if alert.latest_time_span is not None
                    else None
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "trigger_source": "sensor_monitoring",
                    "alert_type": "late_readings",
                    "entity_id": alert.sensor_id,
                    "severity": "WARNING",
                    "details": (
                        "MinutesWithoutReadings: "
                        f"{alert.minutes_without_readings}, "
                        f"LatestTimeSpan: {alert.latest_time_span}"
                    ),
                    "metadata": {
                        "reason": "El sensor presenta lecturas retrasadas.",
                        "measurements": {
                            "minutes_without_readings": (
                                alert.minutes_without_readings
                            ),
                            "latest_time_span": latest_time_span,
                        },
                        "thresholds": {"lookback_hours": lookback},
                    },
                }

        if uc_pipeline_cfg.get("ingestion_control_failed", True):
            for alert in mssql.get_failed_ingestions(database, uc_name):
                key = _build_alert_key(
                    uc_name, "pipeline_failure", alert.ingestion_name
                )
                finished_at = (
                    alert.last_run_finished_at.isoformat()
                    if alert.last_run_finished_at is not None
                    else None
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "trigger_source": "sensor_monitoring",
                    "alert_type": "pipeline_failure",
                    "entity_id": alert.ingestion_name,
                    "severity": "CRITICAL",
                    "details": (
                        f"LastRunStatus: {alert.last_run_status}, "
                        f"LastRunFinishedAt: {alert.last_run_finished_at}, "
                        f"Error: {alert.last_run_error}"
                    ),
                    "metadata": {
                        "reason": "La última ejecución de la ingesta falló.",
                        "evidence": {
                            "last_run_status": alert.last_run_status,
                            "last_run_finished_at": finished_at,
                            "last_run_error": alert.last_run_error,
                        },
                    },
                }

        if uc_pipeline_cfg.get("stale_detection", True) and ingestions_cfg:
            intervals = {
                name: ingestion_cfg.get(
                    "expected_interval_minutes",
                    uc_pipeline_cfg.get(
                        "default_expected_interval_minutes", 720
                    ),
                )
                for name, ingestion_cfg in ingestions_cfg.items()
            }
            default_interval = int(
                uc_pipeline_cfg.get(
                    "default_expected_interval_minutes", 720
                )
            )
            for alert in mssql.get_stale_ingestions(
                database, intervals, default_interval, uc_name
            ):
                key = _build_alert_key(
                    uc_name, "pipeline_stale", alert.ingestion_name
                )
                finished_at = (
                    alert.last_run_finished_at.isoformat()
                    if alert.last_run_finished_at is not None
                    else None
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "trigger_source": "sensor_monitoring",
                    "alert_type": "pipeline_stale",
                    "entity_id": alert.ingestion_name,
                    "severity": "WARNING",
                    "details": (
                        f"LastRunFinishedAt: {alert.last_run_finished_at}, "
                        f"{alert.last_run_error}"
                    ),
                    "metadata": {
                        "reason": "La ingesta excedió su intervalo esperado.",
                        "measurements": {
                            "last_run_finished_at": finished_at,
                        },
                        "thresholds": {
                            "expected_interval_minutes": intervals.get(
                                alert.ingestion_name, default_interval
                            )
                        },
                        "evidence": {
                            "last_run_error": alert.last_run_error,
                        },
                    },
                }

    if pipeline_defaults.get("dagster_run_failure", True):
        for run in _get_recent_dagster_failures(hours=2):
            user_code = run["code_location"]
            if user_code not in user_codes_cfg:
                continue
            key = _build_alert_key(
                user_code, "dagster_run_failure", run["run_id"]
            )
            created_at = (
                run["created"].isoformat()
                if run["created"] is not None
                else None
            )
            updated_at = (
                run["updated"].isoformat()
                if run["updated"] is not None
                else None
            )
            active_conditions[key] = {
                "user_code": user_code,
                "trigger_source": "dagster",
                "alert_type": "dagster_run_failure",
                "entity_id": run["run_id"],
                "severity": "CRITICAL",
                "details": (
                    f"Job: {run['job_name']}, Updated: {run['updated']}"
                ),
                "metadata": {
                    "reason": "Una ejecución de Dagster terminó con error.",
                    "evidence": {
                        "run_id": run["run_id"],
                        "job_name": run["job_name"],
                        "status": run["status"],
                        "created_at": created_at,
                        "updated_at": updated_at,
                    },
                },
            }

    all_existing_alerts = {}
    for user_code, database in uc_databases.items():
        all_existing_alerts.update(
            state_store.get_active_alerts(database, user_code)
        )
    existing_alerts = {
        key: alert
        for key, alert in all_existing_alerts.items()
        if alert.alert_type in _OPERATIONAL_ALERT_TYPES
    }

    connection_recoveries = {}
    for user_code, database in uc_databases.items():
        sensor_ids = [
            alert.entity_id
            for key, alert in existing_alerts.items()
            if key not in active_conditions
            and alert.user_code == user_code
            and alert.alert_type == "connection_lost"
        ]
        connection_recoveries.update(
            {
                _build_alert_key(user_code, "connection_lost", sensor_id): (
                    recovery
                )
                for sensor_id, recovery in mssql.get_sensor_recoveries(
                    database, sensor_ids, user_code
                ).items()
            }
        )

    for key, alert in existing_alerts.items():
        if (
            key not in active_conditions
            and alert.alert_type == "connection_lost"
            and key not in connection_recoveries
        ):
            active_conditions[key] = {
                "user_code": alert.user_code,
                "trigger_source": alert.trigger_source,
                "alert_type": alert.alert_type,
                "entity_id": alert.entity_id,
                "severity": alert.severity,
                "details": alert.details,
                "metadata": alert.metadata,
            }

    sensor_ids_by_user_code: Dict[str, List[str]] = {}
    for condition in active_conditions.values():
        if condition["alert_type"] in _SENSOR_ALERT_TYPES:
            sensor_ids_by_user_code.setdefault(
                condition["user_code"], []
            ).append(condition["entity_id"])
    for key, alert in existing_alerts.items():
        if key not in active_conditions and alert.alert_type in _SENSOR_ALERT_TYPES:
            sensor_ids_by_user_code.setdefault(alert.user_code, []).append(
                alert.entity_id
            )
    dimensions = {}
    for user_code, sensor_ids in sensor_ids_by_user_code.items():
        try:
            dimensions[user_code] = mssql.get_sensor_dimensions(
                uc_databases[user_code], sensor_ids, user_code
            )
        except Exception:
            context.log.warning(
                "Failed to fetch sensor dimensions for %s.",
                user_code,
                exc_info=True,
            )
            dimensions[user_code] = {}
    for condition in active_conditions.values():
        dimension = dimensions.get(condition["user_code"], {}).get(
            condition["entity_id"]
        )
        if dimension is not None:
            condition["metadata"]["context"] = {
                "location_name": dimension.location_name,
                "sublocation_name": dimension.sublocation_name,
                "sublocation_type": dimension.sublocation_type,
                "device_name": dimension.device_name,
                "device_type": dimension.device_type,
                "sensor_name": dimension.sensor_name,
                "sensor_type": dimension.sensor_type,
            }

    opened = 0
    for key, condition in active_conditions.items():
        existing = existing_alerts.get(key)
        alert_key = (
            existing.alert_key
            if existing is not None
            else f"{key}::{uuid.uuid4()}"
        )
        state_store.upsert_alert(
            database=uc_databases[condition["user_code"]],
            alert_key=alert_key,
            user_code=condition["user_code"],
            trigger_source=condition["trigger_source"],
            alert_type=condition["alert_type"],
            entity_id=condition["entity_id"],
            severity=condition["severity"],
            details=condition["details"],
            metadata=condition["metadata"],
            state=ALERT_STATE_OPEN,
            max_notification_attempts=max_notification_attempts,
        )
        if existing is None:
            opened += 1

    resolved = 0
    for key, alert in existing_alerts.items():
        if key in active_conditions:
            continue
        details = alert.details
        metadata = dict(alert.metadata)
        metadata["reason"] = "La condición dejó de detectarse."
        if alert.alert_type == "connection_lost":
            recovery = connection_recoveries.get(key)
            if recovery is None:
                continue
            details = (
                "CommunicationResumedAt: "
                f"{recovery.communication_resumed_at:%Y-%m-%d %H:%M:%S} UTC, "
                "LastReadingAt: "
                f"{recovery.last_reading_at:%Y-%m-%d %H:%M:%S} UTC"
            )
            metadata["reason"] = "La comunicación del sensor se restableció."
            metadata["evidence"] = {
                "communication_resumed_at": (
                    recovery.communication_resumed_at.isoformat()
                ),
                "last_reading_at": recovery.last_reading_at.isoformat(),
            }
            dimension = dimensions.get(alert.user_code, {}).get(
                alert.entity_id
            )
            if dimension is not None:
                metadata["context"] = {
                    "location_name": dimension.location_name,
                    "sublocation_name": dimension.sublocation_name,
                    "sublocation_type": dimension.sublocation_type,
                    "device_name": dimension.device_name,
                    "device_type": dimension.device_type,
                    "sensor_name": dimension.sensor_name,
                    "sensor_type": dimension.sensor_type,
                }
        state_store.mark_resolved(
            uc_databases[alert.user_code],
            alert.user_code,
            alert.alert_key,
            details,
            metadata,
        )
        resolved += 1

    reminded = 0
    suppressed = 0
    reminder_time = time.fromisoformat(reminder_cfg.get("time", "08:00"))
    if reminder_cfg.get("enabled", True) and reminder_now.time() >= reminder_time:
        due_operational = []
        due_trends = []
        for key, alert in all_existing_alerts.items():
            if key not in active_conditions and alert.alert_type != _TREND_ALERT_TYPE:
                continue
            if (
                alert.notification_status != NOTIFICATION_STATUS_SENT
                or alert.last_notified_at is None
            ):
                continue
            if alert.alert_type == _TREND_ALERT_TYPE:
                due_trends.append(alert)
                continue
            last_notified_at = alert.last_notified_at
            if last_notified_at.tzinfo is None:
                last_notified_at = last_notified_at.replace(
                    tzinfo=timezone.utc
                )
            if (
                last_notified_at.astimezone(reminder_timezone).date()
                >= reminder_now.date()
            ):
                continue
            due_operational.append(alert)

        tenants_with_operational_reminder = {
            alert.user_code for alert in due_operational
        }
        due_alerts = due_operational + [
            alert
            for alert in due_trends
            if alert.user_code in tenants_with_operational_reminder
        ]
        if reminders_silenced:
            suppressed = len(due_alerts)
        else:
            for alert in due_alerts:
                condition = active_conditions.get(
                    _build_alert_key(
                        alert.user_code, alert.alert_type, alert.entity_id
                    )
                )
                state_store.queue_reminder(
                    uc_databases[alert.user_code],
                    alert.user_code,
                    alert.alert_key,
                    condition["details"] if condition else alert.details,
                    condition["metadata"] if condition else alert.metadata,
                )
                reminded += 1

    summary = (
        f"Checked {len(user_codes_cfg)} user_codes: "
        f"{len(active_conditions)} active conditions, "
        f"{opened} opened, {reminded} reminders queued, "
        f"{resolved} resolutions queued, {suppressed} reminders suppressed"
    )
    context.log.info(summary)
    return SkipReason(summary)
