"""Central operational alerting sensor.

Every 5 minutes this sensor:
1. Reads alert_config.yaml for the list of user_codes and their expected
   ingestion cadences.
2. Queries each user_code's SQL Server database (read-only) for:
   - Sensors with IsConnectionLost = 1  (observability.factRedingsAudits)
   - Sensors with HasLateReadings = 1   (observability.factRedingsAudits)
   - Ingestions with LastRunStatus = 'FAILED' (ctl.IngestionControl)
   - Ingestions whose LastRunFinishedAt exceeds the expected interval
3. Queries Dagster PostgreSQL for recent runs with status = FAILURE.
4. Reconciles detected conditions against the persisted alert state,
   groups all resulting notifications by user_code, enriches sensor
   alerts with dimension context from dwh.dimSidonProdDimensions, and
   sends **one consolidated email per user_code**.
"""

import json
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
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
    ALERT_STATE_REMINDER,
    AlertStateStoreResource,
)
from ..resources.email_notifier import (
    AlertPayload,
    ConsolidatedPayload,
    EmailNotifierResource,
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
    """Query Dagster PostgreSQL for runs with status FAILURE in the
    last *hours* hours.

    Returns a list of dicts with keys: run_id, job_name,
    code_location, error.
    """
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
            r.pipeline_name  AS job_name,
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


@dataclass
class _PendingAction:
    """Internal: a deferred state-change + notification."""
    action: str          # "open", "reminder", "resolve"
    key: str
    payload: AlertPayload
    user_code: str
    alert_type: str
    entity_id: str
    details: Optional[str] = None


@sensor(
    name="operational_alerts_sensor",
    minimum_interval_seconds=300,
    required_resource_keys={"mssql_reader", "alert_state", "email_notifier"},
    default_status=DefaultSensorStatus.RUNNING,
)
def operational_alerts_sensor(
    context: SensorEvaluationContext,
) -> SkipReason:
    """Central alerting sensor — see module docstring for details."""
    mssql: MssqlReaderResource = context.resources.mssql_reader  # type: ignore[attr-defined]
    state_store: AlertStateStoreResource = context.resources.alert_state  # type: ignore[attr-defined]
    email: EmailNotifierResource = context.resources.email_notifier  # type: ignore[attr-defined]

    config = _load_config()
    defaults = config.get("defaults", {})
    sensor_defaults = defaults.get("sensor_alerts", {})
    pipeline_defaults = defaults.get("pipeline_alerts", {})
    reminder_cfg = defaults.get("daily_reminders", {})
    quiet_cfg = defaults.get("quiet_hours", {})
    user_codes_cfg = config.get("user_codes", {})

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

    # ── 1. Collect all active conditions across user_codes ────────
    active_conditions: Dict[str, Dict[str, Any]] = {}
    uc_databases: Dict[str, str] = {}   # user_code → resolved database name

    for uc_name, uc_cfg in user_codes_cfg.items():
        database = os.environ.get(
            uc_cfg.get("database_env", ""), uc_cfg.get("database", uc_name)
        )
        uc_databases[uc_name] = database
        uc_sensor_cfg = {**sensor_defaults, **uc_cfg.get("sensor_alerts", {})}
        uc_pipeline_cfg = {
            **pipeline_defaults,
            **uc_cfg.get("pipeline_alerts", {}),
        }
        lookback = uc_sensor_cfg.get("lookback_hours", 2)
        ingestions_cfg = uc_cfg.get("ingestions", {}) or {}

        # Sensor: connection lost
        if uc_sensor_cfg.get("connection_lost", True):
            for sa in mssql.get_connection_lost_sensors(
                database, lookback, uc_name
            ):
                key = _build_alert_key(uc_name, "connection_lost", sa.sensor_id)
                active_conditions[key] = {
                    "user_code": uc_name,
                    "alert_type": "connection_lost",
                    "entity_id": sa.sensor_id,
                    "details": (
                        f"MinutesWithoutReadings: {sa.minutes_without_readings}, "
                        f"ConnectionLostAt: {sa.connection_lost_at}"
                    ),
                }

        # Sensor: late readings
        if uc_sensor_cfg.get("late_readings", True):
            for sa in mssql.get_late_reading_sensors(
                database, lookback, uc_name
            ):
                key = _build_alert_key(uc_name, "late_readings", sa.sensor_id)
                active_conditions[key] = {
                    "user_code": uc_name,
                    "alert_type": "late_readings",
                    "entity_id": sa.sensor_id,
                    "details": (
                        f"MinutesWithoutReadings: {sa.minutes_without_readings}, "
                        f"LatestTimeSpan: {sa.latest_time_span}"
                    ),
                }

        # Pipeline: ctl.IngestionControl FAILED
        if uc_pipeline_cfg.get("ingestion_control_failed", True):
            for ia in mssql.get_failed_ingestions(database, uc_name):
                key = _build_alert_key(
                    uc_name, "pipeline_failure", ia.ingestion_name
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "alert_type": "pipeline_failure",
                    "entity_id": ia.ingestion_name,
                    "details": (
                        f"LastRunStatus: {ia.last_run_status}, "
                        f"LastRunFinishedAt: {ia.last_run_finished_at}, "
                        f"Error: {ia.last_run_error}"
                    ),
                }

        # Pipeline: stale ingestions
        if uc_pipeline_cfg.get("stale_detection", True) and ingestions_cfg:
            intervals = {
                name: ing_cfg.get(
                    "expected_interval_minutes",
                    uc_pipeline_cfg.get(
                        "default_expected_interval_minutes", 720
                    ),
                )
                for name, ing_cfg in ingestions_cfg.items()
            }
            default_interval = uc_pipeline_cfg.get(
                "default_expected_interval_minutes", 720
            )
            for ia in mssql.get_stale_ingestions(
                database, intervals, default_interval, uc_name
            ):
                key = _build_alert_key(
                    uc_name, "pipeline_stale", ia.ingestion_name
                )
                active_conditions[key] = {
                    "user_code": uc_name,
                    "alert_type": "pipeline_stale",
                    "entity_id": ia.ingestion_name,
                    "details": (
                        f"LastRunFinishedAt: {ia.last_run_finished_at}, "
                        f"{ia.last_run_error}"
                    ),
                }

    # ── 2. Dagster runs FAILURE ───────────────────────────────────
    if pipeline_defaults.get("dagster_run_failure", True):
        for run in _get_recent_dagster_failures(hours=2):
            uc = run["code_location"]
            if uc not in user_codes_cfg:
                continue
            key = _build_alert_key(
                uc, "dagster_run_failure", run["run_id"]
            )
            active_conditions[key] = {
                "user_code": uc,
                "alert_type": "dagster_run_failure",
                "entity_id": run["run_id"],
                "details": (
                    f"Job: {run['job_name']}, "
                    f"Updated: {run['updated']}"
                ),
            }

    # ── 3. Reconcile: determine pending actions (deferred) ────────
    all_existing_alerts = state_store.get_active_alerts()
    pending_resolved_alerts = state_store.get_pending_resolved_alerts()
    existing_alerts = {
        key: alert
        for key, alert in all_existing_alerts.items()
        if alert.alert_type in _OPERATIONAL_ALERT_TYPES
    }
    connection_recoveries = {}
    for uc_name, database in uc_databases.items():
        sensor_ids = [
            alert.entity_id
            for key, alert in existing_alerts.items()
            if key not in active_conditions
            and alert.user_code == uc_name
            and alert.alert_type == "connection_lost"
        ]
        for sensor_id, recovery in mssql.get_sensor_recoveries(
            database, sensor_ids, uc_name
        ).items():
            key = _build_alert_key(uc_name, "connection_lost", sensor_id)
            connection_recoveries[key] = recovery
    for key, alert in existing_alerts.items():
        if (
            key not in active_conditions
            and alert.alert_type == "connection_lost"
            and key not in connection_recoveries
        ):
            active_conditions[key] = {
                "user_code": alert.user_code,
                "alert_type": alert.alert_type,
                "entity_id": alert.entity_id,
                "details": alert.details,
            }
    pending: List[_PendingAction] = []
    suppressed = 0

    # New or ongoing conditions
    for key, cond in active_conditions.items():
        alert_type = cond["alert_type"]

        if key not in existing_alerts:
            # New alert → OPEN
            occurrence_key = f"{key}::{uuid.uuid4()}"
            pending.append(_PendingAction(
                action="open",
                key=key,
                user_code=cond["user_code"],
                alert_type=alert_type,
                entity_id=cond["entity_id"],
                details=cond.get("details"),
                payload=AlertPayload(
                    alert_key=occurrence_key,
                    user_code=cond["user_code"],
                    alert_type=alert_type,
                    entity_id=cond["entity_id"],
                    state="OPEN",
                    details=cond.get("details"),
                    notification_count=1,
                    first_detected_at=datetime.now(timezone.utc),
                ),
            ))

    # Cleared conditions → RESOLVED
    for key, alert in existing_alerts.items():
        if key not in active_conditions:
            details = alert.details
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
            pending.append(_PendingAction(
                action="resolve",
                key=key,
                user_code=alert.user_code,
                alert_type=alert.alert_type,
                entity_id=alert.entity_id,
                details=details,
                payload=AlertPayload(
                    alert_key=alert.alert_key,
                    user_code=alert.user_code,
                    alert_type=alert.alert_type,
                    entity_id=alert.entity_id,
                    state="RESOLVED",
                    details=details,
                    notification_count=alert.notification_count,
                    first_detected_at=alert.first_detected_at,
                ),
            ))

    resolution_groups: Dict[str, List[_PendingAction]] = defaultdict(list)
    for alert in pending_resolved_alerts.values():
        resolution_groups[alert.user_code].append(_PendingAction(
            action="resolve",
            key=_build_alert_key(
                alert.user_code, alert.alert_type, alert.entity_id
            ),
            user_code=alert.user_code,
            alert_type=alert.alert_type,
            entity_id=alert.entity_id,
            details=alert.details,
            payload=AlertPayload(
                alert_key=alert.alert_key,
                user_code=alert.user_code,
                alert_type=alert.alert_type,
                entity_id=alert.entity_id,
                state="RESOLVED",
                details=alert.details,
                notification_count=alert.notification_count,
                first_detected_at=alert.first_detected_at,
            ),
        ))
    for action in pending:
        if action.action != "resolve":
            continue
        state_store.mark_resolved(action.payload.alert_key, action.details)
        resolution_groups[action.user_code].append(action)

    reminder_groups: Dict[str, List[_PendingAction]] = defaultdict(list)
    trend_reminder_groups: Dict[str, List[_PendingAction]] = defaultdict(list)
    reminder_time = time.fromisoformat(reminder_cfg.get("time", "08:00"))
    if reminder_cfg.get("enabled", True) and reminder_now.time() >= reminder_time:
        resolving_keys = {
            action.key for action in pending if action.action == "resolve"
        }
        for key, alert in all_existing_alerts.items():
            if alert.user_code not in user_codes_cfg or key in resolving_keys:
                continue
            if alert.alert_type != _TREND_ALERT_TYPE:
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
            details = active_conditions.get(key, {}).get(
                "details", alert.details
            )
            action = _PendingAction(
                action="reminder",
                key=key,
                user_code=alert.user_code,
                alert_type=alert.alert_type,
                entity_id=alert.entity_id,
                details=details,
                payload=AlertPayload(
                    alert_key=alert.alert_key,
                    user_code=alert.user_code,
                    alert_type=alert.alert_type,
                    entity_id=alert.entity_id,
                    state="REMINDER",
                    details=details,
                    notification_count=alert.notification_count + 1,
                    first_detected_at=alert.first_detected_at,
                ),
            )
            if alert.alert_type == _TREND_ALERT_TYPE:
                trend_reminder_groups[alert.user_code].append(action)
            else:
                reminder_groups[alert.user_code].append(action)

        for user_code in list(reminder_groups):
            reminder_groups[user_code].extend(
                trend_reminder_groups[user_code]
            )

    if reminders_silenced:
        suppressed = sum(len(actions) for actions in reminder_groups.values())
        reminder_groups.clear()

    # ── 4. Group by user_code ─────────────────────────────────────
    groups: Dict[str, Dict[str, List[_PendingAction]]] = defaultdict(
        lambda: {"active": [], "resolved": []}
    )
    for pa in pending:
        bucket = "resolved" if pa.action == "resolve" else "active"
        groups[pa.user_code][bucket].append(pa)

    # ── 5. Enrich sensor alerts with dimensional context ──────────
    enrichment_groups: Dict[str, List[_PendingAction]] = defaultdict(list)
    opening_tenants = {
        user_code
        for user_code, group in groups.items()
        if group["active"]
    }
    for uc_name, group in groups.items():
        if group["active"]:
            enrichment_groups[uc_name].extend(
                group["active"] + resolution_groups[uc_name]
            )
    for uc_name, actions in reminder_groups.items():
        enrichment_groups[uc_name].extend(actions)
        if uc_name not in opening_tenants:
            enrichment_groups[uc_name].extend(resolution_groups[uc_name])

    for uc_name, all_actions in enrichment_groups.items():
        sensor_ids = [
            pa.entity_id
            for pa in all_actions
            if pa.alert_type in _SENSOR_ALERT_TYPES
        ]
        if not sensor_ids or uc_name not in uc_databases:
            continue
        try:
            dims = mssql.get_sensor_dimensions(
                uc_databases[uc_name], sensor_ids, uc_name
            )
        except Exception:
            context.log.warning(
                "Failed to fetch sensor dimensions for %s, "
                "alerts will be sent without hierarchy.",
                uc_name,
                exc_info=True,
            )
            continue
        for pa in all_actions:
            dim = dims.get(pa.entity_id)
            if dim:
                pa.payload.location_name = dim.location_name
                pa.payload.sublocation_name = dim.sublocation_name
                pa.payload.device_name = dim.device_name
                pa.payload.sensor_name = dim.sensor_name

    # ── 6. Send one consolidated email per user_code ──────────────
    opened = 0
    reminded = 0
    resolved = 0

    for uc_name, group in groups.items():
        active_payloads = [pa.payload for pa in group["active"]]
        resolution_actions = resolution_groups[uc_name]
        resolved_payloads = [pa.payload for pa in resolution_actions]

        if not active_payloads:
            continue

        sent = email.send_consolidated(ConsolidatedPayload(
            user_code=uc_name,
            active_alerts=active_payloads,
            resolved_alerts=resolved_payloads,
        ))

        if sent:
            for pa in group["active"]:
                if pa.action == "open":
                    state_store.upsert_alert(
                        alert_key=pa.payload.alert_key,
                        user_code=pa.user_code,
                        alert_type=pa.alert_type,
                        entity_id=pa.entity_id,
                        details=pa.details,
                        state=ALERT_STATE_OPEN,
                    )
                    opened += 1
            for pa in resolution_actions:
                state_store.mark_resolution_notified(pa.payload.alert_key)
                resolved += 1

    for uc_name, actions in reminder_groups.items():
        resolution_actions = (
            [] if uc_name in opening_tenants else resolution_groups[uc_name]
        )
        sent = email.send_consolidated(ConsolidatedPayload(
            user_code=uc_name,
            active_alerts=[action.payload for action in actions],
            resolved_alerts=[action.payload for action in resolution_actions],
        ))
        if sent:
            for action in actions:
                state_store.upsert_alert(
                    alert_key=action.payload.alert_key,
                    user_code=action.user_code,
                    alert_type=action.alert_type,
                    entity_id=action.entity_id,
                    details=action.details,
                    state=ALERT_STATE_REMINDER,
                )
                reminded += 1
            for action in resolution_actions:
                state_store.mark_resolution_notified(
                    action.payload.alert_key
                )
                resolved += 1

    summary = (
        f"Checked {len(user_codes_cfg)} user_codes: "
        f"{len(active_conditions)} active conditions, "
        f"{opened} opened, {reminded} reminded, {resolved} resolved, "
        f"{suppressed} reminders suppressed"
    )
    context.log.info(summary)
    return SkipReason(summary)
