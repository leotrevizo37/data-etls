"""Tenant alert state and Dagster PostgreSQL row-count history."""

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence

import psycopg2
import psycopg2.extras
from dagster import InitResourceContext, resource

from .mssql_reader import MssqlReaderResource


ALERT_STATE_OPEN = "OPEN"
ALERT_STATE_RESOLVED_PENDING = "RESOLVED_PENDING"
ALERT_STATE_RESOLVED = "RESOLVED"

NOTIFICATION_TYPE_OPEN = "OPEN"
NOTIFICATION_TYPE_REMINDER = "REMINDER"
NOTIFICATION_TYPE_RESOLUTION = "RESOLUTION"

NOTIFICATION_STATUS_PENDING = "PENDING"
NOTIFICATION_STATUS_PROCESSING = "PROCESSING"
NOTIFICATION_STATUS_SENT = "SENT"
NOTIFICATION_STATUS_RETRY_PENDING = "RETRY_PENDING"
NOTIFICATION_STATUS_FAILED = "FAILED"
NOTIFICATION_STATUS_CANCELLED = "CANCELLED"

_ENSURE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS alerting;"

_ENSURE_ROW_COUNT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alerting.asset_row_count_daily (
    observation_date            DATE           NOT NULL,
    code_location               VARCHAR(100)   NOT NULL,
    asset_key                   VARCHAR(500)   NOT NULL,
    metadata_key                VARCHAR(100)   NOT NULL,
    daily_materialization_count INT            NOT NULL,
    daily_row_count_sum         BIGINT         NOT NULL,
    daily_row_count_average     NUMERIC(20, 4) NOT NULL,
    daily_row_count_min         BIGINT         NOT NULL,
    daily_row_count_max         BIGINT         NOT NULL,
    first_result_count          BIGINT         NOT NULL,
    last_result_count           BIGINT         NOT NULL,
    average_1d                  NUMERIC(20, 4) NULL,
    average_7d                  NUMERIC(20, 4) NULL,
    average_28d                 NUMERIC(20, 4) NULL,
    observed_days_7d            SMALLINT       NOT NULL DEFAULT 0,
    observed_days_28d           SMALLINT       NOT NULL DEFAULT 0,
    change_vs_1d_pct            NUMERIC(12, 4) NULL,
    change_vs_7d_pct            NUMERIC(12, 4) NULL,
    change_vs_28d_pct           NUMERIC(12, 4) NULL,
    latest_event_storage_id     BIGINT         NOT NULL,
    latest_run_id               VARCHAR(64)    NULL,
    latest_materialized_at      TIMESTAMPTZ    NOT NULL,
    created_at                  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (observation_date, code_location, asset_key, metadata_key)
);
"""

_ENSURE_ROW_COUNT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_asset_row_count_daily_asset
    ON alerting.asset_row_count_daily
        (code_location, asset_key, metadata_key, observation_date DESC);
"""

_UPSERT_ROW_COUNT_SQL = """
INSERT INTO alerting.asset_row_count_daily (
    observation_date, code_location, asset_key, metadata_key,
    daily_materialization_count, daily_row_count_sum,
    daily_row_count_average, daily_row_count_min, daily_row_count_max,
    first_result_count, last_result_count, latest_event_storage_id,
    latest_run_id, latest_materialized_at, created_at, updated_at
)
VALUES (%s, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
ON CONFLICT (observation_date, code_location, asset_key, metadata_key)
DO UPDATE SET
    daily_materialization_count =
        alerting.asset_row_count_daily.daily_materialization_count + 1,
    daily_row_count_sum =
        alerting.asset_row_count_daily.daily_row_count_sum
        + EXCLUDED.daily_row_count_sum,
    daily_row_count_average = ROUND(
        (
            alerting.asset_row_count_daily.daily_row_count_sum
            + EXCLUDED.daily_row_count_sum
        )::NUMERIC
        / (alerting.asset_row_count_daily.daily_materialization_count + 1),
        4
    ),
    daily_row_count_min = LEAST(
        alerting.asset_row_count_daily.daily_row_count_min,
        EXCLUDED.daily_row_count_min
    ),
    daily_row_count_max = GREATEST(
        alerting.asset_row_count_daily.daily_row_count_max,
        EXCLUDED.daily_row_count_max
    ),
    last_result_count = EXCLUDED.last_result_count,
    latest_event_storage_id = EXCLUDED.latest_event_storage_id,
    latest_run_id = EXCLUDED.latest_run_id,
    latest_materialized_at = EXCLUDED.latest_materialized_at,
    updated_at = NOW()
WHERE alerting.asset_row_count_daily.latest_event_storage_id
      < EXCLUDED.latest_event_storage_id
RETURNING observation_date;
"""

_REFRESH_ROW_COUNT_AVERAGES_SQL = """
UPDATE alerting.asset_row_count_daily AS target
SET average_1d = (
        SELECT AVG(history.daily_row_count_sum)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date = target.observation_date - 1
    ),
    average_7d = (
        SELECT AVG(history.daily_row_count_sum)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= target.observation_date - 7
          AND history.observation_date < target.observation_date
    ),
    average_28d = (
        SELECT AVG(history.daily_row_count_sum)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= target.observation_date - 28
          AND history.observation_date < target.observation_date
    ),
    observed_days_7d = (
        SELECT COUNT(*)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= target.observation_date - 7
          AND history.observation_date < target.observation_date
    ),
    observed_days_28d = (
        SELECT COUNT(*)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= target.observation_date - 28
          AND history.observation_date < target.observation_date
    ),
    updated_at = NOW()
WHERE target.code_location = %s
  AND target.asset_key = %s
  AND target.metadata_key = %s
  AND target.observation_date >= %s;
"""

_REFRESH_ROW_COUNT_CHANGES_SQL = """
UPDATE alerting.asset_row_count_daily
SET change_vs_1d_pct = CASE
        WHEN average_1d IS NULL OR average_1d = 0 THEN NULL
        ELSE ROUND((daily_row_count_sum - average_1d) / average_1d * 100, 4)
    END,
    change_vs_7d_pct = CASE
        WHEN average_7d IS NULL OR average_7d = 0 THEN NULL
        ELSE ROUND((daily_row_count_sum - average_7d) / average_7d * 100, 4)
    END,
    change_vs_28d_pct = CASE
        WHEN average_28d IS NULL OR average_28d = 0 THEN NULL
        ELSE ROUND((daily_row_count_sum - average_28d) / average_28d * 100, 4)
    END,
    updated_at = NOW()
WHERE code_location = %s
  AND asset_key = %s
  AND metadata_key = %s
  AND observation_date >= %s;
"""

_GET_LATEST_ROW_COUNT_TRENDS_SQL = """
SELECT DISTINCT ON (code_location, asset_key, metadata_key)
       observation_date, code_location, asset_key, metadata_key,
       daily_materialization_count, daily_row_count_sum,
       average_1d, average_7d, average_28d,
       observed_days_7d, observed_days_28d,
       change_vs_1d_pct, change_vs_7d_pct, change_vs_28d_pct,
       latest_run_id, latest_materialized_at
FROM alerting.asset_row_count_daily
WHERE observation_date < (NOW() AT TIME ZONE 'UTC')::DATE
ORDER BY code_location, asset_key, metadata_key, observation_date DESC;
"""


@dataclass
class AlertRecord:
    """A row from alert_workflow_state."""
    alert_key: str
    user_code: str
    trigger_source: str
    alert_type: str
    entity_id: str
    state: str
    details: Optional[str]
    first_detected_at: datetime
    last_notified_at: Optional[datetime]
    resolved_at: Optional[datetime]
    notification_count: int
    severity: Optional[str] = None
    notification_type: str = NOTIFICATION_TYPE_OPEN
    notification_status: str = NOTIFICATION_STATUS_SENT
    metadata: Dict[str, Any] = field(default_factory=dict)
    ticket_id: Optional[str] = None
    last_detected_at: Optional[datetime] = None
    notification_queued_at: Optional[datetime] = None
    last_notification_attempt_at: Optional[datetime] = None
    next_retry_at: Optional[datetime] = None
    notification_attempt_count: int = 0
    max_notification_attempts: int = 3
    last_error_code: Optional[str] = None
    last_error_message: Optional[str] = None
    last_error_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass(frozen=True)
class AssetRowCountObservation:
    observation_date: date
    code_location: str
    asset_key: str
    metadata_key: str
    row_count: int
    event_storage_id: int
    run_id: Optional[str]
    materialized_at: datetime


@dataclass(frozen=True)
class AssetRowCountTrend:
    observation_date: date
    code_location: str
    asset_key: str
    metadata_key: str
    daily_materialization_count: int
    daily_row_count: int
    average_1d: Optional[float]
    average_7d: Optional[float]
    average_28d: Optional[float]
    observed_days_7d: int
    observed_days_28d: int
    change_vs_1d_pct: Optional[float]
    change_vs_7d_pct: Optional[float]
    change_vs_28d_pct: Optional[float]
    latest_run_id: Optional[str]
    latest_materialized_at: datetime


class AlertStateStoreResource:
    """Manages tenant alert state and Dagster row-count history."""

    def __init__(self, mssql_reader: MssqlReaderResource) -> None:
        self._mssql_reader = mssql_reader
        self._dsn = self._build_dsn()
        self._ensure_schema()

    @staticmethod
    def _build_dsn() -> str:
        host = os.environ.get("DAGSTER_PG_HOSTNAME", "dagster-postgres")
        port = os.environ.get("DAGSTER_PG_PORT", "5432")
        user = os.environ["DAGSTER_PG_USER"]
        password = os.environ["DAGSTER_PG_PASSWORD"]
        dbname = os.environ.get("DAGSTER_PG_DB", "dagster")
        return (
            f"host={host} port={port} dbname={dbname} "
            f"user={user} password={password}"
        )

    def _get_connection(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(self._dsn)

    def _get_alert_connection(self, database: str, user_code: str):
        return self._mssql_reader._get_connection(
            database,
            user_code,
            read_only=False,
        )

    def _ensure_schema(self) -> None:
        """Idempotently create the PostgreSQL row-count history."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_ENSURE_SCHEMA_SQL)
                cur.execute(_ENSURE_ROW_COUNT_TABLE_SQL)
                cur.execute(_ENSURE_ROW_COUNT_INDEX_SQL)
            conn.commit()

    def record_asset_row_counts(
        self,
        observations: Sequence[AssetRowCountObservation],
    ) -> int:
        affected = {}
        recorded = 0
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                for observation in observations:
                    cur.execute(
                        _UPSERT_ROW_COUNT_SQL,
                        (
                            observation.observation_date,
                            observation.code_location,
                            observation.asset_key,
                            observation.metadata_key,
                            observation.row_count,
                            observation.row_count,
                            observation.row_count,
                            observation.row_count,
                            observation.row_count,
                            observation.row_count,
                            observation.event_storage_id,
                            observation.run_id,
                            observation.materialized_at,
                        ),
                    )
                    if cur.fetchone() is None:
                        continue
                    recorded += 1
                    key = (
                        observation.code_location,
                        observation.asset_key,
                        observation.metadata_key,
                    )
                    affected[key] = min(
                        affected.get(key, observation.observation_date),
                        observation.observation_date,
                    )

                for key, first_date in affected.items():
                    parameters = (*key, first_date)
                    cur.execute(_REFRESH_ROW_COUNT_AVERAGES_SQL, parameters)
                    cur.execute(_REFRESH_ROW_COUNT_CHANGES_SQL, parameters)
            conn.commit()
        return recorded

    def get_latest_asset_row_count_trends(self) -> Sequence[AssetRowCountTrend]:
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_LATEST_ROW_COUNT_TRENDS_SQL)
                return [
                    AssetRowCountTrend(
                        observation_date=row[0],
                        code_location=row[1],
                        asset_key=row[2],
                        metadata_key=row[3],
                        daily_materialization_count=row[4],
                        daily_row_count=row[5],
                        average_1d=float(row[6]) if row[6] is not None else None,
                        average_7d=float(row[7]) if row[7] is not None else None,
                        average_28d=float(row[8]) if row[8] is not None else None,
                        observed_days_7d=row[9],
                        observed_days_28d=row[10],
                        change_vs_1d_pct=(
                            float(row[11]) if row[11] is not None else None
                        ),
                        change_vs_7d_pct=(
                            float(row[12]) if row[12] is not None else None
                        ),
                        change_vs_28d_pct=(
                            float(row[13]) if row[13] is not None else None
                        ),
                        latest_run_id=row[14],
                        latest_materialized_at=row[15],
                    )
                    for row in cur.fetchall()
                ]

    def get_active_alerts(
        self,
        database: str,
        user_code: str,
    ) -> Dict[str, AlertRecord]:
        """Return active alerts keyed by their condition identity."""
        query = self._alert_select_sql() + """
            WHERE state = ?
        """
        result: Dict[str, AlertRecord] = {}
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(query, ALERT_STATE_OPEN)
                for row in cur.fetchall():
                    rec = self._alert_record_from_row(row, user_code)
                    result[
                        f"{rec.user_code}::{rec.alert_type}::{rec.entity_id}"
                    ] = rec
        return result

    @staticmethod
    def _alert_select_sql() -> str:
        return """
            SELECT alert_key, trigger_source, alert_type, entity_id, severity,
                   state, notification_type, notification_status, details,
                   metadata_json, ticket_id, first_detected_at,
                   last_detected_at, notification_queued_at,
                   last_notification_attempt_at, last_notified_at,
                   next_retry_at, resolved_at, notification_count,
                   notification_attempt_count, max_notification_attempts,
                   last_error_code, last_error_message, last_error_at,
                   created_at, updated_at
            FROM alerting.alert_workflow_state
        """

    @staticmethod
    def _alert_record_from_row(row, user_code: str) -> AlertRecord:
        metadata = json.loads(row[9]) if row[9] else {}
        if not isinstance(metadata, dict):
            raise TypeError("metadata_json must contain a JSON object")
        return AlertRecord(
            alert_key=row[0],
            user_code=user_code,
            trigger_source=row[1],
            alert_type=row[2],
            entity_id=row[3],
            severity=row[4],
            state=row[5],
            notification_type=row[6],
            notification_status=row[7],
            details=row[8],
            metadata=metadata,
            ticket_id=row[10],
            first_detected_at=row[11],
            last_detected_at=row[12],
            notification_queued_at=row[13],
            last_notification_attempt_at=row[14],
            last_notified_at=row[15],
            next_retry_at=row[16],
            resolved_at=row[17],
            notification_count=row[18],
            notification_attempt_count=row[19],
            max_notification_attempts=row[20],
            last_error_code=row[21],
            last_error_message=row[22],
            last_error_at=row[23],
            created_at=row[24],
            updated_at=row[25],
        )

    @staticmethod
    def _metadata_json(metadata: Optional[Dict[str, Any]]) -> str:
        return json.dumps(
            metadata or {},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def upsert_alert(
        self,
        database: str,
        alert_key: str,
        user_code: str,
        trigger_source: str,
        alert_type: str,
        entity_id: str,
        details: Optional[str] = None,
        state: str = ALERT_STATE_OPEN,
        notification_count: int = 0,
        severity: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ticket_id: Optional[str] = None,
        max_notification_attempts: int = 3,
    ) -> None:
        """Insert a new alert or refresh its detection evidence."""
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state
            SET trigger_source = ?,
                severity = ?,
                details = ?,
                metadata_json = ?,
                ticket_id = COALESCE(?, ticket_id),
                max_notification_attempts = ?,
                last_detected_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME()
            WHERE alert_key = ?;

            IF @@ROWCOUNT = 0
                INSERT INTO alerting.alert_workflow_state
                    (alert_key, trigger_source, alert_type, entity_id, severity,
                     state, notification_type, notification_status, details,
                     metadata_json, ticket_id, first_detected_at,
                     last_detected_at, notification_queued_at,
                     notification_count, max_notification_attempts,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        SYSUTCDATETIME(), SYSUTCDATETIME(),
                        SYSUTCDATETIME(), ?, ?, SYSUTCDATETIME(),
                        SYSUTCDATETIME());
        """
        metadata_json = self._metadata_json(metadata)
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    trigger_source,
                    severity,
                    details,
                    metadata_json,
                    ticket_id,
                    max_notification_attempts,
                    alert_key,
                    alert_key,
                    trigger_source,
                    alert_type,
                    entity_id,
                    severity,
                    state,
                    NOTIFICATION_TYPE_OPEN,
                    NOTIFICATION_STATUS_PENDING,
                    details,
                    metadata_json,
                    ticket_id,
                    notification_count,
                    max_notification_attempts,
                )
            conn.commit()

    def mark_resolved(
        self,
        database: str,
        user_code: str,
        alert_key: str,
        details: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Queue an alert resolution for a grouped notification."""
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state
            SET state = ?,
                notification_type = ?,
                notification_status = ?,
                details = COALESCE(?, details),
                metadata_json = COALESCE(?, metadata_json),
                notification_queued_at = SYSUTCDATETIME(),
                notification_attempt_count = 0,
                last_notification_attempt_at = NULL,
                next_retry_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                last_error_at = NULL,
                resolved_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME()
            WHERE alert_key = ?
              AND state = ?
        """
        metadata_json = (
            self._metadata_json(metadata) if metadata is not None else None
        )
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    ALERT_STATE_RESOLVED_PENDING,
                    NOTIFICATION_TYPE_RESOLUTION,
                    NOTIFICATION_STATUS_PENDING,
                    details,
                    metadata_json,
                    alert_key,
                    ALERT_STATE_OPEN,
                )
            conn.commit()

    def get_pending_resolved_alerts(
        self,
        database: str,
        user_code: str,
    ) -> Dict[str, AlertRecord]:
        query = self._alert_select_sql() + """
            WHERE state = ?
        """
        result: Dict[str, AlertRecord] = {}
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(query, ALERT_STATE_RESOLVED_PENDING)
                for row in cur.fetchall():
                    rec = self._alert_record_from_row(row, user_code)
                    result[rec.alert_key] = rec
        return result

    def queue_reminder(
        self,
        database: str,
        user_code: str,
        alert_key: str,
        details: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state
            SET notification_type = ?,
                notification_status = ?,
                details = COALESCE(?, details),
                metadata_json = COALESCE(?, metadata_json),
                notification_queued_at = SYSUTCDATETIME(),
                notification_attempt_count = 0,
                last_notification_attempt_at = NULL,
                next_retry_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                last_error_at = NULL,
                updated_at = SYSUTCDATETIME()
            WHERE alert_key = ?
              AND state = ?
              AND notification_status = ?
        """
        metadata_json = (
            self._metadata_json(metadata) if metadata is not None else None
        )
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    NOTIFICATION_TYPE_REMINDER,
                    NOTIFICATION_STATUS_PENDING,
                    details,
                    metadata_json,
                    alert_key,
                    ALERT_STATE_OPEN,
                    NOTIFICATION_STATUS_SENT,
                )
            conn.commit()

    def get_deliverable_notifications(
        self,
        database: str,
        user_code: str,
        processing_timeout_minutes: int = 30,
    ) -> Sequence[AlertRecord]:
        query = self._alert_select_sql() + """
            WHERE state IN (?, ?)
              AND (
                    notification_status = ?
                 OR (notification_status = ?
                     AND next_retry_at <= SYSUTCDATETIME())
                 OR (notification_status = ?
                     AND last_notification_attempt_at <= DATEADD(
                         MINUTE, -?, SYSUTCDATETIME()
                     ))
              )
            ORDER BY notification_queued_at, first_detected_at
        """
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    ALERT_STATE_OPEN,
                    ALERT_STATE_RESOLVED_PENDING,
                    NOTIFICATION_STATUS_PENDING,
                    NOTIFICATION_STATUS_RETRY_PENDING,
                    NOTIFICATION_STATUS_PROCESSING,
                    processing_timeout_minutes,
                )
                return [
                    self._alert_record_from_row(row, user_code)
                    for row in cur.fetchall()
                ]

    def claim_notifications(
        self,
        database: str,
        user_code: str,
        alert_keys: Sequence[str],
        processing_timeout_minutes: int = 30,
    ) -> Sequence[str]:
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state WITH (UPDLOCK, ROWLOCK)
            SET notification_status = ?,
                notification_attempt_count = notification_attempt_count + 1,
                last_notification_attempt_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME()
            OUTPUT INSERTED.alert_key
            WHERE alert_key = ?
              AND state IN (?, ?)
              AND (
                    notification_status = ?
                 OR (notification_status = ?
                     AND next_retry_at <= SYSUTCDATETIME())
                 OR (notification_status = ?
                     AND last_notification_attempt_at <= DATEADD(
                         MINUTE, -?, SYSUTCDATETIME()
                     ))
              )
        """
        claimed = []
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                for alert_key in alert_keys:
                    cur.execute(
                        query,
                        NOTIFICATION_STATUS_PROCESSING,
                        alert_key,
                        ALERT_STATE_OPEN,
                        ALERT_STATE_RESOLVED_PENDING,
                        NOTIFICATION_STATUS_PENDING,
                        NOTIFICATION_STATUS_RETRY_PENDING,
                        NOTIFICATION_STATUS_PROCESSING,
                        processing_timeout_minutes,
                    )
                    row = cur.fetchone()
                    if row is not None:
                        claimed.append(row[0])
            conn.commit()
        return claimed

    def mark_notifications_sent(
        self,
        database: str,
        user_code: str,
        alert_keys: Sequence[str],
    ) -> None:
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state
            SET state = CASE
                    WHEN notification_type = ? THEN ?
                    ELSE state
                END,
                notification_status = ?,
                last_notified_at = SYSUTCDATETIME(),
                notification_count = notification_count + 1,
                next_retry_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                last_error_at = NULL,
                updated_at = SYSUTCDATETIME()
            WHERE alert_key = ?
              AND notification_status = ?
        """
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                for alert_key in alert_keys:
                    cur.execute(
                        query,
                        NOTIFICATION_TYPE_RESOLUTION,
                        ALERT_STATE_RESOLVED,
                        NOTIFICATION_STATUS_SENT,
                        alert_key,
                        NOTIFICATION_STATUS_PROCESSING,
                    )
            conn.commit()

    def mark_notifications_failed(
        self,
        database: str,
        user_code: str,
        alert_keys: Sequence[str],
        error_code: str,
        error_message: str,
        retry_delay_minutes: int,
    ) -> None:
        query = """
            SET QUOTED_IDENTIFIER ON;

            UPDATE alerting.alert_workflow_state
            SET notification_status = CASE
                    WHEN notification_attempt_count >= max_notification_attempts
                        THEN ?
                    ELSE ?
                END,
                next_retry_at = CASE
                    WHEN notification_attempt_count >= max_notification_attempts
                        THEN NULL
                    ELSE DATEADD(MINUTE, ?, SYSUTCDATETIME())
                END,
                last_error_code = ?,
                last_error_message = ?,
                last_error_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME()
            WHERE alert_key = ?
              AND notification_status = ?
        """
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                for alert_key in alert_keys:
                    cur.execute(
                        query,
                        NOTIFICATION_STATUS_FAILED,
                        NOTIFICATION_STATUS_RETRY_PENDING,
                        retry_delay_minutes,
                        error_code[:100],
                        error_message,
                        alert_key,
                        NOTIFICATION_STATUS_PROCESSING,
                    )
            conn.commit()

    def should_send_reminder(
        self,
        alert_key: str,
        reminder_interval_minutes: int,
        existing_alerts: Dict[str, AlertRecord],
    ) -> bool:
        """Check whether enough time has passed since last notification."""
        alert = existing_alerts.get(alert_key)
        if (
            alert is None
            or alert.state != ALERT_STATE_OPEN
            or alert.notification_status != NOTIFICATION_STATUS_SENT
            or alert.last_notified_at is None
        ):
            return False
        now = datetime.now(timezone.utc)
        last = alert.last_notified_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (now - last).total_seconds() / 60
        return elapsed >= reminder_interval_minutes


@resource(required_resource_keys={"mssql_reader"})
def alert_state_store_resource(
    context: InitResourceContext,
) -> AlertStateStoreResource:
    return AlertStateStoreResource(context.resources.mssql_reader)
