"""Tenant alert state and row-count history."""

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence

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

_UPSERT_ROW_COUNT_SQL = """
SET NOCOUNT ON;

DECLARE @observation_date DATE = ?;
DECLARE @code_location NVARCHAR(100) = ?;
DECLARE @asset_key NVARCHAR(500) = ?;
DECLARE @metadata_key NVARCHAR(100) = ?;
DECLARE @row_count BIGINT = ?;
DECLARE @event_storage_id BIGINT = ?;
DECLARE @run_id NVARCHAR(64) = ?;
DECLARE @materialized_at DATETIME2(3) = ?;
DECLARE @recorded BIT = 0;

UPDATE alerting.asset_row_count_daily WITH (UPDLOCK, SERIALIZABLE)
SET daily_materialization_count = daily_materialization_count + 1,
    daily_row_count_sum = daily_row_count_sum + @row_count,
    daily_row_count_average = ROUND(
        CAST(daily_row_count_sum + @row_count AS NUMERIC(38, 4))
        / (daily_materialization_count + 1),
        4
    ),
    daily_row_count_min = CASE
        WHEN daily_row_count_min > @row_count THEN @row_count
        ELSE daily_row_count_min
    END,
    daily_row_count_max = CASE
        WHEN daily_row_count_max < @row_count THEN @row_count
        ELSE daily_row_count_max
    END,
    last_result_count = @row_count,
    latest_event_storage_id = @event_storage_id,
    latest_run_id = @run_id,
    latest_materialized_at = @materialized_at,
    updated_at = SYSUTCDATETIME()
WHERE observation_date = @observation_date
  AND code_location = @code_location
  AND asset_key = @asset_key
  AND metadata_key = @metadata_key
  AND latest_event_storage_id < @event_storage_id;

IF @@ROWCOUNT > 0
    SET @recorded = 1;
ELSE IF NOT EXISTS (
    SELECT 1
    FROM alerting.asset_row_count_daily
    WHERE observation_date = @observation_date
      AND code_location = @code_location
      AND asset_key = @asset_key
      AND metadata_key = @metadata_key
)
    BEGIN
        INSERT INTO alerting.asset_row_count_daily (
            observation_date, code_location, asset_key, metadata_key,
            daily_materialization_count, daily_row_count_sum,
            daily_row_count_average, daily_row_count_min, daily_row_count_max,
            first_result_count, last_result_count, latest_event_storage_id,
            latest_run_id, latest_materialized_at
        )
        VALUES (
            @observation_date, @code_location, @asset_key, @metadata_key,
            1, @row_count, @row_count, @row_count, @row_count,
            @row_count, @row_count, @event_storage_id,
            @run_id, @materialized_at
        );
        SET @recorded = 1;
    END;

SELECT @recorded;
"""

_REFRESH_ROW_COUNT_AVERAGES_SQL = """
UPDATE target
SET average_1d = (
        SELECT AVG(CAST(history.daily_row_count_sum AS NUMERIC(38, 4)))
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date = DATEADD(DAY, -1, target.observation_date)
    ),
    average_7d = (
        SELECT AVG(CAST(history.daily_row_count_sum AS NUMERIC(38, 4)))
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= DATEADD(DAY, -7, target.observation_date)
          AND history.observation_date < target.observation_date
    ),
    average_28d = (
        SELECT AVG(CAST(history.daily_row_count_sum AS NUMERIC(38, 4)))
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= DATEADD(DAY, -28, target.observation_date)
          AND history.observation_date < target.observation_date
    ),
    observed_days_7d = (
        SELECT COUNT(*)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= DATEADD(DAY, -7, target.observation_date)
          AND history.observation_date < target.observation_date
    ),
    observed_days_28d = (
        SELECT COUNT(*)
        FROM alerting.asset_row_count_daily AS history
        WHERE history.code_location = target.code_location
          AND history.asset_key = target.asset_key
          AND history.metadata_key = target.metadata_key
          AND history.observation_date >= DATEADD(DAY, -28, target.observation_date)
          AND history.observation_date < target.observation_date
    ),
    updated_at = SYSUTCDATETIME()
FROM alerting.asset_row_count_daily AS target
WHERE target.code_location = ?
  AND target.asset_key = ?
  AND target.metadata_key = ?
  AND target.observation_date >= ?;
"""

_REFRESH_ROW_COUNT_CHANGES_SQL = """
UPDATE alerting.asset_row_count_daily
SET change_vs_1d_pct = CASE
        WHEN average_1d IS NULL OR average_1d = 0 THEN NULL
        ELSE ROUND(
            (CAST(daily_row_count_sum AS NUMERIC(38, 4)) - average_1d)
            / average_1d * 100,
            4
        )
    END,
    change_vs_7d_pct = CASE
        WHEN average_7d IS NULL OR average_7d = 0 THEN NULL
        ELSE ROUND(
            (CAST(daily_row_count_sum AS NUMERIC(38, 4)) - average_7d)
            / average_7d * 100,
            4
        )
    END,
    change_vs_28d_pct = CASE
        WHEN average_28d IS NULL OR average_28d = 0 THEN NULL
        ELSE ROUND(
            (CAST(daily_row_count_sum AS NUMERIC(38, 4)) - average_28d)
            / average_28d * 100,
            4
        )
    END,
    updated_at = SYSUTCDATETIME()
WHERE code_location = ?
  AND asset_key = ?
  AND metadata_key = ?
  AND observation_date >= ?;
"""

_GET_LATEST_ROW_COUNT_TRENDS_SQL = """
WITH latest AS (
    SELECT observation_date, code_location, asset_key, metadata_key,
           daily_materialization_count, daily_row_count_sum,
           average_1d, average_7d, average_28d,
           observed_days_7d, observed_days_28d,
           change_vs_1d_pct, change_vs_7d_pct, change_vs_28d_pct,
           latest_run_id, latest_materialized_at,
           ROW_NUMBER() OVER (
               PARTITION BY code_location, asset_key, metadata_key
               ORDER BY observation_date DESC
           ) AS row_number
    FROM alerting.asset_row_count_daily
    WHERE code_location = ?
      AND observation_date < CONVERT(DATE, SYSUTCDATETIME())
)
SELECT observation_date, code_location, asset_key, metadata_key,
       daily_materialization_count, daily_row_count_sum,
       average_1d, average_7d, average_28d,
       observed_days_7d, observed_days_28d,
       change_vs_1d_pct, change_vs_7d_pct, change_vs_28d_pct,
       latest_run_id, latest_materialized_at
FROM latest
WHERE row_number = 1;
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
    """Manages tenant alert state and row-count history."""

    def __init__(self, mssql_reader: MssqlReaderResource) -> None:
        self._mssql_reader = mssql_reader

    def _get_alert_connection(self, database: str, user_code: str):
        return self._mssql_reader._get_connection(
            database,
            user_code,
            read_only=False,
        )

    def record_asset_row_counts(
        self,
        database: str,
        user_code: str,
        observations: Sequence[AssetRowCountObservation],
    ) -> int:
        if any(
            observation.code_location != user_code
            for observation in observations
        ):
            raise ValueError(
                "All observations must belong to the target user_code"
            )
        affected = {}
        recorded = 0
        with self._get_alert_connection(database, user_code) as conn:
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
                            observation.event_storage_id,
                            observation.run_id,
                            observation.materialized_at,
                        ),
                    )
                    if not cur.fetchone()[0]:
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

    def get_latest_asset_row_count_trends(
        self,
        database: str,
        user_code: str,
    ) -> Sequence[AssetRowCountTrend]:
        with self._get_alert_connection(database, user_code) as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_LATEST_ROW_COUNT_TRENDS_SQL, user_code)
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
