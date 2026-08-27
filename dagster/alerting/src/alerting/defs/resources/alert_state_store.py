"""Alert delivery state management backed by Dagster PostgreSQL.

Stores alert lifecycle (OPEN -> REMINDER -> RESOLVED) in a dedicated
``alerting.alert_delivery_state`` table within the Dagster PostgreSQL
database.  The table is created idempotently on first use."""

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, Optional, Sequence

import psycopg2
import psycopg2.extras
from dagster import InitResourceContext, resource


ALERT_STATE_OPEN = "OPEN"
ALERT_STATE_REMINDER = "REMINDER"
ALERT_STATE_RESOLVED = "RESOLVED"

_ENSURE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS alerting;"

_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alerting.alert_delivery_state (
    alert_key          VARCHAR(500)  PRIMARY KEY,
    user_code          VARCHAR(100)  NOT NULL,
    alert_type         VARCHAR(50)   NOT NULL,
    entity_id          VARCHAR(250)  NOT NULL,
    state              VARCHAR(20)   NOT NULL,
    details            TEXT          NULL,
    first_detected_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_notified_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    resolved_at        TIMESTAMPTZ   NULL,
    notification_count INT           NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

_ENSURE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS ix_alert_state_active
    ON alerting.alert_delivery_state (state)
    WHERE state IN ('OPEN', 'REMINDER');
"""

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
    """A row from alert_delivery_state."""
    alert_key: str
    user_code: str
    alert_type: str
    entity_id: str
    state: str
    details: Optional[str]
    first_detected_at: datetime
    last_notified_at: datetime
    resolved_at: Optional[datetime]
    notification_count: int


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
    """Manages alert delivery state in Dagster PostgreSQL."""

    def __init__(self) -> None:
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

    def _ensure_schema(self) -> None:
        """Idempotently create the alerting schema and table."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_ENSURE_SCHEMA_SQL)
                cur.execute(_ENSURE_TABLE_SQL)
                cur.execute(_ENSURE_INDEX_SQL)
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

    def get_active_alerts(self) -> Dict[str, AlertRecord]:
        """Return all alerts with state OPEN or REMINDER, keyed by alert_key."""
        query = """
            SELECT alert_key, user_code, alert_type, entity_id, state,
                   details, first_detected_at, last_notified_at,
                   resolved_at, notification_count
            FROM alerting.alert_delivery_state
            WHERE state IN (%s, %s)
        """
        result: Dict[str, AlertRecord] = {}
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ALERT_STATE_OPEN, ALERT_STATE_REMINDER))
                for row in cur.fetchall():
                    rec = AlertRecord(
                        alert_key=row[0],
                        user_code=row[1],
                        alert_type=row[2],
                        entity_id=row[3],
                        state=row[4],
                        details=row[5],
                        first_detected_at=row[6],
                        last_notified_at=row[7],
                        resolved_at=row[8],
                        notification_count=row[9],
                    )
                    result[rec.alert_key] = rec
        return result

    def upsert_alert(
        self,
        alert_key: str,
        user_code: str,
        alert_type: str,
        entity_id: str,
        details: Optional[str] = None,
        state: str = ALERT_STATE_OPEN,
    ) -> None:
        """Insert a new alert or update an existing one (bump notification count)."""
        query = """
            INSERT INTO alerting.alert_delivery_state
                (alert_key, user_code, alert_type, entity_id, state, details,
                 first_detected_at, last_notified_at, notification_count,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), 1, NOW(), NOW())
            ON CONFLICT (alert_key) DO UPDATE SET
                state = EXCLUDED.state,
                details = EXCLUDED.details,
                last_notified_at = NOW(),
                notification_count = alerting.alert_delivery_state.notification_count + 1,
                resolved_at = NULL,
                updated_at = NOW()
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (alert_key, user_code, alert_type, entity_id, state, details),
                )
            conn.commit()

    def mark_resolved(self, alert_key: str) -> None:
        """Transition an alert to RESOLVED and set resolved_at."""
        query = """
            UPDATE alerting.alert_delivery_state
            SET state = %s,
                resolved_at = NOW(),
                updated_at = NOW()
            WHERE alert_key = %s
              AND state IN (%s, %s)
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        ALERT_STATE_RESOLVED,
                        alert_key,
                        ALERT_STATE_OPEN,
                        ALERT_STATE_REMINDER,
                    ),
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
        if alert is None:
            return False
        now = datetime.now(timezone.utc)
        last = alert.last_notified_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (now - last).total_seconds() / 60
        return elapsed >= reminder_interval_minutes


@resource
def alert_state_store_resource(
    _context: InitResourceContext,
) -> AlertStateStoreResource:
    return AlertStateStoreResource()
