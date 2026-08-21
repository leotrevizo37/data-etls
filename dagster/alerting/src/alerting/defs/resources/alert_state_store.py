"""Alert delivery state management backed by Dagster PostgreSQL.

Stores alert lifecycle (OPEN -> REMINDER -> RESOLVED) in a dedicated
``alerting.alert_delivery_state`` table within the Dagster PostgreSQL
database.  The table is created idempotently on first use."""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

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
            conn.commit()

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
