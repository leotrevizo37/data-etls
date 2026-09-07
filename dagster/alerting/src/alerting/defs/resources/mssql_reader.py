"""Read-only SQL Server resource for querying operational signals
across multiple user_code databases."""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import pyodbc
from dagster import InitResourceContext, resource


@dataclass
class SensorAlert:
    """A sensor-level alert detected from factRedingsAudits."""
    sensor_id: str
    minutes_without_readings: Optional[int] = None
    connection_lost_at: Optional[datetime] = None
    latest_time_span: Optional[datetime] = None


@dataclass
class SensorRecovery:
    sensor_id: str
    communication_resumed_at: datetime
    last_reading_at: datetime


@dataclass
class IngestionAlert:
    """A pipeline-level alert detected from ctl.IngestionControl."""
    ingestion_name: str
    last_run_status: str
    last_run_started_at: Optional[datetime] = None
    last_run_finished_at: Optional[datetime] = None
    last_run_error: Optional[str] = None


@dataclass
class SensorDimension:
    """Dimensional context for a sensor from dwh.dimSidonProdDimensions."""
    sensor_id: str
    location_name: Optional[str] = None
    sublocation_name: Optional[str] = None
    sublocation_type: Optional[str] = None
    device_name: Optional[str] = None
    device_type: Optional[str] = None
    sensor_name: Optional[str] = None
    sensor_type: Optional[str] = None


class MssqlReaderResource:
    """Provides read-only access to SQL Server databases for alerting queries."""

    def __init__(self):
        self._host = os.environ["MSSQL_HOST"]
        self._port = os.environ.get("MSSQL_PORT", "1433")
        self._user = os.environ["MSSQL_USER"]
        self._password = os.environ["MSSQL_PASSWORD"]
        self._driver = os.environ.get(
            "MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"
        )

    def _get_connection(
        self,
        database: str,
        user_code: Optional[str] = None,
        read_only: bool = True,
    ) -> pyodbc.Connection:
        prefix = "".join(
            character if character.isalnum() else "_"
            for character in (user_code or "").upper()
        )
        host = os.environ.get(f"{prefix}_MSSQL_HOST", self._host)
        port = os.environ.get(f"{prefix}_MSSQL_PORT", self._port)
        user = os.environ.get(f"{prefix}_MSSQL_USER", self._user)
        password = os.environ.get(f"{prefix}_MSSQL_PASSWORD", self._password)
        conn_str = (
            f"DRIVER={{{self._driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
        if read_only:
            conn_str += "ApplicationIntent=ReadOnly;"
        return pyodbc.connect(conn_str, timeout=30)

    def get_connection_lost_sensors(
        self,
        database: str,
        lookback_hours: int = 2,
        user_code: Optional[str] = None,
    ) -> List[SensorAlert]:
        """Return sensors with IsConnectionLost = 1 in recent timespan."""
        query = """
            WITH LatestSensorStatus AS (
                SELECT
                    CAST(SensorId AS VARCHAR(36)) AS SensorId,
                    ConnectionLostAt,
                    MinutesWithoutReadings,
                    IsConnectionLost,
                    ROW_NUMBER() OVER (
                        PARTITION BY SensorId
                        ORDER BY TimeSpan DESC
                    ) AS RowNumber
                FROM observability.factRedingsAudits
                WHERE TimeSpan >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            )
            SELECT SensorId, ConnectionLostAt, MinutesWithoutReadings
            FROM LatestSensorStatus
            WHERE RowNumber = 1
              AND IsConnectionLost = 1
        """
        results: List[SensorAlert] = []
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, -lookback_hours)
                for row in cursor.fetchall():
                    results.append(
                        SensorAlert(
                            sensor_id=row.SensorId,
                            connection_lost_at=row.ConnectionLostAt,
                            minutes_without_readings=row.MinutesWithoutReadings,
                        )
                    )
        return results

    def get_sensor_recoveries(
        self,
        database: str,
        sensor_ids: List[str],
        user_code: Optional[str] = None,
    ) -> Dict[str, SensorRecovery]:
        if not sensor_ids:
            return {}
        placeholders = ",".join("?" for _ in sensor_ids)
        query = f"""
            WITH LatestAudit AS (
                SELECT
                    SensorId,
                    IsConnectionLost,
                    ROW_NUMBER() OVER (
                        PARTITION BY SensorId
                        ORDER BY TimeSpan DESC
                    ) AS RowNumber
                FROM observability.factRedingsAudits
                WHERE CAST(SensorId AS VARCHAR(36)) IN ({placeholders})
            ),
            LastLostAudit AS (
                SELECT
                    SensorId,
                    ConnectionLostAt,
                    ROW_NUMBER() OVER (
                        PARTITION BY SensorId
                        ORDER BY TimeSpan DESC
                    ) AS RowNumber
                FROM observability.factRedingsAudits
                WHERE IsConnectionLost = 1
                  AND CAST(SensorId AS VARCHAR(36)) IN ({placeholders})
            )
            SELECT
                CAST(latest.SensorId AS VARCHAR(36)) AS SensorId,
                MIN(reading.TimeSpan) AS CommunicationResumedAt,
                MAX(reading.TimeSpan) AS LastReadingAt
            FROM LatestAudit AS latest
            INNER JOIN LastLostAudit AS lost
                ON lost.SensorId = latest.SensorId
               AND lost.RowNumber = 1
            INNER JOIN dwh.factReadingsMeasurement AS reading
                ON reading.SensorId = latest.SensorId
               AND reading.TimeSpan >= lost.ConnectionLostAt
            WHERE latest.RowNumber = 1
              AND latest.IsConnectionLost = 0
            GROUP BY latest.SensorId
        """
        results: Dict[str, SensorRecovery] = {}
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, sensor_ids + sensor_ids)
                for row in cursor.fetchall():
                    results[row.SensorId] = SensorRecovery(
                        sensor_id=row.SensorId,
                        communication_resumed_at=row.CommunicationResumedAt,
                        last_reading_at=row.LastReadingAt,
                    )
        return results

    def get_late_reading_sensors(
        self,
        database: str,
        lookback_hours: int = 2,
        user_code: Optional[str] = None,
    ) -> List[SensorAlert]:
        """Return sensors with HasLateReadings = 1 but NOT connection lost."""
        query = """
            SELECT
                CAST(SensorId AS VARCHAR(36)) AS SensorId,
                MAX(TimeSpan)                 AS LatestTimeSpan,
                MAX(MinutesWithoutReadings)   AS MinutesWithoutReadings
            FROM observability.factRedingsAudits
            WHERE HasLateReadings = 1
              AND IsConnectionLost = 0
              AND TimeSpan >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            GROUP BY SensorId
        """
        results: List[SensorAlert] = []
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, -lookback_hours)
                for row in cursor.fetchall():
                    results.append(
                        SensorAlert(
                            sensor_id=row.SensorId,
                            latest_time_span=row.LatestTimeSpan,
                            minutes_without_readings=row.MinutesWithoutReadings,
                        )
                    )
        return results

    def get_failed_ingestions(
        self,
        database: str,
        user_code: Optional[str] = None,
    ) -> List[IngestionAlert]:
        """Return active ingestions with LastRunStatus = 'FAILED'."""
        query = """
            SELECT
                IngestionName,
                LastRunStatus,
                LastRunStartedAt,
                LastRunFinishedAt,
                LastRunError
            FROM ctl.IngestionControl
            WHERE IsActive = 1
              AND LastRunStatus = 'FAILED'
        """
        results: List[IngestionAlert] = []
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                for row in cursor.fetchall():
                    results.append(
                        IngestionAlert(
                            ingestion_name=row.IngestionName,
                            last_run_status=row.LastRunStatus,
                            last_run_started_at=row.LastRunStartedAt,
                            last_run_finished_at=row.LastRunFinishedAt,
                            last_run_error=row.LastRunError,
                        )
                    )
        return results

    def get_stale_ingestions(
        self,
        database: str,
        ingestion_intervals: Dict[str, int],
        default_interval_minutes: int = 720,
        user_code: Optional[str] = None,
    ) -> List[IngestionAlert]:
        """Return active ingestions whose LastRunFinishedAt exceeds the
        expected interval.

        Only considers ingestions that have completed at least once
        (LastRunStatus not in NEVER_RUN, RUNNING) and are not already
        in FAILED status (those are caught by get_failed_ingestions).
        """
        query = """
            SELECT
                IngestionName,
                LastRunStatus,
                LastRunStartedAt,
                LastRunFinishedAt,
                LastRunError
            FROM ctl.IngestionControl
            WHERE IsActive = 1
              AND LastRunStatus NOT IN ('NEVER_RUN', 'RUNNING', 'FAILED')
              AND LastRunFinishedAt IS NOT NULL
        """
        results: List[IngestionAlert] = []
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                now_utc = datetime.utcnow()
                for row in cursor.fetchall():
                    interval = ingestion_intervals.get(
                        row.IngestionName,
                        default_interval_minutes,
                    )
                    if row.LastRunFinishedAt is None:
                        continue
                    elapsed_minutes = (
                        now_utc - row.LastRunFinishedAt
                    ).total_seconds() / 60
                    if elapsed_minutes > interval:
                        results.append(
                            IngestionAlert(
                                ingestion_name=row.IngestionName,
                                last_run_status=row.LastRunStatus,
                                last_run_started_at=row.LastRunStartedAt,
                                last_run_finished_at=row.LastRunFinishedAt,
                                last_run_error=(
                                    f"Stale: {elapsed_minutes:.0f}min "
                                    f"since last run (expected <={interval}min)"
                                ),
                            )
                        )
        return results

    def get_sensor_dimensions(
        self,
        database: str,
        sensor_ids: List[str],
        user_code: Optional[str] = None,
    ) -> Dict[str, "SensorDimension"]:
        """Fetch dimension context for a set of sensor IDs from
        dwh.dimSidonProdDimensions."""
        if not sensor_ids:
            return {}
        placeholders = ",".join("?" for _ in sensor_ids)
        query = f"""
            SELECT
                CAST(SensorId AS VARCHAR(36)) AS SensorId,
                location_name,
                sublocation_name,
                sublocation_type,
                device_name,
                device_type,
                sensor_name,
                sensor_type
            FROM dwh.dimSidonProdDimensions
            WHERE CAST(SensorId AS VARCHAR(36)) IN ({placeholders})
        """
        results: Dict[str, SensorDimension] = {}
        with self._get_connection(database, user_code) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, sensor_ids)
                for row in cursor.fetchall():
                    results[row.SensorId] = SensorDimension(
                        sensor_id=row.SensorId,
                        location_name=row.location_name,
                        sublocation_name=row.sublocation_name,
                        sublocation_type=row.sublocation_type,
                        device_name=row.device_name,
                        device_type=row.device_type,
                        sensor_name=row.sensor_name,
                        sensor_type=row.sensor_type,
                    )
        return results


@resource
def mssql_reader_resource(_context: InitResourceContext) -> MssqlReaderResource:
    return MssqlReaderResource()
