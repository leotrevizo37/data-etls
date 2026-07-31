from datetime import datetime, timezone
from typing import Optional, Dict, Any

import os
import pyodbc
from dagster import resource


def build_dim_sidon_prod_dimensions_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.dimSidonProdDimensions AS target
USING (
    SELECT
        SensorId,
        SensorTyId,
        DeviceId,
        sensor_name,
        sensor_type,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        LocationId,
        sublocation_type
    FROM stg.dimSidonProdDimensions
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
WHEN MATCHED AND EXISTS (
    SELECT
        source.SensorTyId,
        source.DeviceId,
        source.sensor_name,
        source.sensor_type,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.LocationId,
        source.sublocation_type
    EXCEPT
    SELECT
        target.SensorTyId,
        target.DeviceId,
        target.sensor_name,
        target.sensor_type,
        target.DeviceTyId,
        target.SubLocationId,
        target.Active,
        target.device_name,
        target.device_type,
        target.SubLocationTyId,
        target.sublocation_name,
        target.location_name,
        target.city_name,
        target.CityId,
        target.LocationId,
        target.sublocation_type
) THEN
    UPDATE SET
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.sensor_name = source.sensor_name,
        target.sensor_type = source.sensor_type,
        target.DeviceTyId = source.DeviceTyId,
        target.SubLocationId = source.SubLocationId,
        target.Active = source.Active,
        target.device_name = source.device_name,
        target.device_type = source.device_type,
        target.SubLocationTyId = source.SubLocationTyId,
        target.sublocation_name = source.sublocation_name,
        target.location_name = source.location_name,
        target.city_name = source.city_name,
        target.CityId = source.CityId,
        target.LocationId = source.LocationId,
        target.sublocation_type = source.sublocation_type,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        SensorTyId,
        DeviceId,
        sensor_name,
        sensor_type,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        LocationId,
        sublocation_type
    )
    VALUES (
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.sensor_name,
        source.sensor_type,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.LocationId,
        source.sublocation_type
    )
WHEN NOT MATCHED BY SOURCE AND target.Active = 1 THEN
    UPDATE SET
        target.Active = 0,
        target.ModifiedAt = SYSUTCDATETIME()
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_readings_measurement_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factReadingsMeasurement AS target
USING (
    SELECT
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        Anomalies,
        ReadingsCount
    FROM stg.factReadingsMeasurement
    WHERE RunId = ?
) AS source
    ON target.OperationId = source.OperationId
WHEN MATCHED THEN
    UPDATE SET
        target.SensorId = source.SensorId,
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.MeasurementValue = source.MeasurementValue,
        target.MeasurementStdDev = source.MeasurementStdDev,
        target.Anomalies = source.Anomalies,
        target.ReadingsCount = source.ReadingsCount,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        Anomalies,
        ReadingsCount
    )
    VALUES (
        source.OperationId,
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.LocationId,
        source.DeviceType,
        source.SensorType,
        source.LocalTimeSpan,
        source.MeasurementValue,
        source.MeasurementStdDev,
        source.Anomalies,
        source.ReadingsCount
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_sensor_events_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factSensorEvents AS target
USING (
    SELECT
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    FROM stg.factSensorEvents
    WHERE RunId = ?
) AS source
    ON target.ReadId = source.ReadId
WHEN MATCHED THEN
    UPDATE SET
        target.SensorId = source.SensorId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.LocationName = source.LocationName,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.SensorName = source.SensorName,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.Value = source.Value,
        target.EventMinutes = source.EventMinutes,
        target.IsEventCompleted = source.IsEventCompleted,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    )
    VALUES (
        source.ReadId,
        source.SensorId,
        source.DeviceId,
        source.LocationId,
        source.LocationName,
        source.DeviceType,
        source.SensorType,
        source.SensorName,
        source.LocalTimeSpan,
        source.Value,
        source.EventMinutes,
        source.IsEventCompleted
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_REDINGS_AUDITS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factRedingsAudits AS target
USING (
    SELECT
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    FROM stg.factRedingsAudits
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED THEN
    UPDATE SET
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.ReadingsCount = source.ReadingsCount,
        target.HasLateReadings = CASE
            WHEN target.HasLateReadings = 1 OR source.HasLateReadings = 1 THEN 1
            ELSE 0
        END,
        target.IsConnectionLost = source.IsConnectionLost,
        target.LastReadingAt = source.LastReadingAt,
        target.ConnectionLostAt = source.ConnectionLostAt,
        target.MinutesWithoutReadings = source.MinutesWithoutReadings,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    )
    VALUES (
        source.SensorId,
        source.TimeSpan,
        source.LocalTimeSpan,
        source.ReadingsCount,
        source.HasLateReadings,
        source.IsConnectionLost,
        source.LastReadingAt,
        source.ConnectionLostAt,
        source.MinutesWithoutReadings
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


class SqlServerResource:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout: int,
        command_timeout: int,
        driver: str,
        retries: int,
        retry_interval: int,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver
        self.retries = retries
        self.retry_interval = retry_interval

    def _get_connection(self) -> pyodbc.Connection:
        conn_str = (
            "DRIVER={"
            + self.driver
            + "};SERVER="
            + self.host
            + ","
            + str(self.port)
            + ";DATABASE="
            + self.database
            + ";UID="
            + self.user
            + ";PWD="
            + self.password
            + ";TrustServerCertificate=yes"
            + ";Connection Timeout="
            + str(self.connect_timeout)
            + ";ConnectRetryCount="
            + str(self.retries)
            + ";ConnectRetryInterval="
            + str(self.retry_interval)
        )
        return pyodbc.connect(conn_str, timeout=self.connect_timeout)

    def get_watermark(self, ingestion_name: str) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "EXEC ctl.usp_IngestionControl_GetWatermark @IngestionName = ?",
                        ingestion_name,
                    )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.LastLoadedAt

    def get_min_open_sensor_event_time(self) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "SELECT MIN(LocalTimeSpan) AS OpenEventFrom "
                        "FROM dwh.factSensorEvents "
                        "WHERE IsEventCompleted = 0"
                    )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.OpenEventFrom

    def mark_run_started(self, ingestion_name: str) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunStarted @IngestionName = ?",
                    ingestion_name,
                )
                conn.commit()

    def execute_query(self, query: str, *args) -> Optional[int]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, *args)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    return e

    def fetch_scalar(self, query: str, *args):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, *args)
                row = cursor.fetchone()
                return row[0] if row is not None else None

    def delete_range(self,
                     schema_name: str,
                     table_name: str,
                     time_column: str,
                     from_utc: datetime,
                     to_utc: Optional[datetime] = None) -> int:
        if to_utc is None:
            to_utc = datetime.now(timezone.utc)
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_DeleteRange "
                    "@SchemaName = ?, "
                    "@TableName = ?, "
                    "@TimeColumn = ?, "
                    "@FromUtc = ?, "
                    "@ToUtc = ?",
                    schema_name,
                    table_name,
                    time_column,
                    from_utc,
                    to_utc,
                )
                rows = cursor.fetchone()
                conn.commit()
                if rows is None:
                    return 0
                deleted = rows.DeletedRowCount
                if deleted is None:
                    return 0
                return int(deleted)

    def mark_run_completed(
        self,
        ingestion_name: str,
        new_last_loaded_at: Optional[datetime],
        succeeded: bool,
        error_message: Optional[str],
    ) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunCompleted "
                    "@IngestionName = ?, @NewLastLoadedAt = ?, @Succeeded = ?, @ErrorMessage = ?",
                    ingestion_name,
                    new_last_loaded_at,
                    1 if succeeded else 0,
                    error_message,
                )
                conn.commit()

    def clear_dim_sidon_prod_dimensions_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimSidonProdDimensions WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_dim_sidon_prod_dimensions(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_sidon_prod_dimensions_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_sensor_events_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factSensorEvents WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_sensor_events(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_sensor_events_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_redings_audits_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factRedingsAudits WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_redings_audits(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(FACT_REDINGS_AUDITS_MERGE_SQL, run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_readings_measurement_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factReadingsMeasurement WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_readings_measurement(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_readings_measurement_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def get_basic_options(self) -> Dict[str, Any]:
        url = (
                "jdbc:sqlserver://"
                + self.host
                + ":"
                + str(self.port)
                + ";databaseName="
                + self.database
                + ";encrypt=true;trustServerCertificate=true"
                + ";Connection Timeout="
                + str(self.connect_timeout)
        )
        return {
            "url": url,
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

@resource
def sqlserver_resource(_):
    host = os.getenv("MSSQL_HOST", "localhost")
    port = int(os.getenv("MSSQL_PORT", "1433"))
    database = os.getenv("APP_DB", "YourDatabaseName")
    user = os.getenv("MSSQL_USER", "app_ingestion_user")
    password = os.getenv("MSSQL_PASSWORD", "")
    connect_timeout = int(os.getenv("MSSQL_CONNECT_TIMEOUT", "300"))
    command_timeout = int(os.getenv("MSSQL_COMMAND_TIMEOUT", "4500"))
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    connect_retry_count = int(os.getenv("MSSQL_CONNECT_RETRY_COUNT", "3"))
    connect_retry_interval = int(os.getenv("MSSQL_CONNECT_RETRY_INTERVAL", "60"))
    return SqlServerResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        driver=driver,
        retries=connect_retry_count,
        retry_interval=connect_retry_interval,
    )
