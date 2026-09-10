from datetime import date, datetime
from typing import Any, Dict, Optional

import os
import pyodbc
from dagster import resource


def build_dim_store_schedules_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.dimStoreSchedules AS target
USING (
    SELECT
        StoreId,
        CustomerNo,
        StoreName,
        OpeningDate,
        ClosingDate,
        IsBlocked,
        BlockedFrom,
        BlockedTo,
        ReceivingId,
        DayCode,
        DayNo,
        IsClosed,
        OpeningTime,
        LunchStartTime,
        LunchEndTime,
        ClosingTime,
        CAST(
            CASE
                WHEN ClosingDate IS NOT NULL
                    AND ClosingDate < CONVERT(DATE, SYSUTCDATETIME())
                    THEN 1
                ELSE 0
            END
            AS BIT
        ) AS IsDeleted
    FROM stg.dimStoreSchedules
    WHERE RunId = ?
) AS source
    ON target.StoreId = source.StoreId
    AND target.DayNo = source.DayNo
WHEN MATCHED AND EXISTS (
    SELECT
        source.CustomerNo,
        source.StoreName,
        source.OpeningDate,
        source.ClosingDate,
        source.IsBlocked,
        source.BlockedFrom,
        source.BlockedTo,
        source.ReceivingId,
        source.DayCode,
        source.IsClosed,
        source.OpeningTime,
        source.LunchStartTime,
        source.LunchEndTime,
        source.ClosingTime,
        source.IsDeleted
    EXCEPT
    SELECT
        target.CustomerNo,
        target.StoreName,
        target.OpeningDate,
        target.ClosingDate,
        target.IsBlocked,
        target.BlockedFrom,
        target.BlockedTo,
        target.ReceivingId,
        target.DayCode,
        target.IsClosed,
        target.OpeningTime,
        target.LunchStartTime,
        target.LunchEndTime,
        target.ClosingTime,
        target.IsDeleted
) THEN
    UPDATE SET
        target.CustomerNo = source.CustomerNo,
        target.StoreName = source.StoreName,
        target.OpeningDate = source.OpeningDate,
        target.ClosingDate = source.ClosingDate,
        target.IsBlocked = source.IsBlocked,
        target.BlockedFrom = source.BlockedFrom,
        target.BlockedTo = source.BlockedTo,
        target.ReceivingId = source.ReceivingId,
        target.DayCode = source.DayCode,
        target.IsClosed = source.IsClosed,
        target.OpeningTime = source.OpeningTime,
        target.LunchStartTime = source.LunchStartTime,
        target.LunchEndTime = source.LunchEndTime,
        target.ClosingTime = source.ClosingTime,
        target.IsDeleted = source.IsDeleted,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        StoreId,
        CustomerNo,
        StoreName,
        OpeningDate,
        ClosingDate,
        IsBlocked,
        BlockedFrom,
        BlockedTo,
        ReceivingId,
        DayCode,
        DayNo,
        IsClosed,
        OpeningTime,
        LunchStartTime,
        LunchEndTime,
        ClosingTime,
        IsDeleted,
        CreatedAt,
        ModifiedAt
    )
    VALUES (
        source.StoreId,
        source.CustomerNo,
        source.StoreName,
        source.OpeningDate,
        source.ClosingDate,
        source.IsBlocked,
        source.BlockedFrom,
        source.BlockedTo,
        source.ReceivingId,
        source.DayCode,
        source.DayNo,
        source.IsClosed,
        source.OpeningTime,
        source.LunchStartTime,
        source.LunchEndTime,
        source.ClosingTime,
        source.IsDeleted,
        SYSUTCDATETIME(),
        SYSUTCDATETIME()
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_out_of_hours_sales_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factOutOfHoursSales AS target
USING (
    SELECT
        SaleId,
        SaleNumber,
        Amount,
        LocalTimeSpan,
        UserName,
        StoreId,
        OutOfHoursType,
        DeviationMinutes
    FROM stg.factOutOfHoursSales
    WHERE RunId = ?
) AS source
    ON target.SaleId = source.SaleId
WHEN MATCHED AND source.OutOfHoursType IS NULL THEN
    DELETE
WHEN MATCHED THEN
    UPDATE SET
        target.SaleNumber = source.SaleNumber,
        target.Amount = source.Amount,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.UserName = source.UserName,
        target.StoreId = source.StoreId,
        target.OutOfHoursType = source.OutOfHoursType,
        target.DeviationMinutes = source.DeviationMinutes,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET AND source.OutOfHoursType IS NOT NULL THEN
    INSERT (
        SaleId,
        SaleNumber,
        Amount,
        LocalTimeSpan,
        UserName,
        StoreId,
        OutOfHoursType,
        DeviationMinutes
    )
    VALUES (
        source.SaleId,
        source.SaleNumber,
        source.Amount,
        source.LocalTimeSpan,
        source.UserName,
        source.StoreId,
        source.OutOfHoursType,
        source.DeviationMinutes
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_store_sales_daily_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factStoreSalesDaily AS target
USING (
    SELECT
        StoreId,
        SalesDate,
        SalesCount,
        SalesAmount
    FROM stg.factStoreSalesDaily
    WHERE RunId = ?
) AS source
    ON target.StoreId = source.StoreId
    AND target.SalesDate = source.SalesDate
WHEN MATCHED THEN
    UPDATE SET
        target.SalesCount = source.SalesCount,
        target.SalesAmount = source.SalesAmount,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        StoreId,
        SalesDate,
        SalesCount,
        SalesAmount
    )
    VALUES (
        source.StoreId,
        source.SalesDate,
        source.SalesCount,
        source.SalesAmount
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_manager_access_daily_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factManagerAccessDaily AS target
USING (
    SELECT
        Fecha,
        FechaCorte,
        CorteHora,
        DiasHistorial,
        IdUsuario,
        NoEmpleado,
        NombreGerente,
        NombreUsuario,
        Email,
        Puesto,
        Rol,
        Activo,
        IdTienda,
        ClaveTienda,
        NombreTienda,
        Zona,
        UltimoInicioSesion,
        IngresoAntesDelCorte,
        DiasSinIngresar,
        EstatusCorte
    FROM stg.factManagerAccessDaily
    WHERE RunId = ?
) AS source
    ON target.Fecha = source.Fecha
    AND target.IdUsuario = source.IdUsuario
    AND target.ClaveTienda = source.ClaveTienda
WHEN MATCHED THEN
    UPDATE SET
        target.FechaCorte = source.FechaCorte,
        target.CorteHora = source.CorteHora,
        target.DiasHistorial = source.DiasHistorial,
        target.NoEmpleado = source.NoEmpleado,
        target.NombreGerente = source.NombreGerente,
        target.NombreUsuario = source.NombreUsuario,
        target.Email = source.Email,
        target.Puesto = source.Puesto,
        target.Rol = source.Rol,
        target.Activo = source.Activo,
        target.IdTienda = source.IdTienda,
        target.NombreTienda = source.NombreTienda,
        target.Zona = source.Zona,
        target.UltimoInicioSesion = source.UltimoInicioSesion,
        target.IngresoAntesDelCorte = source.IngresoAntesDelCorte,
        target.DiasSinIngresar = source.DiasSinIngresar,
        target.EstatusCorte = source.EstatusCorte,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Fecha,
        FechaCorte,
        CorteHora,
        DiasHistorial,
        IdUsuario,
        NoEmpleado,
        NombreGerente,
        NombreUsuario,
        Email,
        Puesto,
        Rol,
        Activo,
        IdTienda,
        ClaveTienda,
        NombreTienda,
        Zona,
        UltimoInicioSesion,
        IngresoAntesDelCorte,
        DiasSinIngresar,
        EstatusCorte
    )
    VALUES (
        source.Fecha,
        source.FechaCorte,
        source.CorteHora,
        source.DiasHistorial,
        source.IdUsuario,
        source.NoEmpleado,
        source.NombreGerente,
        source.NombreUsuario,
        source.Email,
        source.Puesto,
        source.Rol,
        source.Activo,
        source.IdTienda,
        source.ClaveTienda,
        source.NombreTienda,
        source.Zona,
        source.UltimoInicioSesion,
        source.IngresoAntesDelCorte,
        source.DiasSinIngresar,
        source.EstatusCorte
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
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_GetWatermark @IngestionName = ?",
                    ingestion_name,
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return row.LastLoadedAt

    def mark_run_started(self, ingestion_name: str) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunStarted @IngestionName = ?",
                    ingestion_name,
                )
                conn.commit()

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

    def execute_query(self, query: str, *args) -> Optional[int]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, *args)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    return e

    def clear_dim_store_schedules_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimStoreSchedules WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def write_dim_store_schedules_raw_page(
        self,
        run_id: str,
        attempt_number: int,
        page_number: int,
        request_cursor: Optional[str],
        payload_json: str,
    ) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
INSERT INTO raw.StoreSchedulesApiPage (
    RunId,
    AttemptNumber,
    PageNumber,
    RequestCursor,
    PayloadJson
)
VALUES (?, ?, ?, ?, ?)
""",
                    run_id,
                    attempt_number,
                    page_number,
                    request_cursor,
                    payload_json,
                )
                conn.commit()

    def write_dim_store_schedules_stage(
        self,
        run_id: str,
        rows: list[Dict[str, Any]],
    ) -> int:
        values = [
            (
                run_id,
                row["StoreId"],
                row["CustomerNo"],
                row["StoreName"],
                date.fromisoformat(row["OpeningDate"])
                if row["OpeningDate"] is not None
                else None,
                date.fromisoformat(row["ClosingDate"])
                if row["ClosingDate"] is not None
                else None,
                row["IsBlocked"],
                date.fromisoformat(row["BlockedFrom"])
                if row["BlockedFrom"] is not None
                else None,
                date.fromisoformat(row["BlockedTo"])
                if row["BlockedTo"] is not None
                else None,
                row["ReceivingId"],
                row["DayCode"],
                row["DayNo"],
                row["IsClosed"],
                datetime.strptime(row["OpeningTime"], "%H:%M:%S").time()
                if row["OpeningTime"] is not None
                else None,
                datetime.strptime(row["LunchStartTime"], "%H:%M:%S").time()
                if row["LunchStartTime"] is not None
                else None,
                datetime.strptime(row["LunchEndTime"], "%H:%M:%S").time()
                if row["LunchEndTime"] is not None
                else None,
                datetime.strptime(row["ClosingTime"], "%H:%M:%S").time()
                if row["ClosingTime"] is not None
                else None,
            )
            for row in rows
        ]
        if not values:
            return 0
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(
                    """
INSERT INTO stg.dimStoreSchedules (
    RunId,
    StoreId,
    CustomerNo,
    StoreName,
    OpeningDate,
    ClosingDate,
    IsBlocked,
    BlockedFrom,
    BlockedTo,
    ReceivingId,
    DayCode,
    DayNo,
    IsClosed,
    OpeningTime,
    LunchStartTime,
    LunchEndTime,
    ClosingTime
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
                    values,
                )
                conn.commit()
                return len(values)

    def merge_dim_store_schedules(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_store_schedules_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_out_of_hours_sales_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factOutOfHoursSales WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_out_of_hours_sales(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_out_of_hours_sales_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_store_sales_daily_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factStoreSalesDaily WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_store_sales_daily(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_store_sales_daily_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_manager_access_daily_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factManagerAccessDaily WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def write_fact_manager_access_daily_stage(
        self,
        run_id: str,
        reference_date: date,
        payload: Dict[str, Any],
    ) -> int:
        cutoff_at = datetime.strptime(
            payload["fechaCorte"][:26],
            "%Y-%m-%dT%H:%M:%S.%f"
            if "." in payload["fechaCorte"]
            else "%Y-%m-%dT%H:%M:%S",
        )
        rows = [
            (
                run_id,
                reference_date,
                cutoff_at,
                payload["corteHora"],
                payload["diasHistorial"],
                row["idUsuario"],
                row["noEmpleado"],
                row["nombreGerente"],
                row["nombreUsuario"],
                row["email"],
                row["puesto"],
                row["rol"],
                row["activo"],
                row["idTienda"],
                row["claveTienda"],
                row["nombreTienda"],
                row["zona"],
                datetime.strptime(
                    row["ultimoInicioSesion"][:26],
                    "%Y-%m-%dT%H:%M:%S.%f"
                    if "." in row["ultimoInicioSesion"]
                    else "%Y-%m-%dT%H:%M:%S",
                )
                if row["ultimoInicioSesion"] is not None
                else None,
                row["ingresoAntesDelCorte"],
                row["diasSinIngresar"],
                row["estatusCorte"],
            )
            for row in payload["data"]
        ]
        if not rows:
            return 0
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(
                    """
INSERT INTO stg.factManagerAccessDaily (
    RunId,
    Fecha,
    FechaCorte,
    CorteHora,
    DiasHistorial,
    IdUsuario,
    NoEmpleado,
    NombreGerente,
    NombreUsuario,
    Email,
    Puesto,
    Rol,
    Activo,
    IdTienda,
    ClaveTienda,
    NombreTienda,
    Zona,
    UltimoInicioSesion,
    IngresoAntesDelCorte,
    DiasSinIngresar,
    EstatusCorte
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
                    rows,
                )
                conn.commit()
                return len(rows)

    def merge_fact_manager_access_daily(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_manager_access_daily_merge_sql(), run_id)
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
    return SqlServerResource(
        host=os.getenv("MSSQL_HOST", "localhost"),
        port=int(os.getenv("MSSQL_PORT", "1433")),
        database=os.getenv("APP_DB", "YourDatabaseName"),
        user=os.getenv("MSSQL_USER", "app_ingestion_user"),
        password=os.getenv("MSSQL_PASSWORD", ""),
        connect_timeout=int(os.getenv("MSSQL_CONNECT_TIMEOUT", "300")),
        command_timeout=int(os.getenv("MSSQL_COMMAND_TIMEOUT", "4500")),
        driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        retries=int(os.getenv("MSSQL_CONNECT_RETRY_COUNT", "3")),
        retry_interval=int(os.getenv("MSSQL_CONNECT_RETRY_INTERVAL", "60")),
    )
