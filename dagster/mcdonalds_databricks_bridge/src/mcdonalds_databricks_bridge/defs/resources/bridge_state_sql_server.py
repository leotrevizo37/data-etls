from __future__ import annotations

from datetime import datetime
from typing import Optional
import os

import pyodbc
from dagster import resource


class BridgeStateSqlServerResource:
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
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver

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
        )
        conn = pyodbc.connect(conn_str)
        conn.timeout = self.command_timeout
        return conn

    def get_watermark(self, ingestion_name: str) -> Optional[datetime]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "EXEC bridge.usp_BridgeIngestionControl_GetWatermark @IngestionName = ?",
                    ingestion_name,
                )
                row = cursor.fetchone()
            except pyodbc.Error:
                return None

        if row is None:
            return None
        return row.LastLoadedAt

    def mark_run_started(self, ingestion_name: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "EXEC bridge.usp_BridgeIngestionControl_MarkRunStarted @IngestionName = ?",
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
            cursor = conn.cursor()
            cursor.execute(
                "EXEC bridge.usp_BridgeIngestionControl_MarkRunCompleted "
                "@IngestionName = ?, @NewLastLoadedAt = ?, @Succeeded = ?, @ErrorMessage = ?",
                ingestion_name,
                new_last_loaded_at,
                1 if succeeded else 0,
                error_message,
            )
            conn.commit()


@resource
def bridge_state_sqlserver_resource(_):
    return BridgeStateSqlServerResource(
        host=os.getenv("MSSQL_HOST", "localhost"),
        port=int(os.getenv("MSSQL_PORT", "1433")),
        database=os.getenv("APP_DB", ""),
        user=os.getenv("MSSQL_USER", ""),
        password=os.getenv("MSSQL_PASSWORD", ""),
        connect_timeout=int(os.getenv("MSSQL_CONNECT_TIMEOUT", "15")),
        command_timeout=int(os.getenv("MSSQL_COMMAND_TIMEOUT", "5000")),
        driver=os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
    )
