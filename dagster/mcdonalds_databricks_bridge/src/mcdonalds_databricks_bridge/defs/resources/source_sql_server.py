from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
from dagster import resource

from .bridge_config import BridgeTableConfig


def _quote_sql_server_ident(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


@dataclass(frozen=True)
class SourceColumn:
    name: str
    sql_type: str
    nullable: bool
    ordinal_position: int
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None


@dataclass(frozen=True)
class ExtractResult:
    row_count: int
    max_updated_at: Optional[datetime]
    columns: List[SourceColumn]
    output_path: Optional[str]


def _sql_server_type_to_arrow(column: SourceColumn) -> pa.DataType:
    sql_type = column.sql_type.lower()

    if sql_type == "bigint":
        return pa.int64()
    if sql_type == "int":
        return pa.int32()
    if sql_type == "smallint":
        return pa.int16()
    if sql_type == "tinyint":
        return pa.int8()
    if sql_type == "bit":
        return pa.bool_()
    if sql_type == "real":
        return pa.float32()
    if sql_type == "float":
        return pa.float64()
    if sql_type in {"decimal", "numeric"}:
        precision = int(column.precision or 38)
        scale = int(column.scale or 0)
        precision = max(1, min(precision, 38))
        scale = max(0, min(scale, precision))
        return pa.decimal128(precision, scale)
    if sql_type == "money":
        return pa.decimal128(19, 4)
    if sql_type == "smallmoney":
        return pa.decimal128(10, 4)
    if sql_type == "date":
        return pa.date32()
    if sql_type in {"datetime", "datetime2", "smalldatetime"}:
        return pa.timestamp("us")
    if sql_type in {
        "uniqueidentifier",
        "time",
        "datetimeoffset",
        "char",
        "nchar",
        "varchar",
        "nvarchar",
        "text",
        "ntext",
        "xml",
    }:
        return pa.string()
    if sql_type in {"binary", "varbinary", "image"}:
        return pa.binary()

    return pa.string()


def _normalize_value(value: Any, sql_type: str) -> Any:
    if value is None:
        return None

    sql_type = sql_type.lower()

    if isinstance(value, uuid.UUID):
        return str(value)

    if sql_type in {"uniqueidentifier", "time", "datetimeoffset"}:
        return str(value)

    if isinstance(value, time):
        return value.isoformat()

    if isinstance(value, (datetime, date, Decimal, bytes, bytearray, bool, int, float, str)):
        return value

    return str(value)


class SourceSqlServerResource:
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
        fetch_batch_size: int,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver
        self.fetch_batch_size = fetch_batch_size

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

    def get_table_schema(self, table_config: BridgeTableConfig) -> List[SourceColumn]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            rows = cursor.columns(
                schema=table_config.source_schema,
                table=table_config.source_table,
            ).fetchall()

        columns: List[SourceColumn] = []
        for row in rows:
            columns.append(
                SourceColumn(
                    name=row.column_name,
                    sql_type=row.type_name,
                    nullable=bool(int(row.nullable)) if row.nullable is not None else True,
                    ordinal_position=int(row.ordinal_position),
                    precision=int(row.column_size) if row.column_size is not None else None,
                    scale=int(row.decimal_digits) if row.decimal_digits is not None else None,
                    length=int(row.column_size) if row.column_size is not None else None,
                )
            )

        columns.sort(key=lambda item: item.ordinal_position)
        if not columns:
            raise RuntimeError(f"No columns found for source table {table_config.source_fqn}.")

        return columns

    def extract_incremental_to_parquet(
        self,
        table_config: BridgeTableConfig,
        columns: List[SourceColumn],
        from_updated_at: Optional[datetime],
        output_path: Path,
    ) -> ExtractResult:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        quoted_table = (
            _quote_sql_server_ident(table_config.source_schema)
            + "."
            + _quote_sql_server_ident(table_config.source_table)
        )
        order_by_columns = [
            _quote_sql_server_ident(table_config.updated_at_column),
            *[_quote_sql_server_ident(key) for key in table_config.primary_keys],
        ]
        order_by = ", ".join(order_by_columns)

        params: List[Any] = []
        query = "SELECT * FROM " + quoted_table
        if from_updated_at is not None:
            query += (
                " WHERE "
                + _quote_sql_server_ident(table_config.updated_at_column)
                + " >= ?"
            )
            params.append(from_updated_at)
        query += " ORDER BY " + order_by

        arrow_schema = pa.schema(
            [
                pa.field(column.name, _sql_server_type_to_arrow(column), nullable=column.nullable)
                for column in columns
            ]
        )
        sql_types_by_name = {column.name: column.sql_type for column in columns}
        updated_index = next(
            index
            for index, column in enumerate(columns)
            if column.name.lower() == table_config.updated_at_column.lower()
        )

        row_count = 0
        max_updated_at: Optional[datetime] = None
        writer: Optional[pq.ParquetWriter] = None

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, *params)

                while True:
                    rows = cursor.fetchmany(self.fetch_batch_size)
                    if not rows:
                        break

                    normalized_rows: List[Dict[str, Any]] = []
                    for row in rows:
                        row_dict: Dict[str, Any] = {}
                        for column, value in zip(columns, row):
                            row_dict[column.name] = _normalize_value(
                                value=value,
                                sql_type=sql_types_by_name[column.name],
                            )
                        normalized_rows.append(row_dict)

                        updated_value = row[updated_index]
                        if updated_value is not None and (
                            max_updated_at is None or updated_value > max_updated_at
                        ):
                            max_updated_at = updated_value

                    table = pa.Table.from_pylist(normalized_rows, schema=arrow_schema)
                    if writer is None:
                        writer = pq.ParquetWriter(output_path.as_posix(), table.schema)
                    writer.write_table(table)
                    row_count += len(normalized_rows)
        finally:
            if writer is not None:
                writer.close()

        return ExtractResult(
            row_count=row_count,
            max_updated_at=max_updated_at,
            columns=columns,
            output_path=output_path.as_posix() if row_count > 0 else None,
        )


@resource
def source_sqlserver_resource(_):
    return SourceSqlServerResource(
        host=os.getenv("BRIDGE_SRCSQL_HOST", "localhost"),
        port=int(os.getenv("BRIDGE_SRCSQL_PORT", "1433")),
        database=os.getenv("BRIDGE_SRCSQL_DB", ""),
        user=os.getenv("BRIDGE_SRCSQL_USER", ""),
        password=os.getenv("BRIDGE_SRCSQL_PASSWORD", ""),
        connect_timeout=int(os.getenv("BRIDGE_SRCSQL_CONNECT_TIMEOUT", "15")),
        command_timeout=int(os.getenv("BRIDGE_SRCSQL_COMMAND_TIMEOUT", "5000")),
        driver=os.getenv("BRIDGE_SRCSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        fetch_batch_size=int(os.getenv("BRIDGE_SRCSQL_FETCH_BATCH_SIZE", "5000")),
    )
