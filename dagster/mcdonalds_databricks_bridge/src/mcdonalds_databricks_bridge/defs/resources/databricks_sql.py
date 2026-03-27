from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable, List
import os

from dagster import resource
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

from .bridge_config import BridgeTableConfig
from .source_sql_server import SourceColumn


def _quote_databricks_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _sql_server_type_to_databricks(column: SourceColumn) -> str:
    sql_type = column.sql_type.lower()

    if sql_type == "bigint":
        return "BIGINT"
    if sql_type == "int":
        return "INT"
    if sql_type == "smallint":
        return "SMALLINT"
    if sql_type == "tinyint":
        return "TINYINT"
    if sql_type == "bit":
        return "BOOLEAN"
    if sql_type == "real":
        return "FLOAT"
    if sql_type == "float":
        return "DOUBLE"
    if sql_type in {"decimal", "numeric"}:
        precision = int(column.precision or 38)
        scale = int(column.scale or 0)
        precision = max(1, min(precision, 38))
        scale = max(0, min(scale, precision))
        return f"DECIMAL({precision},{scale})"
    if sql_type == "money":
        return "DECIMAL(19,4)"
    if sql_type == "smallmoney":
        return "DECIMAL(10,4)"
    if sql_type == "date":
        return "DATE"
    if sql_type in {"datetime", "datetime2", "smalldatetime"}:
        return "TIMESTAMP"
    if sql_type in {"time", "datetimeoffset"}:
        return "STRING"
    if sql_type in {"uniqueidentifier", "char", "nchar", "varchar", "nvarchar", "text", "ntext", "xml"}:
        return "STRING"
    if sql_type in {"binary", "varbinary", "image"}:
        return "BINARY"

    return "STRING"


class DatabricksSqlResource:
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        auth_mode: str,
        access_token: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        target_schema: str,
        staging_schema: str,
        volume_catalog: str,
        volume_schema: str,
        volume_name: str,
        staging_allowed_local_path: str,
    ) -> None:
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.auth_mode = auth_mode.lower()
        self.access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.catalog = catalog
        self.target_schema = target_schema
        self.staging_schema = staging_schema
        self.volume_catalog = volume_catalog
        self.volume_schema = volume_schema
        self.volume_name = volume_name
        self.staging_allowed_local_path = staging_allowed_local_path

    @contextmanager
    def connect(self) -> Generator[sql.Connection, None, None]:
        kwargs = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "catalog": self.catalog,
            "schema": self.target_schema,
            "staging_allowed_local_path": self.staging_allowed_local_path,
        }

        if self.auth_mode == "m2m":
            config = Config(
                host="https://" + self.server_hostname,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            kwargs["credentials_provider"] = oauth_service_principal(config)
        else:
            kwargs["access_token"] = self.access_token

        connection = sql.connect(**kwargs)
        try:
            yield connection
        finally:
            connection.close()

    def _target_fqn(self, table_config: BridgeTableConfig) -> str:
        return ".".join(
            [
                _quote_databricks_ident(self.catalog),
                _quote_databricks_ident(self.target_schema),
                _quote_databricks_ident(table_config.target_table),
            ]
        )

    def _stage_table_name(self, table_config: BridgeTableConfig) -> str:
        return table_config.target_table + "_staging"

    def _stage_fqn(self, table_config: BridgeTableConfig) -> str:
        return ".".join(
            [
                _quote_databricks_ident(self.catalog),
                _quote_databricks_ident(self.staging_schema),
                _quote_databricks_ident(self._stage_table_name(table_config)),
            ]
        )

    def build_remote_batch_dir(self, table_config: BridgeTableConfig, batch_id: str) -> str:
        return (
            "/Volumes/"
            + self.volume_catalog
            + "/"
            + self.volume_schema
            + "/"
            + self.volume_name
            + "/"
            + table_config.target_table
            + "/"
            + batch_id
        )

    def build_remote_file_path(
        self,
        table_config: BridgeTableConfig,
        batch_id: str,
        local_path: str,
    ) -> str:
        return self.build_remote_batch_dir(table_config, batch_id) + "/" + Path(local_path).name

    def ensure_objects(
        self,
        cursor,
        table_config: BridgeTableConfig,
        columns: Iterable[SourceColumn],
    ) -> None:
        cursor.execute(
            "CREATE SCHEMA IF NOT EXISTS "
            + ".".join(
                [
                    _quote_databricks_ident(self.catalog),
                    _quote_databricks_ident(self.target_schema),
                ]
            )
        )
        cursor.execute(
            "CREATE SCHEMA IF NOT EXISTS "
            + ".".join(
                [
                    _quote_databricks_ident(self.catalog),
                    _quote_databricks_ident(self.staging_schema),
                ]
            )
        )
        cursor.execute(
            "CREATE SCHEMA IF NOT EXISTS "
            + ".".join(
                [
                    _quote_databricks_ident(self.volume_catalog),
                    _quote_databricks_ident(self.volume_schema),
                ]
            )
        )
        cursor.execute(
            "CREATE VOLUME IF NOT EXISTS "
            + ".".join(
                [
                    _quote_databricks_ident(self.volume_catalog),
                    _quote_databricks_ident(self.volume_schema),
                    _quote_databricks_ident(self.volume_name),
                ]
            )
        )

        ddl_columns = ", ".join(
            [
                _quote_databricks_ident(column.name)
                + " "
                + _sql_server_type_to_databricks(column)
                for column in columns
            ]
        )

        cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            + self._target_fqn(table_config)
            + " ("
            + ddl_columns
            + ") USING DELTA"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            + self._stage_fqn(table_config)
            + " ("
            + ddl_columns
            + ") USING DELTA"
        )

    def truncate_stage(self, cursor, table_config: BridgeTableConfig) -> None:
        cursor.execute("TRUNCATE TABLE " + self._stage_fqn(table_config))

    def upload_file(self, cursor, local_path: str, remote_path: str) -> None:
        cursor.execute(
            "PUT '"
            + local_path.replace("\\", "/")
            + "' INTO '"
            + remote_path
            + "' OVERWRITE"
        )

    def copy_into_stage(self, cursor, table_config: BridgeTableConfig, remote_batch_dir: str) -> None:
        cursor.execute(
            "COPY INTO "
            + self._stage_fqn(table_config)
            + " FROM '"
            + remote_batch_dir
            + "' FILEFORMAT = PARQUET"
        )

    def merge_stage_into_target(
        self,
        cursor,
        table_config: BridgeTableConfig,
        columns: List[SourceColumn],
    ) -> None:
        all_columns = [_quote_databricks_ident(column.name) for column in columns]
        pk_columns = [_quote_databricks_ident(column) for column in table_config.primary_keys]
        order_expression = (
            "COALESCE("
            + "s."
            + _quote_databricks_ident(table_config.updated_at_column)
            + ", s."
            + _quote_databricks_ident(table_config.created_at_column)
            + ") DESC, s."
            + _quote_databricks_ident(table_config.created_at_column)
            + " DESC"
        )
        join_condition = " AND ".join(
            ["t." + column + " = s." + column for column in pk_columns]
        )
        update_assignments = ", ".join([column + " = s." + column for column in all_columns])
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join(["s." + column for column in all_columns])

        merge_sql = """
MERGE INTO {target} AS t
USING (
    SELECT {selected_columns}
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY {partition_columns}
                ORDER BY {order_expression}
            ) AS _bridge_rownum
        FROM {stage} AS s
    ) AS ranked
    WHERE _bridge_rownum = 1
) AS s
ON {join_condition}
WHEN MATCHED
AND COALESCE(
        s.{updated_at},
        s.{created_at}
    ) >= COALESCE(
        t.{updated_at},
        t.{created_at}
    )
THEN UPDATE SET {update_assignments}
WHEN NOT MATCHED
THEN INSERT ({insert_columns})
VALUES ({insert_values})
""".strip().format(
            target=self._target_fqn(table_config),
            selected_columns=", ".join(all_columns),
            partition_columns=", ".join(pk_columns),
            order_expression=order_expression,
            stage=self._stage_fqn(table_config),
            join_condition=join_condition,
            updated_at=_quote_databricks_ident(table_config.updated_at_column),
            created_at=_quote_databricks_ident(table_config.created_at_column),
            update_assignments=update_assignments,
            insert_columns=insert_columns,
            insert_values=insert_values,
        )

        cursor.execute(merge_sql)

    def remove_remote_file(self, cursor, remote_path: str) -> None:
        cursor.execute("REMOVE '" + remote_path + "'")


@resource
def databricks_sql_resource(_):
    staging_allowed_local_path = os.getenv(
        "DBX_STAGING_ALLOWED_LOCAL_PATH",
        "/tmp/databricks_bridge",
    )
    Path(staging_allowed_local_path).mkdir(parents=True, exist_ok=True)

    return DatabricksSqlResource(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME", ""),
        http_path=os.getenv("DATABRICKS_HTTP_PATH", ""),
        auth_mode=os.getenv("DBX_AUTH_MODE", "pat"),
        access_token=os.getenv("DATABRICKS_TOKEN", ""),
        client_id=os.getenv("DATABRICKS_CLIENT_ID", ""),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET", ""),
        catalog=os.getenv("DBX_CATALOG", "main"),
        target_schema=os.getenv("DBX_TARGET_SCHEMA", "bronze"),
        staging_schema=os.getenv("DBX_STAGING_SCHEMA", "staging"),
        volume_catalog=os.getenv("DBX_VOLUME_CATALOG", os.getenv("DBX_CATALOG", "main")),
        volume_schema=os.getenv("DBX_VOLUME_SCHEMA", "files"),
        volume_name=os.getenv("DBX_VOLUME_NAME", "bridge"),
        staging_allowed_local_path=staging_allowed_local_path,
    )
