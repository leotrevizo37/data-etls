import importlib.util
import os
import sys
import types
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
dagster = types.ModuleType("dagster")
dagster.resource = lambda fn=None, **_kwargs: fn
pyodbc = types.ModuleType("pyodbc")
pyodbc.Connection = object
pyodbc.Error = Exception
path = PROJECT_ROOT / "src" / "bafar" / "defs" / "resources" / "sql_server_dimensions.py"
spec = importlib.util.spec_from_file_location("bafar_sql_server_dimensions_resource", path)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
with patch.dict(sys.modules, {"dagster": dagster, "pyodbc": pyodbc}):
    spec.loader.exec_module(module)


def test_sql_dim_server_resource_builds_source_options() -> None:
    resource = module.SqlDimServerResource(
        host="source-host",
        port=1434,
        database="source-database",
        user="source-user",
        password="source-password",
        connect_timeout=15,
        command_timeout=5000,
        driver="ODBC Driver 18 for SQL Server",
        retries=3,
        retry_interval=60,
    )

    options = resource.get_basic_options()

    assert options == {
        "url": (
            "jdbc:sqlserver://source-host:1434;databaseName=source-database;"
            "encrypt=true;trustServerCertificate=true;Connection Timeout=15"
        ),
        "user": "source-user",
        "password": "source-password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def test_dim_sqlserver_resource_uses_its_own_environment() -> None:
    environment = {
        "DIMSQL_HOST": "source-host",
        "DIMSQL_PORT": "1434",
        "DIM_DB": "source-database",
        "DIMSQL_USER": "source-user",
        "DIMSQL_PASSWORD": "source-password",
        "DIMSQL_CONNECT_TIMEOUT": "20",
        "DIMSQL_COMMAND_TIMEOUT": "6000",
        "DIMSQL_DRIVER": "ODBC Driver 18 for SQL Server",
        "DIMSQL_CONNECT_RETRY_COUNT": "4",
        "DIMSQL_CONNECT_RETRY_INTERVAL": "30",
    }

    with patch.dict(os.environ, environment):
        resource = module.dim_sqlserver_resource(None)

    assert resource.host == "source-host"
    assert resource.port == 1434
    assert resource.database == "source-database"
    assert resource.user == "source-user"
    assert resource.password == "source-password"
    assert resource.connect_timeout == 20
    assert resource.command_timeout == 6000
    assert resource.driver == "ODBC Driver 18 for SQL Server"
    assert resource.retries == 4
    assert resource.retry_interval == 30
