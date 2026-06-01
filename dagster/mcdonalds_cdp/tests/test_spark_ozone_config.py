import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _install_optional_stubs() -> None:
    dagster_module = sys.modules.get("dagster")
    if dagster_module is None:
        try:
            import dagster as dagster_module
        except Exception:
            dagster_module = None
    if dagster_module is None or not hasattr(dagster_module, "resource"):
        dagster = types.ModuleType("dagster")

        class _DagsterBase:
            pass

        def _decorator(*_args, **_kwargs):
            def _wrap(fn):
                return fn
            return _wrap

        dagster.IOManager = _DagsterBase
        dagster.InputContext = _DagsterBase
        dagster.OutputContext = _DagsterBase
        dagster.io_manager = _decorator
        dagster.resource = lambda fn=None, **kwargs: fn if fn else _decorator(**kwargs)
        sys.modules["dagster"] = dagster

    if "pyspark" not in sys.modules and importlib.util.find_spec("pyspark") is None:
        pyspark = types.ModuleType("pyspark")

        class _SparkConf:
            def setAppName(self, *_args):
                return self

            def setMaster(self, *_args):
                return self

            def set(self, *_args):
                return self

        class _StorageLevel:
            MEMORY_AND_DISK = object()

        pyspark.SparkConf = _SparkConf
        pyspark.StorageLevel = _StorageLevel

        pyspark_sql = types.ModuleType("pyspark.sql")

        class _DataFrame:
            pass

        class _SparkSession:
            pass

        pyspark_sql.DataFrame = _DataFrame
        pyspark_sql.SparkSession = _SparkSession

        pyspark_functions = types.ModuleType("pyspark.sql.functions")

        def _function(*_args, **_kwargs):
            return None

        pyspark_functions.col = _function
        pyspark_functions.max = _function
        pyspark_functions.upper = _function

        pyspark_types = types.ModuleType("pyspark.sql.types")

        class _Type:
            def __init__(self, *_args, **_kwargs):
                pass

        pyspark_types.DoubleType = _Type
        pyspark_types.StringType = _Type
        pyspark_types.StructField = _Type
        pyspark_types.StructType = _Type
        pyspark_types.TimestampType = _Type

        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.sql"] = pyspark_sql
        sys.modules["pyspark.sql.functions"] = pyspark_functions
        sys.modules["pyspark.sql.types"] = pyspark_types


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


_install_optional_stubs()
spark_module = _load_module(
    "mcdonalds_cdp_spark_resource",
    PROJECT_ROOT / "src" / "mcdonalds_cdp" / "defs" / "resources" / "spark.py",
)
spark_df_io_manager_module = _load_module(
    "mcdonalds_cdp_spark_df_io_manager",
    PROJECT_ROOT
    / "src"
    / "mcdonalds_cdp"
    / "defs"
    / "resources"
    / "spark_df_io_manager.py",
)
HADOOP_AWS_PACKAGE = spark_module.HADOOP_AWS_PACKAGE
MSSQL_JDBC_PACKAGE = spark_module.MSSQL_JDBC_PACKAGE
SparkResource = spark_module.SparkResource
SparkDataFrameParquetIOManager = (
    spark_df_io_manager_module.SparkDataFrameParquetIOManager
)


class _Context:
    def get_identifier(self):
        return ["run-id", "op-name", "result"]


def _spark_resource(
        ozone_s3_endpoint: str = "",
        adls_account_name: str = "",
        adls_account_key: str | None = None) -> SparkResource:
    return SparkResource(
        master_url="local[1]",
        worker_mem="1G",
        executor_mem="1G",
        executor_cores=1,
        app_name="spark-ozone-config-test",
        adls_account_name=adls_account_name,
        adls_container_name="container",
        adls_base_path="base",
        adls_account_key=adls_account_key,
        ozone_s3_endpoint=ozone_s3_endpoint,
        ozone_s3_region="us-east-1",
        ozone_s3_access_key="access",
        ozone_s3_secret_key="secret",
        ozone_s3_path_style_access="true",
        ozone_s3_bucket_probe="0",
        ozone_s3_change_detection_mode="none",
        jdbc_host="localhost",
        jdbc_port=1433,
        jdbc_database="db",
        jdbc_user="user",
        jdbc_driver="driver",
        jdbc_password="password",
        jdbc_write_num_chunks=1,
        jdbc_write_target_partitions=1,
        jdbc_write_batch_size=1,
    )


class SparkOzoneConfigTests(unittest.TestCase):
    def test_ozone_s3_options_are_empty_without_endpoint(self) -> None:
        spark_res = _spark_resource()

        self.assertEqual(spark_res._build_ozone_s3_options(), {})

    def test_ozone_s3_options_include_s3a_connection_settings(self) -> None:
        spark_res = _spark_resource("http://ozone-01:9878")

        options = spark_res._build_ozone_s3_options()

        self.assertEqual(options["fs.s3a.endpoint"], "http://ozone-01:9878")
        self.assertEqual(options["fs.s3a.path.style.access"], "true")
        self.assertEqual(options["fs.s3a.bucket.probe"], "0")
        self.assertEqual(options["fs.s3a.change.detection.mode"], "none")
        self.assertEqual(options["fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(options["fs.s3a.access.key"], "access")
        self.assertEqual(options["fs.s3a.secret.key"], "secret")

    def test_spark_packages_preserve_adls_jdbc_and_add_hadoop_aws(self) -> None:
        spark_res = _spark_resource(
            ozone_s3_endpoint="http://ozone-01:9878",
            adls_account_name="proddatalake",
            adls_account_key="adls-key",
        )

        packages = spark_res._spark_packages()

        self.assertIn(HADOOP_AWS_PACKAGE, packages)
        self.assertIn(MSSQL_JDBC_PACKAGE, packages)

    def test_spark_df_io_manager_joins_remote_s3a_paths(self) -> None:
        manager = SparkDataFrameParquetIOManager(
            "s3a://mcdonalds-etl/dagster_io/spark_dfs/mcdonalds_cdp"
        )

        path = manager._path_for(_Context())

        self.assertEqual(
            path,
            "s3a://mcdonalds-etl/dagster_io/spark_dfs/mcdonalds_cdp/"
            "run-id/op-name/result",
        )


if __name__ == "__main__":
    unittest.main()
