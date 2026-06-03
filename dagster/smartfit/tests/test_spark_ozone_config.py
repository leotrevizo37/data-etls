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
            dagster_module = types.ModuleType("dagster")
            dagster_module.__path__ = [str(PROJECT_ROOT.parent)]
            sys.modules["dagster"] = dagster_module
    if not hasattr(dagster_module, "__path__"):
        dagster_module.__path__ = [str(PROJECT_ROOT.parent)]

    class _DagsterBase:
        pass

    def _decorator(*_args, **_kwargs):
        def _wrap(fn):
            return fn
        return _wrap

    def _resource(fn=None, **kwargs):
        return fn if fn else _decorator(**kwargs)

    if not hasattr(dagster_module, "IOManager"):
        dagster_module.IOManager = _DagsterBase
    if not hasattr(dagster_module, "InputContext"):
        dagster_module.InputContext = _DagsterBase
    if not hasattr(dagster_module, "OutputContext"):
        dagster_module.OutputContext = _DagsterBase
    if not hasattr(dagster_module, "io_manager"):
        dagster_module.io_manager = _decorator
    if not hasattr(dagster_module, "resource"):
        dagster_module.resource = _resource

    if "pyspark" not in sys.modules and importlib.util.find_spec("pyspark") is None:
        pyspark = types.ModuleType("pyspark")

        class _SparkConf:
            def __init__(self):
                self._settings = {}

            def setAppName(self, app_name):
                self._settings["spark.app.name"] = app_name
                return self

            def setMaster(self, master):
                self._settings["spark.master"] = master
                return self

            def set(self, key, value):
                self._settings[key] = str(value)
                return self

            def get(self, key, default=None):
                return self._settings.get(key, default)

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
    "smartfit_spark_resource",
    PROJECT_ROOT / "src" / "smartfit" / "defs" / "resources" / "spark.py",
)
spark_df_io_manager_module = _load_module(
    "smartfit_spark_df_io_manager",
    PROJECT_ROOT
    / "src"
    / "smartfit"
    / "defs"
    / "resources"
    / "spark_df_io_manager.py",
)
HADOOP_AWS_PACKAGE = spark_module.HADOOP_AWS_PACKAGE
MSSQL_JDBC_PACKAGE = spark_module.MSSQL_JDBC_PACKAGE
build_executor_cores = spark_module._build_executor_cores
build_spark_resource_profile = spark_module._build_spark_resource_profile
build_spark_retry_conf = spark_module._build_spark_retry_conf
spark_master_available_resources = spark_module._spark_master_available_resources
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
        adls_account_key: str | None = None,
        master_url: str = "local[1]",
        executor_mem: str = "1G",
        executor_cores: int = 1,
        app_name: str = "spark-ozone-config-test",
        job_type: str = "default",
        **resource_kwargs) -> SparkResource:
    return SparkResource(
        master_url=master_url,
        worker_mem="1G",
        executor_mem=executor_mem,
        executor_cores=executor_cores,
        app_name=app_name,
        job_type=job_type,
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
        **resource_kwargs,
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
            "s3a://mcdonalds-etl/dagster_io/spark_dfs/smartfit"
        )

        path = manager._path_for(_Context())

        self.assertEqual(
            path,
            "s3a://mcdonalds-etl/dagster_io/spark_dfs/smartfit/"
            "run-id/op-name/result",
        )

    def test_spark_master_available_resources_uses_alive_workers_only(self) -> None:
        resources = spark_master_available_resources(
            {
                "workers": [
                    {"state": "ALIVE", "coresFree": 2, "memoryFree": 2048},
                    {"state": "DEAD", "coresFree": 8, "memoryFree": 8192},
                    {"state": "ALIVE", "coresFree": 1, "memoryFree": "1g"},
                ]
            }
        )

        self.assertEqual(resources, {"cores_free": 3, "memory_free_mb": 3072})

    def test_spark_resource_profile_uses_job_type_overrides(self) -> None:
        profile = build_spark_resource_profile(
            "lecturas",
            {
                "SPARK_LECTURAS_MIN_CORES": "3",
                "SPARK_LECTURAS_MIN_MEMORY_MB": "4096",
                "SPARK_LECTURAS_CORES_MAX": "4",
            },
        )

        self.assertEqual(profile["name"], "lecturas")
        self.assertEqual(profile["min_cores"], 3)
        self.assertEqual(profile["min_memory_mb"], 4096)
        self.assertEqual(profile["cores_max"], 4)

    def test_sensor_expected_values_profile_uses_heavier_defaults(self) -> None:
        profile = build_spark_resource_profile("sensor_expected_values", {})

        self.assertEqual(profile["name"], "sensor_expected_values")
        self.assertEqual(profile["min_cores"], 2)
        self.assertEqual(profile["min_memory_mb"], 2048)
        self.assertEqual(profile["cores_max"], 2)

    def test_spark_retry_policy_uses_defaults_and_env_overrides(self) -> None:
        policy = build_spark_retry_conf(
            {
                "SPARK_TASK_MAX_FAILURES": "6",
                "SPARK_SHUFFLE_IO_RETRY_WAIT": "20s",
            }
        )

        self.assertEqual(policy["spark.task.maxFailures"], "6")
        self.assertEqual(policy["spark.stage.maxConsecutiveAttempts"], "8")
        self.assertEqual(policy["spark.shuffle.io.retryWait"], "20s")

    def test_legacy_worker_cores_fallback_does_not_exceed_profile_cap(self) -> None:
        profile = build_spark_resource_profile("lecturas", {})

        executor_cores = build_executor_cores(
            profile,
            {"SPARK_WORKER_CORES": "4"},
        )

        self.assertEqual(executor_cores, 2)

    def test_spark_app_context_uses_dagster_run_tags(self) -> None:
        class _Run:
            job_name = "fact_lecturas_ingestion_job"
            run_id = "abcdef123456"
            tags = {"user_code": "smartfit", "job_type": "lecturas"}

        class _InitContext:
            dagster_run = _Run()

        context = spark_module._build_spark_app_context(
            _InitContext(),
            default_app_name="FactLecturasIngestion",
            default_user_code="smartfit",
        )

        self.assertEqual(context["job_type"], "lecturas")
        self.assertEqual(
            context["app_name"],
            "smartfit/fact_lecturas_ingestion_job/abcdef12",
        )

    def test_wait_for_spark_capacity_fails_when_cluster_is_full(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            job_type="lecturas",
            resource_wait_seconds=0,
        )
        spark_res._fetch_spark_master_status = lambda: {
            "workers": [{"state": "ALIVE", "coresFree": 1, "memoryFree": 4096}]
        }

        with self.assertRaisesRegex(RuntimeError, "Insufficient Spark resources"):
            spark_res._wait_for_spark_capacity()

    def test_spark_conf_sets_resource_limits_and_app_name(self) -> None:
        spark_res = _spark_resource(
            master_url="spark://spark-master:7077",
            executor_mem="2G",
            executor_cores=1,
            app_name="smartfit/fact_lecturas_ingestion_job/abcdef12",
            job_type="lecturas",
            driver_host="10.0.0.5",
            driver_port="17001",
            block_manager_port="17101",
        )

        conf = spark_res._build_spark_conf()

        self.assertEqual(
            conf.get("spark.app.name"),
            "smartfit/fact_lecturas_ingestion_job/abcdef12",
        )
        self.assertEqual(conf.get("spark.executor.memory"), "2G")
        self.assertEqual(conf.get("spark.executor.cores"), "1")
        self.assertEqual(conf.get("spark.cores.max"), "2")
        self.assertEqual(conf.get("spark.driver.host"), "10.0.0.5")
        self.assertEqual(conf.get("spark.driver.bindAddress"), "0.0.0.0")
        self.assertEqual(conf.get("spark.driver.port"), "17001")
        self.assertEqual(conf.get("spark.blockManager.port"), "17101")
        self.assertEqual(conf.get("spark.task.maxFailures"), "8")
        self.assertEqual(conf.get("spark.shuffle.io.maxRetries"), "10")
        self.assertEqual(conf.get("spark.network.timeout"), "800s")


if __name__ == "__main__":
    unittest.main()
