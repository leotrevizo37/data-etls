import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_spark_module():
    dagster = types.ModuleType("dagster")
    dagster.resource = lambda fn=None, **_kwargs: fn

    pyspark = types.ModuleType("pyspark")

    class SparkConf:
        def __init__(self):
            self.settings = {}

        def setAppName(self, value):
            self.settings["spark.app.name"] = value
            return self

        def setMaster(self, value):
            self.settings["spark.master"] = value
            return self

        def set(self, key, value):
            self.settings[key] = value
            return self

    class StorageLevel:
        MEMORY_AND_DISK = object()

    pyspark.SparkConf = SparkConf
    pyspark.StorageLevel = StorageLevel

    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.DataFrame = object
    pyspark_sql.SparkSession = object

    pyspark_functions = types.ModuleType("pyspark.sql.functions")
    pyspark_functions.lit = lambda value: value

    path = PROJECT_ROOT / "src" / "bafar" / "defs" / "resources" / "spark.py"
    spec = importlib.util.spec_from_file_location("bafar_spark_resource", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    with patch.dict(
        sys.modules,
        {
            "dagster": dagster,
            "pyspark": pyspark,
            "pyspark.sql": pyspark_sql,
            "pyspark.sql.functions": pyspark_functions,
        },
    ):
        spec.loader.exec_module(module)
    return module


class FakeWriter:
    def __init__(self):
        self.values = {}
        self.save_count = 0

    def mode(self, value):
        self.values["mode"] = value
        return self

    def format(self, value):
        self.values["format"] = value
        return self

    def option(self, key, value):
        self.values[key] = value
        return self

    def options(self, **values):
        self.values.update(values)
        return self

    def save(self):
        self.values["saved"] = True
        self.save_count += 1


class FakeDataFrame:
    def __init__(self):
        self.selections = []
        self.select_expressions = []
        self.deduplicated_by = None
        self.columns_added = {}
        self.filters = []
        self.dropped = []
        self.partitions = None
        self.persisted = False
        self.unpersisted = False
        self.write = FakeWriter()

    def select(self, *columns):
        self.selections.append(columns)
        return self

    def selectExpr(self, *expressions):
        self.select_expressions.append(expressions)
        return self

    def distinct(self):
        return self

    def collect(self):
        return [{"__chunk_id": 0}, {"__chunk_id": 1}]

    def filter(self, expression):
        self.filters.append(expression)
        return self

    def drop(self, column):
        self.dropped.append(column)
        return self

    def dropDuplicates(self, columns):
        self.deduplicated_by = columns
        return self

    def withColumn(self, name, value):
        self.columns_added[name] = value
        return self

    def persist(self, _storage_level):
        self.persisted = True
        return self

    def count(self):
        return 2

    def coalesce(self, partitions):
        self.partitions = partitions
        return self

    def unpersist(self):
        self.unpersisted = True


class SparkResourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_spark_module()

    def setUp(self) -> None:
        self.resource = self.module.SparkResource(
            master_url="local[1]",
            worker_mem="4G",
            app_name="bafar/test/run-1",
            executor_mem="1G",
            executor_cores=1,
            jdbc_host="sql-server",
            jdbc_port=1433,
            jdbc_database="bafar_database",
            jdbc_user="test_user",
            jdbc_driver="",
            jdbc_password="",
            jdbc_write_num_chunks=5,
            jdbc_write_target_partitions=2,
            jdbc_write_batch_size=100,
        )

    def test_spark_and_jdbc_configuration_use_the_requested_database(self) -> None:
        conf = self.resource._build_spark_conf()
        wh = self.resource._build_wh_path()

        self.assertEqual(conf.settings["spark.master"], "local[1]")
        self.assertEqual(conf.settings["spark.app.name"], "bafar/test/run-1")
        self.assertEqual(
            conf.settings["spark.jars.packages"],
            self.module.MSSQL_JDBC_PACKAGE,
        )
        self.assertEqual(conf.settings["spark.cores.max"], "1")
        self.assertEqual(conf.settings["spark.task.maxFailures"], "8")
        self.assertEqual(conf.settings["spark.network.timeout"], "800s")
        self.assertEqual(conf.settings["spark.sql.maxPlanStringLength"], "20000")
        self.assertEqual(conf.settings["spark.sql.debug.maxToStringFields"], "100")
        self.assertIn("databaseName=bafar_database", wh["url"])
        self.assertEqual(
            wh["properties"]["driver"],
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        )

    def test_ozone_s3_configuration_is_applied_to_spark(self) -> None:
        self.resource.ozone_s3_endpoint = "http://ozone:9878"
        self.resource.ozone_s3_access_key = "access"
        self.resource.ozone_s3_secret_key = "secret"

        options = self.resource._build_ozone_s3_options()
        conf = self.resource._build_spark_conf()

        self.assertEqual(options["fs.s3a.endpoint"], "http://ozone:9878")
        self.assertEqual(options["fs.s3a.connection.ssl.enabled"], "false")
        self.assertEqual(options["fs.s3a.path.style.access"], "true")
        self.assertEqual(options["fs.s3a.access.key"], "access")
        self.assertEqual(options["fs.s3a.secret.key"], "secret")
        self.assertEqual(
            conf.settings["spark.hadoop.fs.s3a.endpoint"],
            "http://ozone:9878",
        )

    def test_module_stubs_are_restored_after_loading(self) -> None:
        module_names = (
            "dagster",
            "pyspark",
            "pyspark.sql",
            "pyspark.sql.functions",
        )
        original_modules = {
            module_name: sys.modules.get(module_name)
            for module_name in module_names
        }

        _load_spark_module()

        for module_name, original_module in original_modules.items():
            with self.subTest(module_name=module_name):
                if original_module is None:
                    self.assertNotIn(module_name, sys.modules)
                else:
                    self.assertIs(sys.modules[module_name], original_module)

    def test_spark_master_and_profile_helpers_follow_the_shared_pattern(self) -> None:
        status_url = self.module._build_spark_master_status_url(
            "spark://172.168.10.80:7077",
            {
                "SPARK_MASTER_STATUS_URL": "http://spark-master:8080/json/",
                "SPARK_MASTER_UI_PORT": "8081",
            },
        )
        internal_status_url = self.module._build_spark_master_status_url(
            "spark://spark-master:7077",
            {"SPARK_MASTER_UI_PORT": "8081"},
        )
        profile = self.module._build_spark_resource_profile(
            "ventas",
            {
                "SPARK_VENTAS_MIN_CORES": "2",
                "SPARK_VENTAS_MIN_MEMORY_MB": "3072",
                "SPARK_VENTAS_CORES_MAX": "3",
            },
        )
        executor_cores = self.module._build_executor_cores(
            profile,
            {"SPARK_WORKER_CORES": "4"},
        )
        available = self.module._spark_master_available_resources(
            {
                "workers": [
                    {"state": "ALIVE", "coresFree": 2, "memoryFree": 2048},
                    {"state": "DEAD", "coresFree": 8, "memoryFree": 8192},
                    {"state": "ALIVE", "coresfree": 1, "memoryfree": "1g"},
                ]
            }
        )

        self.assertEqual(status_url, "http://172.168.10.80:8081/json/")
        self.assertEqual(internal_status_url, "http://spark-master:8080/json/")
        self.assertEqual(profile["name"], "ventas")
        self.assertEqual(profile["min_cores"], 2)
        self.assertEqual(profile["min_memory_mb"], 3072)
        self.assertEqual(executor_cores, 3)
        self.assertEqual(available, {"cores_free": 3, "memory_free_mb": 3072})
        self.assertTrue(self.module._has_spark_capacity(available, profile))

    def test_spark_app_context_uses_tags_and_infers_maintenance(self) -> None:
        tagged_context = self.module._build_spark_app_context(
            types.SimpleNamespace(
                dagster_run=types.SimpleNamespace(
                    job_name="010_bafar_store_schedules",
                    run_id="abcdef123456",
                    tags={"user_code": "bafar", "job_type": "dimensiones"},
                )
            ),
            default_app_name="bafar_ingestion",
            default_user_code="bafar",
        )
        maintenance_context = self.module._build_spark_app_context(
            types.SimpleNamespace(
                dagster_run=types.SimpleNamespace(
                    job_name="089_maintenance_spark_healthcheck",
                    run_id="1234567890",
                    tags={"user_code": "bafar"},
                )
            ),
            default_app_name="bafar_ingestion",
            default_user_code="bafar",
        )

        self.assertEqual(tagged_context["job_type"], "dimensiones")
        self.assertEqual(
            tagged_context["app_name"],
            "bafar/010_bafar_store_schedules/abcdef12",
        )
        self.assertEqual(maintenance_context["job_type"], "maintenance")

    def test_out_of_hours_reader_preserves_live_and_backfill_boundaries(self) -> None:
        spark = MagicMock()
        source_sqlserver = MagicMock()
        source_sqlserver.get_basic_options.return_value = {"url": "jdbc:test"}
        self.resource.get_spark = MagicMock(return_value=spark)
        query_option = spark.read.format.return_value.options.return_value.option

        self.resource.read_fact_out_of_hours_sales_window(
            source_sqlserver,
            "2026-05-01T00:00:00.000000",
        )

        live_query = query_option.call_args.args[1]
        self.assertIn("FROM Replica.Ventas", live_query)
        self.assertIn(
            "WHERE Fecha > CONVERT(DATETIME2(7), '2026-05-01T00:00:00.000000', 126)",
            live_query,
        )
        self.assertNotIn("AND Fecha <", live_query)

        query_option.reset_mock()
        self.resource.read_fact_out_of_hours_sales_window(
            source_sqlserver,
            "2026-05-01T00:00:00.000000",
            "2026-05-02T00:00:00.000000",
        )

        backfill_query = query_option.call_args.args[1]
        self.assertIn("FROM Replica.Ventas", backfill_query)
        self.assertIn(
            "WHERE Fecha >= CONVERT(DATETIME2(7), '2026-05-01T00:00:00.000000', 126)",
            backfill_query,
        )
        self.assertIn(
            "AND Fecha < CONVERT(DATETIME2(7), '2026-05-02T00:00:00.000000', 126)",
            backfill_query,
        )

    def test_spark_capacity_network_and_port_guards(self) -> None:
        self.resource.master_url = "spark://spark-master:7077"
        self.resource.resource_wait_seconds = 0
        self.resource._fetch_spark_master_status = lambda: {
            "workers": [{"state": "ALIVE", "coresFree": 0, "memoryFree": 4096}]
        }

        with self.assertRaisesRegex(RuntimeError, "Insufficient Spark resources"):
            self.resource._wait_for_spark_capacity()

        self.resource.block_manager_port = "37079"
        self.resource._fetch_spark_master_status = lambda: {
            "workers": [
                {
                    "state": "ALIVE",
                    "host": "172.21.0.4",
                    "coresFree": 1,
                    "memoryFree": 2048,
                }
            ]
        }
        self.resource._worker_endpoint_is_routable = lambda _host, _port: False

        with self.assertRaisesRegex(RuntimeError, "block manager endpoints"):
            self.resource._wait_for_spark_capacity()

        self.resource.driver_port = "37077"
        self.resource.driver_block_manager_port = "37078"
        self.resource._validate_port_assignments()

        self.resource.block_manager_port = "37077"
        with self.assertRaisesRegex(RuntimeError, "port conflict"):
            self.resource._validate_port_assignments()

    def test_jdbc_chunk_writer_uses_existing_chunk_ids(self) -> None:
        df = FakeDataFrame()

        written = self.resource._write_jdbc_in_chunks(
            df,
            "stg.factOutOfHoursSales",
            "SaleId",
        )

        self.assertEqual(written, 2)
        self.assertIn("pmod(hash(`SaleId`), 5) as __chunk_id", df.select_expressions[0])
        self.assertEqual(df.filters, ["__chunk_id = 0", "__chunk_id = 1"])
        self.assertEqual(df.write.save_count, 2)
        self.assertTrue(df.unpersisted)

    def test_sales_stage_column_contracts_include_the_new_metrics(self) -> None:
        self.assertEqual(
            self.module.FACT_OUT_OF_HOURS_SALES_COLUMNS,
            (
                "SaleId",
                "SaleNumber",
                "Amount",
                "LocalTimeSpan",
                "UserName",
                "StoreId",
                "OutOfHoursType",
                "DeviationMinutes",
            ),
        )
        self.assertEqual(
            self.module.FACT_STORE_SALES_DAILY_COLUMNS,
            ("StoreId", "SalesDate", "SalesCount", "SalesAmount"),
        )

    def test_stage_writes_add_run_id_and_use_the_matching_tables(self) -> None:
        cases = [
            (
                "write_dim_store_schedules_stage",
                ["StoreId"],
                "stg.dimStoreSchedules",
                self.module.DIM_STORE_SCHEDULE_COLUMNS,
                1,
            ),
            (
                "write_fact_out_of_hours_sales_stage",
                ["SaleId"],
                "stg.factOutOfHoursSales",
                self.module.FACT_OUT_OF_HOURS_SALES_COLUMNS,
                2,
            ),
            (
                "write_fact_store_sales_daily_stage",
                ["StoreId", "SalesDate"],
                "stg.factStoreSalesDaily",
                self.module.FACT_STORE_SALES_DAILY_COLUMNS,
                2,
            ),
        ]

        for method_name, key_columns, target_table, expected_columns, save_count in cases:
            with self.subTest(method_name=method_name):
                df = FakeDataFrame()

                written = getattr(self.resource, method_name)(df, "run-3")

                self.assertEqual(written, 2)
                self.assertEqual(df.selections[0], expected_columns)
                self.assertIn(("RunId", *expected_columns), df.selections)
                self.assertEqual(df.deduplicated_by, key_columns)
                self.assertEqual(df.columns_added["RunId"], "run-3")
                self.assertEqual(df.write.values["dbtable"], target_table)
                self.assertTrue(df.write.values["saved"])
                self.assertEqual(df.write.save_count, save_count)
                self.assertTrue(df.unpersisted)


if __name__ == "__main__":
    unittest.main()
