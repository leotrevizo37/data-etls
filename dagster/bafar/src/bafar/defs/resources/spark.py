from typing import Any, Dict, List, Optional

import json
import os
import socket
import time
from urllib.parse import urlparse
from urllib.request import urlopen
from dagster import resource
from pyspark import SparkConf, StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
DEFAULT_SPARK_MASTER_STATUS_URL = "http://spark-master:8080/json/"
DEFAULT_SPARK_RESOURCE_WAIT_SECONDS = 300
DEFAULT_SPARK_RESOURCE_POLL_SECONDS = 10
DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS = 3
DEFAULT_SPARK_DRIVER_BIND_ADDRESS = "0.0.0.0"
DEFAULT_SPARK_SQL_MAX_PLAN_STRING_LENGTH = "20000"
DEFAULT_SPARK_SQL_DEBUG_MAX_TO_STRING_FIELDS = "100"
DEFAULT_SPARK_RESOURCE_PROFILES = {
    "default": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "maintenance": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "dimensiones": {"min_cores": 1, "min_memory_mb": 1024, "cores_max": 1},
    "ventas": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
    "backfill": {"min_cores": 2, "min_memory_mb": 2048, "cores_max": 2},
}
SPARK_RETRY_POLICY_ENV = {
    "spark.task.maxFailures": ("SPARK_TASK_MAX_FAILURES", "8"),
    "spark.stage.maxConsecutiveAttempts": ("SPARK_STAGE_MAX_CONSECUTIVE_ATTEMPTS", "8"),
    "spark.shuffle.io.maxRetries": ("SPARK_SHUFFLE_IO_MAX_RETRIES", "10"),
    "spark.shuffle.io.retryWait": ("SPARK_SHUFFLE_IO_RETRY_WAIT", "10s"),
    "spark.network.timeout": ("SPARK_NETWORK_TIMEOUT", "800s"),
    "spark.executor.heartbeatInterval": ("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "60s"),
}
SPARK_S3A_TUNING_ENV = {
    "fs.s3a.connection.maximum": ("SPARK_S3A_CONNECTION_MAXIMUM", "64"),
    "fs.s3a.threads.max": ("SPARK_S3A_THREADS_MAX", "32"),
    "fs.s3a.max.total.tasks": ("SPARK_S3A_MAX_TOTAL_TASKS", "64"),
    "fs.s3a.multipart.size": ("SPARK_S3A_MULTIPART_SIZE", "128M"),
    "fs.s3a.multipart.threshold": ("SPARK_S3A_MULTIPART_THRESHOLD", "128M"),
    "fs.s3a.fast.upload": ("SPARK_S3A_FAST_UPLOAD", "true"),
    "fs.s3a.fast.upload.buffer": ("SPARK_S3A_FAST_UPLOAD_BUFFER", "disk"),
    "fs.s3a.fast.upload.active.blocks": ("SPARK_S3A_FAST_UPLOAD_ACTIVE_BLOCKS", "4"),
    "fs.s3a.attempts.maximum": ("SPARK_S3A_ATTEMPTS_MAXIMUM", "10"),
    "fs.s3a.connection.request.timeout": ("SPARK_S3A_CONNECTION_REQUEST_TIMEOUT", "30m"),
    "fs.s3a.connection.timeout": ("SPARK_S3A_CONNECTION_TIMEOUT", "30s"),
    "fs.s3a.socket.timeout": ("SPARK_S3A_SOCKET_TIMEOUT", "30m"),
}


def _env_int(name: str, default: int, env: Optional[Dict[str, str]] = None) -> int:
    raw = (env or os.environ).get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _build_spark_retry_conf(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    current_env = env or os.environ
    return {
        spark_key: current_env.get(env_key) or default
        for spark_key, (env_key, default) in SPARK_RETRY_POLICY_ENV.items()
    }


def _build_s3a_tuning_conf(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    current_env = env or os.environ
    return {
        spark_key: current_env.get(env_key) or default
        for spark_key, (env_key, default) in SPARK_S3A_TUNING_ENV.items()
        if (current_env.get(env_key) or default)
    }


def _build_spark_master_status_url(
    master_url: str,
    env: Optional[Dict[str, str]] = None,
) -> str:
    current_env = env or os.environ
    explicit_url = (current_env.get("SPARK_MASTER_STATUS_URL") or "").strip()
    parsed_master_url = urlparse(master_url)
    master_host = parsed_master_url.hostname
    if explicit_url and (
        explicit_url != DEFAULT_SPARK_MASTER_STATUS_URL
        or master_host == "spark-master"
        or not master_host
    ):
        return explicit_url

    if not master_host:
        return DEFAULT_SPARK_MASTER_STATUS_URL

    status_port = (current_env.get("SPARK_MASTER_STATUS_PORT") or "").strip()
    if not status_port:
        if master_host == "spark-master":
            status_port = current_env.get("SPARK_MASTER_WEBUI_PORT") or "8080"
        else:
            status_port = (
                current_env.get("SPARK_MASTER_UI_PORT")
                or current_env.get("SPARK_MASTER_WEBUI_PORT")
                or "8080"
            )

    return f"http://{master_host}:{status_port}/json/"


def _coerce_memory_mb(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    text = str(value).strip().lower()
    if not text:
        return 0
    try:
        if text.endswith("g"):
            return int(float(text[:-1]) * 1024)
        if text.endswith("m"):
            return int(float(text[:-1]))
        return int(float(text))
    except ValueError:
        return 0


def _normalize_spark_profile(job_type: str) -> str:
    profile_name = (job_type or "default").strip().lower()
    if profile_name not in DEFAULT_SPARK_RESOURCE_PROFILES:
        return "default"
    return profile_name


def _build_spark_resource_profile(
    job_type: str,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    profile_name = _normalize_spark_profile(job_type)
    defaults = DEFAULT_SPARK_RESOURCE_PROFILES[profile_name]
    prefix = f"SPARK_{profile_name.upper()}_"

    return {
        "name": profile_name,
        "min_cores": _env_int(f"{prefix}MIN_CORES", defaults["min_cores"], env),
        "min_memory_mb": _env_int(
            f"{prefix}MIN_MEMORY_MB",
            defaults["min_memory_mb"],
            env,
        ),
        "cores_max": _env_int(f"{prefix}CORES_MAX", defaults["cores_max"], env),
    }


def _build_executor_cores(
    resource_profile: Dict[str, Any],
    env: Optional[Dict[str, str]] = None,
) -> int:
    current_env = env or os.environ
    if current_env.get("SPARK_EXECUTOR_CORES"):
        return _env_int("SPARK_EXECUTOR_CORES", 1, current_env)

    legacy_worker_cores = _env_int("SPARK_WORKER_CORES", 1, current_env)
    return max(1, min(legacy_worker_cores, int(resource_profile["cores_max"])))


def _spark_master_available_resources(master_status: Dict[str, Any]) -> Dict[str, int]:
    cores_free = 0
    memory_free_mb = 0

    for worker in master_status.get("workers", []):
        w = {k.lower(): v for k, v in worker.items()}
        if str(w.get("state", "")).upper() != "ALIVE":
            continue
        cores_free += int(w.get("coresfree") or 0)
        memory_free_mb += _coerce_memory_mb(w.get("memoryfree"))

    return {"cores_free": cores_free, "memory_free_mb": memory_free_mb}


def _has_spark_capacity(
    available: Dict[str, int],
    resource_profile: Dict[str, Any],
) -> bool:
    return (
        available["cores_free"] >= resource_profile["min_cores"]
        and available["memory_free_mb"] >= resource_profile["min_memory_mb"]
    )


def _clean_app_name_part(value: str, fallback: str) -> str:
    cleaned = str(value or fallback).strip().replace("/", "-")
    return cleaned or fallback


def _short_run_id(run_id: str) -> str:
    return _clean_app_name_part(run_id, "manual")[:8]


def _build_spark_app_name(user_code: str, job_name: str, run_id: str) -> str:
    return "/".join(
        [
            _clean_app_name_part(user_code, "unknown_user_code"),
            _clean_app_name_part(job_name, "unknown_job"),
            _short_run_id(run_id),
        ]
    )


def _build_spark_app_context(
    init_context: Any,
    default_app_name: str,
    default_user_code: str,
) -> Dict[str, str]:
    dagster_run = getattr(init_context, "dagster_run", None)
    tags = dict(getattr(dagster_run, "tags", {}) or {})
    job_name = (
        getattr(dagster_run, "job_name", "")
        or os.getenv("SPARK_JOB_NAME", default_app_name)
    )
    run_id = getattr(dagster_run, "run_id", "") or os.getenv("SPARK_RUN_ID", "manual")
    user_code = tags.get("user_code") or os.getenv("SPARK_USER_CODE", default_user_code)
    job_type = tags.get("job_type") or os.getenv("SPARK_JOB_TYPE")
    if not job_type and "_maintenance_" in job_name.lower():
        job_type = "maintenance"
    if not job_type:
        job_type = "default"
    if tags.get("backfill"):
        job_type = "backfill"

    return {
        "user_code": user_code,
        "job_name": job_name,
        "job_type": job_type,
        "run_id": _short_run_id(run_id),
        "app_name": _build_spark_app_name(user_code, job_name, run_id),
    }


DIM_STORE_SCHEDULE_COLUMNS = (
    "StoreId",
    "OpeningDate",
    "ClosingDate",
    "ReceptionHours",
    "MondayOpeningTime",
    "MondayLunchStartTime",
    "MondayLunchEndTime",
    "MondayClosingTime",
    "TuesdayOpeningTime",
    "TuesdayLunchStartTime",
    "TuesdayLunchEndTime",
    "TuesdayClosingTime",
    "WednesdayOpeningTime",
    "WednesdayLunchStartTime",
    "WednesdayLunchEndTime",
    "WednesdayClosingTime",
    "ThursdayOpeningTime",
    "ThursdayLunchStartTime",
    "ThursdayLunchEndTime",
    "ThursdayClosingTime",
    "FridayOpeningTime",
    "FridayLunchStartTime",
    "FridayLunchEndTime",
    "FridayClosingTime",
    "SaturdayOpeningTime",
    "SaturdayLunchStartTime",
    "SaturdayLunchEndTime",
    "SaturdayClosingTime",
    "SundayOpeningTime",
    "SundayLunchStartTime",
    "SundayLunchEndTime",
    "SundayClosingTime",
    "CreatedAt",
    "CreatedBy",
    "SalesOfficeId",
)
FACT_OUT_OF_HOURS_SALES_COLUMNS = (
    "SaleId",
    "SaleNumber",
    "Amount",
    "LocalTimeSpan",
    "UserName",
    "StoreId",
    "OutOfHoursType",
    "DeviationMinutes",
)
FACT_STORE_SALES_DAILY_COLUMNS = (
    "StoreId",
    "SalesDate",
    "SalesCount",
    "SalesAmount",
)


class SparkResource:
    def __init__(
        self,
        master_url: str,
        worker_mem: str,
        executor_mem: str,
        executor_cores: int,
        app_name: str,
        jdbc_host: str,
        jdbc_port: int,
        jdbc_database: str,
        jdbc_user: str,
        jdbc_driver: str,
        jdbc_password: str,
        jdbc_write_num_chunks: int,
        jdbc_write_target_partitions: int,
        jdbc_write_batch_size: int,
        user_code: str = "",
        job_name: str = "",
        job_type: str = "default",
        run_id: str = "",
        master_status_url: str = DEFAULT_SPARK_MASTER_STATUS_URL,
        resource_wait_seconds: int = DEFAULT_SPARK_RESOURCE_WAIT_SECONDS,
        resource_poll_seconds: int = DEFAULT_SPARK_RESOURCE_POLL_SECONDS,
        driver_host: str = "",
        driver_bind_address: str = "",
        driver_port: str = "",
        driver_block_manager_port: str = "",
        block_manager_port: str = "",
        worker_network_timeout_seconds: int = (
            DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS
        ),
        resource_profile: Optional[Dict[str, Any]] = None,
        ozone_s3_endpoint: str = "",
        ozone_s3_region: str = "us-east-1",
        ozone_s3_access_key: Optional[str] = None,
        ozone_s3_secret_key: Optional[str] = None,
        ozone_s3_path_style_access: str = "true",
        ozone_s3_bucket_probe: str = "0",
        ozone_s3_change_detection_mode: str = "none",
    ) -> None:
        self.master_url = master_url
        self.app_name = app_name
        self.worker_mem = worker_mem
        self.executor_mem = executor_mem
        self.executor_cores = executor_cores
        self.user_code = user_code
        self.job_name = job_name
        self.job_type = job_type
        self.run_id = run_id
        self.master_status_url = master_status_url
        self.resource_wait_seconds = resource_wait_seconds
        self.resource_poll_seconds = resource_poll_seconds
        self.driver_host = driver_host
        self.driver_bind_address = driver_bind_address
        self.driver_port = driver_port
        self.driver_block_manager_port = driver_block_manager_port
        self.block_manager_port = block_manager_port
        self.worker_network_timeout_seconds = worker_network_timeout_seconds
        self.resource_profile = resource_profile or _build_spark_resource_profile(job_type)
        self.ozone_s3_endpoint = ozone_s3_endpoint
        self.ozone_s3_region = ozone_s3_region
        self.ozone_s3_access_key = ozone_s3_access_key
        self.ozone_s3_secret_key = ozone_s3_secret_key
        self.ozone_s3_path_style_access = ozone_s3_path_style_access
        self.ozone_s3_bucket_probe = ozone_s3_bucket_probe
        self.ozone_s3_change_detection_mode = ozone_s3_change_detection_mode
        self.jdbc_host = jdbc_host
        self.jdbc_port = jdbc_port
        self.jdbc_database = jdbc_database
        self.jdbc_user = jdbc_user
        self.jdbc_driver = jdbc_driver
        self.jdbc_password = jdbc_password
        self.jdbc_write_num_chunks = jdbc_write_num_chunks
        self.jdbc_write_target_partitions = jdbc_write_target_partitions
        self.jdbc_write_batch_size = jdbc_write_batch_size
        self._spark: Optional[SparkSession] = None

    def _spark_packages(self) -> List[str]:
        return [MSSQL_JDBC_PACKAGE]

    def _build_ozone_s3_options(self) -> Dict[str, str]:
        if not self.ozone_s3_endpoint:
            return {}

        options = {
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.endpoint": self.ozone_s3_endpoint,
            "fs.s3a.path.style.access": self.ozone_s3_path_style_access,
            "fs.s3a.bucket.probe": self.ozone_s3_bucket_probe,
            "fs.s3a.change.detection.mode": self.ozone_s3_change_detection_mode,
            "fs.s3a.connection.ssl.enabled": (
                "false"
                if self.ozone_s3_endpoint.lower().startswith("http://")
                else "true"
            ),
        }
        if self.ozone_s3_region:
            options["fs.s3a.endpoint.region"] = self.ozone_s3_region
        options.update(_build_s3a_tuning_conf())
        if self.ozone_s3_access_key and self.ozone_s3_secret_key:
            options["fs.s3a.access.key"] = self.ozone_s3_access_key
            options["fs.s3a.secret.key"] = self.ozone_s3_secret_key
            options["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        return options

    def _build_spark_conf(self) -> SparkConf:
        conf = (
            SparkConf()
            .setAppName(self.app_name)
            .setMaster(self.master_url)
            .set("spark.executor.memory", self.executor_mem)
            .set("spark.executor.cores", str(self.executor_cores))
            .set("spark.cores.max", str(self.resource_profile["cores_max"]))
            .set(
                "spark.sql.maxPlanStringLength",
                os.getenv(
                    "SPARK_SQL_MAX_PLAN_STRING_LENGTH",
                    DEFAULT_SPARK_SQL_MAX_PLAN_STRING_LENGTH,
                ),
            )
            .set(
                "spark.sql.debug.maxToStringFields",
                os.getenv(
                    "SPARK_SQL_DEBUG_MAX_TO_STRING_FIELDS",
                    DEFAULT_SPARK_SQL_DEBUG_MAX_TO_STRING_FIELDS,
                ),
            )
        )
        if self.driver_host:
            conf.set("spark.driver.host", self.driver_host)
            conf.set(
                "spark.driver.bindAddress",
                self.driver_bind_address or DEFAULT_SPARK_DRIVER_BIND_ADDRESS,
            )
        if self.driver_port:
            conf.set("spark.driver.port", str(self.driver_port))
        if self.driver_block_manager_port:
            conf.set(
                "spark.driver.blockManager.port",
                str(self.driver_block_manager_port),
            )
        if self.block_manager_port:
            conf.set("spark.blockManager.port", str(self.block_manager_port))

        for key, value in _build_spark_retry_conf().items():
            conf.set(key, value)

        for key, value in self._build_ozone_s3_options().items():
            conf.set("spark.hadoop." + key, value)

        conf.set("spark.jars.packages", ",".join(self._spark_packages()))
        return conf

    def _fetch_spark_master_status(self) -> Dict[str, Any]:
        with urlopen(self.master_status_url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    def _worker_endpoint_is_routable(self, host: str, port: int) -> bool:
        try:
            with socket.create_connection(
                (host, port),
                timeout=max(self.worker_network_timeout_seconds, 1),
            ):
                return True
        except ConnectionRefusedError:
            return True
        except OSError:
            return False

    def _validate_spark_worker_network(self, master_status: Dict[str, Any]) -> None:
        if not self.block_manager_port:
            return

        block_manager_port = int(self.block_manager_port)
        unreachable = []
        for worker in master_status.get("workers", []):
            w = {k.lower(): v for k, v in worker.items()}
            if str(w.get("state", "")).upper() != "ALIVE":
                continue
            host = str(w.get("host") or "").strip()
            if not host:
                continue
            if not self._worker_endpoint_is_routable(host, block_manager_port):
                unreachable.append(f"{host}:{block_manager_port}")

        if unreachable:
            raise RuntimeError(
                "Spark worker block manager endpoints are not reachable from "
                f"the driver: {', '.join(unreachable)}. Configure each worker "
                "to advertise a routable host/IP and publish "
                "SPARK_BLOCK_MANAGER_PORT for executor shuffle traffic."
            )

    def _validate_port_assignments(self) -> None:
        ports = {}
        for label, value in [
            ("spark.driver.port", self.driver_port),
            ("spark.driver.blockManager.port", self.driver_block_manager_port),
            ("spark.blockManager.port", self.block_manager_port),
        ]:
            if not value:
                continue
            port_str = str(value).strip()
            if not port_str:
                continue
            if port_str in ports:
                raise RuntimeError(
                    f"Spark port conflict: {label} and {ports[port_str]} "
                    f"are both set to port {port_str}. Each Spark port "
                    f"(driver, driver block manager, executor block manager) "
                    f"must be unique."
                )
            ports[port_str] = label

    def _wait_for_spark_capacity(self) -> None:
        if self.master_url.lower().startswith("local"):
            return
        if not self.master_status_url:
            raise RuntimeError(
                "SPARK_MASTER_STATUS_URL is required for Spark capacity checks."
            )

        deadline = time.monotonic() + max(self.resource_wait_seconds, 0)
        last_available = {"cores_free": 0, "memory_free_mb": 0}

        while True:
            try:
                master_status = self._fetch_spark_master_status()
            except Exception as exc:
                raise RuntimeError(
                    "Could not read Spark master status from "
                    f"{self.master_status_url}: {exc}"
                ) from exc

            last_available = _spark_master_available_resources(master_status)
            if _has_spark_capacity(last_available, self.resource_profile):
                self._validate_spark_worker_network(master_status)
                return

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Insufficient Spark resources for "
                    f"job_type={self.job_type!r}: required "
                    f"{self.resource_profile['min_cores']} cores and "
                    f"{self.resource_profile['min_memory_mb']} MB free memory; "
                    f"available {last_available['cores_free']} cores and "
                    f"{last_available['memory_free_mb']} MB free memory."
                )

            time.sleep(max(self.resource_poll_seconds, 1))

    def get_spark(self) -> SparkSession:
        if self._spark is not None:
            return self._spark

        self._validate_port_assignments()
        self._wait_for_spark_capacity()
        conf = self._build_spark_conf()

        self._spark = (
            SparkSession.builder
            .config(conf=conf)
            .getOrCreate()
        )
        for key, value in self._build_ozone_s3_options().items():
            self._spark._jsc.hadoopConfiguration().set(key, value)
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None

    def _build_wh_path(self) -> Dict[str, Any]:
        return {
            "url": (
                "jdbc:sqlserver://"
                + self.jdbc_host
                + ":"
                + str(self.jdbc_port)
                + ";databaseName="
                + self.jdbc_database
                + ";encrypt=true;trustServerCertificate=true"
            ),
            "properties": {
                "user": self.jdbc_user,
                "password": self.jdbc_password,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            },
        }

    def _write_jdbc_in_chunks(
        self,
        df: DataFrame,
        table: str,
        chunk_column: str,
        properties: Optional[Dict[str, str]] = None,
        storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    ) -> int:
        wh = self._build_wh_path()
        url = wh["url"]
        write_properties = dict(properties or wh["properties"])
        write_properties.pop("batchsize", None)

        df_chunked = (
            df
            .selectExpr(
                "*",
                f"pmod(hash(`{chunk_column}`), {self.jdbc_write_num_chunks}) as __chunk_id",
            )
            .persist(storage_level)
        )

        try:
            row_count = df_chunked.count()
            if row_count == 0:
                return 0

            chunk_ids = [
                row["__chunk_id"]
                for row in df_chunked.select("__chunk_id").distinct().collect()
            ]
            for chunk_id in chunk_ids:
                df_chunk = (
                    df_chunked
                    .filter(f"__chunk_id = {chunk_id}")
                    .drop("__chunk_id")
                    .coalesce(self.jdbc_write_target_partitions)
                )

                (
                    df_chunk.write
                    .mode("append")
                    .format("jdbc")
                    .option("url", url)
                    .option("dbtable", table)
                    .option("batchsize", str(self.jdbc_write_batch_size))
                    .option("isolationLevel", "READ_COMMITTED")
                    .options(**write_properties)
                    .save()
                )
        finally:
            df_chunked.unpersist()

        return row_count

    def write_dim_store_schedules_stage(self, df: DataFrame, run_id: str) -> int:
        df_stage = (
            df
            .select(*DIM_STORE_SCHEDULE_COLUMNS)
            .dropDuplicates(["StoreId"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *DIM_STORE_SCHEDULE_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            row_count = df_stage.count()
            if row_count == 0:
                return 0
            wh = self._build_wh_path()
            (
                df_stage
                .coalesce(self.jdbc_write_target_partitions)
                .write
                .mode("append")
                .format("jdbc")
                .option("url", wh["url"])
                .option("dbtable", "stg.dimStoreSchedules")
                .option("batchsize", str(self.jdbc_write_batch_size))
                .option("isolationLevel", "READ_COMMITTED")
                .options(**wh["properties"])
                .save()
            )
            return row_count
        finally:
            df_stage.unpersist()

    def write_fact_out_of_hours_sales_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        df_stage = (
            df
            .select(*FACT_OUT_OF_HOURS_SALES_COLUMNS)
            .dropDuplicates(["SaleId"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *FACT_OUT_OF_HOURS_SALES_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            row_count = df_stage.count()
            if row_count == 0:
                return 0
            wh = self._build_wh_path()
            (
                df_stage
                .coalesce(self.jdbc_write_target_partitions)
                .write
                .mode("append")
                .format("jdbc")
                .option("url", wh["url"])
                .option("dbtable", "stg.factOutOfHoursSales")
                .option("batchsize", str(self.jdbc_write_batch_size))
                .option("isolationLevel", "READ_COMMITTED")
                .options(**wh["properties"])
                .save()
            )
            return row_count
        finally:
            df_stage.unpersist()

    def write_fact_store_sales_daily_stage(
        self,
        df: DataFrame,
        run_id: str,
    ) -> int:
        df_stage = (
            df
            .select(*FACT_STORE_SALES_DAILY_COLUMNS)
            .dropDuplicates(["StoreId", "SalesDate"])
            .withColumn("RunId", lit(run_id))
            .select("RunId", *FACT_STORE_SALES_DAILY_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            row_count = df_stage.count()
            if row_count == 0:
                return 0
            wh = self._build_wh_path()
            (
                df_stage
                .coalesce(self.jdbc_write_target_partitions)
                .write
                .mode("append")
                .format("jdbc")
                .option("url", wh["url"])
                .option("dbtable", "stg.factStoreSalesDaily")
                .option("batchsize", str(self.jdbc_write_batch_size))
                .option("isolationLevel", "READ_COMMITTED")
                .options(**wh["properties"])
                .save()
            )
            return row_count
        finally:
            df_stage.unpersist()


@resource
def spark_resource(init_context):
    master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    default_app_name = os.getenv("SPARK_APP_NAME", "bafar_ingestion")
    default_user_code = __name__.split(".")[0]
    app_context = _build_spark_app_context(
        init_context,
        default_app_name=default_app_name,
        default_user_code=default_user_code,
    )
    resource_profile = _build_spark_resource_profile(app_context["job_type"])
    worker_mem = os.getenv("SPARK_WORKER_MEM", "4G")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEM", "3G")
    executor_cores = _build_executor_cores(resource_profile)
    master_status_url = _build_spark_master_status_url(master_url)
    resource_wait_seconds = int(
        os.getenv("SPARK_RESOURCE_WAIT_SECONDS", str(DEFAULT_SPARK_RESOURCE_WAIT_SECONDS))
    )
    resource_poll_seconds = int(
        os.getenv("SPARK_RESOURCE_POLL_SECONDS", str(DEFAULT_SPARK_RESOURCE_POLL_SECONDS))
    )
    driver_host = os.getenv("SPARK_DRIVER_HOST", "")
    driver_bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "")
    driver_port = os.getenv("SPARK_DRIVER_PORT", "")
    driver_block_manager_port = os.getenv("SPARK_DRIVER_BLOCK_MANAGER_PORT", "")
    block_manager_port = os.getenv("SPARK_BLOCK_MANAGER_PORT", "")
    worker_network_timeout_seconds = int(
        os.getenv(
            "SPARK_WORKER_NETWORK_TIMEOUT_SECONDS",
            str(DEFAULT_SPARK_WORKER_NETWORK_TIMEOUT_SECONDS),
        )
    )
    jdbc_host = os.getenv("MSSQL_HOST", "localhost")
    jdbc_port = int(os.getenv("MSSQL_PORT", "1433"))
    jdbc_database = os.getenv("APP_DB", "")
    jdbc_user = os.getenv("MSSQL_USER", "")
    jdbc_driver = os.getenv("MSSQL_DRIVER", "")
    jdbc_password = os.getenv("MSSQL_PASSWORD", "")
    jdbc_write_num_chunks = int(os.getenv("JDBC_WRITE_NUM_CHUNKS", "5"))
    jdbc_write_target_partitions = int(os.getenv("JDBC_WRITE_TARGET_PARTITIONS", "2"))
    jdbc_write_batch_size = int(os.getenv("JDBC_WRITE_BATCH_SIZE", "100"))
    ozone_s3_endpoint = os.getenv("OZONE_S3_ENDPOINT", "")
    ozone_s3_region = os.getenv("OZONE_S3_REGION", "us-east-1")
    ozone_s3_access_key = os.getenv("OZONE_S3_ACCESS_KEY")
    ozone_s3_secret_key = os.getenv("OZONE_S3_SECRET_KEY")
    ozone_s3_path_style_access = os.getenv("OZONE_S3_PATH_STYLE_ACCESS", "true")
    ozone_s3_bucket_probe = os.getenv("OZONE_S3_BUCKET_PROBE", "0")
    ozone_s3_change_detection_mode = os.getenv(
        "OZONE_S3_CHANGE_DETECTION_MODE",
        "none",
    )
    spark_res = SparkResource(
        master_url=master_url,
        app_name=app_context["app_name"],
        worker_mem=worker_mem,
        executor_mem=executor_mem,
        executor_cores=executor_cores,
        user_code=app_context["user_code"],
        job_name=app_context["job_name"],
        job_type=app_context["job_type"],
        run_id=app_context["run_id"],
        master_status_url=master_status_url,
        resource_wait_seconds=resource_wait_seconds,
        resource_poll_seconds=resource_poll_seconds,
        driver_host=driver_host,
        driver_bind_address=driver_bind_address,
        driver_port=driver_port,
        driver_block_manager_port=driver_block_manager_port,
        block_manager_port=block_manager_port,
        worker_network_timeout_seconds=worker_network_timeout_seconds,
        resource_profile=resource_profile,
        jdbc_host=jdbc_host,
        jdbc_port=jdbc_port,
        jdbc_database=jdbc_database,
        jdbc_user=jdbc_user,
        jdbc_driver=jdbc_driver,
        jdbc_password=jdbc_password,
        jdbc_write_num_chunks=jdbc_write_num_chunks,
        jdbc_write_target_partitions=jdbc_write_target_partitions,
        jdbc_write_batch_size=jdbc_write_batch_size,
        ozone_s3_endpoint=ozone_s3_endpoint,
        ozone_s3_region=ozone_s3_region,
        ozone_s3_access_key=ozone_s3_access_key,
        ozone_s3_secret_key=ozone_s3_secret_key,
        ozone_s3_path_style_access=ozone_s3_path_style_access,
        ozone_s3_bucket_probe=ozone_s3_bucket_probe,
        ozone_s3_change_detection_mode=ozone_s3_change_detection_mode,
    )
    try:
        yield spark_res
    finally:
        spark_res.stop()
