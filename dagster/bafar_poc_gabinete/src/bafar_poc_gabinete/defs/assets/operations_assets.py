from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from time import perf_counter
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max
from pyspark import StorageLevel
from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    RetryPolicy,
    RetryRequested,
    graph_multi_asset,
    op,
)

from ..resources import SparkResource, SqlServerResource, TransformationOps
from ..transformations import datalake_readings_extended
from ...operations_filters import limit_operations_rows_to_closed_hours
from ...operations_window import build_operations_window, resolve_operations_watermark


OPERATIONS_TIMEZONE = ZoneInfo("America/Ciudad_Juarez")
OPERATIONS_MAX_RETRIES = 3
OPERATIONS_RETRY_DELAY_SECONDS = 60
OPERATIONS_RETRY_POLICY = RetryPolicy(
    max_retries=OPERATIONS_MAX_RETRIES,
    delay=OPERATIONS_RETRY_DELAY_SECONDS,
)

def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def _coerce_local_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return _truncate_to_seconds(dt)
    return _truncate_to_seconds(
        dt.astimezone(OPERATIONS_TIMEZONE).replace(tzinfo=None)
    )


def _current_operational_hour_start() -> datetime:
    return _truncate_to_seconds(
        datetime.now(OPERATIONS_TIMEZONE)
        .replace(minute=0, second=0, microsecond=0, tzinfo=None)
    )


def _get_latest_real_local_hour(devices_df: DataFrame) -> Optional[datetime]:
    if "ReadingsCount" not in devices_df.columns or len(devices_df.head(1)) == 0:
        return None

    latest_row = (
        devices_df
        .filter(col("ReadingsCount") > 0)
        .agg(spark_max("LocalTimeSpan").alias("max_ts"))
        .collect()[0]
    )
    return latest_row.max_ts


@op(
    required_resource_keys={
        "sqlserver"
    },
    ins={
        "deps": In(Optional[str])
    },
    out=Out(Dict[str, Any]),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def start_operations_run_and_get_range_op(
        context,
        deps: Optional[str] = None) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver

    if not deps:
        deps = " "

    last_loaded_at = sql.get_watermark("factReadingsMeasurement")
    if last_loaded_at is None:
        last_loaded_at = datetime(2025, 10, 1)
        context.log.info("Operation tables are empty, "
                         "will load factLecturas from beginning.")

    window = build_operations_window(last_loaded_at)
    if window["watermark_in_future"]:
        context.log.warning(
            "factReadingsMeasurement watermark %s is ahead of current local hour %s. "
            "This run will clean future rows and clamp processing to the current local window.",
            window["from_local"],
            window["max_local_hour"],
        )

    sql.mark_run_started("factReadingsMeasurement")
    context.log.info(
        "factReadingsMeasurement local run window from "
        + str(window["fetch_from"])
        + " to "
        + str(window["max_local_hour"])
        + " (cleanup<"
        + str(window["delete_to_exclusive"])
        + ")"
        + " [tz=America/Ciudad_Juarez]"
        + deps
    )

    return window

@op(
    required_resource_keys={
        "sqlserver"
    },
    ins={
        "window": In(Dict[str, Any])
    },
    out=Out(Optional[str]),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def window_cleanup(
        context: OpExecutionContext,
        window: Dict[str, Any]) -> Optional[str]:
    sqlserver: SqlServerResource = context.resources.sqlserver

    context.log.info("Deleting incomplete last period from "
                     "factReadingsMeasurement...")
    deleted_rows = sqlserver.delete_range(
        schema_name="dwh",
        table_name="factReadingsMeasurement",
        time_column="LocalTimeSpan",
        from_utc=window["delete_from"],
        to_utc=window["delete_to_exclusive"],
    )
    if deleted_rows is None or deleted_rows == 0:
        context.log.info("No rows deleted from "
                         "dwh.factReadingsMeasurement for this period.")
    else:
        context.log.info("Deleted "
                         + str(deleted_rows)
                         + " rows from dwh.factReadingsMeasurement.")

@op(
    required_resource_keys={
        "spark",
    },
    ins={
        "cleaning": In(Optional[str])
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def load_raw_dimensions_op(
        context: OpExecutionContext,
        cleaning: Optional[str] = None) -> DataFrame:
    spark_res: SparkResource = context.resources.spark

    if not cleaning:
        cleaning = " "

    context.log.info("Loading dimensions table..." + cleaning)
    dimensions_df = spark_res.read_dim_prod_dimensions()

    return dimensions_df.filter(col("Active"))

@op(
    required_resource_keys={
        "spark",
    },
    ins={
        "raw_dims_df": In(DataFrame),
        "window": In(Dict[str, Any])
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def load_datalake_readings_extended_op(
        context: OpExecutionContext,
        raw_dims_df: DataFrame,
        window: Dict[str, Any]) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    context.log.info("Joining datalake readings period with dimensions table...")

    readings_extended_df = datalake_readings_extended(
        spark_res=spark_res,
        raw_dimensions=raw_dims_df,
        fetch_readings_from=window["fetch_from"],
        fetch_readings_to=window["read_to_utc"],
    )

    if readings_extended_df is None:
        _df: DataFrame = spark_res.create_emptiness_dataframe()
        context.log.warning("Data Lake readings are empty.")
        return _df

    return readings_extended_df

@op(
    required_resource_keys={
        "transformation_ops"
    },
    ins={
        "window": In(Dict[str, Any]),
        "dims": In(Optional[DataFrame]),
        "readings_extended_df": In(Optional[DataFrame])
    },
    out=Out(Optional[DataFrame], io_manager_key="spark_df_io_manager"),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def build_devices_df_op(
        context: OpExecutionContext,
        window: Dict[str, Any],
        dims: Optional[DataFrame] = None,
        readings_extended_df: Optional[DataFrame] = None) -> Optional[DataFrame]:
    transformation_ops: TransformationOps = context.resources.transformation_ops

    union_df: Optional[DataFrame] = None
    if len(readings_extended_df.columns) <= 1:
        context.log.warning("Readings_extended_df is empty.")
        return readings_extended_df

    readings_extended_df = readings_extended_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        methods = [
            "gabinete",
        ]

        for method in methods:
            context.log.info("Materializing dataframe for method: " + method)
            fn = getattr(transformation_ops, method)
            method_df = fn(
                readings_extended_df,
                dims,
                window["fetch_from"],
                window["max_local_hour"],
            )

            if method_df is None:
                context.log.info("Method returned no dataframe: " + method)
                continue

            df = method_df.select(
                col("OperationId"),
                col("DeviceId"),
                col("LocationId"),
                col("SensorTyId"),
                col("SensorId"),
                col("MeasurementValue"),
                col("MeasurementStdDev"),
                col("Anomalies"),
                col("LocalTimeSpan"),
                col("ReadingsCount"),
                col("sensor_type").alias("SensorType"),
                col("device_type").alias("DeviceType"),
            )

            if len(df.head(1)) == 0:
                context.log.info("No rows for method: " + method)
                continue

            if union_df is None:
                union_df = df
            else:
                union_df = union_df.unionByName(df)

        if union_df is None:
            context.log.info("No rows across all transformations.")
            return readings_extended_df.limit(0)

        limited_df = limit_operations_rows_to_closed_hours(
            union_df,
            window["max_local_hour"],
        )
        latest_real_local_hour = _get_latest_real_local_hour(limited_df)

        if latest_real_local_hour is None:
            context.log.info(
                "No real readings found in the current operations window. "
                "Skipping scaffold-only rows and keeping the watermark in place."
            )
            return limited_df.limit(0)

        context.log.info(
            "Latest real operations hour found at %s. "
            "Rows scaffolded beyond that hour will not be materialized.",
            latest_real_local_hour,
        )
        return limited_df.filter(col("LocalTimeSpan") <= lit(latest_real_local_hour))
    finally:
        readings_extended_df.unpersist()

@op(
    required_resource_keys={
        "spark",
        "sqlserver",
    },
    ins={
        "devices_df": In(DataFrame),
        "window": In(Dict[str, Any])
    },
    out={
        "asset": Out(Nothing),
        "result": Out(Dict[str, Any])
    },
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def write_factreadingsmeasurement_op(
        context,
        devices_df: DataFrame,
        window: Dict[str, Any]) -> Tuple[None, Dict[str, Any]]:
    start_time = perf_counter()
    spark_res = context.resources.spark

    inserted = 0
    rows_with_readings_count_gt_0 = 0
    max_time_span: Optional[datetime] = None
    succeeded = False
    error_message: Optional[str] = None

    sqlserver: SqlServerResource = context.resources.sqlserver
    retry_number = getattr(context, "retry_number", 0)

    def cleanup_partial_write() -> None:
        deleted_rows = sqlserver.delete_range(
            schema_name="dwh",
            table_name="factReadingsMeasurement",
            time_column="LocalTimeSpan",
            from_utc=window["delete_from"],
            to_utc=window["delete_to_exclusive"],
        )
        context.log.info(
            "Deleted "
            + str(deleted_rows or 0)
            + " partial factReadingsMeasurement rows before retry."
        )

    def add_asset_metadata() -> None:
        metadata: Dict[str, Any] = {
            "row_count": inserted,
            "rows_with_readings_count_gt_0": rows_with_readings_count_gt_0,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
            "from_local": str(window["fetch_from"]),
            "to_local": str(window["max_local_hour"]),
            "max_time_span": str(max_time_span) if max_time_span else None,
            "succeeded": succeeded,
        }
        if error_message:
            metadata["error_message"] = error_message
        context.add_output_metadata(metadata, output_name="asset")

    try:
        if retry_number > 0:
            cleanup_partial_write()

        if len(devices_df.columns) <= 1 or len(devices_df.head(1)) == 0:
            succeeded = True
        else:
            rows_with_readings_count_gt_0 = devices_df.filter(
                col("ReadingsCount") > 0
            ).count()
            inserted = spark_res.write_fact_operations(
                devices_df
                .select(
                    "OperationId",
                    "SensorId",
                    "SensorTyId",
                    "DeviceId",
                    "LocationId",
                    "DeviceType",
                    "SensorType",
                    "LocalTimeSpan",
                    "MeasurementValue",
                    "MeasurementStdDev",
                    "ReadingsCount",
                    "Anomalies"
                )
            )
            max_time_span = spark_res.get_max_time_span(devices_df, "LocalTimeSpan")
            succeeded = True
            context.log.info(str(inserted) + "rows inserted.")
    except Exception as ex:
        error_message = str(ex)
        succeeded = False
        try:
            cleanup_partial_write()
        except Exception as cleanup_ex:
            context.log.warning(
                "Could not clean partial factReadingsMeasurement rows before retry: %s",
                cleanup_ex,
            )
        if retry_number < OPERATIONS_MAX_RETRIES:
            context.log.warning(
                "factReadingsMeasurement write failed on attempt %s of %s. "
                "Retrying operation.",
                retry_number + 1,
                OPERATIONS_MAX_RETRIES + 1,
            )
            raise RetryRequested(
                max_retries=OPERATIONS_MAX_RETRIES,
                seconds_to_wait=OPERATIONS_RETRY_DELAY_SECONDS,
            ) from ex

    add_asset_metadata()

    return (
        None,
        {
            "to_local": window["max_local_hour"],
            "original_last_loaded_at": window["original_last_loaded_at"],
            "max_time_span": max_time_span,
            "succeeded": succeeded,
            "error_message": error_message,
        },
    )


@op(
    required_resource_keys={
        "sqlserver"
    },
    ins={
        "hourly_readings": In(Dict[str, Any])
    },
    out=Out(Optional[Dict[str, Any]]),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def finalize_operations_run_op(
        context,
        hourly_readings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sql = context.resources.sqlserver

    succeeded: bool = hourly_readings["succeeded"]
    error_message: Optional[str] = hourly_readings.get("error_message")

    if not succeeded:
        context.log.warning("error readings: " + str(error_message))

    to_local: datetime = hourly_readings["to_local"]
    original_last_loaded_at: datetime = hourly_readings["original_last_loaded_at"]
    max_time_span: Optional[datetime] = hourly_readings["max_time_span"]

    new_watermark = resolve_operations_watermark(
        succeeded=succeeded,
        original_last_loaded_at=original_last_loaded_at,
        max_time_span=max_time_span,
        max_local_hour=to_local,
    )

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            "factReadingsMeasurement run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = original_last_loaded_at
            context.log.info(
                "factReadingsMeasurement run succeeded with no rows containing real readings. "
                "Keeping watermark at " + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            context.log.info(
                "factReadingsMeasurement run succeeded. Advancing watermark to " + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name="factReadingsMeasurement",
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )
    metadata: Dict[str, Any] = {
        "succeeded": succeeded,
        "to_local": str(to_local),
        "max_time_span": str(max_time_span) if max_time_span else None,
        "original_last_loaded_at": str(original_last_loaded_at),
    }
    if error_message:
        metadata["error_message"] = error_message
    context.add_output_metadata(metadata)

@op(
    ins={
        "deps": In(Optional[Dict[str, Any]])
    },
    out=Out(Optional[str]),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def upstream_deps_gate(context,
                       deps:Optional[Dict[str, Any]] = None) -> Optional[str]:
    if deps is None:
        context.log.info("Updating deps...")
    else:
        context.log.info("Updating deps..." + str(deps))
    return None

@graph_multi_asset(
    outs={
        "dwh_factReadingsMeasurement": AssetOut(
            key=AssetKey(["bafar_poc_gabinete", "dwh_factReadingsMeasurement"])),
        "ctl_IngestionControl" : AssetOut(
            key=AssetKey(["bafar_poc_gabinete", "ctl_IngestionControl"])),
    },
    ins={
        "data_transformations": AssetIn(
            key=AssetKey(["bafar_poc_gabinete", "data_transformations"]), dagster_type=Nothing),
    },
    group_name="bafar_poc_gabinete",
)
def operations_assets(data_transformations):
    deps = upstream_deps_gate(data_transformations)
    fetch_from_ts = start_operations_run_and_get_range_op(deps)
    cleaning = window_cleanup(fetch_from_ts)
    raw_dims_df = load_raw_dimensions_op(cleaning)
    readings_extended_df = load_datalake_readings_extended_op(
        raw_dims_df=raw_dims_df,
        window=fetch_from_ts
    )
    devices_df = build_devices_df_op(
        window= fetch_from_ts,
        dims=raw_dims_df,
        readings_extended_df=readings_extended_df)
    hourly_readings_out = write_factreadingsmeasurement_op(
        devices_df=devices_df,
        window=fetch_from_ts
    )
    finalize = finalize_operations_run_op(
        hourly_readings=hourly_readings_out.result
    )

    return {
        "dwh_factReadingsMeasurement": hourly_readings_out.asset,
        "ctl_IngestionControl": finalize,
    }
