from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    graph_multi_asset,
    op,
)

from ..resources import SparkResource, SqlServerResource, TransformationOps
from ..transformations import factlecturas_extended, factReadingsMeasurement_device
from ...operations_filters import limit_operations_rows_to_closed_hours
from ...operations_window import build_operations_window, resolve_operations_watermark


OPERATIONS_TIMEZONE = ZoneInfo("America/Ciudad_Juarez")

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


@op(
    required_resource_keys={
        "sqlserver"
    },
    ins={
        "deps": In(Optional[str])
    },
    out=Out(Dict[str, Any]),
)
def start_operations_run_and_get_range_op(
        context,
        deps: Optional[str] = None) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver

    if not deps:
        deps = " "

    last_loaded_at = sql.get_watermark("HourlyDeviceStatus")
    if last_loaded_at is None:
        last_loaded_at = datetime(2025, 10, 1)
        context.log.info("Operation tables are empty, "
                         "will load factLecturas from beginning.")

    window = build_operations_window(last_loaded_at)
    if window["watermark_in_future"]:
        context.log.warning(
            "HourlyDeviceStatus watermark %s is ahead of current local hour %s. "
            "This run will clean future rows and clamp processing to the current local window.",
            window["from_local"],
            window["max_local_hour"],
        )

    sql.mark_run_started("HourlyDeviceStatus")
    context.log.info(
        "HourlyDeviceStatus local run window from "
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
)
def window_cleanup(
        context: OpExecutionContext,
        window: Dict[str, Any]) -> Optional[str]:
    sqlserver: SqlServerResource = context.resources.sqlserver

    context.log.info("Deleting incomplete last period from "
                     "factHourlyDeviceStatus and factReadingsMeasurement...")
    deleted_rows = sqlserver.delete_range(
        schema_name="dwh",
        table_name="factHourlyDeviceStatus",
        time_column="LocalTimeSpan",
        from_utc=window["delete_from"],
        to_utc=window["delete_to_exclusive"],
    )
    if deleted_rows is None or deleted_rows == 0:
        context.log.info("No rows deleted from "
                         "dwh.factHourlyDeviceStatus for this period.")
    else:
        context.log.info("Deleted "
                         + str(deleted_rows)
                         + " rows from dwh.factHourlyDeviceStatus with cascade.")

@op(
    required_resource_keys={
        "spark"
    },
    ins={
        "cleaning": In(Optional[str])
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
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
        "sqlserver"
    },
    ins={
        "raw_dims_df": In(DataFrame),
        "window": In(Dict[str, Any])
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
)
def load_factlecturas_extended_op(
        context: OpExecutionContext,
        raw_dims_df: DataFrame,
        window: Dict[str, Any]) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    sqlserver: SqlServerResource = context.resources.sqlserver
    context.log.info("Joining factLecuras data period with dimensions table...")

    factlecturas_extended_df = factlecturas_extended(
        spark_res=spark_res,
        sqlserver=sqlserver,
        raw_dimensions=raw_dims_df,
        fetch_factlecturas_from=window["fetch_from"],
    )

    if factlecturas_extended_df is None:
        _df: DataFrame = spark_res.create_emptiness_dataframe()
        context.log.warning("FactLecturas table is empty.")
        return _df
    elif factlecturas_extended_df.rdd.isEmpty():
        _df: DataFrame = spark_res.create_emptiness_dataframe()
        context.log.warning("FactLecturas table is empty.")
        return _df

    return factlecturas_extended_df

@op(
    required_resource_keys={
        "transformation_ops"
    },
    ins={
        "window": In(Dict[str, Any]),
        "dims": In(Optional[DataFrame]),
        "factlecturas_extended_df": In(Optional[DataFrame])
    },
    out=Out(Optional[DataFrame], io_manager_key="spark_df_io_manager"),
)
def build_devices_df_op(
        context: OpExecutionContext,
        window: Dict[str, Any],
        dims: Optional[DataFrame] = None,
        factlecturas_extended_df: Optional[DataFrame] = None) -> Optional[DataFrame]:
    transformation_ops: TransformationOps = context.resources.transformation_ops

    union_df: Optional[DataFrame] = None
    if len(factlecturas_extended_df.columns) <= 1:
        context.log.warning("Factlecturas_extended_df is empty.")
        return factlecturas_extended_df

    methods = [
        "maquinas_nieve",
        "maquinas_soda",
        "cuartos_refrigeracion",
        "cuartos_congelacion",
        "luces",
        "termostatos",
    ]

    for method in methods:
        context.log.info("Materializing dataframe for method: " + method)
        fn = getattr(transformation_ops, method)
        df = fn(
            factlecturas_extended_df,
            dims,
            window["fetch_from"],
            window["max_local_hour"],
        ).select(
            col("OperationId"),
            col("DeviceId"),
            col("LocationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("SensorId"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("LocalTimeSpan"),
            col("ReadingsCount"),
            col("_sensor_type"),
        )

        if df.rdd.isEmpty():
            context.log.info("No rows for method: " + method)
            continue

        if union_df is None:
            union_df = df
        else:
            union_df = union_df.unionByName(df)

    if union_df is None:
        context.log.info("No rows across all transformations.")
        return factlecturas_extended_df.limit(0)

    return limit_operations_rows_to_closed_hours(
        union_df,
        window["max_local_hour"],
    )

@op(
    required_resource_keys={
        "spark"
    },
    ins={
        "devices_df": In(DataFrame),
        "window": In(Dict[str, Any]),
        "fact_hourly_result": In(Optional[Dict[str, Any]])
    },
    out={
        "asset": Out(Nothing),
        "result": Out(Dict[str, Any])
    },
)
def write_factreadingsmeasurement_op(
        context,
        devices_df: DataFrame,
        window: Dict[str, Any],
        fact_hourly_result: Optional[Dict[str, Any]] = None) -> Tuple[None, Dict[str, Any]]:
    spark_res = context.resources.spark

    if not fact_hourly_result["succeeded"]:
        context.log.warning("write_facthourlydevicestatus_op failed")
        return (
            None,
            {
                "to_local": window["max_local_hour"],
                "original_last_loaded_at": window["original_last_loaded_at"],
                "max_time_span": None,
                "succeeded": False,
                "error_message": "write_facthourlydevicestatus_op failed",
            },
    )
    max_time_span: Optional[datetime] = None
    succeeded = False
    error_message: Optional[str] = None

    try:
        if devices_df.rdd.isEmpty() or len(devices_df.columns) <= 1:
            succeeded = True
        else:
            inserted_ = spark_res.write_fact_operations(
                devices_df
                .select(
                    "OperationId",
                    "SensorId",
                    "SensorTyId",
                    "StatusCode",
                    "MeasurementValue",
                    "ReadingsCount",
                    "Anomalies"
                )
            )
            max_time_span = spark_res.get_max_time_span(devices_df, "LocalTimeSpan")
            succeeded = True
            context.log.info(str(inserted_) + "rows inserted.")
    except Exception as ex:
        error_message = str(ex)
        succeeded = False

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
        "spark"
    },
    ins={
        "devices_df": In(DataFrame),
        "window": In(Dict[str, Any]),
    },
    out={
        "asset": Out(Nothing),
        "result": Out(Dict[str, Any])
    },
)
def write_facthourlydevicestatus_op(
        context,
        devices_df: DataFrame,
        window: Dict[str, Any]) -> Tuple[None, Dict[str, Any]]:
    spark_res = context.resources.spark

    max_time_span: Optional[datetime] = None
    succeeded = False
    error_message: Optional[str] = None

    try:
        if devices_df.rdd.isEmpty() or len(devices_df.columns) <= 1:
            context.log.info("FactLecturas window was empty")
            succeeded = True
        else:
            aggregated_df = factReadingsMeasurement_device(devices_df)
            inserted_ = spark_res.write_fact_operations_device(aggregated_df)
            max_time_span = spark_res.get_max_time_span(aggregated_df, "LocalTimeSpan")
            succeeded = True
            if inserted_ == 0:
                context.log.warning("written 0 rows to factHourlyDeviceStatus")
    except Exception as ex:
        error_message = str(ex)
        context.log.warning(ex)
        succeeded = False

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
        "hourly_status": In(Dict[str, Any]),
        "hourly_readings": In(Dict[str, Any])
    },
    out=Out(Optional[Dict[str, Any]]),
)
def finalize_operations_run_op(
        context,
        hourly_status: Dict[str, Any],
        hourly_readings: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sql = context.resources.sqlserver

    succeeded: bool = False
    error_message: Optional[str] = None

    if hourly_status["succeeded"]:
        if hourly_readings["succeeded"]:
            succeeded: bool = True
        else:
            error_message: Optional[str] = hourly_readings["error_message"]
            context.log.warning("error readings: " + str(error_message))

    else:
        error_message = hourly_status["error_message"]
        context.log.warning("error readings: " + str(error_message))

    to_local: datetime = hourly_status["to_local"]
    original_last_loaded_at: datetime = hourly_status["original_last_loaded_at"]
    max_time_span: Optional[datetime] = hourly_status["max_time_span"]

    new_watermark = resolve_operations_watermark(
        succeeded=succeeded,
        original_last_loaded_at=original_last_loaded_at,
        max_time_span=max_time_span,
        max_local_hour=to_local,
    )

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            "HourlyDeviceStatus run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_local
            context.log.info(
                "HourlyDeviceStatus run succeeded with no rows. Advancing watermark to " + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            context.log.info(
                "HourlyDeviceStatus run succeeded. Advancing watermark to " + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name="HourlyDeviceStatus",
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )

@op(
    ins={
        "deps": In(Optional[Dict[str, Any]])
    },
    out=Out(Optional[str])
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
        "dwh_factHourlyDeviceStatus": AssetOut(key=AssetKey("dwh_factHourlyDeviceStatus")),
        "dwh_factReadingsMeasurement": AssetOut(key=AssetKey("dwh_factReadingsMeasurement")),
        "ctl_IngestionControl" : AssetOut(key=AssetKey("ctl_IngestionControl")),
    },
    ins={
        "data_transformations": AssetIn(key=AssetKey("data_transformations"), dagster_type=Nothing),
    },
    group_name="sense_operations",
)
def operations_assets(data_transformations):
    deps = upstream_deps_gate(data_transformations)
    fetch_from_ts = start_operations_run_and_get_range_op(deps)
    cleaning = window_cleanup(fetch_from_ts)
    raw_dims_df = load_raw_dimensions_op(cleaning)
    factlecturas_extended_df = load_factlecturas_extended_op(
        raw_dims_df=raw_dims_df,
        window=fetch_from_ts
    )
    devices_df = build_devices_df_op(
        window= fetch_from_ts,
        dims=raw_dims_df,
        factlecturas_extended_df=factlecturas_extended_df)
    hourly_status_out = write_facthourlydevicestatus_op(
        devices_df=devices_df,
        window=fetch_from_ts
    )
    hourly_readings_out = write_factreadingsmeasurement_op(
        devices_df=devices_df,
        window=fetch_from_ts,
        fact_hourly_result=hourly_status_out.result
    )
    finalize = finalize_operations_run_op(
        hourly_status=hourly_status_out.result,
        hourly_readings=hourly_readings_out.result
    )

    return {
        "dwh_factHourlyDeviceStatus": hourly_status_out.asset,
        "dwh_factReadingsMeasurement": hourly_readings_out.asset,
        "ctl_IngestionControl": finalize,
    }
