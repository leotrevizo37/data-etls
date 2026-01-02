from datetime import timedelta, datetime
from typing import Optional, Tuple, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    AssetSelection,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    define_asset_job,
    graph_multi_asset,
    op,
)

from ..resources import SparkResource, SqlServerResource, SqlDimServerResource, TransformationOps
from ..transformations import raw_dimensions, factlecturas_extended, factReadingsMeasurement_device

def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)

@op(
    required_resource_keys={
        "sqlserver"
    },
    ins={
        "ingestion_name": In(str)
    },
    out=Out(Dict[str, Any]),
)
def start_run_and_get_range_op(context, ingestion_name:str) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(ingestion_name)
    if last_loaded_at is None:
        last_loaded_at = datetime(2025, 10, 1)

    from_utc = _truncate_to_seconds(last_loaded_at)
    now_utc = _truncate_to_seconds(datetime.utcnow())

    sql.mark_run_started(ingestion_name)

    context.log.info( ingestion_name + "run window from " + str(from_utc) + " to " + str(now_utc))

    return {
        "from_utc": from_utc,
        "to_utc": now_utc,
        "original_last_loaded_at": last_loaded_at,
    }


@op(
    required_resource_keys={
        "sqlserver"
    },
    out=Out(Tuple[Optional[str],
        Optional[str]]),
)
def compute_window_and_cleanup(
        context: OpExecutionContext) -> Tuple[Optional[str], Optional[str]]:
    sqlserver: SqlServerResource = context.resources.sqlserver

    max_op_ts = sqlserver.get_watermark("HourlyDeviceStatus")
    if max_op_ts is None:
        context.log.info("Operation tables are empty, "
                         "will load factLecturas from beginning.")
        return None, None

    fetch_from_ts = max_op_ts - timedelta(hours=6)
    delete_from_ts = max_op_ts - timedelta(hours=4)

    context.log.info("Deleting incomplete last period from "
                     "factHourlyDeviceStatus and factReadingsMeasurement...")
    deleted_rows = sqlserver.delete_range(
        schema_name="dwh",
        table_name="factHourlyDeviceStatus",
        time_column="LocalTimeSpan",
        from_utc=delete_from_ts,
    )
    if deleted_rows is None or deleted_rows == 0:
        context.log.info("No rows deleted from "
                         "dwh.factHourlyDeviceStatus for this period.")
    else:
        context.log.info("Deleted "
                         + str(deleted_rows)
                         + " rows from dwh.factHourlyDeviceStatus with cascade.")

    return str(fetch_from_ts), str(delete_from_ts)

@op(
    required_resource_keys={
        "spark",
        "dim_sqlserver"
    },
    out=Out(DataFrame),
)
def load_raw_dimensions_op(context: OpExecutionContext) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    context.log.info("Loading dimensions table...")
    return raw_dimensions(spark_res, dim_sql)

@op(
    required_resource_keys={
        "spark",
        "sqlserver"
    },
    ins={
        "raw_dims_df": In(DataFrame),
        "fetch_from_ts": In(Optional[str])
    },
    out=Out(DataFrame),
)
def load_factlecturas_extended_op(
    context: OpExecutionContext,
        raw_dims_df: DataFrame,
        fetch_from_ts: Optional[str]
) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    sqlserver: SqlServerResource = context.resources.sqlserver
    context.log.info("Joining factLecuras data periodo with dimensions table...")
    return factlecturas_extended(
        spark_res=spark_res,
        sqlserver=sqlserver,
        raw_dimensions=raw_dims_df,
        fetch_factlecturas_from=fetch_from_ts,
    )


@op(
    required_resource_keys={
        "transformation_ops"
    },
    ins={
        "factlecturas_extended_df": In(DataFrame)
    },
    out=Out(DataFrame),
)
def build_devices_df_op(
        context: OpExecutionContext,
        factlecturas_extended_df: DataFrame) -> DataFrame:
    transformation_ops: TransformationOps = context.resources.transformation_ops

    union_df: Optional[DataFrame] = None
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
        df = fn(factlecturas_extended_df).select(
            col("OperationId"),
            col("DeviceId"),
            col("LocationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("LocalTimeSpan"),
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

    return union_df


@op(
    required_resource_keys={
        "spark"
    },
    ins={
        "devices_df": In(DataFrame)
    },
    out=Out(Nothing),
)
def write_factreadingsmeasurement_op(
        context: OpExecutionContext,
        devices_df: DataFrame) -> Nothing:
    spark_res: SparkResource = context.resources.spark

    if devices_df.rdd.isEmpty():
        context.log.info("No rows in devices_df; skipping write to stg.factReadingsMeasurement.")
        yield Output(None, metadata={"row_count": 0})
        return

    to_write = devices_df.select(
        col("OperationId"),
        col("SensorTyId"),
        col("StatusCode"),
        col("MeasurementValue"),
        col("Anomalies"),
    )

    inserted_ = spark_res.write_fact_operations(to_write)
    context.log.info(str(inserted_) + " rows written into stg.factReadingsMeasurement.")
    yield Output(None, metadata={"row_count": inserted_})


@op(
    required_resource_keys={
        "spark"
    },
    ins={
        "devices_df": In(DataFrame)
    },
    out=Out(Nothing),
)
def write_facthourlydevicestatus_op(
        context: OpExecutionContext,
        devices_df: DataFrame) -> Nothing:
    spark_res: SparkResource = context.resources.spark

    if devices_df.rdd.isEmpty():
        context.log.info("No rows in devices_df; "
                         "skipping write to dwh.factHourlyDeviceStatus.")
        yield Output(None, metadata={"row_count": 0})
        return

    context.log.info("Transforming to device-level data...")
    aggregated_df = factReadingsMeasurement_device(devices_df)
    if aggregated_df.rdd.isEmpty():
        context.log.info("Aggregated device-level dataframe is empty; "
                         "skipping write.")
        yield Output(None, metadata={"row_count": 0})
        return

    inserted_ = spark_res.write_fact_operations_device(aggregated_df)
    context.log.info(str(inserted_)
                     + " rows written into dwh.factHourlyDeviceStatus.")
    yield Output(None, metadata={"row_count": inserted_})

@op(
    required_resource_keys={
        "sqlserver",
        "spark"
    },
    ins={
        "result": In(Optional[DataFrame])
    },
    out=Out(),
)
def finalize_run_op(context, result: Optional[DataFrame] = None) -> None:
    sql: SqlServerResource = context.resources.sqlserver
    spark : SparkResource = context.resources.spark

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_time_span: Optional[datetime] = result["max_time_span"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            "HourlyDeviceStatus run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_utc
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

@graph_multi_asset(
    outs={
        "dwh_factHourlyDeviceStatus": AssetOut(key=AssetKey("dwh_factHourlyDeviceStatus")),
        "stg_factReadingsMeasurement": AssetOut(key=AssetKey("stg_factReadingsMeasurement")),
    },
    ins={
        "dbo_sensors": AssetIn(key=AssetKey("dbo_sensors"), dagster_type=Nothing),
        "dbo_sensor_tys": AssetIn(key=AssetKey("dbo_sensor_tys"), dagster_type=Nothing),
        "dbo_devices": AssetIn(key=AssetKey("dbo_devices"), dagster_type=Nothing),
        "dbo_device_tys": AssetIn(key=AssetKey("dbo_device_tys"), dagster_type=Nothing),
        "dbo_sublocations": AssetIn(key=AssetKey("dbo_sublocations"), dagster_type=Nothing),
        "dbo_sublocation_tys": AssetIn(key=AssetKey("dbo_sublocation_tys"), dagster_type=Nothing),
        "dwh_factlecturas": AssetIn(key=AssetKey("dwh_factlecturas"), dagster_type=Nothing),
    },
)
def ops_hourly_tables(
    dbo_sensors,
    dbo_sensor_tys,
    dbo_devices,
    dbo_device_tys,
    dbo_sublocations,
    dbo_sublocation_tys,
    dwh_factlecturas,
):
    fetch_from_ts, _ = compute_window_and_cleanup()

    raw_dims_df = load_raw_dimensions_op()
    factlecturas_extended_df = load_factlecturas_extended_op(raw_dims_df, fetch_from_ts)
    devices_df = build_devices_df_op(factlecturas_extended_df)

    hourly_status_out = write_facthourlydevicestatus_op(devices_df)
    hourly_readings_out = write_factreadingsmeasurement_op(devices_df)
    finalize_run_op(hourly_status_out)
    return {
        "dwh_factHourlyDeviceStatus": hourly_status_out,
        "stg_factReadingsMeasurement": hourly_readings_out,
    }
