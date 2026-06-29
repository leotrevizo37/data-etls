from datetime import datetime
from typing import Any, Dict, Optional

from dagster import AssetKey, AssetOut, In, Out, RetryRequested, graph_multi_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ..resources import SparkResource, SqlServerResource, TransformationOps
from ..transformations import datalake_readings_extended
from .dwh_factSensorEvents import (
    SENSOR_EVENTS_MAX_RETRIES,
    SENSOR_EVENTS_RETRY_DELAY_SECONDS,
    SENSOR_EVENTS_RETRY_POLICY,
    TEMPERATURE_SENSOR_TYPE,
    finalize_run_opff,
    start_run_and_get_range_opff,
)
from .operations_assets import (
    OPERATIONS_RETRY_POLICY,
    build_devices_df_op,
    finalize_operations_run_op,
    load_raw_dimensions_op,
    start_operations_run_and_get_range_op,
    window_cleanup,
    write_factreadingsmeasurement_op,
)


@op(
    ins={"operations_window": In(Dict[str, Any]), "sensor_events_window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def resolve_sense_readings_window_op(context, operations_window, sensor_events_window):
    read_from = min(
        operations_window["fetch_from"],
        sensor_events_window["from_utc"],
        sensor_events_window["temperature_from_utc"],
    )
    read_to = max(operations_window["read_to_utc"], sensor_events_window["to_utc"])
    context.log.info(
        "Sense shared readings window from %s to %s.",
        read_from.isoformat(),
        read_to.isoformat(),
    )
    return {"fetch_from": read_from, "read_to_utc": read_to}


@op(
    required_resource_keys={"spark"},
    ins={"raw_dims_df": In(DataFrame), "window": In(Dict[str, Any])},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=OPERATIONS_RETRY_POLICY,
)
def load_sense_readings_extended_op(context, raw_dims_df, window):
    spark_res = context.resources.spark
    readings_extended_df = datalake_readings_extended(
        spark_res,
        raw_dims_df,
        window["fetch_from"],
        window["read_to_utc"],
    )
    if readings_extended_df is None:
        context.log.info("No extended readings found in the shared Sense window.")
        return spark_res.create_emptiness_dataframe()
    return readings_extended_df


@op(
    required_resource_keys={"sqlserver", "spark", "transformation_ops"},
    ins={
        "window": In(Dict[str, Any]),
        "readings_extended_df": In(DataFrame),
        "operations_done": In(Optional[Dict[str, Any]]),
    },
    out=Out(Dict[str, Any]),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def transform_and_load_sensor_events_from_readings_op(context, window, readings_extended_df, operations_done):
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    transformation_ops: TransformationOps = context.resources.transformation_ops

    from_utc: datetime = window["from_utc"]
    temperature_from_utc: datetime = window["temperature_from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]

    sql.delete_range(
        schema_name="dwh",
        table_name="factSensorEvents",
        time_column="LocalTimeSpan",
        from_utc=from_utc,
        to_utc=to_utc,
    )
    if temperature_from_utc < from_utc:
        sql.delete_sensor_events_range(
            sensor_type=TEMPERATURE_SENSOR_TYPE,
            from_utc=temperature_from_utc,
            to_utc=from_utc,
        )

    succeeded = False
    error_message: Optional[str] = None
    max_time_span: Optional[datetime] = None
    row_count = 0
    retry_number = getattr(context, "retry_number", 0)

    events_df = None
    checkpointed_readings_df: Optional[DataFrame] = None

    try:
        if len(readings_extended_df.columns) <= 1:
            context.log.info("No extended readings found after joining dimensions.")
            succeeded = True
        else:
            checkpointed_readings_df = readings_extended_df.localCheckpoint(
                eager=True,
                storageLevel=StorageLevel.MEMORY_AND_DISK,
            )
            to_utc_str = to_utc.strftime("%Y-%m-%d %H:%M:%S")
            presence_extended_df = checkpointed_readings_df.filter(
                col("LocalTimeSpan") >= from_utc
            )
            temperature_extended_df = checkpointed_readings_df.filter(
                col("LocalTimeSpan") >= temperature_from_utc
            )
            expected_values_df = spark_res.read_dim_sensor_expected_values()
            transformations = [
                (
                    "sensor_events",
                    (presence_extended_df, to_utc_str),
                ),
                (
                    "temperature_events",
                    (temperature_extended_df, expected_values_df, to_utc_str),
                ),
            ]

            for method, args in transformations:
                context.log.info("Materializing dataframe for method: " + method)
                fn = getattr(transformation_ops, method)
                df = fn(*args)
                if df is None:
                    context.log.info("No dataframe returned for method: " + method)
                    continue
                if events_df is None:
                    events_df = df
                else:
                    events_df = events_df.unionByName(df)

            if events_df is None:
                context.log.info("No rows across sensor event transformations.")
                checkpointed_readings_df.unpersist(blocking=True)
                checkpointed_readings_df = None
                succeeded = True
                row_count = 0
                max_time_span = None
            else:
                events_df = events_df.persist(StorageLevel.MEMORY_AND_DISK)
                row_count = spark_res.write_fact_sensor_events(events_df)
                checkpointed_readings_df.unpersist(blocking=True)
                checkpointed_readings_df = None
                if row_count > 0:
                    max_time_span = spark_res.get_max_time_span(
                        events_df,
                        "LocalTimeSpan",
                    )
                context.log.info(
                    str(row_count)
                    + " sensor event rows written to dwh.factSensorEvents. "
                    + "Max LocalTimeSpan "
                    + str(max_time_span)
                )
                succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading FactSensorEvents window: " + error_message)
        try:
            sql.delete_range(
                schema_name="dwh",
                table_name="factSensorEvents",
                time_column="LocalTimeSpan",
                from_utc=from_utc,
                to_utc=to_utc,
            )
            if temperature_from_utc < from_utc:
                sql.delete_sensor_events_range(
                    sensor_type=TEMPERATURE_SENSOR_TYPE,
                    from_utc=temperature_from_utc,
                    to_utc=from_utc,
                )
        except Exception as cleanup_ex:
            context.log.warning(
                "Could not clean partial factSensorEvents rows after failure: %s",
                cleanup_ex,
            )
        if retry_number < SENSOR_EVENTS_MAX_RETRIES:
            context.log.warning(
                "FactSensorEvents window load failed on attempt %s of %s. Retrying operation.",
                retry_number + 1,
                SENSOR_EVENTS_MAX_RETRIES + 1,
            )
            raise RetryRequested(
                max_retries=SENSOR_EVENTS_MAX_RETRIES,
                seconds_to_wait=SENSOR_EVENTS_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if events_df is not None:
            events_df.unpersist()
        if checkpointed_readings_df is not None:
            checkpointed_readings_df.unpersist()

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "open_event_from": window.get("open_event_from"),
        "temperature_from_utc": temperature_from_utc,
        "temperature_open_event_from": window.get("temperature_open_event_from"),
        "max_time_span": max_time_span,
        "succeeded": succeeded,
        "error_message": error_message,
        "row_count": row_count,
        "start_time": window.get("start_time"),
    }


@graph_multi_asset(
    outs={
        "dwh_factReadingsMeasurement": AssetOut(key=AssetKey(["carlsjr", "dwh_factReadingsMeasurement"])),
        "ctl_IngestionControl": AssetOut(key=AssetKey(["carlsjr", "ctl_IngestionControl"])),
        "dwh_factsensorevents": AssetOut(key=AssetKey(["carlsjr", "dwh_factsensorevents"])),
    },
    group_name="carlsjr",
)
def sense_operational_assets():
    operations_window = start_operations_run_and_get_range_op()
    sensor_events_window = start_run_and_get_range_opff()
    cleaning = window_cleanup(operations_window)
    raw_dims_df = load_raw_dimensions_op(cleaning)
    shared_window = resolve_sense_readings_window_op(operations_window, sensor_events_window)
    readings_extended_df = load_sense_readings_extended_op(raw_dims_df=raw_dims_df, window=shared_window)
    devices_df = build_devices_df_op(
        window=operations_window,
        dims=raw_dims_df,
        readings_extended_df=readings_extended_df,
    )
    hourly_readings_out = write_factreadingsmeasurement_op(devices_df=devices_df, window=operations_window)
    finalize_operations = finalize_operations_run_op(hourly_readings=hourly_readings_out.result)
    sensor_events_result = transform_and_load_sensor_events_from_readings_op(
        window=sensor_events_window,
        readings_extended_df=readings_extended_df,
        operations_done=finalize_operations,
    )
    finalize_events = finalize_run_opff(result=sensor_events_result)
    return {
        "dwh_factReadingsMeasurement": hourly_readings_out.asset,
        "ctl_IngestionControl": finalize_operations,
        "dwh_factsensorevents": finalize_events,
    }
