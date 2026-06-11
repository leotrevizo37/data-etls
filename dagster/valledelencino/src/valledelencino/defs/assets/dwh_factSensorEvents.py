from datetime import datetime
from typing import Any, Dict, Optional

from dagster import In, Out, op, graph_asset, MaterializeResult, RetryPolicy, RetryRequested
from pyspark.sql import DataFrame
from pyspark import StorageLevel
from time import perf_counter

from ..resources import SqlServerResource, SparkResource
from ..transformations import datalake_readings_extended, sensor_events

SENSOR_EVENTS_MAX_RETRIES = 3
SENSOR_EVENTS_RETRY_DELAY_SECONDS = 60
SENSOR_EVENTS_RETRY_POLICY = RetryPolicy(
    max_retries=SENSOR_EVENTS_MAX_RETRIES,
    delay=SENSOR_EVENTS_RETRY_DELAY_SECONDS,
)

INGESTION_NAME = "FactSensorEvents"


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def start_run_and_get_range_opff(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    if last_loaded_at is None:
        last_loaded_at = datetime(2026, 1, 16)

    from_utc = _truncate_to_seconds(last_loaded_at)
    now_utc = _truncate_to_seconds(datetime.utcnow())

    sql.mark_run_started(INGESTION_NAME)

    context.log.info(
        INGESTION_NAME + " run window from " + str(from_utc) + " to " + str(now_utc)
    )

    return {
        "from_utc": from_utc,
        "to_utc": now_utc,
        "original_last_loaded_at": last_loaded_at,
        "start_time": start_time,
    }


@op(
    required_resource_keys={"sqlserver", "spark"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def transform_and_load_sensor_events_op(context, window: Dict[str, Any]) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark

    from_utc: datetime = window["from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]

    sql.delete_range(
        schema_name="dwh",
        table_name="factSensorEvents",
        time_column="LocalTimeSpan",
        from_utc=from_utc,
        to_utc=to_utc,
    )

    succeeded = False
    error_message: Optional[str] = None
    max_time_span: Optional[datetime] = None
    row_count = 0
    retry_number = getattr(context, "retry_number", 0)

    readings_df: Optional[DataFrame] = None
    try:
        readings_df = spark_res.read_lecturas_range(from_utc, to_utc)
        readings_df = readings_df.persist(StorageLevel.MEMORY_AND_DISK)

        if len(readings_df.head(1)) == 0:
            context.log.info(
                "No rows found in Data Lake for window "
                + str(from_utc) + " to " + str(to_utc)
            )
            succeeded = True
        else:
            dimensions_df = spark_res.read_dim_prod_dimensions()

            extended_df = datalake_readings_extended(
                spark_res=spark_res,
                raw_dimensions=dimensions_df,
                fetch_readings_from=from_utc,
                fetch_readings_to=to_utc,
            )

            if extended_df is None:
                context.log.info("No extended readings found after joining dimensions.")
                succeeded = True
            else:
                to_utc_str = to_utc.strftime("%Y-%m-%d %H:%M:%S")
                events_df = sensor_events(extended_df, to_utc_str)

                if events_df is None:
                    context.log.info(
                        "No door/cava state-change events found in window."
                    )
                    succeeded = True
                else:
                    row_count = spark_res.write_fact_sensor_events(events_df)
                    max_time_span = spark_res.get_max_time_span(events_df, "LocalTimeSpan")
                    context.log.info(
                        str(row_count)
                        + " sensor event rows written to dwh.factSensorEvents. "
                        + "Max LocalTimeSpan " + str(max_time_span)
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
        if readings_df is not None:
            readings_df.unpersist()

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "max_time_span": max_time_span,
        "succeeded": succeeded,
        "error_message": error_message,
        "row_count": row_count,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=SENSOR_EVENTS_RETRY_POLICY,
)
def finalize_run_opff(context, result: Dict[str, Any]):
    sql: SqlServerResource = context.resources.sqlserver

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_time_span: Optional[datetime] = result["max_time_span"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
        context.log.warning(
            INGESTION_NAME + " run failed. Keeping watermark at " + str(new_watermark)
        )
    else:
        if max_time_span is None:
            new_watermark = to_utc
            context.log.info(
                INGESTION_NAME + " run succeeded with no events. "
                "Advancing watermark to " + str(new_watermark)
            )
        else:
            new_watermark = max_time_span
            context.log.info(
                INGESTION_NAME + " run succeeded. Advancing watermark to "
                + str(new_watermark)
            )

    sql.mark_run_completed(
        ingestion_name=INGESTION_NAME,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )

    start_time = result.get("start_time")
    elapsed = round(perf_counter() - start_time, 3) if start_time else 0.0

    return MaterializeResult(
        metadata={
            "row_count": result.get("row_count", 0),
            "elapsed_seconds": elapsed,
            "to_utc": str(to_utc),
            "original_last_loaded_at": str(original_last_loaded_at),
        }
    )


@graph_asset(name="dwh_factsensorevents", group_name="valledelencino", key_prefix=["valledelencino"])
def dwh_factsensorevents():
    window = start_run_and_get_range_opff()
    result = transform_and_load_sensor_events_op(window)
    return finalize_run_opff(result)
