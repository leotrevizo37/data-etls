from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Dict, Tuple
from zoneinfo import ZoneInfo

import json
import os

from dagster import (
    AssetIn,
    AssetKey,
    AssetOut,
    In,
    Nothing,
    Out,
    RetryPolicy,
    RetryRequested,
    graph_multi_asset,
    op,
)
from pyspark.sql import DataFrame

from ..resources import SparkResource, SqlServerResource
from ..transformations.datalake_readings_extended import datalake_readings_extended
from ..transformations.device_operational_intelligence import (
    FEATURE_SET_VERSION,
    MODEL_SCOPE,
    SCORING_VERSION,
    build_device_operational_raw_hourly_features,
    build_device_operational_daily,
    build_device_operational_hourly,
    feature_schema_json,
)
from ..transformations.device_operational_model import load_device_operational_model_bytes


USER_CODE = "carlsjr"
INGESTION_NAME = "DeviceOperationalInsight"
DOI_TIMEZONE = ZoneInfo("America/Ciudad_Juarez")
DOI_MAX_RETRIES = 3
DOI_RETRY_DELAY_SECONDS = 60
DOI_RETRY_POLICY = RetryPolicy(max_retries=DOI_MAX_RETRIES, delay=DOI_RETRY_DELAY_SECONDS)


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def _current_closed_local_hour() -> datetime:
    return _truncate_to_seconds(
        datetime.now(DOI_TIMEZONE)
        .replace(minute=0, second=0, microsecond=0, tzinfo=None)
    )


def _local_to_utc_naive(dt: datetime) -> datetime:
    return _truncate_to_seconds(
        dt.replace(tzinfo=DOI_TIMEZONE)
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
    )


def _empty_sensor_events_df(spark_res: SparkResource) -> DataFrame:
    return spark_res.get_spark().createDataFrame(
        [],
        "DeviceId string, LocationId string, LocalTimeSpan timestamp, EventMinutes double, IsEventCompleted boolean",
    )


def _state_counts(df: DataFrame) -> Dict[str, int]:
    return {
        str(row.OperationalState): int(row["count"])
        for row in df.groupBy("OperationalState").count().collect()
    }


def _min_max_local_dates(df: DataFrame):
    row = df.agg({"LocalDate": "min"}).collect()[0]
    min_date = row["min(LocalDate)"]
    row = df.agg({"LocalDate": "max"}).collect()[0]
    max_date = row["max(LocalDate)"]
    return min_date, max_date


@op(
    ins={
        "sensor_events": In(Nothing),
    },
    out=Out(Nothing),
    retry_policy=DOI_RETRY_POLICY,
)
def device_operational_upstream_gate(context):
    context.log.info("DeviceOperationalInsight upstream dependencies resolved.")


@op(
    required_resource_keys={"sqlserver"},
    ins={"deps": In(Nothing)},
    out=Out(Dict[str, Any]),
    retry_policy=DOI_RETRY_POLICY,
)
def start_device_operational_run_and_get_range_op(context) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    started_at = datetime.utcnow().replace(microsecond=0)
    initial_lookback_days = int(os.getenv("SENSE_DOI_INITIAL_LOOKBACK_DAYS", "30"))
    feature_window_days = int(os.getenv("SENSE_DOI_FEATURE_WINDOW_DAYS", "30"))
    model_confidence_threshold = float(os.getenv("SENSE_DOI_MODEL_CONFIDENCE_THRESHOLD", "0.65"))
    original_last_loaded_at = sql.get_watermark(INGESTION_NAME)
    to_local = _current_closed_local_hour()
    if original_last_loaded_at is None:
        from_local = to_local - timedelta(days=initial_lookback_days)
    else:
        from_local = _truncate_to_seconds(original_last_loaded_at)
    feature_from_local = from_local - timedelta(days=feature_window_days)

    sql.mark_run_started(INGESTION_NAME)
    sql.register_device_operational_feature_set(
        user_code=USER_CODE,
        feature_set_version=FEATURE_SET_VERSION,
        feature_schema_json=json.dumps(feature_schema_json(), sort_keys=True),
    )
    context.log.info(
        "DeviceOperationalInsight local run window from %s to %s. Feature window from %s.",
        from_local,
        to_local,
        feature_from_local,
    )
    return {
        "start_time": perf_counter(),
        "started_at": started_at,
        "original_last_loaded_at": original_last_loaded_at,
        "from_local": from_local,
        "to_local": to_local,
        "feature_from_local": feature_from_local,
        "read_to_utc": _local_to_utc_naive(to_local),
        "has_window": from_local < to_local,
        "model_confidence_threshold": model_confidence_threshold,
    }


@op(
    required_resource_keys={"spark", "sqlserver"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=DOI_RETRY_POLICY,
)
def load_device_operational_model_op(context, window: Dict[str, Any]) -> Dict[str, Any]:
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    active_model = sql.get_active_device_operational_model(USER_CODE, MODEL_SCOPE)
    if active_model is None:
        return {"model_version": None, "model_artifact": None}
    if active_model.get("feature_set_version") != FEATURE_SET_VERSION:
        context.log.warning(
            "Active DeviceOperationalInsight model %s uses feature set %s; current feature set is %s. "
            "Skipping model inference for this run.",
            active_model["model_version"],
            active_model.get("feature_set_version"),
            FEATURE_SET_VERSION,
        )
        return {"model_version": None, "model_artifact": None}

    model_bytes = spark_res.read_device_operational_model_artifact(
        active_model["artifact_path"]
    )
    return {
        "model_version": active_model["model_version"],
        "model_artifact": load_device_operational_model_bytes(model_bytes),
    }


@op(
    required_resource_keys={"spark"},
    ins={"window": In(Dict[str, Any])},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def load_device_operational_dimensions_op(context, window: Dict[str, Any]) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    return spark_res.read_dim_prod_dimensions().filter("Active = true")


@op(
    required_resource_keys={"spark"},
    ins={"window": In(Dict[str, Any])},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def load_device_operational_expected_values_op(context, window: Dict[str, Any]) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    return spark_res.read_dim_sensor_expected_values()


@op(
    required_resource_keys={"spark"},
    ins={
        "dimensions_df": In(DataFrame),
        "window": In(Dict[str, Any]),
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def load_device_operational_raw_readings_op(
    context,
    dimensions_df: DataFrame,
    window: Dict[str, Any],
) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    if not window["has_window"]:
        context.log.info("DeviceOperationalInsight has no open local window.")
        return spark_res.create_emptiness_dataframe()

    readings_extended_df = datalake_readings_extended(
        spark_res=spark_res,
        raw_dimensions=dimensions_df,
        fetch_readings_from=window["feature_from_local"],
        fetch_readings_to=window["read_to_utc"],
    )
    if readings_extended_df is None:
        context.log.info("DeviceOperationalInsight ADLS raw readings are empty.")
        return spark_res.create_emptiness_dataframe()
    return readings_extended_df


@op(
    ins={
        "raw_readings_df": In(DataFrame),
        "dimensions_df": In(DataFrame),
        "expected_values_df": In(DataFrame),
        "window": In(Dict[str, Any]),
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def build_device_operational_raw_hourly_features_op(
    context,
    raw_readings_df: DataFrame,
    dimensions_df: DataFrame,
    expected_values_df: DataFrame,
    window: Dict[str, Any],
) -> DataFrame:
    return build_device_operational_raw_hourly_features(
        raw_readings_df=raw_readings_df,
        dimensions_df=dimensions_df,
        expected_values_df=expected_values_df,
        feature_from_local=window["feature_from_local"],
        to_local=window["to_local"],
    )


@op(
    required_resource_keys={"spark"},
    ins={"window": In(Dict[str, Any])},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def load_device_operational_sensor_events_op(context, window: Dict[str, Any]) -> DataFrame:
    spark_res: SparkResource = context.resources.spark
    if not window["has_window"]:
        return _empty_sensor_events_df(spark_res)
    return spark_res.read_fact_sensor_events_range(
        window["from_local"],
        window["to_local"],
    )


@op(
    ins={
        "raw_hourly_features_df": In(DataFrame),
        "sensor_events_df": In(DataFrame),
        "model_info": In(Dict[str, Any]),
        "window": In(Dict[str, Any]),
    },
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def build_device_operational_hourly_op(
    context,
    raw_hourly_features_df: DataFrame,
    sensor_events_df: DataFrame,
    model_info: Dict[str, Any],
    window: Dict[str, Any],
) -> DataFrame:
    return build_device_operational_hourly(
        raw_hourly_features_df=raw_hourly_features_df,
        sensor_events_df=sensor_events_df,
        user_code=USER_CODE,
        from_local=window["from_local"],
        to_local=window["to_local"],
        model_version=model_info.get("model_version"),
        model_artifact=model_info.get("model_artifact"),
        model_confidence_threshold=window["model_confidence_threshold"],
    )


@op(
    ins={"hourly_df": In(DataFrame)},
    out=Out(DataFrame, io_manager_key="spark_df_io_manager"),
    retry_policy=DOI_RETRY_POLICY,
)
def build_device_operational_daily_op(context, hourly_df: DataFrame) -> DataFrame:
    return build_device_operational_daily(hourly_df)


@op(
    required_resource_keys={"spark", "sqlserver"},
    ins={
        "hourly_df": In(DataFrame),
        "daily_df": In(DataFrame),
        "model_info": In(Dict[str, Any]),
        "window": In(Dict[str, Any]),
    },
    out={
        "hourly": Out(Dict[str, Any]),
        "daily": Out(Dict[str, Any]),
        "scoring": Out(Dict[str, Any]),
        "review": Out(Dict[str, Any]),
    },
    retry_policy=DOI_RETRY_POLICY,
)
def write_device_operational_outputs_op(
    context,
    hourly_df: DataFrame,
    daily_df: DataFrame,
    model_info: Dict[str, Any],
    window: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    start_time = window["start_time"]
    started_at = window["started_at"]
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)
    from_local = window["from_local"]
    to_local = window["to_local"]
    feature_from_local = window["feature_from_local"]
    original_last_loaded_at = window["original_last_loaded_at"]
    model_version = model_info.get("model_version")
    succeeded = False
    error_message = None
    hourly_rows = 0
    review_rows = 0
    merged_hourly_rows = 0
    merged_daily_rows = 0
    counts: Dict[str, int] = {}
    max_time_span = None

    try:
        if retry_number > 0:
            sql.clear_device_operational_insight_hourly_stage(run_id)
            sql.clear_device_operational_insight_daily_stage(run_id)

        hourly_rows = hourly_df.count()
        if hourly_rows > 0:
            daily_rows = daily_df.count()
            if daily_rows > 0:
                counts = _state_counts(daily_df)
            sql.clear_device_operational_insight_hourly_stage(run_id)
            sql.clear_device_operational_insight_daily_stage(run_id)
            spark_res.write_device_operational_insight_hourly_stage(hourly_df, run_id)
            spark_res.write_device_operational_insight_daily_stage(daily_df, run_id)
            merged_hourly_rows = sql.merge_device_operational_insight_hourly(run_id)
            merged_daily_rows = sql.merge_device_operational_insight_daily(run_id)
            if daily_rows > 0:
                min_date, max_date = _min_max_local_dates(daily_df)
                review_rows = sql.enqueue_device_operational_reviews(
                    USER_CODE,
                    min_date,
                    max_date,
                )
            max_time_span = spark_res.get_max_time_span(hourly_df, "LocalTimeSpan")
        else:
            context.log.info(
                "DeviceOperationalInsight run succeeded with no ADLS raw readings. "
                "No synthetic insight rows will be materialized."
            )
        succeeded = True
    except Exception as exc:
        error_message = str(exc)
        try:
            sql.clear_device_operational_insight_hourly_stage(run_id)
            sql.clear_device_operational_insight_daily_stage(run_id)
        except Exception:
            pass
        if retry_number < DOI_MAX_RETRIES:
            raise RetryRequested(
                max_retries=DOI_MAX_RETRIES,
                seconds_to_wait=DOI_RETRY_DELAY_SECONDS,
            ) from exc
        raise
    finally:
        try:
            sql.clear_device_operational_insight_hourly_stage(run_id)
            sql.clear_device_operational_insight_daily_stage(run_id)
        except Exception:
            pass

    completed_at = datetime.utcnow().replace(microsecond=0)
    if succeeded:
        new_watermark = max_time_span or original_last_loaded_at
    else:
        new_watermark = original_last_loaded_at
    sql.mark_run_completed(
        ingestion_name=INGESTION_NAME,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )
    metrics = {
        "duration_seconds": round(perf_counter() - start_time, 4),
        "merged_hourly_rows": merged_hourly_rows,
        "merged_daily_rows": merged_daily_rows,
        "window_start": str(from_local),
        "window_end": str(to_local),
        "feature_window_start": str(feature_from_local),
    }
    sql.record_device_operational_scoring_run(
        run_id=run_id,
        user_code=USER_CODE,
        window_start=from_local,
        window_end=to_local,
        feature_set_version=FEATURE_SET_VERSION,
        scoring_version=SCORING_VERSION,
        model_version=model_version,
        processed_rows=hourly_rows,
        hourly_rows=merged_hourly_rows,
        daily_rows=merged_daily_rows,
        normal_count=counts.get("NORMAL", 0),
        watch_count=counts.get("WATCH", 0),
        degraded_count=counts.get("DEGRADED", 0)
        + counts.get("OUT_OF_RANGE", 0)
        + counts.get("SENSOR_FAULT_SUSPECTED", 0)
        + counts.get("EXPECTED_VALUE_DRIFT", 0),
        critical_count=counts.get("CRITICAL", 0),
        data_unreliable_count=counts.get("DATA_UNRELIABLE", 0),
        review_queue_rows=review_rows,
        metrics=metrics,
        started_at=started_at,
        completed_at=completed_at,
        succeeded=succeeded,
        error_message=error_message,
    )
    output_metadata = {
        "hourly_rows": merged_hourly_rows,
        "daily_rows": merged_daily_rows,
        "review_queue_rows": review_rows,
        "model_version": model_version,
        "window_start": str(from_local),
        "window_end": str(to_local),
    }
    return (
        output_metadata,
        output_metadata,
        output_metadata,
        output_metadata,
    )


@graph_multi_asset(
    outs={
        "dwh_factDeviceOperationalInsightHourly": AssetOut(
            key=AssetKey(["carlsjr", "dwh_factDeviceOperationalInsightHourly"])
        ),
        "dwh_factDeviceOperationalInsightDaily": AssetOut(
            key=AssetKey(["carlsjr", "dwh_factDeviceOperationalInsightDaily"])
        ),
        "ctl_SenseDeviceOperationalScoringRuns": AssetOut(
            key=AssetKey(["carlsjr", "ctl_SenseDeviceOperationalScoringRuns"])
        ),
        "ctl_SenseDeviceInsightReviewQueue": AssetOut(
            key=AssetKey(["carlsjr", "ctl_SenseDeviceInsightReviewQueue"])
        ),
    },
    ins={
        "dwh_factsensorevents": AssetIn(
            key=AssetKey(["carlsjr", "dwh_factsensorevents"]),
            dagster_type=Nothing,
        ),
    },
    group_name="carlsjr",
)
def device_operational_intelligence_assets(
    dwh_factsensorevents,
):
    deps = device_operational_upstream_gate(
        sensor_events=dwh_factsensorevents,
    )
    window = start_device_operational_run_and_get_range_op(deps)
    dimensions_df = load_device_operational_dimensions_op(window)
    expected_values_df = load_device_operational_expected_values_op(window)
    model_info = load_device_operational_model_op(window)
    raw_readings_df = load_device_operational_raw_readings_op(dimensions_df, window)
    raw_hourly_features_df = build_device_operational_raw_hourly_features_op(
        raw_readings_df,
        dimensions_df,
        expected_values_df,
        window,
    )
    sensor_events_df = load_device_operational_sensor_events_op(window)
    hourly_df = build_device_operational_hourly_op(
        raw_hourly_features_df,
        sensor_events_df,
        model_info,
        window,
    )
    daily_df = build_device_operational_daily_op(hourly_df)
    result = write_device_operational_outputs_op(
        hourly_df,
        daily_df,
        model_info,
        window,
    )
    return {
        "dwh_factDeviceOperationalInsightHourly": result.hourly,
        "dwh_factDeviceOperationalInsightDaily": result.daily,
        "ctl_SenseDeviceOperationalScoringRuns": result.scoring,
        "ctl_SenseDeviceInsightReviewQueue": result.review,
    }
