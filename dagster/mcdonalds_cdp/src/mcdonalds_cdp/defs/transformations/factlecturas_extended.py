from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp

from ..resources import SqlServerResource, SparkResource
from ...operations_window import OPERATIONS_TIMEZONE, OPERATIONS_TIMEZONE_NAME


LOCAL_TIME_SPAN_FORMAT = "yyyy-MM-dd HH:mm:ss"
PYTHON_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _local_to_utc(local_naive: datetime) -> datetime:
    local_aware = local_naive.replace(tzinfo=OPERATIONS_TIMEZONE)
    return local_aware.astimezone(timezone.utc).replace(tzinfo=None)


def _build_factlecturas_query(fetch_from: Optional[datetime], fetch_to: Optional[datetime]) -> str:
    base_query = (
        "SELECT ReadId, "
        "TimeSpan, "
        "CONVERT(VARCHAR(19), LocalTimeSpan, 120) AS LocalTimeSpanText, "
        "Value, "
        "SensorId "
        "FROM dwh.FactLecturas"
    )
    
    where_clauses = []
    if fetch_from:
        where_clauses.append(f"LocalTimeSpan >= '{fetch_from.strftime(PYTHON_DATETIME_FORMAT)}'")
    if fetch_to:
        where_clauses.append(f"LocalTimeSpan < '{fetch_to.strftime(PYTHON_DATETIME_FORMAT)}'")
        
    if not where_clauses:
        return "(" + base_query + ") AS src"
        
    return "(" + base_query + " WHERE " + " AND ".join(where_clauses) + ") AS src"


def _get_factlecturas_cache_bounds(
        sqlserver: SqlServerResource) -> Tuple[Optional[datetime], Optional[datetime]]:
    bounds_query = "FROM dwh.FactLecturas WHERE LocalTimeSpan IS NOT NULL"
    min_local_ts = sqlserver.fetch_scalar("SELECT MIN(LocalTimeSpan) " + bounds_query)
    max_local_ts = sqlserver.fetch_scalar("SELECT MAX(LocalTimeSpan) " + bounds_query)
    return min_local_ts, max_local_ts


def _cache_covers_requested_window(
        cache_min_local_ts: Optional[datetime],
        cache_max_local_ts: Optional[datetime],
        fetch_from_local: datetime,
        fetch_to_local: datetime) -> bool:
    required_cache_max = fetch_to_local - timedelta(seconds=1)
    return (
        cache_min_local_ts is not None
        and cache_max_local_ts is not None
        and cache_min_local_ts <= fetch_from_local
        and cache_max_local_ts >= required_cache_max
    )


def _restore_local_timespan(df: DataFrame) -> DataFrame:
    # LocalTimeSpan already comes in local wall-clock time from SQL Server.
    # Reading it as text avoids the JDBC timestamp timezone shift.
    return (
        df.withColumn(
            "LocalTimeSpan",
            to_timestamp(col("LocalTimeSpanText"), LOCAL_TIME_SPAN_FORMAT),
        )
        .drop("LocalTimeSpanText")
    )


def factlecturas_extended(
        spark_res: SparkResource,
        sqlserver: SqlServerResource,
        raw_dimensions: DataFrame,
        window: Dict[str, Any]) -> Optional[DataFrame]:
    spark = spark_res.get_spark()

    fetch_from_local: datetime = window["fetch_from"]
    fetch_to_local: datetime = window["delete_to_exclusive"]

    use_datalake = True
    dwh_factlecturas_df: Optional[DataFrame] = None
    
    try:
        min_local_ts, max_local_ts = _get_factlecturas_cache_bounds(sqlserver)
        use_datalake = not _cache_covers_requested_window(
            min_local_ts,
            max_local_ts,
            fetch_from_local,
            fetch_to_local,
        )
    except Exception as e:
        print(f"Warning: Failed to verify dwh.FactLecturas cache bounds. Falling back to Data Lake. Error: {e}")

    if not use_datalake:
        dwh_factlecturas_df = _restore_local_timespan(
            spark.read.format("jdbc")
            .option("dbtable", _build_factlecturas_query(fetch_from_local, fetch_to_local))
            .options(**sqlserver.get_basic_options())
            .load()
        )
        if dwh_factlecturas_df.rdd.isEmpty():
            use_datalake = True

    if use_datalake:
        from_utc = _local_to_utc(fetch_from_local)
        to_utc = _local_to_utc(fetch_to_local)
        
        dl_df = spark_res.read_lecturas_range(from_utc, to_utc)
        if dl_df.rdd.isEmpty():
            dwh_factlecturas_df = dl_df
        else:
            dwh_factlecturas_df = dl_df.withColumn(
                "LocalTimeSpan",
                from_utc_timestamp(col("TimeSpan"), OPERATIONS_TIMEZONE_NAME)
            )

    if dwh_factlecturas_df.rdd.isEmpty():
        return None

    dwh_factlecturas_extended_df = (
        dwh_factlecturas_df
        .alias("fl")
        .join(
            raw_dimensions.alias("dim"), on="SensorId", how="left"
        )
        .select(
            col("fl.ReadId"),
            col("fl.TimeSpan"),
            col("fl.LocalTimeSpan"),
            col("fl.Value"),
            col("dim.SensorId"),
            col("dim.SensorTyId"),
            col("dim.DeviceId"),
            col("dim.sensor_name"),
            col("dim.sensor_type"),
            col("dim.DeviceTyId"),
            col("dim.SubLocationId"),
            col("dim.device_name"),
            col("dim.device_type"),
            col("dim.SubLocationTyId"),
            col("dim.sublocation_name"),
            col("dim.LocationId"),
            col("dim.sublocation_type")
        )
    )

    if dwh_factlecturas_extended_df.rdd.isEmpty():
        return None

    return dwh_factlecturas_extended_df
