from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    date_trunc,
    avg,
    lit,
    sum as spark_sum,
    count,
    when as spark_when,
    stddev)

def with_in_range_read_flag(
        base: DataFrame,
        expected_values_df: Optional[DataFrame] = None
) -> DataFrame:
    if "InRangeReadFlag" in base.columns:
        return base
    if expected_values_df is None or len(expected_values_df.columns) <= 1:
        return base.withColumn(
            "InRangeReadFlag",
            F.when(F.col("Value").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )

    def time_seconds(column_name: str) -> F.Column:
        value = F.regexp_extract(
            F.col(column_name).cast("string"),
            r"(\d{1,2}:\d{2}(?::\d{2})?)",
            1,
        )
        value = F.when(F.length(value) == 5, F.concat(value, F.lit(":00"))).otherwise(value)
        return (
            F.regexp_extract(value, r"(\d{1,2}):\d{2}:\d{2}", 1).cast("int") * 3600
            + F.regexp_extract(value, r"\d{1,2}:(\d{2}):\d{2}", 1).cast("int") * 60
            + F.regexp_extract(value, r"\d{1,2}:\d{2}:(\d{2})", 1).cast("int")
        )

    scheduled_days = F.regexp_replace(
        F.coalesce(F.col("scheduledDays").cast("string"), F.lit("1-7")),
        r"\s+",
        "",
    )
    scheduled_days = F.when(
        (scheduled_days == "") | (F.lower(scheduled_days) == "*"),
        F.lit("1-7"),
    ).otherwise(scheduled_days)

    expected_schedules = (
        expected_values_df
        .select(
            F.col("SensorId").cast("string").alias("ExpectedSensorId"),
            F.col("DeviceId").cast("string").alias("ExpectedDeviceId"),
            F.col("min_expected_value").cast("double").alias("ExpectedMinCandidate"),
            F.col("max_expected_value").cast("double").alias("ExpectedMaxCandidate"),
            F.col("StartTime").alias("ExpectedStartTime"),
            F.col("EndTime").alias("ExpectedEndTime"),
            F.explode(F.split(scheduled_days, ",")).alias("ScheduledDayToken"),
        )
        .withColumn("ExpectedStartSecond", time_seconds("ExpectedStartTime"))
        .withColumn("ExpectedEndSecond", time_seconds("ExpectedEndTime"))
        .withColumn("ScheduledDayParts", F.split(F.col("ScheduledDayToken"), "-"))
        .withColumn("ScheduledDayStart", F.col("ScheduledDayParts").getItem(0).cast("int"))
        .withColumn(
            "ScheduledDayEnd",
            F.coalesce(
                F.col("ScheduledDayParts").getItem(1).cast("int"),
                F.col("ScheduledDayStart"),
            ),
        )
        .filter(
            F.col("ExpectedStartSecond").isNotNull()
            & F.col("ExpectedEndSecond").isNotNull()
            & (F.col("ExpectedStartSecond") <= F.col("ExpectedEndSecond"))
            & F.col("ScheduledDayStart").between(1, 7)
            & F.col("ScheduledDayEnd").between(1, 7)
            & (F.col("ScheduledDayStart") <= F.col("ScheduledDayEnd"))
        )
        .withColumn(
            "ExpectedScheduledDay",
            F.explode(F.sequence(F.col("ScheduledDayStart"), F.col("ScheduledDayEnd"))),
        )
        .select(
            "ExpectedSensorId",
            "ExpectedDeviceId",
            "ExpectedMinCandidate",
            "ExpectedMaxCandidate",
            "ExpectedStartSecond",
            "ExpectedEndSecond",
            "ExpectedScheduledDay",
        )
    )
    base_columns = base.columns
    return (
        base
        .withColumn("InRangeRowId", F.monotonically_increasing_id())
        .withColumn("ReadingLocalDay", F.pmod(F.dayofweek(F.col("LocalTimeSpan")) + F.lit(5), F.lit(7)) + F.lit(1))
        .withColumn(
            "ReadingLocalSecond",
            (F.hour(F.col("LocalTimeSpan")) * 3600)
            + (F.minute(F.col("LocalTimeSpan")) * 60)
            + F.second(F.col("LocalTimeSpan")),
        )
        .join(
            expected_schedules,
            (F.col("SensorId").cast("string") == F.col("ExpectedSensorId"))
            & (F.col("DeviceId").cast("string") == F.col("ExpectedDeviceId"))
            & (F.col("ReadingLocalDay") == F.col("ExpectedScheduledDay"))
            & (F.col("ReadingLocalSecond") >= F.col("ExpectedStartSecond"))
            & (F.col("ReadingLocalSecond") <= F.col("ExpectedEndSecond")),
            "left",
        )
        .withColumn(
            "InRangeCandidate",
            F.when(
                F.col("ExpectedSensorId").isNull(),
                F.when(F.col("Value").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            ).otherwise(
                F.when(
                    F.col("Value").isNotNull()
                    & (
                        F.col("ExpectedMinCandidate").isNull()
                        | (F.col("Value").cast("double") >= F.col("ExpectedMinCandidate"))
                    )
                    & (
                        F.col("ExpectedMaxCandidate").isNull()
                        | (F.col("Value").cast("double") <= F.col("ExpectedMaxCandidate"))
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ),
        )
        .groupBy("InRangeRowId")
        .agg(
            *[F.first(F.col(column), False).alias(column) for column in base_columns],
            F.max("InRangeCandidate").cast("integer").alias("InRangeReadFlag"),
        )
    )


def stats_calculator(
        base: DataFrame,
        sensor_type: Optional[str] = "Corriente",
        min_expected_value: Optional[float] = None,
        max_expected_value: Optional[float] = None
) -> Optional[DataFrame]:

    base = with_in_range_read_flag(base)
    base = (
        base
        .filter((col("sensor_type") == sensor_type))
        .withColumn("HourStart", date_trunc("hour", col("LocalTimeSpan")))
    )

    agg_exprs = [
        avg("Value").alias("Val_dummy"),
        count("*").alias("total_readings"),
        spark_sum(F.coalesce(col("InRangeReadFlag"), lit(0))).cast("integer").alias("InRangeReads"),
        stddev("Value").alias("Val_std"),
    ]

    anomaly_cond = None
    if min_expected_value is not None:
        anomaly_cond = (col("Value") < lit(min_expected_value))
    if max_expected_value is not None:
        max_cond = (col("Value") > lit(max_expected_value))
        if anomaly_cond is not None:
            anomaly_cond = anomaly_cond | max_cond
        else:
            anomaly_cond = max_cond

    if anomaly_cond is not None:
        agg_exprs.append(
            spark_sum(spark_when(anomaly_cond, 1).otherwise(0)).alias("out_of_range_count")
        )

    stats_df = (
        base
        .groupBy(
            "DeviceId",
            "HourStart",
            "SensorId",
            "LocationId",
            "SensorTyId",
            "sensor_type",
            "device_type",
            "device_name"
        ).agg(*agg_exprs)
        .withColumnRenamed("total_readings", "ReadingsCount")
    )

    if anomaly_cond is not None:
        stats_df = stats_df.withColumn(
            "Anomalies",
            spark_when(col("out_of_range_count") > 0, lit(1)).otherwise(lit(0))
        ).drop("out_of_range_count")
    else:
        stats_df = stats_df.withColumn("Anomalies", lit(0).cast("integer"))

    stats_current_df = (
        stats_df
        .withColumnRenamed("Val_dummy", "MeasurementValue")
        .withColumnRenamed("LocalTimeSpan", "_LocalTimeSpan")
        .withColumnRenamed("HourStart", "LocalTimeSpan")
        .withColumnRenamed("Val_std", "MeasurementStdDev")
    )

    if len(stats_current_df.head(1)) == 0:
        return None

    return stats_current_df
