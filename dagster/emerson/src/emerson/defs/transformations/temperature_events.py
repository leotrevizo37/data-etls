from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lag, lead, lit, when, unix_timestamp
from pyspark.sql.window import Window


TEMPERATURE_SENSOR_TYPE = "Temperatura"
TEMPERATURE_EVENT_MINUTES = 120


def temperature_events(
    factlecturas_extended_df: DataFrame,
    expected_values_df: DataFrame,
    to_utc_str: str,
) -> DataFrame:
    expected_values = expected_values_df.select(
        col("SensorId").alias("ExpectedSensorId"),
        col("DeviceId").alias("ExpectedDeviceId"),
        col("min_expected_value"),
        col("max_expected_value"),
        col("StartTime"),
        col("EndTime"),
        col("scheduledDays"),
    )

    base = (
        factlecturas_extended_df.alias("rd")
        .filter(col("sensor_type") == TEMPERATURE_SENSOR_TYPE)
        .join(
            expected_values.alias("ev"),
            (
                F.upper(col("rd.SensorId").cast("string"))
                == F.upper(col("ev.ExpectedSensorId").cast("string"))
            )
            & (
                F.upper(col("rd.DeviceId").cast("string"))
                == F.upper(col("ev.ExpectedDeviceId").cast("string"))
            ),
            how="inner",
        )
        .select(
            col("rd.ReadId"),
            col("rd.SensorId"),
            col("rd.DeviceId"),
            col("rd.LocationId"),
            col("rd.sublocation_name"),
            col("rd.device_type"),
            col("rd.sensor_type"),
            col("rd.sensor_name"),
            col("rd.TimeSpan"),
            col("rd.LocalTimeSpan"),
            col("rd.Value"),
            col("ev.min_expected_value"),
            col("ev.max_expected_value"),
            col("ev.StartTime"),
            col("ev.EndTime"),
            col("ev.scheduledDays"),
        )
    )

    time_pattern = r"(\d{1,2}:\d{2}(?::\d{2})?)"
    scheduled = (
        base
        .withColumn(
            "StartTimeText",
            F.regexp_extract(col("StartTime").cast("string"), time_pattern, 1),
        )
        .withColumn(
            "EndTimeText",
            F.regexp_extract(col("EndTime").cast("string"), time_pattern, 1),
        )
        .withColumn(
            "StartTimeText",
            when(
                F.length(col("StartTimeText")) == lit(5),
                F.concat(col("StartTimeText"), lit(":00")),
            ).otherwise(col("StartTimeText")),
        )
        .withColumn(
            "EndTimeText",
            when(
                F.length(col("EndTimeText")) == lit(5),
                F.concat(col("EndTimeText"), lit(":00")),
            ).otherwise(col("EndTimeText")),
        )
        .withColumn(
            "StartTimeValue",
            F.date_format(
                F.to_timestamp(
                    F.concat(lit("2000-01-01 "), col("StartTimeText")),
                    "yyyy-MM-dd H:mm:ss",
                ),
                "HH:mm:ss",
            ),
        )
        .withColumn(
            "EndTimeValue",
            F.date_format(
                F.to_timestamp(
                    F.concat(lit("2000-01-01 "), col("EndTimeText")),
                    "yyyy-MM-dd H:mm:ss",
                ),
                "HH:mm:ss",
            ),
        )
        .withColumn("LocalTimeValue", F.date_format(col("LocalTimeSpan"), "HH:mm:ss"))
        .withColumn(
            "scheduledDaysValue",
            F.regexp_replace(
                F.lower(F.trim(col("scheduledDays").cast("string"))),
                r"\s+",
                "",
            ),
        )
        .withColumn(
            "LocalDayValue",
            (
                F.pmod(F.dayofweek(col("LocalTimeSpan")) + lit(5), lit(7))
                + lit(1)
            ).cast("int"),
        )
        .withColumn(
            "RangeStartDay",
            when(
                col("scheduledDaysValue").rlike(r"^\d-\d$"),
                F.split(col("scheduledDaysValue"), "-").getItem(0).cast("int"),
            ),
        )
        .withColumn(
            "RangeEndDay",
            when(
                col("scheduledDaysValue").rlike(r"^\d-\d$"),
                F.split(col("scheduledDaysValue"), "-").getItem(1).cast("int"),
            ),
        )
        .filter(
            col("StartTimeValue").isNotNull()
            & col("EndTimeValue").isNotNull()
            & (
                (col("scheduledDaysValue") == lit("*"))
                | (col("scheduledDaysValue") == lit("1-7"))
                | (
                    col("scheduledDaysValue").rlike(r"^\d-\d$")
                    & (col("LocalDayValue") >= col("RangeStartDay"))
                    & (col("LocalDayValue") <= col("RangeEndDay"))
                )
                | F.array_contains(
                    F.split(col("scheduledDaysValue"), ","),
                    col("LocalDayValue").cast("string"),
                )
            )
            & (
                (
                    (col("StartTimeValue") <= col("EndTimeValue"))
                    & (col("LocalTimeValue") >= col("StartTimeValue"))
                    & (col("LocalTimeValue") <= col("EndTimeValue"))
                )
                | (
                    (col("StartTimeValue") > col("EndTimeValue"))
                    & (
                        (col("LocalTimeValue") >= col("StartTimeValue"))
                        | (col("LocalTimeValue") <= col("EndTimeValue"))
                    )
                )
            )
        )
        .withColumn(
            "is_out_of_range",
            (
                col("min_expected_value").isNotNull()
                & (col("Value") < col("min_expected_value"))
            )
            | (
                col("max_expected_value").isNotNull()
                & (col("Value") > col("max_expected_value"))
            ),
        )
    )

    event_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")
    state_changes = (
        scheduled
        .withColumn("prev_is_out_of_range", lag("is_out_of_range").over(event_window))
        .filter(
            col("prev_is_out_of_range").isNull()
            | (col("is_out_of_range") != col("prev_is_out_of_range"))
        )
        .withColumn("next_event_time", lead("LocalTimeSpan").over(event_window))
    )

    return (
        state_changes
        .filter(col("is_out_of_range"))
        .withColumn(
            "EventMinutes",
            when(
                col("next_event_time").isNotNull(),
                (
                    unix_timestamp(col("next_event_time"))
                    - unix_timestamp(col("LocalTimeSpan"))
                )
                / lit(60),
            ).otherwise(
                (
                    unix_timestamp(lit(to_utc_str))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
            ),
        )
        .withColumn(
            "IsEventCompleted",
            when(col("next_event_time").isNotNull(), lit(True)).otherwise(lit(False)),
        )
        .filter(col("EventMinutes") >= lit(TEMPERATURE_EVENT_MINUTES))
        .select(
            col("ReadId"),
            col("SensorId"),
            col("DeviceId"),
            col("LocationId"),
            col("sublocation_name").alias("LocationName"),
            col("device_type").alias("DeviceType"),
            col("sensor_type").alias("SensorType"),
            col("sensor_name").alias("SensorName"),
            col("LocalTimeSpan"),
            col("Value"),
            col("EventMinutes"),
            col("IsEventCompleted"),
        )
    )
