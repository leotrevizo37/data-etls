from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, lead, lit, max, row_number, sum, when, unix_timestamp
from pyspark.sql.window import Window


OPEN_VALUE = 1
CLOSE_VALUE = 0
DOOR_DEVICE_TYPE_PATTERN = "(?iu)^cuarto\\s+(fr(?:i|\\u00ed)o|congelaci(?:o|\\u00f3)n|residuos)$"
DOOR_DEVICE_NAME_PATTERN = "(?iu)^cuarto\\s+(fr(?:i|\\u00ed)o\\s+(2|3|4)|de\\s+congelaci(?:o|\\u00f3)n|de\\s+residuos)$"
DOOR_SENSOR_PATTERN = "(?iu)^estado\\s+de\\s+puerta$"
COMMUNICATION_THRESHOLD_MINUTES = 60


def resolve_sensor_events_from(
    control_from: datetime,
    open_event_from: Optional[datetime],
) -> datetime:
    if open_event_from is None:
        return control_from
    return min(control_from, open_event_from)


def sensor_events(
    factlecturas_extended_df: DataFrame,
    to_utc_str: str,
) -> DataFrame:
    base = factlecturas_extended_df.filter(
        col("device_type").rlike(DOOR_DEVICE_TYPE_PATTERN)
        & col("device_name").rlike(DOOR_DEVICE_NAME_PATTERN)
        & col("sensor_type").rlike(DOOR_SENSOR_PATTERN)
        & (col("Value").isin(OPEN_VALUE, CLOSE_VALUE))
    )

    sensor_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")

    with_prev = (
        base
        .withColumn("prev_value", lag("Value").over(sensor_window))
        .withColumn("prev_time", lag("TimeSpan").over(sensor_window))
    )

    with_event_id = (
        with_prev
        .withColumn(
            "is_new_event",
            when(
                col("prev_value").isNull()
                | (col("Value") != col("prev_value"))
                | (
                    unix_timestamp(col("TimeSpan"))
                    - unix_timestamp(col("prev_time"))
                    >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60)
                ),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn(
            "event_id",
            sum("is_new_event").over(
                sensor_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
    )

    event_window = Window.partitionBy("SensorId", "event_id")

    state_changes = (
        with_event_id
        .withColumn("last_event_time", max("TimeSpan").over(event_window))
        .withColumn(
            "event_row_number",
            row_number().over(event_window.orderBy("LocalTimeSpan", "ReadId")),
        )
        .filter(col("event_row_number") == lit(1))
    )

    change_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")

    with_next_event = (
        state_changes
        .withColumn("next_event_time", lead("LocalTimeSpan").over(change_window))
        .withColumn("next_event_utc_time", lead("TimeSpan").over(change_window))
    )

    result = with_next_event.withColumn(
        "EventMinutes",
        when(
            col("next_event_time").isNotNull(),
            when(
                unix_timestamp(col("next_event_utc_time"))
                - unix_timestamp(col("last_event_time"))
                >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
                (
                    unix_timestamp(col("last_event_time"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
                + lit(COMMUNICATION_THRESHOLD_MINUTES),
            ).otherwise(
                (
                    unix_timestamp(col("next_event_time"))
                    - unix_timestamp(col("LocalTimeSpan"))
                )
                / lit(60)
            ),
        ).otherwise(
            when(
                unix_timestamp(lit(to_utc_str))
                - unix_timestamp(col("last_event_time"))
                >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
                (
                    unix_timestamp(col("last_event_time"))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
                + lit(COMMUNICATION_THRESHOLD_MINUTES),
            ).otherwise(
                (
                    unix_timestamp(lit(to_utc_str))
                    - unix_timestamp(col("TimeSpan"))
                )
                / lit(60)
            )
        ),
    ).withColumn(
        "IsEventCompleted",
        when(col("next_event_time").isNotNull(), lit(True))
        .when(
            unix_timestamp(lit(to_utc_str))
            - unix_timestamp(col("last_event_time"))
            >= lit(COMMUNICATION_THRESHOLD_MINUTES * 60),
            lit(True),
        )
        .otherwise(lit(False)),
    )

    return result.select(
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
