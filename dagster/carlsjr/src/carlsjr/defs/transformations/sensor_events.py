from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, lead, lit, when, unix_timestamp
from pyspark.sql.window import Window


PRESENCE_DEVICE_PATTERN = r"(?iu)(mostrador|monitoreo)"
PRESENCE_SENSOR_TYPE = "Camara de presencia"
PRESENCE_VALUE = 1
ABSENCE_VALUE = 0


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
        (col("device_type").rlike(PRESENCE_DEVICE_PATTERN))
        & (col("sensor_type") == PRESENCE_SENSOR_TYPE)
        & (col("Value").isin(PRESENCE_VALUE, ABSENCE_VALUE))
    )

    sensor_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")

    with_prev = base.withColumn(
        "prev_value", lag("Value").over(sensor_window)
    )

    state_changes = with_prev.filter(
        col("prev_value").isNull() | (col("Value") != col("prev_value"))
    )

    change_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")

    with_next_event = state_changes.withColumn(
        "next_event_time", lead("LocalTimeSpan").over(change_window)
    )

    result = with_next_event.withColumn(
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
    ).withColumn(
        "IsEventCompleted",
        when(col("next_event_time").isNotNull(), lit(True)).otherwise(lit(False)),
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
