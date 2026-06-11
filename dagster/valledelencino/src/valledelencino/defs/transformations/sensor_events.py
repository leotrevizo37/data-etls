from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, lead, lit, when, unix_timestamp
from pyspark.sql.window import Window


OPEN_VALUE = 1
CLOSE_VALUE = 0


def sensor_events(
    factlecturas_extended_df: DataFrame,
    to_utc_str: str,
) -> Optional[DataFrame]:
    base = factlecturas_extended_df.filter(
        (col("device_type").isin("Puerta", "Cava"))
        & (col("sensor_type") == "Estado")
        & (col("Value").isin(OPEN_VALUE, CLOSE_VALUE))
    )

    if len(base.head(1)) == 0:
        return None

    sensor_window = Window.partitionBy("SensorId").orderBy("LocalTimeSpan", "ReadId")

    with_prev = base.withColumn(
        "prev_value", lag("Value").over(sensor_window)
    )

    state_changes = with_prev.filter(
        col("prev_value").isNull() | (col("Value") != col("prev_value"))
    )

    with_next_close = state_changes.withColumn(
        "next_close_time",
        when(
            col("Value") == lit(OPEN_VALUE),
            lead("LocalTimeSpan").over(sensor_window),
        ),
    )

    result = with_next_close.withColumn(
        "EventMinutes",
        when(
            col("Value") == lit(OPEN_VALUE),
            when(
                col("next_close_time").isNotNull(),
                (
                    unix_timestamp(col("next_close_time"))
                    - unix_timestamp(col("LocalTimeSpan"))
                )
                / lit(60),
            ).otherwise(
                (
                    unix_timestamp(lit(to_utc_str))
                    - unix_timestamp(col("LocalTimeSpan"))
                )
                / lit(60),
            ),
        ).otherwise(lit(0)),
    ).withColumn(
        "IsEventCompleted",
        when(col("Value") == lit(CLOSE_VALUE), lit(True)).when(
            (col("Value") == lit(OPEN_VALUE))
            & col("next_close_time").isNotNull(),
            lit(True),
        ).otherwise(lit(False)),
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
