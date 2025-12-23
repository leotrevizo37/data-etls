from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    max as spark_max,
    when,
    lit,
)

def factoperaciones_device(
        factoperations_df: DataFrame) -> DataFrame:

    grouped = (
        factoperations_df
        .groupBy("DeviceId", "LocationId", "LocalTimeSpan")
        .agg(
            spark_sum("Anomalias").alias("Anomalias"),

            spark_max(
                when(
                    (col("_sensor_type") == "Corriente") & (col("Estatus") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Corriente") & (col("Estatus") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("estatus_corriente"),

            spark_max(
                when(
                    (col("_sensor_type") == "Presión") & (col("Estatus") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Presión") & (col("Estatus") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("estatus_presion"),

            spark_max(
                when(
                    (col("_sensor_type") == "Temperatura") & (col("Estatus") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Temperatura") & (col("Estatus") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("estatus_temperatura"),

            spark_max(
                when(
                    (col("_sensor_type") == "Encendido") & (col("Estatus") == 1),
                    lit(1),
                )
                .when(
                    (col("_sensor_type") == "Encendido") & (col("Estatus") == 3),
                    lit(0),
                )
                .otherwise(lit(None))
            ).alias("estatus_encendido"),
        )
    )

    result = (
        grouped
        .withColumn(
            "Estatus",
            when(
                (col("estatus_corriente") == 1)
                & col("estatus_presion").isNull()
                & col("estatus_temperatura").isNull()
                & col("estatus_encendido").isNull(),
                lit(1),
            )
            .when(
                (col("estatus_corriente") == 1)
                & (col("estatus_presion") == 1)
                & col("estatus_temperatura").isNull()
                & col("estatus_encendido").isNull(),
                lit(1),
            )
            .when(
                (col("estatus_corriente") == 1)
                & (col("estatus_presion") == 0)
                & col("estatus_temperatura").isNull()
                & col("estatus_encendido").isNull(),
                lit(2),
            )
            .when(
                (col("estatus_corriente") == 1)
                & (col("estatus_presion").isNull())
                & (col("estatus_temperatura") == 0)
                & col("estatus_encendido").isNull(),
                lit(4),
            )
            .when(
                (col("estatus_corriente") == 1)
                & (col("estatus_presion").isNull())
                & (col("estatus_temperatura") == 1)
                & col("estatus_encendido").isNull(),
                lit(1),
            )
            .when(
                (col("estatus_corriente").isNull())
                & (col("estatus_presion").isNull())
                & (col("estatus_temperatura") == 1)
                & col("estatus_encendido").isNull(),
                lit(1),
            )
            .when(
                (col("estatus_corriente").isNull())
                & (col("estatus_presion").isNull())
                & (col("estatus_temperatura") == 0)
                & col("estatus_encendido").isNull(),
                lit(4),
            )
            .otherwise(lit(3))
        )
        .select(
            col("DeviceId"),
            col("LocalTimeSpan"),
            col("Estatus"),
            col("Anomalias"),
        )
    )

    return result
