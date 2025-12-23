from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit
from dagster import (
    AssetExecutionContext,
)
from .stats_calculator import stats_calculator

def termostatos(
        factlecturas_extended_df: DataFrame) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter(
            (
                    (col("_device_type") == "A/C de Cocina") |
                    (col("_device_name") == "A/C de Comedor") |
                    (col("_device_name") == "Ambiente playland")
            )
        )
    )

    state_stats_df = (
        stats_calculator(
            base,
            "On/Off Termostato",
            1)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "Promedio",
            "_sensor_type",
        )
        .withColumnRenamed("Promedio", "Valor")
        .withColumn("Anomalias", lit(None))
        .withColumn("Estatus", lit(1))
    )

    temperature_stats_df = (
        stats_calculator(
            base,
            "Temperatura",
            66)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "Estatus",
            "LocalTimeSpan",
            "Promedio",
            "_sensor_type",
        )
        .withColumnRenamed("Promedio", "Valor")
        .withColumn("Anomalias", lit(None))
    )

    termostatos_df = (
        state_stats_df.unionByName(temperature_stats_df)
        .select(
            "DeviceId",
            "LocationId",
            "SensorTyId",
            "LocalTimeSpan",
            "Estatus",
            "Valor",
            "Anomalias",
            "_sensor_type",
        )
    )

    if termostatos_df.rdd.isEmpty():
        return None

    return termostatos_df