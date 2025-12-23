from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit
from dagster import (
    AssetExecutionContext,
)
from .stats_calculator import stats_calculator

def luces(
        factlecturas_extended_df: DataFrame, context: AssetExecutionContext) -> Optional[DataFrame]:
    base = (
        factlecturas_extended_df
        .filter((col("_device_type") == "Luminaria"))
    )
    context.log.info("2 - luminaria devices:" + str(base.count()))

    sates_stats_df = (
        stats_calculator(
            base,
            "Encendido",
            1)
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

    context.log.info("3 - luminaaria devices:" + str(sates_stats_df.count()) + "...")

    luces_df = (
        sates_stats_df
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

    if luces_df.rdd.isEmpty():
        return None

    return luces_df