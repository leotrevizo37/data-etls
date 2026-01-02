from datetime import timedelta
from typing import  Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext,
        op,
        In,
        Out)

from ..resources import (SparkResource,
                         SqlServerResource,
                         SqlDimServerResource,
                         TransformationOps)

from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               factReadingsMeasurement_device)


@asset(
    non_argument_deps={
        AssetKey("dbo_sensors"),
        AssetKey("dbo_sensor_tys"),
        AssetKey("dbo_devices"),
        AssetKey("dbo_device_tys"),
        AssetKey("dbo_sublocations"),
        AssetKey("dbo_sublocation_tys"),
        AssetKey("dwh_factlecturas")
    },
    required_resource_keys={
        "spark",
        "sqlserver",
        "dim_sqlserver",
        "transformation_ops"
    },
)
def dwh_factHourlyDeviceStatus(
    context: AssetExecutionContext) -> MaterializeResult:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver
    transformation_ops: TransformationOps = context.resources.transformation_ops

    max_op_ts = sqlserver.get_watermark("FactHourlyDeviceStatus")
    fetch_from_ts = max_op_ts
    if max_op_ts is None:
        context.log.info("FactHourlyDeviceStatus is empty, "
                         "will be loading facLecturas data from beginning...")
    else:
        fetch_from_ts = max_op_ts - timedelta(hours=6)
        delete_from_ts = max_op_ts - timedelta(hours=4)
        context.log.info("Deleting incomplete last period from ops tables.")
        deleted_rows = sqlserver.delete_range(
            schema_name="dwh",
            table_name="factHourlyDeviceStatus",
            time_column="LocalTimeSpan",
            from_utc=delete_from_ts
        )
        if deleted_rows is None or deleted_rows == 0:
            context.log.info(
                "No new rows deleted from factHourlyDeviceStatus "
                "for this period."
            )
        else:
            context.log.info(
                "Deleted " + str(deleted_rows) + " rows from factHourlyDeviceStatus with cascade."
            )
        context.log.info("Loading factLecturas data from date:" + str(fetch_from_ts) + "...")

    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    factlecturas_extended_df = factlecturas_extended(
        spark_res=spark_res,
        sqlserver=sqlserver,
        raw_dimensions=raw_dims_df,
        fetch_factlecturas_from=str(fetch_from_ts)
    )

    devices_dataframes = data_pipeline(context, transformation_ops, factlecturas_extended_df)
    devices_df = (
        devices_dataframes.get("maquinas_nieve_df")
        .unionByName(devices_dataframes.get("maquinas_soda_df"))
        .unionByName(devices_dataframes.get("cuartos_refrigeracion_df"))
        .unionByName(devices_dataframes.get("cuartos_conservacion_df"))
        .unionByName(devices_dataframes.get("luces_df"))
        .unionByName(devices_dataframes.get("termostatos_df"))
    )

    inserted_facthourlydevicestatus = write_into_facthourlydevicestatus(context, devices_df)
    inserted_factreadingsmeasurement = write_into_factreadingsmeasurement(context, devices_df)

    return MaterializeResult(
        metadata={"row_count": (inserted_factreadingsmeasurement + inserted_facthourlydevicestatus)},
    )

def materialize_dataframes(
        context: AssetExecutionContext,
        transformation_ops: TransformationOps,
        dataframe: DataFrame,
        transformation_method: str):
    if dataframe.rdd.isEmpty():
        context.log.info("No rows for dataframe.")
        return None
    fn = getattr(transformation_ops, transformation_method)
    return fn(dataframe).select(
            col("OperationId"),
            col("DeviceId"),
            col("LocationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("LocalTimeSpan"),
            col("_sensor_type")
        )

def data_pipeline(context: AssetExecutionContext,
                  transformation_ops: TransformationOps,
                  factlecturas_extended_df: DataFrame):
    context.log.info("Materializing maquinas de nieve dataframe...")
    maquinas_nieve_df = materialize_dataframes(context,
                                               transformation_ops,
                                               factlecturas_extended_df,
                                               "maquinas_nieve")
    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(maquinas_nieve_df.count()) + " rows from maquinas nieve.")

    context.log.info("Materializing maquinas de soda dataframe...")
    maquinas_soda_df = materialize_dataframes(context,
                                              transformation_ops,
                                              factlecturas_extended_df,
                                              "maquinas_soda")
    if maquinas_soda_df is None:
        context.log.info("No rows in maquinas soda; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(maquinas_soda_df.count()) + " rows from maquinas soda.")

    context.log.info("Materializing cuartos refrigeracion dataframe...")
    cuartos_refrigeracion_df = materialize_dataframes(context,
                                                      transformation_ops,
                                                      factlecturas_extended_df,
                                                      "cuartos_refrigeracion")
    if cuartos_refrigeracion_df is None:
        context.log.info("No rows for cuartos refrigeracion; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(cuartos_refrigeracion_df.count()) + " rows from cuartos refrigeracion.")

    context.log.info("Materializing cuartos refrigeracion dataframe...")
    cuartos_conservacion_df = materialize_dataframes(context,
                                                     transformation_ops,
                                                     factlecturas_extended_df,
                                                     "cuartos_congelacion")
    if cuartos_conservacion_df is None:
        context.log.info("No rows for cuartos conservacion; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(cuartos_conservacion_df.count()) + " rows from cuartos conserevacion.")

    context.log.info("Materializing luces dataframe...")
    luces_df = materialize_dataframes(context,
                                      transformation_ops,
                                      factlecturas_extended_df,
                                      "luces")
    if luces_df is None:
        context.log.info("No rows for luces; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(luces_df.count()) + " rows fromluces.")

    context.log.info("Materializing termostatos dataframe...")
    termostatos_df = materialize_dataframes(context,
                                            transformation_ops,
                                            factlecturas_extended_df,
                                            "termostatos")
    if termostatos_df is None:
        context.log.info("No rows for termostatos; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(termostatos_df.count()) + " rows from termostatos.")

    resp = {
        "maquinas_nieve_df": maquinas_nieve_df,
        "maquinas_soda_df": maquinas_soda_df,
        "cuartos_conservacion_df": cuartos_conservacion_df,
        "cuartos_refrigeracion_df": cuartos_refrigeracion_df,
        "luces_df": luces_df,
        "termostatos_df": termostatos_df,
    }
    return resp

def write_into_factreadingsmeasurement(
        context: AssetExecutionContext,
        dataframe: DataFrame):
    spark_res: SparkResource = context.resources.spark
    if dataframe.rdd.isEmpty():
        context.log.info("No rows for dataframe.")
        return None

    context.log.info(
        "Writing " + str(dataframe.count()) + " rows into factReadingsMeasurement."
    )

    try:
        inserted_ = spark_res.write_fact_operations(
            (
            dataframe
            .select(
                col("OperationId"),
                col("SensorTyId"),
                col("StatusCode"),
                col("MeasurementValue"),
                col("Anomalies"),
            )
        )
    )
    except Exception as e:
        context.log.error("Couldn´t merge data into factReadingsMeasurement: " + str(e))
        return None

    context.log.info(
        str(inserted_) + " rows correctly written into factReadingsMeasurement."
    )

    return inserted_


def write_into_facthourlydevicestatus(
        context: AssetExecutionContext,
        dataframe: DataFrame):
    spark_res: SparkResource = context.resources.spark
    if dataframe.rdd.isEmpty():
        context.log.info("No rows in dataframe.")
        return None

    context.log.info("Aggregating da into factHourlyDeviceStatus device-level dataframe...")
    aggregated_df = factReadingsMeasurement_device(dataframe)

    if aggregated_df.rdd.isEmpty():
        context.log.info("Aggregated device-level dataframe is empty; skipping write.")
        return MaterializeResult(metadata={"row_count": 0})

    row_count = aggregated_df.count()
    context.log.info(
        "Writing " + str(row_count) + " rows into dwh.factHourlyDeviceStatuss."
    )

    try:
        inserted_ = spark_res.write_fact_operations_device(aggregated_df)
    except Exception as e:
        context.log.error("Couldn´t write data into dwh.factHourlyDeviceStatus: " + str(e))
        return MaterializeResult(
            metadata={
                "row_count": 0,
                "error": str(e),
            }
        )

    context.log.info(
        str(inserted_) + " rows correctly written into dwh.factHourlyDeviceStatus."
    )

    return inserted_