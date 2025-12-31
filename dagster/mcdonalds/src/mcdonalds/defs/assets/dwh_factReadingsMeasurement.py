from datetime import timedelta

from dagster import (
    AssetKey,
    asset,
    MaterializeResult,
    AssetExecutionContext,
)
from pyspark.sql.functions import col

from ..resources import SparkResource, SqlServerResource, SqlDimServerResource
from ..transformations import (raw_dimensions,
                               factlecturas_extended,
                               factReadingsMeasurement_device,
                               maquinas_nieve,
                               maquinas_soda, termostatos)
from ..transformations.cuartos_congelacion import cuartos_conservacion
from ..transformations.cuartos_refrigeracion import cuartos_refrigeracion
from ..transformations.luces import luces


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
    },
)
def dwh_factReadingsMeasurement(
    context: AssetExecutionContext) -> MaterializeResult:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver

    max_op_ts = sqlserver.fetch_scalar(
        "SELECT MAX(LocalTimeSpan) FROM dwh.factReadingsMeasurement"
    )
    if max_op_ts is None:
        context.log.info("Loading facLecturas data...")
        fetch_from_ts = max_op_ts
    else:
        fetch_from_ts = max_op_ts - timedelta(hours=4)
        delete_from_ts = max_op_ts - timedelta(hours=2)
        context.log.info("Deleting last period from factReadingsMeasurement.")
        deleted_rows = sqlserver.execute_query(
            "DELETE FROM dwh.factReadingsMeasurement WHERE LocalTimeSpan >= ?",
            delete_from_ts,
        )
        context.log.info("Deleting last period from factHourlyDeviceStatus.")
        deleted_rows_maq = sqlserver.execute_query(
            "DELETE FROM dwh.factHourlyDeviceStatus WHERE LocalTimeSpan >= ?",
            delete_from_ts,
        )
        if deleted_rows is None or deleted_rows == 0:
            context.log.info(
                "No new rows deleted from factReadingsMeasurement "
                "for this period."
            )
        else:
            context.log.info(
                "Deleted " + str(deleted_rows) + " rows from factReadingsMeasurement."
            )
            context.log.info(
                "Deleted " + str(deleted_rows_maq) + " rows from factHourlyDeviceStatus."
            )

    if max_op_ts is None:
        context.log.info("Loading factLecturas data from beginning...")
    else:
        context.log.info("Loading factLecturas data from date:" + str(max_op_ts) + "...")

    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    factlecturas_extended_df = factlecturas_extended(
        spark_res, sqlserver, raw_dims_df, fetch_from_ts
    )

    context.log.info("Materializing maquinas de nieve dataframe...")
    maquinas_nieve_df = maquinas_nieve(factlecturas_extended_df)

    maquinas_nieve_df = (
        maquinas_nieve_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(maquinas_nieve_df.count()) + " rows from maquinas nieve.")

    context.log.info("Materializing maquinas de soda dataframe...")
    maquinas_soda_df = maquinas_soda(factlecturas_extended_df)

    maquinas_soda_df = (
        maquinas_soda_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if maquinas_soda_df is None:
        context.log.info("No rows in maquinas soda; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(maquinas_soda_df.count()) + " rows from maquinas soda.")

    context.log.info("Materializing cuartos refrigeracion dataframe...")
    cuartos_refrigeracion_df = cuartos_refrigeracion(factlecturas_extended_df)

    cuartos_refrigeracion_df = (
        cuartos_refrigeracion_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if cuartos_refrigeracion_df is None:
        context.log.info("No rows for cuartos refrigeracion; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(cuartos_refrigeracion_df.count()) + " rows from cuartos refrigeracion.")

    context.log.info("Materializing cuartos refrigeracion dataframe...")
    cuartos_conservacion_df = cuartos_conservacion(factlecturas_extended_df)

    cuartos_conservacion_df = (
        cuartos_conservacion_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if cuartos_conservacion_df is None:
        context.log.info("No rows for cuartos conservacion; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(cuartos_conservacion_df.count()) + " rows from cuartos conserevacion.")

    context.log.info("Materializing luces dataframe...")
    luces_df = luces(factlecturas_extended_df)

    luces_df = (
        luces_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if luces_df is None:
        context.log.info("No rows for luces; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(luces_df.count()) + " rows fromluces.")


    context.log.info("Materializing termostatos dataframe...")
    termostatos_df = termostatos(factlecturas_extended_df)

    termostatos_df = (
        termostatos_df
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
            col("_sensor_type")
        )
    )

    if termostatos_df is None:
        context.log.info("No rows for termostatos; skipping write.")
        return None
    else:
        context.log.info("Processing " + str(termostatos_df.count()) + " rows from termostatos.")



    factoperations_df = (
        maquinas_nieve_df
        .unionByName(maquinas_soda_df)
        .unionByName(cuartos_refrigeracion_df)
        .unionByName(cuartos_conservacion_df)
        .unionByName(luces_df)
        .unionByName(termostatos_df)
        .select(
            col("OperationId"),
            col("SensorTyId"),
            col("StatusCode"),
            col("MeasurementValue"),
            col("Anomalies"),
        )
    )

    context.log.info(
        "Writing " + str(factoperations_df.count()) + " rows into factReadingsMeasurement."
    )
    try:
        inserted_ = spark_res.write_fact_operations(factoperations_df)
    except Exception as e:
        context.log.error("Couldn´t merge data into factReadingsMeasurement: " + str(e))
        return None
    context.log.info(
        str(inserted_) + " rows correctly written into factReadingsMeasurement."
    )

    context.log.info("Aggregating factReadingsMeasurement dataframe to device-level view...")
    aggregated_df = factReadingsMeasurement_device(
        maquinas_nieve_df
        .unionByName(maquinas_soda_df)
        .unionByName(cuartos_refrigeracion_df)
        .unionByName(cuartos_conservacion_df)
        .unionByName(luces_df)
        .unionByName(termostatos_df)
    )

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

    return MaterializeResult(
        metadata={"row_count": inserted_},
    )