from datetime import timedelta

from pyspark.sql.functions import col

from dagster import (asset,
                     AssetKey,
                     MaterializeResult,
                     AssetExecutionContext)

from ..resources import SparkResource, SqlServerResource, SqlDimServerResource
from ..transformations import raw_dimensions, factlecturas_extended, maquinas_nieve, maquinas_soda

@asset(
    non_argument_deps={
        AssetKey("dbo_sensors"),
        AssetKey("dbo_sensor_tys"),
        AssetKey("dbo_devices"),
        AssetKey("dbo_device_tys"),
        AssetKey("dbo_sublocations"),
        AssetKey("dbo_sublocation_tys"),
    },
    required_resource_keys={
        "spark",
        "sqlserver",
        "dim_sqlserver"
    },
)
def factoperations(context: AssetExecutionContext) -> MaterializeResult:
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver

    max_op_ts = sqlserver.fetch_scalar(
        "SELECT MAX(LocalTimeSpan) FROM dwh.factOperaciones"
    )
    if max_op_ts is None:
        context.log.info("Loading all factLecturas data")
        fetch_from_ts = max_op_ts
    else:
        fetch_from_ts = max_op_ts - timedelta(hours=4)
        delete_from_ts = max_op_ts - timedelta(hours=2)
        context.log.info("Deleting last period from FactOperaciones.")
        deleted_rows = sqlserver.execute_query(
            "DELETE FROM dwh.factOperaciones WHERE LocalTimeSpan >= ?",
            delete_from_ts,
        )
        if deleted_rows is None or deleted_rows == 0:
            context.log.info("No new rows deleted from factOperaciones "
                             "for this period.")
        else:
            context.log.info("Deleted " + str(deleted_rows) + " rows from FactOperaciones.")

    if max_op_ts is None:
        context.log.info("Loading factLecturas data from beginning")
    else:
        context.log.info("Loading factLecturas data from date:" + str(max_op_ts))
    raw_dims_df = raw_dimensions(spark_res, dim_sql)
    factlecturas_extended_df = factlecturas_extended(spark_res, sqlserver, raw_dims_df, fetch_from_ts)

    if factlecturas_extended_df is None:
        context.log.info("No new rows detected in factLecturas; skipping write.")
        return None

    context.log.info("Materializing maquinas de nieve ETL")
    maquinas_nieve_df = maquinas_nieve(factlecturas_extended_df)

    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
        return None

    maquinas_nieve_df = (
        maquinas_nieve_df
        .select(
            col("DeviceId"),
            col("LocationId"),
            col("SensorTyId"),
            col("LocalTimeSpan"),
            col("Estatus"),
            col("Valor"),
            col("Anomalias"),
        )
    )

    context.log.info("Writing " + str(maquinas_nieve_df.count()) + " rows into factOperaciones.")
    try:
        inserted_nieve = spark_res.write_fact_operations_incremental(maquinas_nieve_df)
    except Exception as e:
        context.log.error("Couldn´t merge data into factOperaciones: " + str(e))
        return None
    context.log.info(str(inserted_nieve) + " rows correctly written into factOperaciones.")

    context.log.info("Materializing maquinas de soda ETL")
    maquinas_soda_df = maquinas_soda(factlecturas_extended_df)

    if maquinas_nieve_df is None:
        context.log.info("No rows in maquinas nieve; skipping write.")
        return None

    maquinas_soda_df = (
        maquinas_soda_df
        .select(
            col("DeviceId"),
            col("LocationId"),
            col("SensorTyId"),
            col("LocalTimeSpan"),
            col("Estatus"),
            col("Valor"),
            col("Anomalias"),
        )
    )

    context.log.info("Writing " + str(maquinas_soda_df.count()) + " rows into factOperaciones.")
    try:
        inserted_soda = spark_res.write_fact_operations_incremental(maquinas_soda_df)
    except Exception as e:
        context.log.error("Couldn´t merge data into factOperaciones: " + str(e))
        return None
    context.log.info(str(inserted_soda) + " rows correctly written into factOperaciones.")

    return MaterializeResult(
        metadata={"row_count": (inserted_nieve + inserted_soda)},
    )
