from dagster import (
        AssetKey,
        asset,
        MaterializeResult,
        AssetExecutionContext)
from time import perf_counter

from ..resources import (
    SparkResource,
    SqlDimServerResource,
    SqlServerResource,
)
from ..transformations.sensor_expected_values import sensor_expected_values


@asset(
    non_argument_deps={
        AssetKey(["carlsjr", "dbo_ProgramProfileTys"]),
        AssetKey(["carlsjr", "dbo_ProgramProfiles"]),
        AssetKey(["carlsjr", "dbo_ProgramSchedules"]),
        AssetKey(["carlsjr", "dbo_GroupSensors"]),
        AssetKey(["carlsjr", "dbo_GroupHasSchedules"]),
        AssetKey(["carlsjr", "dbo_GroupHasSensors"]),
        AssetKey(["carlsjr", "dbo_sensors"]),
        AssetKey(["carlsjr", "dbo_sensor_tys"]),
        AssetKey(["carlsjr", "dbo_devices"]),
        AssetKey(["carlsjr", "dbo_device_tys"]),
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver",
        "sqlserver",
    },
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_dimsensorexpectedval(
    context: AssetExecutionContext) -> MaterializeResult:
    start_time = perf_counter()
    spark_res: SparkResource = context.resources.spark
    dim_sql: SqlDimServerResource = context.resources.dim_sqlserver
    sqlserver: SqlServerResource = context.resources.sqlserver

    context.log.info("Changes detected, materializing dimSensorExpectedVal...")
    expected_values_df = sensor_expected_values(spark_res, dim_sql)

    context.log.info("Refreshing dwh.dimSensorExpectedVal...")
    cleared = sqlserver.execute_query("DELETE FROM dwh.dimSensorExpectedVal")
    if isinstance(cleared, Exception):
        raise RuntimeError("Failed to clear dwh.dimSensorExpectedVal.") from cleared

    inserted = spark_res.write_dim_sensor_expected_values(expected_values_df)

    return MaterializeResult(
        metadata={
            "row_count": inserted,
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        },
    )
