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
        AssetKey(["valledelencino", "dbo_ProgramProfileTys"]),
        AssetKey(["valledelencino", "dbo_ProgramProfiles"]),
        AssetKey(["valledelencino", "dbo_ProgramSchedules"]),
        AssetKey(["valledelencino", "dbo_GroupSensors"]),
        AssetKey(["valledelencino", "dbo_GroupHasSchedules"]),
        AssetKey(["valledelencino", "dbo_GroupHasSensors"]),
        AssetKey(["valledelencino", "dbo_sensors"]),
        AssetKey(["valledelencino", "dbo_sensor_tys"]),
        AssetKey(["valledelencino", "dbo_devices"]),
        AssetKey(["valledelencino", "dbo_device_tys"]),
    },
    required_resource_keys={
        "spark",
        "dim_sqlserver",
        "sqlserver",
    },
    group_name="valledelencino",
    key_prefix=["valledelencino"],
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
