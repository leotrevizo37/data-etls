from datetime import datetime, timedelta, timezone
from time import perf_counter

import pyodbc
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dagster import AssetExecutionContext, MaterializeResult, RetryPolicy, asset

from ..resources import ChirpStackApiResource, SparkResource, SqlServerResource
from ..transformations.chirpstack_metrics import (
    build_device_metric_row,
    build_gateway_metric_row,
    device_profile_supports_battery,
    device_references,
    normalize_dev_eui,
)

INGESTION_NAME = "factChirpStackMetrics"
CHIRPSTACK_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)
DEVICE_METRICS_SCHEMA = StructType(
    [
        StructField("DevEui", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("ApplicationId", StringType(), False),
        StructField("DeviceProfileId", StringType(), False),
        StructField("UplinkIntervalSeconds", IntegerType(), True),
        StructField("ExpectedUplinkCount", DecimalType(18, 6), True),
        StructField("ReceivedUplinkCount", LongType(), True),
        StructField("AverageRssi", DecimalType(18, 6), True),
        StructField("AverageSnr", DecimalType(18, 6), True),
        StructField("LinkErrorCount", LongType(), True),
        StructField("BatteryLevel", DecimalType(7, 4), True),
        StructField("ExternalPowerSource", BooleanType(), True),
        StructField("BatteryLevelSource", StringType(), True),
        StructField("BatteryObservedAt", TimestampType(), True),
        StructField("MetricsAvailable", BooleanType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)
GATEWAY_METRICS_SCHEMA = StructType(
    [
        StructField("GatewayId", StringType(), False),
        StructField("TimeSpan", TimestampType(), False),
        StructField("TenantId", StringType(), False),
        StructField("GatewayName", StringType(), False),
        StructField("ReceivedPacketCount", LongType(), True),
        StructField("TransmittedPacketCount", LongType(), True),
        StructField("TxOkCount", LongType(), True),
        StructField("TxErrorCount", LongType(), True),
        StructField("GatewayState", StringType(), False),
        StructField("GatewayLastSeenAt", TimestampType(), True),
        StructField("StatsIntervalSeconds", IntegerType(), True),
        StructField("MetricsAvailable", BooleanType(), False),
        StructField("ExtractedAt", TimestampType(), False),
    ]
)


@asset(
    name="observability_chirpstackmetrics",
    group_name="emerson",
    key_prefix=["emerson"],
    required_resource_keys={"chirpstack_api", "spark", "sqlserver"},
    retry_policy=CHIRPSTACK_RETRY_POLICY,
)
def observability_chirpstackmetrics(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    chirpstack: ChirpStackApiResource = context.resources.chirpstack_api
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    to_utc = extracted_at.replace(minute=0, second=0, microsecond=0)
    from_utc = to_utc - timedelta(hours=1)
    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    sql.mark_run_started(INGESTION_NAME)
    run_id = context.run_id
    device_staged_rows = 0
    gateway_staged_rows = 0
    device_merged_rows = 0
    gateway_merged_rows = 0

    try:
        sql.clear_chirpstack_metrics_stage(run_id)
        dimension_rows = (
            spark_res.read_dim_prod_dimensions()
            .filter(col("Active") == lit(True))
            .filter(col("PhysicalIdentifier").isNotNull())
            .select("PhysicalIdentifier")
            .distinct()
            .collect()
        )
        dev_euis = sorted(
            {
                normalize_dev_eui(row["PhysicalIdentifier"])
                for row in dimension_rows
            }
        )
        if not dev_euis:
            raise RuntimeError("No active ChirpStack DevEUIs were found in dimensions")

        applications = {}
        device_profiles = {}
        device_rows = []
        for dev_eui in dev_euis:
            device_payload = chirpstack.get_device(dev_eui)
            application_id, device_profile_id = device_references(
                device_payload,
                dev_eui,
            )
            if application_id not in applications:
                applications[application_id] = chirpstack.get_application(
                    application_id
                )
            if device_profile_id not in device_profiles:
                device_profiles[device_profile_id] = chirpstack.get_device_profile(
                    device_profile_id
                )
            link_metrics_payload = chirpstack.get_device_link_metrics(
                dev_eui,
                from_utc,
                to_utc,
            )
            device_metrics_payload = None
            if device_profile_supports_battery(
                device_profiles[device_profile_id]
            ):
                device_metrics_payload = chirpstack.get_device_metrics(
                    dev_eui,
                    from_utc,
                    to_utc,
                )
            device_rows.append(
                build_device_metric_row(
                    dev_eui=dev_eui,
                    device_payload=device_payload,
                    application_payload=applications[application_id],
                    device_profile_payload=device_profiles[device_profile_id],
                    link_metrics_payload=link_metrics_payload,
                    device_metrics_payload=device_metrics_payload,
                    time_span=from_utc,
                    extracted_at=extracted_at,
                )
            )

        gateway_rows = []
        gateway_ids = set()
        for tenant_id in sorted({row["TenantId"] for row in device_rows}):
            tenant_gateways = chirpstack.list_gateways(tenant_id)
            if not tenant_gateways:
                raise RuntimeError(
                    "ChirpStack returned no gateways for a monitored device tenant"
                )
            for gateway_list_item in tenant_gateways:
                gateway_id = normalize_dev_eui(gateway_list_item.get("gatewayId"))
                if gateway_id in gateway_ids:
                    raise RuntimeError("ChirpStack returned a duplicate gatewayId")
                gateway_ids.add(gateway_id)
                gateway_rows.append(
                    build_gateway_metric_row(
                        gateway_list_item=gateway_list_item,
                        gateway_payload=chirpstack.get_gateway(gateway_id),
                        gateway_metrics_payload=chirpstack.get_gateway_metrics(
                            gateway_id,
                            from_utc,
                            to_utc,
                        ),
                        expected_tenant_id=tenant_id,
                        time_span=from_utc,
                        extracted_at=extracted_at,
                    )
                )

        spark = spark_res.get_spark()
        device_df = spark.createDataFrame(device_rows, DEVICE_METRICS_SCHEMA)
        gateway_df = spark.createDataFrame(gateway_rows, GATEWAY_METRICS_SCHEMA)
        device_staged_rows = spark_res.write_chirpstack_device_metrics_stage(
            device_df,
            run_id,
        )
        gateway_staged_rows = spark_res.write_chirpstack_gateway_metrics_stage(
            gateway_df,
            run_id,
        )
        device_merged_rows, gateway_merged_rows = sql.merge_chirpstack_metrics(
            run_id
        )
        sql.clear_chirpstack_metrics_stage(run_id)
        sql.mark_run_completed(
            ingestion_name=INGESTION_NAME,
            new_last_loaded_at=extracted_at,
            succeeded=True,
            error_message=None,
        )
    except Exception as ex:
        try:
            sql.clear_chirpstack_metrics_stage(run_id)
        except pyodbc.Error:
            context.log.warning("Could not clear ChirpStack staging after the failed run")
        try:
            sql.mark_run_completed(
                ingestion_name=INGESTION_NAME,
                new_last_loaded_at=last_loaded_at,
                succeeded=False,
                error_message=str(ex),
            )
        except pyodbc.Error:
            context.log.warning("Could not persist the failed ChirpStack run state")
        raise

    return MaterializeResult(
        metadata={
            "device_row_count": device_merged_rows,
            "device_staged_rows": device_staged_rows,
            "gateway_row_count": gateway_merged_rows,
            "gateway_staged_rows": gateway_staged_rows,
            "from_utc": str(from_utc),
            "to_utc": str(to_utc),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
        }
    )
