from datetime import datetime
import os
from time import perf_counter
from typing import Any, Dict, Optional

from dagster import In, MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import MantizMySqlResource, SparkResource, SqlServerResource
from ..transformations.mantiz_tickets import build_mantiz_ticket_lifecycle_df


MANTIZ_LIFECYCLE_MAX_RETRIES = 3
MANTIZ_LIFECYCLE_RETRY_DELAY_SECONDS = 60
MANTIZ_LIFECYCLE_RETRY_POLICY = RetryPolicy(
    max_retries=MANTIZ_LIFECYCLE_MAX_RETRIES,
    delay=MANTIZ_LIFECYCLE_RETRY_DELAY_SECONDS,
)
LIFECYCLE_INGESTION_NAME = "factMantizTicketLifecycle"


def _mysql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _mysql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _read_mantiz_lifecycle_mysql_table(
    spark_res: SparkResource,
    mysql: MantizMySqlResource,
    table_name: str,
) -> DataFrame:
    spark = spark_res.get_spark()
    return (
        spark.read
        .format("jdbc")
        .option("dbtable", table_name)
        .options(**mysql.get_basic_options())
        .load()
    )


def _read_mantiz_lifecycle_mysql_query(
    spark_res: SparkResource,
    mysql: MantizMySqlResource,
    query: str,
    alias: str,
) -> DataFrame:
    spark = spark_res.get_spark()
    return (
        spark.read
        .format("jdbc")
        .option("dbtable", "(" + query + ") AS " + alias)
        .options(**mysql.get_basic_options())
        .load()
    )


def _changed_lifecycle_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    from_str = _mysql_datetime(from_utc)
    to_str = _mysql_datetime(to_utc)
    org = _mysql_literal(organization_id)
    return (
        "SELECT DISTINCT t.* "
        "FROM Ticket t "
        "LEFT JOIN Branch b ON b.id = t.branchId "
        "LEFT JOIN Client c ON c.id = b.clientId "
        "WHERE c.organizationId = " + org + " "
        "AND COALESCE(t.updatedAt, t.createdAt) >= '" + from_str + "' "
        "AND COALESCE(t.updatedAt, t.createdAt) < '" + to_str + "'"
    )


def _validate_mantiz_lifecycle_config(mysql: MantizMySqlResource) -> None:
    if not mysql.organization_id:
        raise ValueError("MANTIZ_ORGANIZATION_ID is required")


def _get_mantiz_lifecycle_start_date() -> datetime:
    start_date = os.getenv("MANTIZ_START_DATE", "2026-01-01")
    return datetime.strptime(start_date, "%Y-%m-%d")


def _start_mantiz_lifecycle_run(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(LIFECYCLE_INGESTION_NAME)
    if last_loaded_at is None:
        last_loaded_at = _get_mantiz_lifecycle_start_date()

    from_utc = last_loaded_at.replace(microsecond=0)
    to_utc = datetime.utcnow().replace(microsecond=0)
    sql.mark_run_started(LIFECYCLE_INGESTION_NAME)

    context.log.info(
        LIFECYCLE_INGESTION_NAME
        + " run window from "
        + str(from_utc)
        + " to "
        + str(to_utc)
    )

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": last_loaded_at,
        "start_time": start_time,
    }


def _finalize_mantiz_lifecycle_run(context, result: Dict[str, Any]):
    sql: SqlServerResource = context.resources.sqlserver

    to_utc: datetime = result["to_utc"]
    original_last_loaded_at: datetime = result["original_last_loaded_at"]
    max_source_changed_at: Optional[datetime] = result["max_source_changed_at"]
    succeeded: bool = result["succeeded"]
    error_message: Optional[str] = result["error_message"]

    if not succeeded:
        new_watermark = original_last_loaded_at
    elif max_source_changed_at is None:
        new_watermark = to_utc
    else:
        new_watermark = max(max_source_changed_at, original_last_loaded_at)

    sql.mark_run_completed(
        ingestion_name=LIFECYCLE_INGESTION_NAME,
        new_last_loaded_at=new_watermark,
        succeeded=succeeded,
        error_message=error_message,
    )

    start_time = result.get("start_time")
    elapsed = round(perf_counter() - start_time, 3) if start_time else 0.0

    return MaterializeResult(
        metadata={
            "row_count": result.get("row_count", 0),
            "merged_rows": result.get("merged_rows", 0),
            "elapsed_seconds": elapsed,
        }
    )


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_LIFECYCLE_RETRY_POLICY,
)
def start_mantiz_lifecycle_run_and_get_range_op(context) -> Dict[str, Any]:
    return _start_mantiz_lifecycle_run(context)


@op(
    required_resource_keys={"sqlserver", "spark", "mantiz_mysql"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=MANTIZ_LIFECYCLE_RETRY_POLICY,
)
def transform_and_load_mantiz_lifecycle_op(
    context,
    window: Dict[str, Any],
) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    mysql: MantizMySqlResource = context.resources.mantiz_mysql
    _validate_mantiz_lifecycle_config(mysql)

    from_utc: datetime = window["from_utc"]
    to_utc: datetime = window["to_utc"]
    original_last_loaded_at: datetime = window["original_last_loaded_at"]
    run_id = context.run_id
    retry_number = getattr(context, "retry_number", 0)

    succeeded = False
    error_message: Optional[str] = None
    max_source_changed_at: Optional[datetime] = None
    row_count = 0
    merged_rows = 0
    result_df: Optional[DataFrame] = None

    try:
        if retry_number > 0:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)

        tickets_df = _read_mantiz_lifecycle_mysql_query(
            spark_res,
            mysql,
            _changed_lifecycle_tickets_query(from_utc, to_utc, mysql.organization_id),
            "changed_mantiz_tickets",
        )

        if len(tickets_df.head(1)) == 0:
            context.log.info("No Mantiz lifecycle tickets found in window.")
            succeeded = True
        else:
            result_df = build_mantiz_ticket_lifecycle_df(
                tickets_df=tickets_df,
                branches_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "Branch"),
                clients_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "Client"),
                equipment_instances_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "EquipmentInstance"),
                equipment_catalog_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "EquipmentCatalog"),
                failure_types_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "FailureType"),
                suppliers_df=_read_mantiz_lifecycle_mysql_table(spark_res, mysql, "Supplier"),
                organization_id=mysql.organization_id,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            row_count = result_df.count()
            if row_count > 0:
                max_source_changed_at = spark_res.get_max_time_span(
                    result_df,
                    "SourceLastChangedAt",
                )
                sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
                spark_res.write_fact_mantiz_ticket_lifecycle_stage(result_df, run_id)
                merged_rows = sql.merge_fact_mantiz_ticket_lifecycle(run_id)

            context.log.info(
                str(row_count)
                + " Mantiz lifecycle rows prepared; "
                + str(merged_rows)
                + " merged into dwh.factMantizTicketLifecycle."
            )
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading factMantizTicketLifecycle window: " + error_message)
        try:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
        except Exception:
            pass
        if retry_number < MANTIZ_LIFECYCLE_MAX_RETRIES:
            raise RetryRequested(
                max_retries=MANTIZ_LIFECYCLE_MAX_RETRIES,
                seconds_to_wait=MANTIZ_LIFECYCLE_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if result_df is not None:
            result_df.unpersist()
        try:
            sql.clear_fact_mantiz_ticket_lifecycle_stage(run_id)
        except Exception:
            pass

    return {
        "from_utc": from_utc,
        "to_utc": to_utc,
        "original_last_loaded_at": original_last_loaded_at,
        "max_source_changed_at": max_source_changed_at,
        "succeeded": succeeded,
        "error_message": error_message,
        "row_count": row_count,
        "merged_rows": merged_rows,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=MANTIZ_LIFECYCLE_RETRY_POLICY,
)
def finalize_mantiz_lifecycle_run_op(context, result: Dict[str, Any]):
    return _finalize_mantiz_lifecycle_run(context, result)


@graph_asset(
    name="dwh_factmantizticketlifecycle",
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_factmantizticketlifecycle():
    window = start_mantiz_lifecycle_run_and_get_range_op()
    result = transform_and_load_mantiz_lifecycle_op(window)
    return finalize_mantiz_lifecycle_run_op(result)
