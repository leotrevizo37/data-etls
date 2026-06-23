from datetime import datetime
import os
from time import perf_counter
from typing import Any, Dict, Optional

from dagster import MaterializeResult, RetryPolicy
from pyspark.sql import DataFrame

from ..resources import MantizMySqlResource, SparkResource, SqlServerResource


MANTIZ_MAX_RETRIES = 3
MANTIZ_RETRY_DELAY_SECONDS = 60
MANTIZ_RETRY_POLICY = RetryPolicy(
    max_retries=MANTIZ_MAX_RETRIES,
    delay=MANTIZ_RETRY_DELAY_SECONDS,
)

LIFECYCLE_INGESTION_NAME = "factMantizTicketLifecycle"
TEXT_INSIGHTS_INGESTION_NAME = "factMantizTicketTextInsights"


def truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def mysql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def mysql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def read_mysql_table(
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


def read_mysql_query(
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


def changed_lifecycle_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    from_str = mysql_datetime(from_utc)
    to_str = mysql_datetime(to_utc)
    org = mysql_literal(organization_id)
    return (
        "SELECT DISTINCT t.* "
        "FROM Ticket t "
        "LEFT JOIN Branch b ON b.id = t.branchId "
        "LEFT JOIN Client c ON c.id = b.clientId "
        "WHERE c.organizationId = " + org + " "
        "AND COALESCE(t.updatedAt, t.createdAt) >= '" + from_str + "' "
        "AND COALESCE(t.updatedAt, t.createdAt) < '" + to_str + "'"
    )


def changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    from_str = mysql_datetime(from_utc)
    to_str = mysql_datetime(to_utc)
    org = mysql_literal(organization_id)
    return (
        "SELECT DISTINCT t.* "
        "FROM Ticket t "
        "LEFT JOIN Branch b ON b.id = t.branchId "
        "LEFT JOIN Client c ON c.id = b.clientId "
        "WHERE c.organizationId = " + org + " "
        "AND ("
        "(COALESCE(t.updatedAt, t.createdAt) >= '" + from_str + "' "
        "AND COALESCE(t.updatedAt, t.createdAt) < '" + to_str + "') "
        "OR EXISTS ("
        "SELECT 1 FROM TicketComment tc "
        "WHERE tc.ticketId = t.id "
        "AND COALESCE(tc.updatedAt, tc.createdAt) >= '" + from_str + "' "
        "AND COALESCE(tc.updatedAt, tc.createdAt) < '" + to_str + "'"
        ")"
        ")"
    )


def branches_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT b.id, b.clientId, b.name "
        "FROM Branch b "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.branchId = b.id"
    )


def clients_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    org = mysql_literal(organization_id)
    return (
        "SELECT DISTINCT c.id, c.organizationId "
        "FROM Client c "
        "INNER JOIN Branch b ON b.clientId = c.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.branchId = b.id "
        "WHERE c.organizationId = " + org
    )


def equipment_instances_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ei.id, ei.catalogId, ei.location, ei.customAssetId, ei.serialNumber "
        "FROM EquipmentInstance ei "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.equipmentId = ei.id"
    )


def equipment_catalog_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ec.id, ec.name "
        "FROM EquipmentCatalog ec "
        "INNER JOIN EquipmentInstance ei ON ei.catalogId = ec.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.equipmentId = ei.id"
    )


def failure_types_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT ft.id, ft.name "
        "FROM FailureType ft "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.failureId = ft.id"
    )


def suppliers_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT s.id, s.name "
        "FROM Supplier s "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.supplierId = s.id"
    )


def ticket_spare_parts_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT DISTINCT tsp.* "
        "FROM TicketSparePart tsp "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.id = tsp.ticketId"
    )


def spare_parts_for_changed_text_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    org = mysql_literal(organization_id)
    return (
        "SELECT DISTINCT sp.* "
        "FROM SparePart sp "
        "INNER JOIN TicketSparePart tsp ON tsp.sparePartId = sp.id "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON changed_tickets.id = tsp.ticketId "
        "WHERE sp.organizationId = " + org
    )


def comments_for_changed_tickets_query(
    from_utc: datetime,
    to_utc: datetime,
    organization_id: str,
) -> str:
    changed_tickets = changed_text_tickets_query(from_utc, to_utc, organization_id)
    return (
        "SELECT tc.* "
        "FROM TicketComment tc "
        "INNER JOIN (" + changed_tickets + ") changed_tickets "
        "ON tc.ticketId = changed_tickets.id"
    )


def validate_mantiz_config(mysql: MantizMySqlResource) -> None:
    if not mysql.organization_id:
        raise ValueError("MANTIZ_ORGANIZATION_ID is required")


def get_mantiz_start_date() -> datetime:
    start_date = os.getenv("MANTIZ_START_DATE", "2026-01-01")
    return datetime.strptime(start_date, "%Y-%m-%d")


def start_run(
    context,
    ingestion_name: str,
) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(ingestion_name)
    if last_loaded_at is None:
        last_loaded_at = get_mantiz_start_date()

    from_utc = truncate_to_seconds(last_loaded_at)
    to_utc = truncate_to_seconds(datetime.utcnow())
    sql.mark_run_started(ingestion_name)

    context.log.info(
        ingestion_name
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


def finalize_run(
    context,
    ingestion_name: str,
    result: Dict[str, Any],
):
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
        ingestion_name=ingestion_name,
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
            "to_utc": str(to_utc),
            "original_last_loaded_at": str(original_last_loaded_at),
            "max_source_changed_at": str(max_source_changed_at),
            "succeeded": succeeded,
        }
    )
