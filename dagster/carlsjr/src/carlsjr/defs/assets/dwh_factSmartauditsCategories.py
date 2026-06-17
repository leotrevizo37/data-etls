from datetime import datetime
import os
from time import perf_counter
from typing import Any, Dict, Optional

from dagster import In, MaterializeResult, Out, RetryPolicy, RetryRequested, graph_asset, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..resources import SmartAuditsMySqlResource, SparkResource, SqlServerResource
from ..transformations.smartaudits_categories import (
    build_smartaudits_categories_df,
    build_smartaudits_comment_review_queue_df,
    empty_classifier_predictions_df,
    unresolved_comments_df,
)
from ..transformations.smartaudits_classifier import (
    CATEGORY_UNCLASSIFIED,
    load_model_bytes,
    predict_result_category,
)


SMARTAUDITS_MAX_RETRIES = 3
SMARTAUDITS_RETRY_DELAY_SECONDS = 60
SMARTAUDITS_RETRY_POLICY = RetryPolicy(
    max_retries=SMARTAUDITS_MAX_RETRIES,
    delay=SMARTAUDITS_RETRY_DELAY_SECONDS,
)

INGESTION_NAME = "factSmartauditsCategories"


def _truncate_to_seconds(dt: datetime) -> datetime:
    return dt.replace(microsecond=0)


def _mysql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _read_mysql_table(
    spark_res: SparkResource,
    mysql: SmartAuditsMySqlResource,
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


def _read_mysql_query(
    spark_res: SparkResource,
    mysql: SmartAuditsMySqlResource,
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


def _changed_plan_results_query(from_utc: datetime, to_utc: datetime) -> str:
    from_str = _mysql_datetime(from_utc)
    to_str = _mysql_datetime(to_utc)
    return (
        "SELECT DISTINCT pr.* "
        "FROM plan_results pr "
        "INNER JOIN work_plan wp ON pr.workPlanId = wp.id "
        "INNER JOIN tasks t ON wp.taskId = t.id "
        "LEFT JOIN evidence_photos ep ON ep.planResultsId = pr.id "
        "WHERE (t.reviewAI = 1 OR t.reviewAIAutomatically = 1) "
        "AND ("
        "(pr.reviewAIDate >= '" + from_str + "' AND pr.reviewAIDate < '" + to_str + "') "
        "OR (ep.createdAt >= '" + from_str + "' AND ep.createdAt < '" + to_str + "')"
        ")"
    )


def _changed_evidence_query(from_utc: datetime, to_utc: datetime) -> str:
    ids_query = _changed_plan_results_query(from_utc, to_utc)
    return (
        "SELECT ep.* "
        "FROM evidence_photos ep "
        "INNER JOIN (" + ids_query + ") changed_pr "
        "ON ep.planResultsId = changed_pr.id"
    )


def _load_active_model(
    context,
    sql: SqlServerResource,
    spark_res: SparkResource,
) -> tuple[Optional[Any], Optional[str]]:
    model_info = sql.get_active_smartaudits_classifier_model()
    if not model_info:
        return None, None

    try:
        content = spark_res.read_smartaudits_model_artifact(model_info["artifact_path"])
        return load_model_bytes(content), str(model_info["model_version"])
    except Exception as exc:
        context.log.warning(
            "Could not load active SmartAudits classifier model: %s",
            exc,
        )
        return None, None


def _build_predictions_df(
    context,
    spark_res: SparkResource,
    evidence_df: DataFrame,
    sql: SqlServerResource,
) -> DataFrame:
    spark = spark_res.get_spark()
    model, model_version = _load_active_model(context, sql, spark_res)
    if model is None:
        return empty_classifier_predictions_df(spark)

    threshold = float(os.getenv("SMARTAUDITS_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.75"))
    unresolved_rows = unresolved_comments_df(evidence_df).collect()
    predictions = []
    for row in unresolved_rows:
        prediction = predict_result_category(
            model,
            row.CommentsAI,
            model_version,
            threshold,
        )
        predictions.append(
            (
                row.NormalizedComment,
                prediction.category,
                "ML" if prediction.category != CATEGORY_UNCLASSIFIED else "SIN_CLASIFICAR",
                prediction.model_version,
                prediction.confidence,
            )
        )

    if not predictions:
        return empty_classifier_predictions_df(spark)

    return spark.createDataFrame(
        predictions,
        "NormalizedComment string, PredictedCategory string, "
        "PredictionMethod string, ClassifierModelVersion string, "
        "ClassifierConfidence double",
    )


@op(
    required_resource_keys={"sqlserver"},
    ins={},
    out=Out(Dict[str, Any]),
    retry_policy=SMARTAUDITS_RETRY_POLICY,
)
def start_smartaudits_run_and_get_range_op(context) -> Dict[str, Any]:
    start_time = perf_counter()
    sql: SqlServerResource = context.resources.sqlserver

    last_loaded_at = sql.get_watermark(INGESTION_NAME)
    if last_loaded_at is None:
        start_date = os.getenv("SMARTAUDITS_CATEGORIES_START_DATE", "2026-01-01")
        last_loaded_at = datetime.strptime(start_date, "%Y-%m-%d")

    from_utc = _truncate_to_seconds(last_loaded_at)
    to_utc = _truncate_to_seconds(datetime.utcnow())
    sql.mark_run_started(INGESTION_NAME)

    context.log.info(
        INGESTION_NAME
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


@op(
    required_resource_keys={"sqlserver", "spark", "smartaudits_mysql"},
    ins={"window": In(Dict[str, Any])},
    out=Out(Dict[str, Any]),
    retry_policy=SMARTAUDITS_RETRY_POLICY,
)
def transform_and_load_smartaudits_categories_op(
    context,
    window: Dict[str, Any],
) -> Dict[str, Any]:
    sql: SqlServerResource = context.resources.sqlserver
    spark_res: SparkResource = context.resources.spark
    mysql: SmartAuditsMySqlResource = context.resources.smartaudits_mysql

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
    review_queue_rows = 0
    review_queue_merged_rows = 0
    result_df: Optional[DataFrame] = None

    try:
        if retry_number > 0:
            sql.clear_fact_smartaudits_categories_stage(run_id)
            sql.clear_smartaudits_comment_review_queue_stage(run_id)

        plan_results_df = _read_mysql_query(
            spark_res,
            mysql,
            _changed_plan_results_query(from_utc, to_utc),
            "changed_plan_results",
        )
        evidence_df = _read_mysql_query(
            spark_res,
            mysql,
            _changed_evidence_query(from_utc, to_utc),
            "changed_evidence_photos",
        )

        if len(plan_results_df.head(1)) == 0:
            context.log.info("No SmartAudits plan results found in window.")
            succeeded = True
        else:
            comment_lookup_df = None
            try:
                comment_lookup_df = spark_res.read_smartaudits_ai_comment_lookup()
            except Exception as exc:
                context.log.warning(
                    "Could not read dwh.dimSmartauditsAiCommentLookup: %s",
                    exc,
                )

            predictions_df = _build_predictions_df(context, spark_res, evidence_df, sql)
            try:
                threshold = float(os.getenv("SMARTAUDITS_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.75"))
                queue_df = build_smartaudits_comment_review_queue_df(
                    evidence_photos_df=evidence_df,
                    comment_lookup_df=comment_lookup_df,
                    classifier_predictions_df=predictions_df,
                    confidence_threshold=threshold,
                ).persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    review_queue_rows = queue_df.count()
                    if review_queue_rows > 0:
                        sql.clear_smartaudits_comment_review_queue_stage(run_id)
                        spark_res.write_smartaudits_comment_review_queue_stage(queue_df, run_id)
                        review_queue_merged_rows = sql.merge_smartaudits_comment_review_queue(run_id)
                finally:
                    queue_df.unpersist()
                    try:
                        sql.clear_smartaudits_comment_review_queue_stage(run_id)
                    except Exception:
                        pass
            except Exception as queue_ex:
                context.log.warning(
                    "Could not enqueue SmartAudits comment review candidates: %s",
                    queue_ex,
                )

            result_df = build_smartaudits_categories_df(
                plan_results_df=plan_results_df,
                evidence_photos_df=evidence_df,
                work_plan_df=_read_mysql_table(spark_res, mysql, "work_plan"),
                tasks_df=_read_mysql_table(spark_res, mysql, "tasks"),
                checkpoints_df=_read_mysql_table(spark_res, mysql, "checkpoints"),
                sublocations_df=_read_mysql_table(spark_res, mysql, "sublocations"),
                locations_df=_read_mysql_table(spark_res, mysql, "locations"),
                task_categories_df=_read_mysql_table(spark_res, mysql, "task_categories"),
                categories_df=_read_mysql_table(spark_res, mysql, "categories"),
                comment_lookup_df=comment_lookup_df,
                classifier_predictions_df=predictions_df,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            row_count = result_df.count()
            if row_count > 0:
                max_source_changed_at = spark_res.get_max_time_span(
                    result_df,
                    "SourceLastChangedAt",
                )
                sql.clear_fact_smartaudits_categories_stage(run_id)
                spark_res.write_fact_smartaudits_categories_stage(result_df, run_id)
                merged_rows = sql.merge_fact_smartaudits_categories(run_id)

            context.log.info(
                str(row_count)
                + " SmartAudits category rows prepared; "
                + str(merged_rows)
                + " merged into dwh.factSmartauditsCategories; "
                + str(review_queue_merged_rows)
                + " review queue rows merged."
            )
            succeeded = True
    except Exception as ex:
        error_message = str(ex)
        context.log.error("Error loading factSmartauditsCategories window: " + error_message)
        try:
            sql.clear_fact_smartaudits_categories_stage(run_id)
        except Exception as cleanup_ex:
            context.log.warning(
                "Could not clean factSmartauditsCategories staging rows: %s",
                cleanup_ex,
            )
        try:
            sql.clear_smartaudits_comment_review_queue_stage(run_id)
        except Exception:
            pass
        if retry_number < SMARTAUDITS_MAX_RETRIES:
            raise RetryRequested(
                max_retries=SMARTAUDITS_MAX_RETRIES,
                seconds_to_wait=SMARTAUDITS_RETRY_DELAY_SECONDS,
            ) from ex
        succeeded = False
    finally:
        if result_df is not None:
            result_df.unpersist()
        try:
            sql.clear_fact_smartaudits_categories_stage(run_id)
        except Exception:
            pass
        try:
            sql.clear_smartaudits_comment_review_queue_stage(run_id)
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
        "review_queue_rows": review_queue_rows,
        "review_queue_merged_rows": review_queue_merged_rows,
        "start_time": window.get("start_time"),
    }


@op(
    required_resource_keys={"sqlserver"},
    ins={"result": In(Dict[str, Any])},
    out=Out(),
    retry_policy=SMARTAUDITS_RETRY_POLICY,
)
def finalize_smartaudits_run_op(context, result: Dict[str, Any]):
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
        ingestion_name=INGESTION_NAME,
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
            "review_queue_rows": result.get("review_queue_rows", 0),
            "review_queue_merged_rows": result.get("review_queue_merged_rows", 0),
            "elapsed_seconds": elapsed,
            "to_utc": str(to_utc),
            "original_last_loaded_at": str(original_last_loaded_at),
            "max_source_changed_at": str(max_source_changed_at),
            "succeeded": succeeded,
        }
    )


@graph_asset(
    name="dwh_factsmartauditscategories",
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def dwh_factsmartauditscategories():
    window = start_smartaudits_run_and_get_range_op()
    result = transform_and_load_smartaudits_categories_op(window)
    return finalize_smartaudits_run_op(result)
