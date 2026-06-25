from datetime import datetime
import os
from typing import Any, Optional

from dagster import Out, RetryPolicy, job, op
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..resources import SmartAuditsMySqlResource, SparkResource, SqlServerResource
from ..transformations.smartaudits_categories import (
    ai_result_int_column,
    build_smartaudits_categories_df,
    deterministic_category_column,
    empty_classifier_predictions_df,
    normalize_comment_column,
    unresolved_comments_df,
)
from ..transformations.smartaudits_classifier import (
    CATEGORY_COMPLIANCE,
    CATEGORY_UNCLASSIFIED,
    dump_model_bytes,
    predict_result_category,
    train_result_category_model,
)


TRAINING_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)


def _read_mysql_table(
    spark_res: SparkResource,
    mysql: SmartAuditsMySqlResource,
    table_name: str,
):
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


def _plan_results_id_range_query(min_plan_result_id: int, max_plan_result_id: int) -> str:
    return (
        "SELECT pr.* "
        "FROM plan_results pr "
        "INNER JOIN work_plan wp ON pr.workPlanId = wp.id "
        "INNER JOIN tasks t ON wp.taskId = t.id "
        "WHERE pr.id >= "
        + str(min_plan_result_id)
        + " AND pr.id <= "
        + str(max_plan_result_id)
        + " AND (t.reviewAI = 1 AND t.reviewAIAutomatically = 1)"
    )


def _evidence_id_range_query(min_plan_result_id: int, max_plan_result_id: int) -> str:
    return (
        "SELECT ep.* "
        "FROM evidence_photos ep "
        "WHERE ep.planResultsId >= "
        + str(min_plan_result_id)
        + " AND ep.planResultsId <= "
        + str(max_plan_result_id)
    )


def _build_predictions_df(
    spark_res: SparkResource,
    evidence_df: DataFrame,
    model: Any,
    model_version: str,
    threshold: float,
) -> DataFrame:
    spark = spark_res.get_spark()
    predictions = []
    for row in unresolved_comments_df(evidence_df).collect():
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


def _reclassify_unclassified_rows(
    context,
    spark_res: SparkResource,
    sql: SqlServerResource,
    mysql: SmartAuditsMySqlResource,
    model: Any,
    model_version: str,
    min_plan_result_id: Optional[int],
    max_plan_result_id: Optional[int],
) -> int:
    if min_plan_result_id is None or max_plan_result_id is None:
        return 0

    keys_df = spark_res.read_smartaudits_unclassified_fact_keys().persist(
        StorageLevel.MEMORY_AND_DISK,
    )
    result_df: Optional[DataFrame] = None
    run_id = context.run_id + "-reclassify"

    try:
        if len(keys_df.head(1)) == 0:
            return 0

        plan_results_df = (
            _read_mysql_query(
                spark_res,
                mysql,
                _plan_results_id_range_query(min_plan_result_id, max_plan_result_id),
                "reclassify_plan_results",
            )
            .join(
                keys_df,
                F.col("id").cast("long") == F.col("PlanResultId").cast("long"),
                "inner",
            )
            .drop("PlanResultId")
        )
        if len(plan_results_df.head(1)) == 0:
            return 0

        evidence_df = (
            _read_mysql_query(
                spark_res,
                mysql,
                _evidence_id_range_query(min_plan_result_id, max_plan_result_id),
                "reclassify_evidence_photos",
            )
            .join(
                keys_df,
                F.col("planResultsId").cast("long") == F.col("PlanResultId").cast("long"),
                "inner",
            )
            .drop("PlanResultId")
        )

        comment_lookup_df = None
        try:
            comment_lookup_df = spark_res.read_smartaudits_ai_comment_lookup()
        except Exception as exc:
            context.log.warning(
                "Could not read ctl.SmartauditsAiCommentLookup: %s",
                exc,
            )

        threshold = float(os.getenv("SMARTAUDITS_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.75"))
        predictions_df = _build_predictions_df(
            spark_res,
            evidence_df,
            model,
            model_version,
            threshold,
        )
        result_df = build_smartaudits_categories_df(
            plan_results_df=plan_results_df,
            evidence_photos_df=evidence_df,
            work_plan_df=_read_mysql_table(spark_res, mysql, "work_plan"),
            tasks_df=_read_mysql_table(spark_res, mysql, "tasks"),
            checkpoints_df=_read_mysql_table(spark_res, mysql, "checkpoints"),
            sublocations_df=_read_mysql_table(spark_res, mysql, "sublocations"),
            locations_df=_read_mysql_table(spark_res, mysql, "locations"),
            priority_types_df=_read_mysql_table(spark_res, mysql, "priority_types"),
            task_categories_df=_read_mysql_table(spark_res, mysql, "task_categories"),
            categories_df=_read_mysql_table(spark_res, mysql, "categories"),
            comment_lookup_df=comment_lookup_df,
            classifier_predictions_df=predictions_df,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        row_count = result_df.count()
        if row_count == 0:
            return 0

        sql.clear_fact_smartaudits_categories_stage(run_id)
        spark_res.write_fact_smartaudits_categories_stage(result_df, run_id)
        return sql.merge_fact_smartaudits_categories(run_id)
    finally:
        if result_df is not None:
            result_df.unpersist()
        keys_df.unpersist()
        try:
            sql.clear_fact_smartaudits_categories_stage(run_id)
        except Exception:
            pass


@op(
    required_resource_keys={"spark", "sqlserver", "smartaudits_mysql"},
    out=Out(dict),
    retry_policy=TRAINING_RETRY_POLICY,
)
def train_smartaudits_classifier_op(context) -> dict:
    spark_res: SparkResource = context.resources.spark
    sql: SqlServerResource = context.resources.sqlserver
    mysql: SmartAuditsMySqlResource = context.resources.smartaudits_mysql

    min_training_rows = int(os.getenv("SMARTAUDITS_CLASSIFIER_MIN_TRAINING_ROWS", "50"))
    min_accuracy = float(os.getenv("SMARTAUDITS_CLASSIFIER_MIN_ACCURACY", "0.85"))
    min_macro_f1 = float(os.getenv("SMARTAUDITS_CLASSIFIER_MIN_MACRO_F1", "0.80"))
    retrain_threshold = int(
        os.getenv("SMARTAUDITS_CLASSIFIER_RETRAIN_UNCLASSIFIED_THRESHOLD", "100")
    )
    model_base_path = os.getenv(
        "SMARTAUDITS_MODEL_BASE_PATH",
        "s3a://data-etls/models/carlsjr/smartaudits/result_category",
    ).rstrip("/")
    active_model = sql.get_active_smartaudits_classifier_model()
    unclassified_stats = sql.get_smartaudits_unclassified_stats()
    unclassified_count = int(unclassified_stats["pending_count"] or 0)

    if active_model and unclassified_count < retrain_threshold:
        metadata = {
            "human_training_rows": 0,
            "rule_training_rows": 0,
            "training_rows": 0,
            "promoted": False,
            "reason": "unclassified_threshold_not_reached",
            "unclassified_pending": unclassified_count,
            "retrain_threshold": retrain_threshold,
        }
        context.add_output_metadata(metadata)
        return metadata

    human_rows = []
    try:
        human_lookup = (
            spark_res.read_smartaudits_ai_comment_lookup()
            .filter(F.upper(F.col("ClassificationMethod")) == F.lit("HUMAN"))
            .filter(F.col("AiResult").cast("byte") == F.lit(0))
            .filter(F.col("ResultCategory") != F.lit(CATEGORY_COMPLIANCE))
            .select(
                F.col("NormalizedComment").cast("string").alias("NormalizedComment"),
                F.col("ResultCategory").cast("string").alias("ResultCategory"),
            )
            .filter(F.col("NormalizedComment").isNotNull())
            .filter(F.col("NormalizedComment") != F.lit(""))
            .dropDuplicates(["NormalizedComment", "ResultCategory"])
        )
        human_rows = human_lookup.collect()
    except Exception as exc:
        context.log.warning(
            "Could not read human SmartAudits lookup labels: %s",
            exc,
        )

    evidence = (
        _read_mysql_table(spark_res, mysql, "evidence_photos")
        .select(
            F.col("commentsAI").cast("string").alias("CommentsAI"),
            ai_result_int_column(F.col("resultAI")).cast("byte").alias("AiResult"),
        )
        .filter(F.col("CommentsAI").isNotNull())
        .filter(F.col("AiResult") == F.lit(0))
        .withColumn("NormalizedComment", normalize_comment_column(F.col("CommentsAI")))
        .withColumn(
            "ResultCategory",
            deterministic_category_column(F.col("NormalizedComment"), F.col("AiResult")),
        )
        .filter(F.col("ResultCategory").isNotNull())
        .filter(F.col("ResultCategory") != F.lit(CATEGORY_COMPLIANCE))
        .dropDuplicates(["NormalizedComment", "ResultCategory"])
    )

    human_samples = [
        (row.NormalizedComment, row.ResultCategory)
        for row in human_rows
    ]
    human_normalized_comments = {
        row.NormalizedComment
        for row in human_rows
    }
    rule_samples = [
        (row.NormalizedComment, row.ResultCategory)
        for row in evidence.select("NormalizedComment", "ResultCategory").collect()
        if row.NormalizedComment not in human_normalized_comments
    ]
    samples = human_samples + rule_samples
    training_result = train_result_category_model(
        samples,
        min_training_rows=min_training_rows,
    )

    if training_result is None:
        metadata = {
            "human_training_rows": len(human_samples),
            "rule_training_rows": len(rule_samples),
            "training_rows": len(samples),
            "promoted": False,
            "reason": "not_enough_labeled_data",
        }
        context.add_output_metadata(metadata)
        return metadata

    model_version = datetime.utcnow().strftime("carlsjr-smartaudits-%Y%m%d%H%M%S")
    artifact_path = model_base_path + "/" + model_version
    model_bytes = dump_model_bytes(training_result.model)
    spark_res.write_smartaudits_model_artifact(
        artifact_path,
        model_version,
        model_bytes,
    )

    promote = (
        training_result.accuracy >= min_accuracy
        and training_result.macro_f1 >= min_macro_f1
    )
    sql.register_smartaudits_classifier_model(
        model_version=model_version,
        artifact_path=artifact_path,
        accuracy=training_result.accuracy,
        macro_f1=training_result.macro_f1,
        training_rows=training_result.training_rows,
        promote=promote,
    )
    reclassified_rows = 0
    if promote:
        reclassified_rows = _reclassify_unclassified_rows(
            context=context,
            spark_res=spark_res,
            sql=sql,
            mysql=mysql,
            model=training_result.model,
            model_version=model_version,
            min_plan_result_id=unclassified_stats["min_plan_result_id"],
            max_plan_result_id=unclassified_stats["max_plan_result_id"],
        )

    metadata = {
        "model_version": model_version,
        "artifact_path": artifact_path,
        "accuracy": training_result.accuracy,
        "macro_f1": training_result.macro_f1,
        "human_training_rows": len(human_samples),
        "rule_training_rows": len(rule_samples),
        "training_rows": training_result.training_rows,
        "promoted": promote,
        "unclassified_pending": unclassified_count,
        "retrain_threshold": retrain_threshold,
        "reclassified_rows": reclassified_rows,
    }
    context.add_output_metadata(metadata)
    return metadata


@job(name="111_smartaudits_classifier_training", tags={"job_type": "smartaudits", "user_code": "carlsjr"})
def smartaudits_classifier_training_job():
    train_smartaudits_classifier_op()
