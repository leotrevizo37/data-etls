from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window, Column
from pyspark.sql import functions as F

from .smartaudits_classifier import (
    CATEGORY_CLEANING_NON_COMPLIANCE,
    CATEGORY_COMPLIANCE,
    CATEGORY_GENERAL_NON_COMPLIANCE,
    CATEGORY_IMAGE_NOT_LEGIBLE,
    CATEGORY_IMAGE_NOT_PROCESSABLE,
    CATEGORY_OUT_OF_RANGE,
    OUT_OF_RANGE_PATTERNS,
    CATEGORY_UNCLASSIFIED,
    CLEANING_KEYWORDS,
    GENERAL_NON_COMPLIANCE_KEYWORDS,
    IMAGE_NOT_LEGIBLE_KEYWORDS,
    IMAGE_NOT_PROCESSABLE_KEYWORDS,
    OUT_OF_RANGE_KEYWORDS,
    RESULT_CATEGORY_ORDER,
)


OUTPUT_COLUMNS = [
    "PlanResultId",
    "WorkPlanId",
    "EvidencePhotoId",
    "LocationId",
    "SublocationId",
    "Checkpoint",
    "Task",
    "LocationName",
    "SublocationName",
    "CheckpointName",
    "TaskName",
    "ExecutorName",
    "AuditorName",
    "TaskCategory",
    "Priority",
    "ResultCategory",
    "AiResult",
    "ReviewDate",
    "ReviewAIDate",
    "SourceLastChangedAt",
    "ClassificationMethod",
    "ClassifierModelVersion",
    "ClassifierConfidence",
    "FailedEvidenceCount",
    "EvidenceCount",
]

REVIEW_QUEUE_COLUMNS = [
    "NormalizedCommentHash",
    "AiResult",
    "NormalizedComment",
    "SampleComment",
    "CandidateCount",
    "FirstSeenAt",
    "LastSeenAt",
    "LastPlanResultId",
    "LastEvidencePhotoId",
    "SuggestedCategory",
    "SuggestedMethod",
    "SuggestedConfidence",
]


def normalize_comment_column(column):
    return F.trim(
        F.regexp_replace(
            F.lower(
                F.translate(
                    F.coalesce(column.cast("string"), F.lit("")),
                    "ÁÀÂÄÃáàâäãÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÖÕóòôöõÚÙÛÜúùûüÑñÇç",
                    "AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc",
                )
            ),
            "\\s+",
            " ",
        )
    )


def ai_result_int_column(column):
    value = F.lower(F.coalesce(column.cast("string"), F.lit("")))
    return (
        F.when(value.isin("true", "1"), F.lit(1))
        .when(value.isin("false", "0"), F.lit(0))
        .otherwise(F.lit(None).cast("int"))
    )


def _keyword_match(column, keywords: tuple[str, ...]):
    expr = None
    for keyword in keywords:
        current = column.contains(keyword)
        expr = current if expr is None else expr | current
    return expr if expr is not None else F.lit(False)

def _pattern_match(
    column: Column,
    patterns: Sequence[str],
) -> Column:
    current = F.lit(False)

    for pattern in patterns:
        current = current | column.rlike(pattern)

    return current

def deterministic_category_column(normalized_comment, result_ai):
    result_ai_int = ai_result_int_column(result_ai)
    return (
        F.when(result_ai_int == F.lit(1), F.lit(CATEGORY_COMPLIANCE))
        .when(
            _keyword_match(normalized_comment, IMAGE_NOT_LEGIBLE_KEYWORDS),
            F.lit(CATEGORY_IMAGE_NOT_LEGIBLE),
        )
        .when(
            _keyword_match(normalized_comment, IMAGE_NOT_PROCESSABLE_KEYWORDS),
            F.lit(CATEGORY_IMAGE_NOT_PROCESSABLE),
        )
        .when(
            _keyword_match(
                normalized_comment,
                OUT_OF_RANGE_KEYWORDS,
            )
            | _pattern_match(
                normalized_comment,
                OUT_OF_RANGE_PATTERNS,
            ),
            F.lit(CATEGORY_OUT_OF_RANGE),
        )
        .when(
            _keyword_match(normalized_comment, CLEANING_KEYWORDS),
            F.lit(CATEGORY_CLEANING_NON_COMPLIANCE),
        )
        .when(
            _keyword_match(normalized_comment, GENERAL_NON_COMPLIANCE_KEYWORDS),
            F.lit(CATEGORY_GENERAL_NON_COMPLIANCE),
        )
    )


def _category_priority_column(category_column):
    expr = F.lit(len(RESULT_CATEGORY_ORDER))
    for index, category in reversed(list(enumerate(RESULT_CATEGORY_ORDER))):
        expr = F.when(category_column == F.lit(category), F.lit(index)).otherwise(expr)
    return expr


def unresolved_comments_df(evidence_photos_df: DataFrame) -> DataFrame:
    evidence = (
        evidence_photos_df
        .withColumn("NormalizedComment", normalize_comment_column(F.col("commentsAI")))
        .withColumn("ResultAiInt", ai_result_int_column(F.col("resultAI")))
        .withColumn(
            "DeterministicResultCategory",
            deterministic_category_column(
                F.col("NormalizedComment"),
                F.col("ResultAiInt"),
            ),
        )
    )

    return (
        evidence
        .filter(F.col("ResultAiInt") == F.lit(0))
        .filter(F.col("commentsAI").isNotNull())
        .filter(F.col("NormalizedComment") != F.lit(""))
        .filter(F.col("DeterministicResultCategory").isNull())
        .select("NormalizedComment", F.col("commentsAI").alias("CommentsAI"))
        .dropDuplicates(["NormalizedComment"])
    )


def empty_classifier_predictions_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [],
        "NormalizedComment string, PredictedCategory string, "
        "PredictionMethod string, ClassifierModelVersion string, "
        "ClassifierConfidence double",
    )


def build_smartaudits_categories_df(
    plan_results_df: DataFrame,
    evidence_photos_df: DataFrame,
    work_plan_df: DataFrame,
    tasks_df: DataFrame,
    checkpoints_df: DataFrame,
    sublocations_df: DataFrame,
    locations_df: DataFrame,
    users_df: DataFrame,
    priority_types_df: DataFrame,
    task_categories_df: DataFrame,
    categories_df: DataFrame,
    comment_lookup_df: Optional[DataFrame] = None,
    classifier_predictions_df: Optional[DataFrame] = None,
) -> DataFrame:
    spark = plan_results_df.sparkSession
    if classifier_predictions_df is None:
        classifier_predictions_df = empty_classifier_predictions_df(spark)

    task_category_window = Window.partitionBy("taskId").orderBy("categoryId")
    task_category_df = (
        task_categories_df.alias("tc")
        .join(
            categories_df.alias("cat"),
            F.col("tc.categoryId") == F.col("cat.id"),
            "left",
        )
        .select(
            F.col("tc.taskId"),
            F.col("cat.name").alias("TaskCategory"),
            F.row_number().over(task_category_window).alias("rn"),
        )
        .filter(F.col("rn") == F.lit(1))
        .drop("rn")
    )
    executor_users_df = (
        users_df
        .select(
            F.upper(F.trim(F.col("Id").cast("string"))).alias("ExecutorUserId"),
            F.upper(
                F.trim(
                    F.concat_ws(
                        " ",
                        F.trim(F.col("name").cast("string")),
                        F.trim(F.col("lastName").cast("string")),
                    )
                )
            ).alias("ExecutorName"),
        )
        .dropDuplicates(["ExecutorUserId"])
    )
    auditor_users_df = (
        users_df
        .select(
            F.upper(F.trim(F.col("Id").cast("string"))).alias("AuditorUserId"),
            F.upper(
                F.trim(
                    F.concat_ws(
                        " ",
                        F.trim(F.col("name").cast("string")),
                        F.trim(F.col("lastName").cast("string")),
                    )
                )
            ).alias("AuditorName"),
        )
        .dropDuplicates(["AuditorUserId"])
    )

    base = (
        plan_results_df.alias("pr")
        .join(
            work_plan_df.alias("wp"),
            F.col("pr.workPlanId") == F.col("wp.id"),
            "inner",
        )
        .join(tasks_df.alias("t"), F.col("wp.taskId") == F.col("t.id"), "left")
        .join(
            checkpoints_df.alias("cp"),
            F.col("wp.checkpointId") == F.col("cp.id"),
            "left",
        )
        .join(
            sublocations_df.alias("sl"),
            F.col("cp.sublocationId") == F.col("sl.id"),
            "left",
        )
        .join(
            locations_df.alias("loc"),
            F.col("sl.locationId") == F.col("loc.id"),
            "left",
        )
        .join(
            executor_users_df.alias("eu"),
            F.upper(F.trim(F.col("pr.userId").cast("string"))) == F.col("eu.ExecutorUserId"),
            "left",
        )
        .join(
            auditor_users_df.alias("au"),
            F.upper(F.trim(F.col("pr.validatorUserId").cast("string"))) == F.col("au.AuditorUserId"),
            "left",
        )
        .join(
            priority_types_df.alias("pt"),
            F.col("t.priorityTypeId") == F.col("pt.id"),
            "left",
        )
        .join(task_category_df.alias("tc"), F.col("t.id") == F.col("tc.taskId"), "left")
        .select(
            F.col("pr.id").cast("long").alias("PlanResultId"),
            F.col("pr.workPlanId").cast("long").alias("WorkPlanId"),
            F.upper(F.trim(F.col("loc.id").cast("string"))).alias("LocationId"),
            F.upper(F.trim(F.col("sl.SublocationId").cast("string"))).alias("SublocationId"),
            F.col("cp.id").cast("string").alias("Checkpoint"),
            F.col("t.id").cast("string").alias("Task"),
            F.col("loc.Name").cast("string").alias("LocationName"),
            F.col("sl.description").cast("string").alias("SublocationName"),
            F.col("cp.description").cast("string").alias("CheckpointName"),
            F.col("t.description").cast("string").alias("TaskName"),
            F.col("eu.ExecutorName").cast("string").alias("ExecutorName"),
            F.col("au.AuditorName").cast("string").alias("AuditorName"),
            F.coalesce(F.col("tc.TaskCategory"), F.lit("SIN_CATEGORIA")).alias("TaskCategory"),
            F.col("pt.name").cast("string").alias("Priority"),
            ai_result_int_column(F.col("pr.okAI")).cast("byte").alias("AiResult"),
            F.col("pr.reviewDate").cast("timestamp").alias("ReviewDate"),
            F.col("pr.reviewAIDate").cast("timestamp").alias("ReviewAIDate"),
        )
    )

    evidence = (
        evidence_photos_df
        .select(
            F.col("id").cast("long").alias("EvidencePhotoId"),
            F.col("planResultsId").cast("long").alias("PlanResultId"),
            ai_result_int_column(F.col("resultAI")).cast("byte").alias("EvidenceAiResult"),
            F.col("commentsAI").cast("string").alias("CommentsAI"),
            F.col("createdAt").cast("timestamp").alias("EvidenceCreatedAt"),
        )
        .withColumn("NormalizedComment", normalize_comment_column(F.col("CommentsAI")))
        .withColumn(
            "DeterministicResultCategory",
            deterministic_category_column(
                F.col("NormalizedComment"),
                F.col("EvidenceAiResult"),
            ),
        )
    )

    if comment_lookup_df is not None:
        lookup = comment_lookup_df.select(
            F.col("NormalizedComment").alias("LookupNormalizedComment"),
            F.col("AiResult").cast("byte").alias("LookupAiResult"),
            F.col("ResultCategory").alias("LookupCategory"),
            F.col("ClassificationMethod").alias("LookupMethod"),
            F.col("ClassifierModelVersion").alias("LookupModelVersion"),
            F.col("ClassifierConfidence").cast("double").alias("LookupConfidence"),
        )
        evidence = (
            evidence
            .join(
                lookup,
                (F.col("NormalizedComment") == F.col("LookupNormalizedComment"))
                & (F.col("EvidenceAiResult") == F.col("LookupAiResult")),
                "left",
            )
            .drop("LookupNormalizedComment", "LookupAiResult")
        )
    else:
        evidence = (
            evidence
            .withColumn("LookupCategory", F.lit(None).cast("string"))
            .withColumn("LookupMethod", F.lit(None).cast("string"))
            .withColumn("LookupModelVersion", F.lit(None).cast("string"))
            .withColumn("LookupConfidence", F.lit(None).cast("double"))
        )

    predictions = classifier_predictions_df.select(
        F.col("NormalizedComment").alias("PredictionNormalizedComment"),
        F.col("PredictedCategory"),
        F.col("PredictionMethod"),
        F.col("ClassifierModelVersion").alias("PredictionModelVersion"),
        F.col("ClassifierConfidence").cast("double").alias("PredictionConfidence"),
    )

    evidence = (
        evidence
        .join(
            predictions,
            F.col("NormalizedComment") == F.col("PredictionNormalizedComment"),
            "left",
        )
        .drop("PredictionNormalizedComment")
        .withColumn(
            "HumanLookupCategory",
            F.when(
                F.upper(F.coalesce(F.col("LookupMethod"), F.lit(""))) == F.lit("HUMAN"),
                F.col("LookupCategory"),
            ),
        )
        .withColumn(
            "HumanLookupConfidence",
            F.when(F.col("HumanLookupCategory").isNotNull(), F.col("LookupConfidence")),
        )
        .withColumn(
            "EvidenceCategory",
            F.when(F.col("EvidenceAiResult") == F.lit(1), F.lit(CATEGORY_COMPLIANCE))
            .otherwise(
                F.coalesce(
                    F.col("HumanLookupCategory"),
                    F.col("DeterministicResultCategory"),
                    F.col("PredictedCategory"),
                    F.when(F.col("EvidenceAiResult") == F.lit(0), F.lit(CATEGORY_UNCLASSIFIED)),
                )
            ),
        )
        .withColumn(
            "EvidenceMethod",
            F.when(F.col("EvidenceAiResult") == F.lit(1), F.lit("RULE"))
            .when(F.col("HumanLookupCategory").isNotNull(), F.col("LookupMethod"))
            .when(F.col("DeterministicResultCategory").isNotNull(), F.lit("RULE"))
            .when(F.col("PredictedCategory").isNotNull(), F.col("PredictionMethod"))
            .when(F.col("EvidenceAiResult") == F.lit(0), F.lit("SIN_CLASIFICAR")),
        )
        .withColumn(
            "EvidenceModelVersion",
            F.when(F.col("HumanLookupCategory").isNotNull(), F.col("LookupModelVersion"))
            .otherwise(F.col("PredictionModelVersion")),
        )
        .withColumn(
            "EvidenceConfidence",
            F.when(F.col("EvidenceAiResult") == F.lit(1), F.lit(1.0))
            .when(F.col("HumanLookupCategory").isNotNull(), F.coalesce(F.col("HumanLookupConfidence"), F.lit(1.0)))
            .when(F.col("DeterministicResultCategory").isNotNull(), F.lit(1.0))
            .otherwise(F.coalesce(F.col("PredictionConfidence"), F.lit(0.0))),
        )
    )

    evidence_counts = (
        evidence
        .groupBy("PlanResultId")
        .agg(
            F.count("*").cast("int").alias("EvidenceCount"),
            F.sum(F.when(F.col("EvidenceAiResult") == F.lit(0), 1).otherwise(0))
            .cast("int")
            .alias("FailedEvidenceCount"),
            F.max("EvidenceCreatedAt").alias("MaxEvidenceCreatedAt"),
        )
    )

    modal_window = Window.partitionBy("PlanResultId").orderBy(
        F.col("CategoryCount").desc(),
        F.col("CategoryPriority").asc(),
        F.col("EvidencePhotoId").asc(),
    )
    modal_failed = (
        evidence
        .filter(F.col("EvidenceAiResult") == F.lit(0))
        .groupBy("PlanResultId", "EvidenceCategory")
        .agg(
            F.count("*").alias("CategoryCount"),
            F.min("EvidencePhotoId").alias("EvidencePhotoId"),
            F.first("EvidenceMethod", ignorenulls=True).alias("ClassificationMethod"),
            F.first("EvidenceModelVersion", ignorenulls=True).alias("ClassifierModelVersion"),
            F.max("EvidenceConfidence").alias("ClassifierConfidence"),
        )
        .withColumn("CategoryPriority", _category_priority_column(F.col("EvidenceCategory")))
        .withColumn("rn", F.row_number().over(modal_window))
        .filter(F.col("rn") == F.lit(1))
        .select(
            "PlanResultId",
            "EvidencePhotoId",
            F.col("EvidenceCategory").alias("FailedResultCategory"),
            "ClassificationMethod",
            "ClassifierModelVersion",
            "ClassifierConfidence",
        )
    )

    result = (
        base
        .join(evidence_counts, "PlanResultId", "left")
        .join(modal_failed, "PlanResultId", "left")
        .withColumn("EvidenceCount", F.coalesce(F.col("EvidenceCount"), F.lit(0)).cast("int"))
        .withColumn(
            "FailedEvidenceCount",
            F.coalesce(F.col("FailedEvidenceCount"), F.lit(0)).cast("int"),
        )
        .withColumn(
            "ResultCategory",
            F.when(F.col("AiResult") == F.lit(1), F.lit(CATEGORY_COMPLIANCE))
            .when(F.col("FailedEvidenceCount") > F.lit(0), F.col("FailedResultCategory"))
            .when(F.col("AiResult") == F.lit(0), F.lit(CATEGORY_UNCLASSIFIED))
            .otherwise(F.lit(CATEGORY_UNCLASSIFIED)),
        )
        .withColumn(
            "EvidencePhotoId",
            F.when(F.col("ResultCategory") == F.lit(CATEGORY_COMPLIANCE), F.lit(None).cast("long"))
            .otherwise(F.col("EvidencePhotoId")),
        )
        .withColumn(
            "ClassificationMethod",
            F.when(F.col("ResultCategory") == F.lit(CATEGORY_COMPLIANCE), F.lit("RULE"))
            .otherwise(F.coalesce(F.col("ClassificationMethod"), F.lit("SIN_CLASIFICAR"))),
        )
        .withColumn(
            "ClassifierConfidence",
            F.when(F.col("ResultCategory") == F.lit(CATEGORY_COMPLIANCE), F.lit(1.0))
            .otherwise(F.coalesce(F.col("ClassifierConfidence"), F.lit(0.0))),
        )
        .withColumn(
            "SourceLastChangedAt",
            F.coalesce(
                F.greatest(F.col("ReviewAIDate"), F.col("MaxEvidenceCreatedAt")),
                F.col("ReviewDate"),
                F.lit("1900-01-01 00:00:00").cast("timestamp"),
            ),
        )
    )

    return result.select(*OUTPUT_COLUMNS)


def build_smartaudits_comment_review_queue_df(
    evidence_photos_df: DataFrame,
    comment_lookup_df: Optional[DataFrame] = None,
    classifier_predictions_df: Optional[DataFrame] = None,
    confidence_threshold: float = 0.75,
) -> DataFrame:
    spark = evidence_photos_df.sparkSession
    if classifier_predictions_df is None:
        classifier_predictions_df = empty_classifier_predictions_df(spark)

    evidence = (
        evidence_photos_df
        .select(
            F.col("id").cast("long").alias("EvidencePhotoId"),
            F.col("planResultsId").cast("long").alias("PlanResultId"),
            ai_result_int_column(F.col("resultAI")).cast("byte").alias("AiResult"),
            F.col("commentsAI").cast("string").alias("CommentsAI"),
            F.col("createdAt").cast("timestamp").alias("EvidenceCreatedAt"),
        )
        .withColumn("NormalizedComment", normalize_comment_column(F.col("CommentsAI")))
        .withColumn("NormalizedCommentHash", F.sha2(F.col("NormalizedComment"), 256))
        .withColumn(
            "DeterministicResultCategory",
            deterministic_category_column(
                F.col("NormalizedComment"),
                F.col("AiResult"),
            ),
        )
    )

    if comment_lookup_df is not None:
        lookup = comment_lookup_df.select(
            F.col("NormalizedComment").alias("LookupNormalizedComment"),
            F.col("AiResult").cast("byte").alias("LookupAiResult"),
            F.col("ResultCategory").alias("LookupCategory"),
            F.col("ClassificationMethod").alias("LookupMethod"),
            F.col("ClassifierConfidence").cast("double").alias("LookupConfidence"),
        )
        evidence = (
            evidence
            .join(
                lookup,
                (F.col("NormalizedComment") == F.col("LookupNormalizedComment"))
                & (F.col("AiResult") == F.col("LookupAiResult")),
                "left",
            )
            .drop("LookupNormalizedComment", "LookupAiResult")
        )
    else:
        evidence = (
            evidence
            .withColumn("LookupCategory", F.lit(None).cast("string"))
            .withColumn("LookupMethod", F.lit(None).cast("string"))
            .withColumn("LookupConfidence", F.lit(None).cast("double"))
        )

    predictions = classifier_predictions_df.select(
        F.col("NormalizedComment").alias("PredictionNormalizedComment"),
        F.col("PredictedCategory"),
        F.col("PredictionMethod"),
        F.col("ClassifierConfidence").cast("double").alias("PredictionConfidence"),
    )

    evidence = (
        evidence
        .join(
            predictions,
            F.col("NormalizedComment") == F.col("PredictionNormalizedComment"),
            "left",
        )
        .drop("PredictionNormalizedComment")
        .withColumn(
            "HumanLookupCategory",
            F.when(
                F.upper(F.coalesce(F.col("LookupMethod"), F.lit(""))) == F.lit("HUMAN"),
                F.col("LookupCategory"),
            ),
        )
        .withColumn(
            "SuggestedCategory",
            F.coalesce(
                F.col("PredictedCategory"),
                F.lit(CATEGORY_UNCLASSIFIED),
            ),
        )
        .withColumn(
            "SuggestedMethod",
            F.coalesce(
                F.col("PredictionMethod"),
                F.lit("SIN_CLASIFICAR"),
            ),
        )
        .withColumn(
            "SuggestedConfidence",
            F.coalesce(F.col("PredictionConfidence"), F.lit(0.0)),
        )
    )

    return (
        evidence
        .filter(F.col("AiResult") == F.lit(0))
        .filter(F.col("CommentsAI").isNotNull())
        .filter(F.col("NormalizedComment") != F.lit(""))
        .filter(F.col("HumanLookupCategory").isNull())
        .filter(F.col("DeterministicResultCategory").isNull())
        .filter(
            F.col("PredictedCategory").isNull()
            | (F.col("PredictedCategory") == F.lit(CATEGORY_UNCLASSIFIED))
            | (F.col("SuggestedConfidence") < F.lit(float(confidence_threshold)))
        )
        .groupBy("NormalizedCommentHash", "AiResult", "NormalizedComment")
        .agg(
            F.first("CommentsAI", ignorenulls=True).alias("SampleComment"),
            F.count("*").cast("int").alias("CandidateCount"),
            F.min("EvidenceCreatedAt").alias("FirstSeenAt"),
            F.max("EvidenceCreatedAt").alias("LastSeenAt"),
            F.max("PlanResultId").alias("LastPlanResultId"),
            F.max("EvidencePhotoId").alias("LastEvidencePhotoId"),
            F.first("SuggestedCategory", ignorenulls=True).alias("SuggestedCategory"),
            F.first("SuggestedMethod", ignorenulls=True).alias("SuggestedMethod"),
            F.max("SuggestedConfidence").alias("SuggestedConfidence"),
        )
        .select(*REVIEW_QUEUE_COLUMNS)
    )
