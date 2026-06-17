from datetime import datetime, timezone
from typing import Optional, Dict, Any

import os
import pyodbc
from dagster import resource


def build_fact_smartaudits_categories_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factSmartauditsCategories AS target
USING (
    SELECT
        PlanResultId,
        WorkPlanId,
        EvidencePhotoId,
        Location,
        Sublocation,
        [Checkpoint],
        [Task],
        LocationName,
        SublocationName,
        CheckpointName,
        TaskName,
        TaskCategory,
        ResultCategory,
        AiResult,
        ReviewDate,
        ReviewAIDate,
        SourceLastChangedAt,
        ClassificationMethod,
        ClassifierModelVersion,
        ClassifierConfidence,
        FailedEvidenceCount,
        EvidenceCount
    FROM stg.factSmartauditsCategories
    WHERE RunId = ?
) AS source
    ON target.PlanResultId = source.PlanResultId
WHEN MATCHED THEN
    UPDATE SET
        target.WorkPlanId = source.WorkPlanId,
        target.EvidencePhotoId = source.EvidencePhotoId,
        target.Location = source.Location,
        target.Sublocation = source.Sublocation,
        target.[Checkpoint] = source.[Checkpoint],
        target.[Task] = source.[Task],
        target.LocationName = source.LocationName,
        target.SublocationName = source.SublocationName,
        target.CheckpointName = source.CheckpointName,
        target.TaskName = source.TaskName,
        target.TaskCategory = source.TaskCategory,
        target.ResultCategory = source.ResultCategory,
        target.AiResult = source.AiResult,
        target.ReviewDate = source.ReviewDate,
        target.ReviewAIDate = source.ReviewAIDate,
        target.SourceLastChangedAt = source.SourceLastChangedAt,
        target.ClassificationMethod = source.ClassificationMethod,
        target.ClassifierModelVersion = source.ClassifierModelVersion,
        target.ClassifierConfidence = source.ClassifierConfidence,
        target.FailedEvidenceCount = source.FailedEvidenceCount,
        target.EvidenceCount = source.EvidenceCount,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        PlanResultId,
        WorkPlanId,
        EvidencePhotoId,
        Location,
        Sublocation,
        [Checkpoint],
        [Task],
        LocationName,
        SublocationName,
        CheckpointName,
        TaskName,
        TaskCategory,
        ResultCategory,
        AiResult,
        ReviewDate,
        ReviewAIDate,
        SourceLastChangedAt,
        ClassificationMethod,
        ClassifierModelVersion,
        ClassifierConfidence,
        FailedEvidenceCount,
        EvidenceCount
    )
    VALUES (
        source.PlanResultId,
        source.WorkPlanId,
        source.EvidencePhotoId,
        source.Location,
        source.Sublocation,
        source.[Checkpoint],
        source.[Task],
        source.LocationName,
        source.SublocationName,
        source.CheckpointName,
        source.TaskName,
        source.TaskCategory,
        source.ResultCategory,
        source.AiResult,
        source.ReviewDate,
        source.ReviewAIDate,
        source.SourceLastChangedAt,
        source.ClassificationMethod,
        source.ClassifierModelVersion,
        source.ClassifierConfidence,
        source.FailedEvidenceCount,
        source.EvidenceCount
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_smartaudits_comment_review_queue_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE ctl.SmartauditsAiCommentReviewQueue AS target
USING (
    SELECT
        NormalizedCommentHash,
        AiResult,
        NormalizedComment,
        SampleComment,
        CandidateCount,
        FirstSeenAt,
        LastSeenAt,
        LastPlanResultId,
        LastEvidencePhotoId,
        SuggestedCategory,
        SuggestedMethod,
        SuggestedConfidence
    FROM stg.SmartauditsAiCommentReviewQueue
    WHERE RunId = ?
) AS source
    ON target.NormalizedCommentHash = source.NormalizedCommentHash
    AND target.AiResult = source.AiResult
WHEN MATCHED THEN
    UPDATE SET
        target.NormalizedComment = source.NormalizedComment,
        target.SampleComment = COALESCE(target.SampleComment, source.SampleComment),
        target.CandidateCount =
            CASE
                WHEN target.LastSeenAt IS NULL THEN target.CandidateCount + source.CandidateCount
                WHEN source.LastSeenAt IS NULL THEN target.CandidateCount
                WHEN source.LastSeenAt > target.LastSeenAt THEN target.CandidateCount + source.CandidateCount
                ELSE target.CandidateCount
            END,
        target.FirstSeenAt =
            CASE
                WHEN target.FirstSeenAt IS NULL THEN source.FirstSeenAt
                WHEN source.FirstSeenAt IS NULL THEN target.FirstSeenAt
                WHEN source.FirstSeenAt < target.FirstSeenAt THEN source.FirstSeenAt
                ELSE target.FirstSeenAt
            END,
        target.LastSeenAt =
            CASE
                WHEN target.LastSeenAt IS NULL THEN source.LastSeenAt
                WHEN source.LastSeenAt IS NULL THEN target.LastSeenAt
                WHEN source.LastSeenAt > target.LastSeenAt THEN source.LastSeenAt
                ELSE target.LastSeenAt
            END,
        target.LastPlanResultId = COALESCE(source.LastPlanResultId, target.LastPlanResultId),
        target.LastEvidencePhotoId = COALESCE(source.LastEvidencePhotoId, target.LastEvidencePhotoId),
        target.SuggestedCategory = COALESCE(source.SuggestedCategory, target.SuggestedCategory),
        target.SuggestedMethod = COALESCE(source.SuggestedMethod, target.SuggestedMethod),
        target.SuggestedConfidence = COALESCE(source.SuggestedConfidence, target.SuggestedConfidence),
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        NormalizedCommentHash,
        AiResult,
        NormalizedComment,
        SampleComment,
        CandidateCount,
        FirstSeenAt,
        LastSeenAt,
        LastPlanResultId,
        LastEvidencePhotoId,
        SuggestedCategory,
        SuggestedMethod,
        SuggestedConfidence,
        ReviewStatus
    )
    VALUES (
        source.NormalizedCommentHash,
        source.AiResult,
        source.NormalizedComment,
        source.SampleComment,
        source.CandidateCount,
        source.FirstSeenAt,
        source.LastSeenAt,
        source.LastPlanResultId,
        source.LastEvidencePhotoId,
        source.SuggestedCategory,
        source.SuggestedMethod,
        source.SuggestedConfidence,
        N'PENDING'
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_smartaudits_comment_lookup_promotion_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.dimSmartauditsAiCommentLookup AS target
USING (
    SELECT
        NormalizedCommentHash,
        NormalizedComment,
        AiResult,
        ReviewedResultCategory
    FROM ctl.SmartauditsAiCommentReviewQueue
    WHERE UPPER(ReviewStatus) = N'APPROVED'
    AND ReviewedResultCategory IS NOT NULL
    AND LTRIM(RTRIM(ReviewedResultCategory)) <> N''
) AS source
    ON target.NormalizedCommentHash = source.NormalizedCommentHash
    AND target.AiResult = source.AiResult
WHEN MATCHED THEN
    UPDATE SET
        target.NormalizedComment = source.NormalizedComment,
        target.ResultCategory = source.ReviewedResultCategory,
        target.ClassificationMethod = N'HUMAN',
        target.ClassifierModelVersion = NULL,
        target.ClassifierConfidence = 1.0,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        NormalizedCommentHash,
        NormalizedComment,
        AiResult,
        ResultCategory,
        ClassificationMethod,
        ClassifierModelVersion,
        ClassifierConfidence
    )
    VALUES (
        source.NormalizedCommentHash,
        source.NormalizedComment,
        source.AiResult,
        source.ReviewedResultCategory,
        N'HUMAN',
        NULL,
        1.0
    )
OUTPUT $action INTO @MergeActions;

UPDATE ctl.SmartauditsAiCommentReviewQueue
SET
    ReviewStatus = N'PROMOTED',
    ReviewedAt = COALESCE(ReviewedAt, SYSUTCDATETIME()),
    ModifiedAt = SYSUTCDATETIME()
WHERE UPPER(ReviewStatus) = N'APPROVED'
AND ReviewedResultCategory IS NOT NULL
AND LTRIM(RTRIM(ReviewedResultCategory)) <> N'';

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


class SqlServerResource:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout: int,
        command_timeout: int,
        driver: str,
        retries: int,
        retry_interval: int,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self.driver = driver
        self.retries = retries
        self.retry_interval = retry_interval

    def _get_connection(self) -> pyodbc.Connection:
        conn_str = (
            "DRIVER={"
            + self.driver
            + "};SERVER="
            + self.host
            + ","
            + str(self.port)
            + ";DATABASE="
            + self.database
            + ";UID="
            + self.user
            + ";PWD="
            + self.password
            + ";TrustServerCertificate=yes"
            + ";Connection Timeout="
            + str(self.connect_timeout)
            + ";ConnectRetryCount="
            + str(self.retries)
            + ";ConnectRetryInterval="
            + str(self.retry_interval)
        )
        return pyodbc.connect(conn_str, timeout=self.connect_timeout)

    def get_watermark(self, ingestion_name: str) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "EXEC ctl.usp_IngestionControl_GetWatermark @IngestionName = ?",
                        ingestion_name,
                    )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.LastLoadedAt

    def get_min_open_sensor_event_time(self) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        "SELECT MIN(LocalTimeSpan) AS OpenEventFrom "
                        "FROM dwh.factSensorEvents "
                        "WHERE IsEventCompleted = 0"
                    )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.OpenEventFrom

    def mark_run_started(self, ingestion_name: str) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunStarted @IngestionName = ?",
                    ingestion_name,
                )
                conn.commit()

    def execute_query(self, query: str, *args) -> Optional[int]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(query, *args)
                    conn.commit()
                    return cursor.rowcount
                except pyodbc.Error as e:
                    return e

    def fetch_scalar(self, query: str, *args):
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, *args)
                row = cursor.fetchone()
                return row[0] if row is not None else None

    def delete_range(self,
                     schema_name: str,
                     table_name: str,
                     time_column: str,
                     from_utc: datetime,
                     to_utc: Optional[datetime] = None) -> int:
        if to_utc is None:
            to_utc = datetime.now(timezone.utc)
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_DeleteRange "
                    "@SchemaName = ?, "
                    "@TableName = ?, "
                    "@TimeColumn = ?, "
                    "@FromUtc = ?, "
                    "@ToUtc = ?",
                    schema_name,
                    table_name,
                    time_column,
                    from_utc,
                    to_utc,
                )
                rows = cursor.fetchone()
                conn.commit()
                if rows is None:
                    return 0
                deleted = rows.DeletedRowCount
                if deleted is None:
                    return 0
                return int(deleted)

    def mark_run_completed(
        self,
        ingestion_name: str,
        new_last_loaded_at: Optional[datetime],
        succeeded: bool,
        error_message: Optional[str],
    ) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "EXEC ctl.usp_IngestionControl_MarkRunCompleted "
                    "@IngestionName = ?, @NewLastLoadedAt = ?, @Succeeded = ?, @ErrorMessage = ?",
                    ingestion_name,
                    new_last_loaded_at,
                    1 if succeeded else 0,
                    error_message,
                )
                conn.commit()

    def clear_fact_smartaudits_categories_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factSmartauditsCategories WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_smartaudits_categories(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_smartaudits_categories_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_smartaudits_comment_review_queue_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.SmartauditsAiCommentReviewQueue WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_smartaudits_comment_review_queue(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_smartaudits_comment_review_queue_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def promote_smartaudits_reviewed_comments(self) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_smartaudits_comment_lookup_promotion_sql())
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def get_active_smartaudits_classifier_model(self) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT TOP (1) ModelVersion, ArtifactPath, Accuracy, MacroF1 "
                    "FROM ctl.SmartauditsClassifierModels "
                    "WHERE IsActive = 1 "
                    "ORDER BY PromotedAt DESC, CreatedAt DESC"
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return {
                    "model_version": row.ModelVersion,
                    "artifact_path": row.ArtifactPath,
                    "accuracy": row.Accuracy,
                    "macro_f1": row.MacroF1,
                }

    def get_smartaudits_unclassified_stats(self) -> Dict[str, Optional[int]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(1) AS PendingCount, "
                    "MIN(PlanResultId) AS MinPlanResultId, "
                    "MAX(PlanResultId) AS MaxPlanResultId "
                    "FROM dwh.factSmartauditsCategories "
                    "WHERE ResultCategory = 'SIN_CLASIFICAR'"
                )
                row = cursor.fetchone()
                if row is None:
                    return {
                        "pending_count": 0,
                        "min_plan_result_id": None,
                        "max_plan_result_id": None,
                    }
                return {
                    "pending_count": int(row.PendingCount or 0),
                    "min_plan_result_id": (
                        int(row.MinPlanResultId)
                        if row.MinPlanResultId is not None
                        else None
                    ),
                    "max_plan_result_id": (
                        int(row.MaxPlanResultId)
                        if row.MaxPlanResultId is not None
                        else None
                    ),
                }

    def register_smartaudits_classifier_model(
        self,
        model_version: str,
        artifact_path: str,
        accuracy: Optional[float],
        macro_f1: Optional[float],
        training_rows: int,
        promote: bool,
    ) -> None:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                if promote:
                    cursor.execute(
                        "UPDATE ctl.SmartauditsClassifierModels "
                        "SET IsActive = 0 "
                        "WHERE IsActive = 1"
                    )
                cursor.execute(
                    "MERGE ctl.SmartauditsClassifierModels AS target "
                    "USING (SELECT ? AS ModelVersion) AS source "
                    "ON target.ModelVersion = source.ModelVersion "
                    "WHEN MATCHED THEN UPDATE SET "
                    "ArtifactPath = ?, Accuracy = ?, MacroF1 = ?, "
                    "TrainingRows = ?, IsActive = ?, "
                    "PromotedAt = CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE PromotedAt END "
                    "WHEN NOT MATCHED THEN INSERT "
                    "(ModelVersion, ArtifactPath, Accuracy, MacroF1, TrainingRows, IsActive, PromotedAt) "
                    "VALUES (?, ?, ?, ?, ?, ?, CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END);",
                    model_version,
                    artifact_path,
                    accuracy,
                    macro_f1,
                    training_rows,
                    1 if promote else 0,
                    1 if promote else 0,
                    model_version,
                    artifact_path,
                    accuracy,
                    macro_f1,
                    training_rows,
                    1 if promote else 0,
                    1 if promote else 0,
                )
                conn.commit()

    def get_basic_options(self) -> Dict[str, Any]:
        url = (
                "jdbc:sqlserver://"
                + self.host
                + ":"
                + str(self.port)
                + ";databaseName="
                + self.database
                + ";encrypt=true;trustServerCertificate=true"
                + ";Connection Timeout="
                + str(self.connect_timeout)
        )
        return {
            "url": url,
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

@resource
def sqlserver_resource(_):
    host = os.getenv("MSSQL_HOST", "localhost")
    port = int(os.getenv("MSSQL_PORT", "1433"))
    database = os.getenv("APP_DB", "YourDatabaseName")
    user = os.getenv("MSSQL_USER", "app_ingestion_user")
    password = os.getenv("MSSQL_PASSWORD", "")
    connect_timeout = int(os.getenv("MSSQL_CONNECT_TIMEOUT", "300"))
    command_timeout = int(os.getenv("MSSQL_COMMAND_TIMEOUT", "4500"))
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    connect_retry_count = int(os.getenv("MSSQL_CONNECT_RETRY_COUNT", "3"))
    connect_retry_interval = int(os.getenv("MSSQL_CONNECT_RETRY_INTERVAL", "60"))
    return SqlServerResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        driver=driver,
        retries=connect_retry_count,
        retry_interval=connect_retry_interval,
    )
