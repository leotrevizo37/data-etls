from datetime import datetime, timezone
from typing import Optional, Dict, Any

import os
import pyodbc
from dagster import resource


def build_dim_sidon_prod_dimensions_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.dimSidonProdDimensions AS target
USING (
    SELECT
        SensorId,
        SensorTyId,
        DeviceId,
        sensor_name,
        sensor_type,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        TimeZone,
        LocationId,
        sublocation_type
    FROM stg.dimSidonProdDimensions
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
WHEN MATCHED AND EXISTS (
    SELECT
        source.SensorTyId,
        source.DeviceId,
        source.sensor_name,
        source.sensor_type,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.TimeZone,
        source.LocationId,
        source.sublocation_type
    EXCEPT
    SELECT
        target.SensorTyId,
        target.DeviceId,
        target.sensor_name,
        target.sensor_type,
        target.DeviceTyId,
        target.SubLocationId,
        target.Active,
        target.device_name,
        target.device_type,
        target.SubLocationTyId,
        target.sublocation_name,
        target.location_name,
        target.city_name,
        target.CityId,
        target.TimeZone,
        target.LocationId,
        target.sublocation_type
) THEN
    UPDATE SET
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.sensor_name = source.sensor_name,
        target.sensor_type = source.sensor_type,
        target.DeviceTyId = source.DeviceTyId,
        target.SubLocationId = source.SubLocationId,
        target.Active = source.Active,
        target.device_name = source.device_name,
        target.device_type = source.device_type,
        target.SubLocationTyId = source.SubLocationTyId,
        target.sublocation_name = source.sublocation_name,
        target.location_name = source.location_name,
        target.city_name = source.city_name,
        target.CityId = source.CityId,
        target.TimeZone = source.TimeZone,
        target.LocationId = source.LocationId,
        target.sublocation_type = source.sublocation_type,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        SensorTyId,
        DeviceId,
        sensor_name,
        sensor_type,
        DeviceTyId,
        SubLocationId,
        Active,
        device_name,
        device_type,
        SubLocationTyId,
        sublocation_name,
        location_name,
        city_name,
        CityId,
        TimeZone,
        LocationId,
        sublocation_type
    )
    VALUES (
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.sensor_name,
        source.sensor_type,
        source.DeviceTyId,
        source.SubLocationId,
        source.Active,
        source.device_name,
        source.device_type,
        source.SubLocationTyId,
        source.sublocation_name,
        source.location_name,
        source.city_name,
        source.CityId,
        source.TimeZone,       
        source.LocationId,
        source.sublocation_type
    )
WHEN NOT MATCHED BY SOURCE AND target.Active = 1 THEN
    UPDATE SET
        target.Active = 0,
        target.ModifiedAt = SYSUTCDATETIME()
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_dim_aspnet_users_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.dimAspNetUsers AS target
USING (
    SELECT
        Id,
        UserName,
        Name,
        LastName,
        Position
    FROM stg.dimAspNetUsers
    WHERE RunId = ?
) AS source
    ON target.Id = source.Id
WHEN MATCHED AND EXISTS (
    SELECT
        source.UserName,
        source.Name,
        source.LastName,
        source.Position
    EXCEPT
    SELECT
        target.UserName,
        target.Name,
        target.LastName,
        target.Position
) THEN
    UPDATE SET
        target.UserName = source.UserName,
        target.Name = source.Name,
        target.LastName = source.LastName,
        target.Position = source.Position,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id,
        UserName,
        Name,
        LastName,
        Position
    )
    VALUES (
        source.Id,
        source.UserName,
        source.Name,
        source.LastName,
        source.Position
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_sidon_user_usage_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factSidonUserUsage AS target
USING (
    SELECT
        UserId,
        [Date],
        HasConnected,
        MadeCompleteInteraction,
        TimeConnected,
        AvgLatency,
        Latency95thPercentile
    FROM stg.factSidonUserUsage
    WHERE RunId = ?
) AS source
    ON target.[Date] = source.[Date]
    AND target.UserId = source.UserId
WHEN MATCHED AND EXISTS (
    SELECT
        source.HasConnected,
        source.MadeCompleteInteraction,
        source.TimeConnected,
        source.AvgLatency,
        source.Latency95thPercentile
    EXCEPT
    SELECT
        target.HasConnected,
        target.MadeCompleteInteraction,
        target.TimeConnected,
        target.AvgLatency,
        target.Latency95thPercentile
) THEN
    UPDATE SET
        target.HasConnected = source.HasConnected,
        target.MadeCompleteInteraction = source.MadeCompleteInteraction,
        target.TimeConnected = source.TimeConnected,
        target.AvgLatency = source.AvgLatency,
        target.Latency95thPercentile = source.Latency95thPercentile,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        UserId,
        [Date],
        HasConnected,
        MadeCompleteInteraction,
        TimeConnected,
        AvgLatency,
        Latency95thPercentile
    )
    VALUES (
        source.UserId,
        source.[Date],
        source.HasConnected,
        source.MadeCompleteInteraction,
        source.TimeConnected,
        source.AvgLatency,
        source.Latency95thPercentile
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_readings_measurement_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factReadingsMeasurement AS target
USING (
    SELECT
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        Anomalies,
        ReadingsCount
    FROM stg.factReadingsMeasurement
    WHERE RunId = ?
) AS source
    ON target.OperationId = source.OperationId
WHEN MATCHED THEN
    UPDATE SET
        target.SensorId = source.SensorId,
        target.SensorTyId = source.SensorTyId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.MeasurementValue = source.MeasurementValue,
        target.MeasurementStdDev = source.MeasurementStdDev,
        target.Anomalies = source.Anomalies,
        target.ReadingsCount = source.ReadingsCount,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        OperationId,
        SensorId,
        SensorTyId,
        DeviceId,
        LocationId,
        DeviceType,
        SensorType,
        LocalTimeSpan,
        MeasurementValue,
        MeasurementStdDev,
        Anomalies,
        ReadingsCount
    )
    VALUES (
        source.OperationId,
        source.SensorId,
        source.SensorTyId,
        source.DeviceId,
        source.LocationId,
        source.DeviceType,
        source.SensorType,
        source.LocalTimeSpan,
        source.MeasurementValue,
        source.MeasurementStdDev,
        source.Anomalies,
        source.ReadingsCount
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_sensor_events_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factSensorEvents AS target
USING (
    SELECT
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    FROM stg.factSensorEvents
    WHERE RunId = ?
) AS source
    ON target.ReadId = source.ReadId
WHEN MATCHED THEN
    UPDATE SET
        target.SensorId = source.SensorId,
        target.DeviceId = source.DeviceId,
        target.LocationId = source.LocationId,
        target.LocationName = source.LocationName,
        target.DeviceType = source.DeviceType,
        target.SensorType = source.SensorType,
        target.SensorName = source.SensorName,
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.Value = source.Value,
        target.EventMinutes = source.EventMinutes,
        target.IsEventCompleted = source.IsEventCompleted,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ReadId,
        SensorId,
        DeviceId,
        LocationId,
        LocationName,
        DeviceType,
        SensorType,
        SensorName,
        LocalTimeSpan,
        Value,
        EventMinutes,
        IsEventCompleted
    )
    VALUES (
        source.ReadId,
        source.SensorId,
        source.DeviceId,
        source.LocationId,
        source.LocationName,
        source.DeviceType,
        source.SensorType,
        source.SensorName,
        source.LocalTimeSpan,
        source.Value,
        source.EventMinutes,
        source.IsEventCompleted
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


FACT_REDINGS_AUDITS_MERGE_SQL = """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE observability.factRedingsAudits AS target
USING (
    SELECT
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    FROM stg.factRedingsAudits
    WHERE RunId = ?
) AS source
    ON target.SensorId = source.SensorId
    AND target.TimeSpan = source.TimeSpan
WHEN MATCHED THEN
    UPDATE SET
        target.LocalTimeSpan = source.LocalTimeSpan,
        target.ReadingsCount = source.ReadingsCount,
        target.HasLateReadings = CASE
            WHEN target.HasLateReadings = 1 OR source.HasLateReadings = 1 THEN 1
            ELSE 0
        END,
        target.IsConnectionLost = source.IsConnectionLost,
        target.LastReadingAt = source.LastReadingAt,
        target.ConnectionLostAt = source.ConnectionLostAt,
        target.MinutesWithoutReadings = source.MinutesWithoutReadings,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SensorId,
        TimeSpan,
        LocalTimeSpan,
        ReadingsCount,
        HasLateReadings,
        IsConnectionLost,
        LastReadingAt,
        ConnectionLostAt,
        MinutesWithoutReadings
    )
    VALUES (
        source.SensorId,
        source.TimeSpan,
        source.LocalTimeSpan,
        source.ReadingsCount,
        source.HasLateReadings,
        source.IsConnectionLost,
        source.LastReadingAt,
        source.ConnectionLostAt,
        source.MinutesWithoutReadings
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


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
        LocationId,
        SublocationId,
        [Checkpoint],
        [Task],
        LocationName,
        SublocationName,
        CheckpointName,
        TaskName,
        ExecutorName,
        AuditorName,
        TaskCategory,
        Priority,
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
        target.LocationId = source.LocationId,
        target.SublocationId = source.SublocationId,
        target.[Checkpoint] = source.[Checkpoint],
        target.[Task] = source.[Task],
        target.LocationName = source.LocationName,
        target.SublocationName = source.SublocationName,
        target.CheckpointName = source.CheckpointName,
        target.TaskName = source.TaskName,
        target.ExecutorName = source.ExecutorName,
        target.AuditorName = source.AuditorName,
        target.TaskCategory = source.TaskCategory,
        target.Priority = source.Priority,
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
        LocationId,
        SublocationId,
        [Checkpoint],
        [Task],
        LocationName,
        SublocationName,
        CheckpointName,
        TaskName,
        ExecutorName,
        AuditorName,
        TaskCategory,
        Priority,
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
        source.LocationId,
        source.SublocationId,
        source.[Checkpoint],
        source.[Task],
        source.LocationName,
        source.SublocationName,
        source.CheckpointName,
        source.TaskName,
        source.ExecutorName,
        source.AuditorName,
        source.TaskCategory,
        source.Priority,
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

MERGE ctl.SmartauditsAiCommentLookup AS target
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


def build_fact_mantiz_ticket_lifecycle_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE dwh.factMantizTicketLifecycle AS target
USING (
    SELECT
        OrganizationId,
        ClientId,
        BranchId,
        BranchName,
        TicketId,
        Folio,
        EquipmentId,
        EquipmentName,
        EquipmentCatalogName,
        FailureTypeId,
        FailureTypeName,
        SupplierId,
        SupplierName,
        TicketStatus,
        TicketPriority,
        TicketType,
        TicketSource,
        CreatedAt,
        ScheduledAt,
        ResolvedAt,
        ApprovedAt,
        CancelledAt,
        UpdatedAt,
        OpenToScheduledHours,
        ScheduledToResolvedHours,
        ResolvedToApprovedHours,
        OpenToResolvedHours,
        OpenToApprovedHours,
        OpenToCancelledHours,
        CurrentAgeHours,
        IsOpen,
        IsScheduled,
        IsResolved,
        IsApproved,
        IsCancelled,
        IsTerminal,
        HasEquipment,
        HasFailureType,
        HasPhoto,
        HasResolutionPhoto,
        SourceLastChangedAt
    FROM stg.factMantizTicketLifecycle
    WHERE RunId = ?
) AS source
    ON target.OrganizationId = source.OrganizationId
    AND target.TicketId = source.TicketId
WHEN MATCHED THEN
    UPDATE SET
        target.ClientId = source.ClientId,
        target.BranchId = source.BranchId,
        target.BranchName = source.BranchName,
        target.Folio = source.Folio,
        target.EquipmentId = source.EquipmentId,
        target.EquipmentName = source.EquipmentName,
        target.EquipmentCatalogName = source.EquipmentCatalogName,
        target.FailureTypeId = source.FailureTypeId,
        target.FailureTypeName = source.FailureTypeName,
        target.SupplierId = source.SupplierId,
        target.SupplierName = source.SupplierName,
        target.TicketStatus = source.TicketStatus,
        target.TicketPriority = source.TicketPriority,
        target.TicketType = source.TicketType,
        target.TicketSource = source.TicketSource,
        target.CreatedAt = source.CreatedAt,
        target.ScheduledAt = source.ScheduledAt,
        target.ResolvedAt = source.ResolvedAt,
        target.ApprovedAt = source.ApprovedAt,
        target.CancelledAt = source.CancelledAt,
        target.UpdatedAt = source.UpdatedAt,
        target.OpenToScheduledHours = source.OpenToScheduledHours,
        target.ScheduledToResolvedHours = source.ScheduledToResolvedHours,
        target.ResolvedToApprovedHours = source.ResolvedToApprovedHours,
        target.OpenToResolvedHours = source.OpenToResolvedHours,
        target.OpenToApprovedHours = source.OpenToApprovedHours,
        target.OpenToCancelledHours = source.OpenToCancelledHours,
        target.CurrentAgeHours = source.CurrentAgeHours,
        target.IsOpen = source.IsOpen,
        target.IsScheduled = source.IsScheduled,
        target.IsResolved = source.IsResolved,
        target.IsApproved = source.IsApproved,
        target.IsCancelled = source.IsCancelled,
        target.IsTerminal = source.IsTerminal,
        target.HasEquipment = source.HasEquipment,
        target.HasFailureType = source.HasFailureType,
        target.HasPhoto = source.HasPhoto,
        target.HasResolutionPhoto = source.HasResolutionPhoto,
        target.SourceLastChangedAt = source.SourceLastChangedAt,
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        OrganizationId,
        ClientId,
        BranchId,
        BranchName,
        TicketId,
        Folio,
        EquipmentId,
        EquipmentName,
        EquipmentCatalogName,
        FailureTypeId,
        FailureTypeName,
        SupplierId,
        SupplierName,
        TicketStatus,
        TicketPriority,
        TicketType,
        TicketSource,
        CreatedAt,
        ScheduledAt,
        ResolvedAt,
        ApprovedAt,
        CancelledAt,
        UpdatedAt,
        OpenToScheduledHours,
        ScheduledToResolvedHours,
        ResolvedToApprovedHours,
        OpenToResolvedHours,
        OpenToApprovedHours,
        OpenToCancelledHours,
        CurrentAgeHours,
        IsOpen,
        IsScheduled,
        IsResolved,
        IsApproved,
        IsCancelled,
        IsTerminal,
        HasEquipment,
        HasFailureType,
        HasPhoto,
        HasResolutionPhoto,
        SourceLastChangedAt
    )
    VALUES (
        source.OrganizationId,
        source.ClientId,
        source.BranchId,
        source.BranchName,
        source.TicketId,
        source.Folio,
        source.EquipmentId,
        source.EquipmentName,
        source.EquipmentCatalogName,
        source.FailureTypeId,
        source.FailureTypeName,
        source.SupplierId,
        source.SupplierName,
        source.TicketStatus,
        source.TicketPriority,
        source.TicketType,
        source.TicketSource,
        source.CreatedAt,
        source.ScheduledAt,
        source.ResolvedAt,
        source.ApprovedAt,
        source.CancelledAt,
        source.UpdatedAt,
        source.OpenToScheduledHours,
        source.ScheduledToResolvedHours,
        source.ResolvedToApprovedHours,
        source.OpenToResolvedHours,
        source.OpenToApprovedHours,
        source.OpenToCancelledHours,
        source.CurrentAgeHours,
        source.IsOpen,
        source.IsScheduled,
        source.IsResolved,
        source.IsApproved,
        source.IsCancelled,
        source.IsTerminal,
        source.HasEquipment,
        source.HasFailureType,
        source.HasPhoto,
        source.HasResolutionPhoto,
        source.SourceLastChangedAt
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_fact_mantiz_ticket_text_insights_replace_sql() -> str:
    return """
SET NOCOUNT ON;

DELETE target
FROM dwh.factMantizTicketTextInsights AS target
INNER JOIN (
    SELECT DISTINCT OrganizationId, TicketId
    FROM stg.MantizTicketTextInsightChangedTicket
    WHERE RunId = ?
) AS changed
    ON target.OrganizationId = changed.OrganizationId
    AND target.TicketId = changed.TicketId;

INSERT INTO dwh.factMantizTicketTextInsights (
    OrganizationId,
    TicketId,
    Folio,
    BranchId,
    EquipmentId,
    EquipmentName,
    FailureCode,
    FailureSubject,
    FailureProblem,
    SolutionCode,
    SolutionSubject,
    SolutionPartName,
    SparePartName,
    SparePartUnitPrice,
    SparePartCurrency,
    InsightStatus,
    ClassificationMethod,
    ClassifierConfidence,
    NeedsReview,
    InsightsJson,
    TextEvidenceJson,
    SourceLastChangedAt
)
SELECT
    OrganizationId,
    TicketId,
    Folio,
    BranchId,
    EquipmentId,
    EquipmentName,
    FailureCode,
    FailureSubject,
    FailureProblem,
    SolutionCode,
    SolutionSubject,
    SolutionPartName,
    SparePartName,
    SparePartUnitPrice,
    SparePartCurrency,
    InsightStatus,
    ClassificationMethod,
    ClassifierConfidence,
    NeedsReview,
    InsightsJson,
    TextEvidenceJson,
    SourceLastChangedAt
FROM stg.factMantizTicketTextInsights
WHERE RunId = ?;

SELECT @@ROWCOUNT AS AffectedRows;
"""


def build_mantiz_text_insight_review_queue_merge_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @MergeActions TABLE (MergeAction NVARCHAR(10) NOT NULL);

MERGE ctl.MantizTextInsightReviewQueue AS target
USING (
    SELECT
        InsightType,
        ConceptHash,
        CanonicalCode,
        CanonicalName,
        RepresentativeText,
        RepresentativeRawText,
        FirstSeenAt,
        LastSeenAt,
        CandidateCount,
        TicketCount,
        LastTicketId,
        SuggestedByMethod
    FROM stg.MantizTextInsightReviewQueue
    WHERE RunId = ?
) AS source
    ON target.InsightType = source.InsightType
    AND target.ConceptHash = source.ConceptHash
WHEN MATCHED THEN
    UPDATE SET
        target.CanonicalCode = COALESCE(target.ReviewedCanonicalCode, target.CanonicalCode, source.CanonicalCode),
        target.CanonicalName = COALESCE(target.ReviewedCanonicalName, target.CanonicalName, source.CanonicalName),
        target.RepresentativeText = COALESCE(target.RepresentativeText, source.RepresentativeText),
        target.RepresentativeRawText = COALESCE(target.RepresentativeRawText, source.RepresentativeRawText),
        target.CandidateCount =
            CASE
                WHEN target.LastSeenAt IS NULL THEN target.CandidateCount + source.CandidateCount
                WHEN source.LastSeenAt IS NULL THEN target.CandidateCount
                WHEN source.LastSeenAt > target.LastSeenAt THEN target.CandidateCount + source.CandidateCount
                ELSE target.CandidateCount
            END,
        target.TicketCount = source.TicketCount,
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
        target.LastTicketId = COALESCE(source.LastTicketId, target.LastTicketId),
        target.SuggestedByMethod = COALESCE(target.SuggestedByMethod, source.SuggestedByMethod),
        target.ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        InsightType,
        ConceptHash,
        CanonicalCode,
        CanonicalName,
        RepresentativeText,
        RepresentativeRawText,
        ConceptStatus,
        CandidateCount,
        TicketCount,
        FirstSeenAt,
        LastSeenAt,
        LastTicketId,
        SuggestedByMethod,
        ReviewStatus
    )
    VALUES (
        source.InsightType,
        source.ConceptHash,
        source.CanonicalCode,
        source.CanonicalName,
        source.RepresentativeText,
        source.RepresentativeRawText,
        N'CANDIDATE',
        source.CandidateCount,
        source.TicketCount,
        source.FirstSeenAt,
        source.LastSeenAt,
        source.LastTicketId,
        source.SuggestedByMethod,
        N'PENDING'
    )
OUTPUT $action INTO @MergeActions;

SELECT COUNT(1) AS AffectedRows FROM @MergeActions;
"""


def build_mantiz_text_insight_review_promotion_sql() -> str:
    return """
SET NOCOUNT ON;

DECLARE @PromotedRows INT = 0;

UPDATE ctl.MantizTextInsightReviewQueue
SET
    CanonicalCode = COALESCE(
        NULLIF(LTRIM(RTRIM(ReviewedCanonicalCode)), N''),
        CanonicalCode
    ),
    CanonicalName = COALESCE(
        NULLIF(LTRIM(RTRIM(ReviewedCanonicalName)), N''),
        NULLIF(LTRIM(RTRIM(ReviewedCanonicalCode)), N''),
        CanonicalName
    ),
    ConceptStatus = N'ACTIVE',
    ReviewStatus = N'PROMOTED',
    ReviewedAt = COALESCE(ReviewedAt, SYSUTCDATETIME()),
    ModifiedAt = SYSUTCDATETIME()
WHERE UPPER(ReviewStatus) = N'APPROVED'
AND COALESCE(
    NULLIF(LTRIM(RTRIM(ReviewedCanonicalCode)), N''),
    NULLIF(LTRIM(RTRIM(CanonicalCode)), N'')
) IS NOT NULL;

SET @PromotedRows = @PromotedRows + @@ROWCOUNT;

UPDATE ctl.MantizTextInsightReviewQueue
SET
    ConceptStatus = N'REJECTED',
    ReviewStatus = N'PROMOTED',
    ReviewedAt = COALESCE(ReviewedAt, SYSUTCDATETIME()),
    ModifiedAt = SYSUTCDATETIME()
WHERE UPPER(ReviewStatus) = N'REJECTED';

SET @PromotedRows = @PromotedRows + @@ROWCOUNT;

SELECT @PromotedRows AS AffectedRows;
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

    def get_min_open_sensor_event_time(
        self,
        sensor_type: Optional[str] = None,
    ) -> Optional[datetime]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    if sensor_type is None:
                        cursor.execute(
                            "SELECT MIN(LocalTimeSpan) AS OpenEventFrom "
                            "FROM dwh.factSensorEvents "
                            "WHERE IsEventCompleted = 0"
                        )
                    else:
                        cursor.execute(
                            "SELECT MIN(LocalTimeSpan) AS OpenEventFrom "
                            "FROM dwh.factSensorEvents "
                            "WHERE IsEventCompleted = 0 "
                            "AND SensorType = ?",
                            sensor_type,
                        )
                    row = cursor.fetchone()
                except pyodbc.Error:
                    return None
                if row is None:
                    return None
                return row.OpenEventFrom

    def delete_sensor_events_range(
        self,
        sensor_type: str,
        from_utc: datetime,
        to_utc: datetime,
    ) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dwh.factSensorEvents "
                    "WHERE SensorType = ? "
                    "AND LocalTimeSpan >= ? "
                    "AND LocalTimeSpan <= ?",
                    sensor_type,
                    from_utc,
                    to_utc,
                )
                deleted = cursor.rowcount
                conn.commit()
                if deleted is None or deleted < 0:
                    return 0
                return int(deleted)

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

    def clear_dim_sidon_prod_dimensions_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimSidonProdDimensions WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_dim_sidon_prod_dimensions(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_sidon_prod_dimensions_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_dim_aspnet_users_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.dimAspNetUsers WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_dim_aspnet_users(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_dim_aspnet_users_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_sidon_user_usage_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factSidonUserUsage WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_sidon_user_usage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_sidon_user_usage_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_sensor_events_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factSensorEvents WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_sensor_events(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_sensor_events_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_redings_audits_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factRedingsAudits WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_redings_audits(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(FACT_REDINGS_AUDITS_MERGE_SQL, run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_readings_measurement_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factReadingsMeasurement WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_readings_measurement(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_readings_measurement_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

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

    def promote_mantiz_text_insight_reviews(self) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_mantiz_text_insight_review_promotion_sql())
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

    def clear_fact_mantiz_ticket_lifecycle_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factMantizTicketLifecycle WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount
                conn.commit()
                return int(deleted or 0)

    def merge_fact_mantiz_ticket_lifecycle(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_fact_mantiz_ticket_lifecycle_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def clear_fact_mantiz_ticket_text_insights_stage(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM stg.factMantizTicketTextInsights WHERE RunId = ?",
                    run_id,
                )
                deleted = cursor.rowcount or 0
                cursor.execute(
                    "DELETE FROM stg.MantizTicketTextInsightDetail WHERE RunId = ?",
                    run_id,
                )
                detail_deleted = cursor.rowcount or 0
                cursor.execute(
                    "DELETE FROM stg.MantizTicketTextInsightChangedTicket WHERE RunId = ?",
                    run_id,
                )
                changed_deleted = cursor.rowcount or 0
                cursor.execute(
                    "DELETE FROM stg.MantizTextInsightReviewQueue WHERE RunId = ?",
                    run_id,
                )
                review_deleted = cursor.rowcount or 0
                conn.commit()
                return int(deleted + detail_deleted + changed_deleted + review_deleted)

    def replace_fact_mantiz_ticket_text_insights(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    build_fact_mantiz_ticket_text_insights_replace_sql(),
                    run_id,
                    run_id,
                )
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def merge_mantiz_text_insight_review_queue(self, run_id: str) -> int:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(build_mantiz_text_insight_review_queue_merge_sql(), run_id)
                row = cursor.fetchone()
                conn.commit()
                if row is None or row.AffectedRows is None:
                    return 0
                return int(row.AffectedRows)

    def get_active_mantiz_text_classifier_models(self) -> Dict[str, Dict[str, Any]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT InsightType, ModelVersion, ArtifactPath, Accuracy, MacroF1 "
                    "FROM ctl.MantizTextInsightClassifierModels "
                    "WHERE IsActive = 1"
                )
                rows = cursor.fetchall()
                return {
                    str(row.InsightType): {
                        "model_version": row.ModelVersion,
                        "artifact_path": row.ArtifactPath,
                        "accuracy": row.Accuracy,
                        "macro_f1": row.MacroF1,
                    }
                    for row in rows
                }

    def get_mantiz_text_classifier_training_samples(
        self,
        insight_type: str,
    ) -> list[tuple[str, str]]:
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COALESCE(queue.RepresentativeRawText, queue.RepresentativeText) AS RepresentativeText, "
                    "COALESCE(queue.ReviewedCanonicalCode, queue.CanonicalCode) AS Label "
                    "FROM ctl.MantizTextInsightReviewQueue AS queue "
                    "WHERE queue.InsightType = ? "
                    "AND ("
                    "UPPER(queue.ReviewStatus) IN (N'APPROVED', N'PROMOTED') "
                    "OR UPPER(queue.ConceptStatus) = N'ACTIVE'"
                    ") "
                    "AND UPPER(queue.ConceptStatus) <> N'REJECTED' "
                    "AND COALESCE(queue.ReviewedCanonicalCode, queue.CanonicalCode) IS NOT NULL "
                    "AND LTRIM(RTRIM(COALESCE(queue.ReviewedCanonicalCode, queue.CanonicalCode))) <> N'' "
                    "AND COALESCE(queue.RepresentativeRawText, queue.RepresentativeText) IS NOT NULL "
                    "AND LTRIM(RTRIM(COALESCE(queue.RepresentativeRawText, queue.RepresentativeText))) <> N''",
                    insight_type,
                )
                return [
                    (str(row.RepresentativeText), str(row.Label))
                    for row in cursor.fetchall()
                ]

    def register_mantiz_text_classifier_model(
        self,
        insight_type: str,
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
                        "UPDATE ctl.MantizTextInsightClassifierModels "
                        "SET IsActive = 0 "
                        "WHERE InsightType = ? "
                        "AND IsActive = 1",
                        insight_type,
                    )
                cursor.execute(
                    "MERGE ctl.MantizTextInsightClassifierModels AS target "
                    "USING (SELECT ? AS InsightType, ? AS ModelVersion) AS source "
                    "ON target.InsightType = source.InsightType "
                    "AND target.ModelVersion = source.ModelVersion "
                    "WHEN MATCHED THEN UPDATE SET "
                    "ArtifactPath = ?, Accuracy = ?, MacroF1 = ?, "
                    "TrainingRows = ?, IsActive = ?, "
                    "PromotedAt = CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE PromotedAt END "
                    "WHEN NOT MATCHED THEN INSERT "
                    "(InsightType, ModelVersion, ArtifactPath, Accuracy, MacroF1, TrainingRows, IsActive, PromotedAt) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END);",
                    insight_type,
                    model_version,
                    artifact_path,
                    accuracy,
                    macro_f1,
                    training_rows,
                    1 if promote else 0,
                    1 if promote else 0,
                    insight_type,
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
