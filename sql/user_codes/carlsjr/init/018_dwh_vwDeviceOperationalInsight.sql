IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'dwh'
    )
    BEGIN
        EXEC ('CREATE SCHEMA dwh');
    END;
GO

CREATE OR ALTER VIEW dwh.vwDeviceOperationalInsight
AS
SELECT
    daily.UserCode,
    daily.DeviceId,
    daily.LocationId,
    daily.SubLocationId,
    daily.DeviceType,
    daily.DeviceName,
    daily.LocalDate,
    daily.HealthScore,
    daily.OperationalState,
    daily.WorstHourlyState,
    daily.TrendDirection,
    daily.ConfidenceScore,
    daily.FailureRiskScore,
    daily.DominantReasonCode,
    daily.RecommendedAction,
    daily.CriticalHours,
    daily.DegradedHours,
    daily.WatchHours,
    daily.EventMinutes,
    daily.OpenEvents,
    daily.SevenDayHealthScore,
    daily.ThirtyDayHealthScore,
    daily.PeerPercentileRisk,
    daily.EvidenceJson,
    daily.FeatureSetVersion,
    daily.ScoringVersion,
    daily.ModelVersion,
    daily.ModifiedAt
FROM dwh.factDeviceOperationalInsightDaily AS daily;
GO
