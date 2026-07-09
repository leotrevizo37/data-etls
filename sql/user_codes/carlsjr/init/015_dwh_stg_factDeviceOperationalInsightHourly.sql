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

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'stg'
    )
    BEGIN
        EXEC ('CREATE SCHEMA stg');
    END;
GO

IF OBJECT_ID(N'stg.factDeviceOperationalInsightHourly', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factDeviceOperationalInsightHourly;
    END;
GO

IF OBJECT_ID(N'dwh.factDeviceOperationalInsightHourly', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factDeviceOperationalInsightHourly;
    END;
GO

CREATE TABLE dwh.factDeviceOperationalInsightHourly
(
    UserCode NVARCHAR(100) NOT NULL,
    DeviceId UNIQUEIDENTIFIER NOT NULL,
    LocationId UNIQUEIDENTIFIER NOT NULL,
    SubLocationId UNIQUEIDENTIFIER NOT NULL,
    DeviceType NVARCHAR(255) NULL,
    DeviceName NVARCHAR(255) NULL,
    LocalTimeSpan DATETIME2(0) NOT NULL,
    HealthScore DECIMAL(9, 4) NOT NULL,
    OperationalState NVARCHAR(50) NOT NULL,
    TrendDirection NVARCHAR(30) NOT NULL,
    ConfidenceScore DECIMAL(9, 4) NOT NULL,
    CoverageScore DECIMAL(9, 4) NOT NULL,
    ExpectedValueComplianceScore DECIMAL(9, 4) NULL,
    SensorReliabilityScore DECIMAL(9, 4) NOT NULL,
    EventStabilityScore DECIMAL(9, 4) NOT NULL,
    BehaviorStabilityScore DECIMAL(9, 4) NOT NULL,
    PeerAlignmentScore DECIMAL(9, 4) NOT NULL,
    FailureRiskScore DECIMAL(9, 6) NOT NULL,
    DominantReasonCode NVARCHAR(100) NOT NULL,
    RecommendedAction NVARCHAR(500) NOT NULL,
    EvidenceJson NVARCHAR(MAX) NOT NULL,
    FeatureSetVersion NVARCHAR(100) NOT NULL,
    ScoringVersion NVARCHAR(100) NOT NULL,
    ModelVersion NVARCHAR(100) NULL,
    CreatedAt DATETIME2(0) NOT NULL
    CONSTRAINT DF_factDeviceOperationalInsightHourly_CreatedAt
    DEFAULT (SYSUTCDATETIME()),
    ModifiedAt DATETIME2(0) NOT NULL
    CONSTRAINT DF_factDeviceOperationalInsightHourly_ModifiedAt
    DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_factDeviceOperationalInsightHourly
    PRIMARY KEY CLUSTERED (UserCode, DeviceId, LocationId, SubLocationId, LocalTimeSpan)
);
GO

CREATE TABLE stg.factDeviceOperationalInsightHourly
(
    RunId NVARCHAR(100) NOT NULL,
    UserCode NVARCHAR(100) NOT NULL,
    DeviceId UNIQUEIDENTIFIER NOT NULL,
    LocationId UNIQUEIDENTIFIER NOT NULL,
    SubLocationId UNIQUEIDENTIFIER NOT NULL,
    DeviceType NVARCHAR(255) NULL,
    DeviceName NVARCHAR(255) NULL,
    LocalTimeSpan DATETIME2(0) NOT NULL,
    HealthScore DECIMAL(9, 4) NOT NULL,
    OperationalState NVARCHAR(50) NOT NULL,
    TrendDirection NVARCHAR(30) NOT NULL,
    ConfidenceScore DECIMAL(9, 4) NOT NULL,
    CoverageScore DECIMAL(9, 4) NOT NULL,
    ExpectedValueComplianceScore DECIMAL(9, 4) NULL,
    SensorReliabilityScore DECIMAL(9, 4) NOT NULL,
    EventStabilityScore DECIMAL(9, 4) NOT NULL,
    BehaviorStabilityScore DECIMAL(9, 4) NOT NULL,
    PeerAlignmentScore DECIMAL(9, 4) NOT NULL,
    FailureRiskScore DECIMAL(9, 6) NOT NULL,
    DominantReasonCode NVARCHAR(100) NOT NULL,
    RecommendedAction NVARCHAR(500) NOT NULL,
    EvidenceJson NVARCHAR(MAX) NOT NULL,
    FeatureSetVersion NVARCHAR(100) NOT NULL,
    ScoringVersion NVARCHAR(100) NOT NULL,
    ModelVersion NVARCHAR(100) NULL
);
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'dwh'
            AND o.name = N'factDeviceOperationalInsightHourly'
            AND i.name = N'ix_factDeviceOperationalInsightHourly_local_state'
    )
    BEGIN
        CREATE INDEX ix_factDeviceOperationalInsightHourly_local_state
            ON dwh.factDeviceOperationalInsightHourly
            (UserCode, LocalTimeSpan, OperationalState, FailureRiskScore);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE
            s.name = N'stg'
            AND o.name = N'factDeviceOperationalInsightHourly'
            AND i.name = N'ix_stg_factDeviceOperationalInsightHourly_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factDeviceOperationalInsightHourly_run
            ON stg.factDeviceOperationalInsightHourly
            (RunId, UserCode, DeviceId, LocationId, SubLocationId, LocalTimeSpan);
    END;
GO
