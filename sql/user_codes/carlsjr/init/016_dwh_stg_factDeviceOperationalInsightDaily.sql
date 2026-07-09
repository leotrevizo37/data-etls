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

IF OBJECT_ID(N'dwh.factDeviceOperationalInsightDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factDeviceOperationalInsightDaily
        (
            UserCode NVARCHAR(100) NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            DeviceType NVARCHAR(255) NULL,
            DeviceName NVARCHAR(255) NULL,
            LocalDate DATE NOT NULL,
            HealthScore DECIMAL(9, 4) NOT NULL,
            WorstHourlyState NVARCHAR(50) NOT NULL,
            OperationalState NVARCHAR(50) NOT NULL,
            CriticalHours INT NOT NULL,
            WatchHours INT NOT NULL,
            DegradedHours INT NOT NULL,
            EventMinutes DECIMAL(19, 4) NOT NULL,
            OpenEvents INT NOT NULL,
            SevenDayHealthScore DECIMAL(9, 4) NOT NULL,
            ThirtyDayHealthScore DECIMAL(9, 4) NOT NULL,
            TrendDirection NVARCHAR(30) NOT NULL,
            ConfidenceScore DECIMAL(9, 4) NOT NULL,
            PeerPercentileRisk DECIMAL(9, 4) NOT NULL,
            FailureRiskScore DECIMAL(9, 6) NOT NULL,
            DominantReasonCode NVARCHAR(100) NOT NULL,
            RecommendedAction NVARCHAR(500) NOT NULL,
            EvidenceJson NVARCHAR(MAX) NOT NULL,
            FeatureSetVersion NVARCHAR(100) NOT NULL,
            ScoringVersion NVARCHAR(100) NOT NULL,
            ModelVersion NVARCHAR(100) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factDeviceOperationalInsightDaily_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factDeviceOperationalInsightDaily_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factDeviceOperationalInsightDaily
            PRIMARY KEY CLUSTERED (UserCode, DeviceId, LocationId, SubLocationId, LocalDate)
        );
    END;
GO

IF OBJECT_ID(N'stg.factDeviceOperationalInsightDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factDeviceOperationalInsightDaily
        (
            RunId NVARCHAR(100) NOT NULL,
            UserCode NVARCHAR(100) NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            DeviceType NVARCHAR(255) NULL,
            DeviceName NVARCHAR(255) NULL,
            LocalDate DATE NOT NULL,
            HealthScore DECIMAL(9, 4) NOT NULL,
            WorstHourlyState NVARCHAR(50) NOT NULL,
            OperationalState NVARCHAR(50) NOT NULL,
            CriticalHours INT NOT NULL,
            WatchHours INT NOT NULL,
            DegradedHours INT NOT NULL,
            EventMinutes DECIMAL(19, 4) NOT NULL,
            OpenEvents INT NOT NULL,
            SevenDayHealthScore DECIMAL(9, 4) NOT NULL,
            ThirtyDayHealthScore DECIMAL(9, 4) NOT NULL,
            TrendDirection NVARCHAR(30) NOT NULL,
            ConfidenceScore DECIMAL(9, 4) NOT NULL,
            PeerPercentileRisk DECIMAL(9, 4) NOT NULL,
            FailureRiskScore DECIMAL(9, 6) NOT NULL,
            DominantReasonCode NVARCHAR(100) NOT NULL,
            RecommendedAction NVARCHAR(500) NOT NULL,
            EvidenceJson NVARCHAR(MAX) NOT NULL,
            FeatureSetVersion NVARCHAR(100) NOT NULL,
            ScoringVersion NVARCHAR(100) NOT NULL,
            ModelVersion NVARCHAR(100) NULL
        );
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
            s.name = N'dwh'
            AND o.name = N'factDeviceOperationalInsightDaily'
            AND i.name = N'ix_factDeviceOperationalInsightDaily_date_state'
    )
    BEGIN
        CREATE INDEX ix_factDeviceOperationalInsightDaily_date_state
            ON dwh.factDeviceOperationalInsightDaily
            (UserCode, LocalDate, OperationalState, FailureRiskScore);
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
            AND o.name = N'factDeviceOperationalInsightDaily'
            AND i.name = N'ix_stg_factDeviceOperationalInsightDaily_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factDeviceOperationalInsightDaily_run
            ON stg.factDeviceOperationalInsightDaily
            (RunId, UserCode, DeviceId, LocationId, SubLocationId, LocalDate);
    END;
GO
