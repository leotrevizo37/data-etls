IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'ctl'
    )
    BEGIN
        EXEC ('CREATE SCHEMA ctl');
    END;
GO

IF OBJECT_ID(N'ctl.SenseDeviceOperationalFeatureSets', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SenseDeviceOperationalFeatureSets
        (
            FeatureSetVersion NVARCHAR(100) NOT NULL,
            UserCode NVARCHAR(100) NOT NULL,
            FeatureSchemaJson NVARCHAR(MAX) NOT NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SenseDeviceOperationalFeatureSets_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_SenseDeviceOperationalFeatureSets
            PRIMARY KEY CLUSTERED (UserCode, FeatureSetVersion)
        );
    END;
GO

IF OBJECT_ID(N'ctl.SenseDeviceOperationalModels', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SenseDeviceOperationalModels
        (
            UserCode NVARCHAR(100) NOT NULL,
            ModelScope NVARCHAR(100) NOT NULL,
            ModelVersion NVARCHAR(100) NOT NULL,
            ModelFamily NVARCHAR(100) NOT NULL,
            ArtifactPath NVARCHAR(1000) NOT NULL,
            FeatureSetVersion NVARCHAR(100) NOT NULL,
            ScoringVersion NVARCHAR(100) NOT NULL,
            TrainingWindowStart DATETIME2(0) NOT NULL,
            TrainingWindowEnd DATETIME2(0) NOT NULL,
            TrainingRows INT NOT NULL,
            ValidationRows INT NOT NULL,
            TestRows INT NOT NULL,
            Accuracy DECIMAL(9, 6) NULL,
            MacroF1 DECIMAL(9, 6) NULL,
            WeightedF1 DECIMAL(9, 6) NULL,
            CriticalRecall DECIMAL(9, 6) NULL,
            CriticalPrecision DECIMAL(9, 6) NULL,
            CalibrationError DECIMAL(9, 6) NULL,
            MetricsJson NVARCHAR(MAX) NOT NULL,
            FeatureSchemaJson NVARCHAR(MAX) NOT NULL,
            TrainingDistributionJson NVARCHAR(MAX) NOT NULL,
            Status NVARCHAR(30) NOT NULL,
            IsActive BIT NOT NULL
            CONSTRAINT DF_SenseDeviceOperationalModels_IsActive
            DEFAULT (0),
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SenseDeviceOperationalModels_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            PromotedAt DATETIME2(0) NULL,
            PromotedBy NVARCHAR(255) NULL,
            CONSTRAINT PK_SenseDeviceOperationalModels
            PRIMARY KEY CLUSTERED (UserCode, ModelScope, ModelVersion)
        );
    END;
GO

IF OBJECT_ID(N'ctl.SenseDeviceInsightReviewQueue', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SenseDeviceInsightReviewQueue
        (
            ReviewId UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT DF_SenseDeviceInsightReviewQueue_ReviewId
            DEFAULT (NEWID()),
            UserCode NVARCHAR(100) NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            LocalDate DATE NOT NULL,
            OperationalState NVARCHAR(50) NOT NULL,
            DominantReasonCode NVARCHAR(100) NOT NULL,
            RecommendedAction NVARCHAR(500) NOT NULL,
            ConfidenceScore DECIMAL(9, 4) NOT NULL,
            FailureRiskScore DECIMAL(9, 6) NOT NULL,
            EvidenceJson NVARCHAR(MAX) NOT NULL,
            ReviewStatus NVARCHAR(30) NOT NULL
            CONSTRAINT DF_SenseDeviceInsightReviewQueue_ReviewStatus
            DEFAULT (N'PENDING'),
            ReviewedState NVARCHAR(50) NULL,
            ReviewedReasonCode NVARCHAR(100) NULL,
            ReviewerNotes NVARCHAR(2000) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SenseDeviceInsightReviewQueue_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ReviewedAt DATETIME2(0) NULL,
            CONSTRAINT PK_SenseDeviceInsightReviewQueue
            PRIMARY KEY CLUSTERED (ReviewId)
        );
    END;
GO

IF OBJECT_ID(N'ctl.SenseDeviceOperationalScoringRuns', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SenseDeviceOperationalScoringRuns
        (
            RunId NVARCHAR(100) NOT NULL,
            UserCode NVARCHAR(100) NOT NULL,
            WindowStart DATETIME2(0) NOT NULL,
            WindowEnd DATETIME2(0) NOT NULL,
            FeatureSetVersion NVARCHAR(100) NOT NULL,
            ScoringVersion NVARCHAR(100) NOT NULL,
            ModelVersion NVARCHAR(100) NULL,
            ProcessedRows INT NOT NULL,
            HourlyRows INT NOT NULL,
            DailyRows INT NOT NULL,
            NormalCount INT NOT NULL,
            WatchCount INT NOT NULL,
            DegradedCount INT NOT NULL,
            CriticalCount INT NOT NULL,
            DataUnreliableCount INT NOT NULL,
            ReviewQueueRows INT NOT NULL,
            MetricsJson NVARCHAR(MAX) NOT NULL,
            StartedAt DATETIME2(0) NOT NULL,
            CompletedAt DATETIME2(0) NULL,
            Succeeded BIT NOT NULL,
            ErrorMessage NVARCHAR(MAX) NULL,
            CONSTRAINT PK_SenseDeviceOperationalScoringRuns
            PRIMARY KEY CLUSTERED (UserCode, RunId)
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
            s.name = N'ctl'
            AND o.name = N'SenseDeviceInsightReviewQueue'
            AND i.name = N'ix_SenseDeviceInsightReviewQueue_status'
    )
    BEGIN
        CREATE INDEX ix_SenseDeviceInsightReviewQueue_status
            ON ctl.SenseDeviceInsightReviewQueue
            (UserCode, ReviewStatus, LocalDate, FailureRiskScore);
    END;
GO
