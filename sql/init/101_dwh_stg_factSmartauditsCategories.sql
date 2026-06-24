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

IF OBJECT_ID(N'dwh.factSmartauditsCategories', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factSmartauditsCategories
        (
            PlanResultId BIGINT NOT NULL,
            WorkPlanId BIGINT NOT NULL,
            EvidencePhotoId BIGINT NULL,
            LocationId NVARCHAR(100) NULL,
            SublocationId NVARCHAR(100) NULL,
            [Checkpoint] NVARCHAR(100) NULL,
            [Task] NVARCHAR(100) NULL,
            LocationName NVARCHAR(500) NULL,
            SublocationName NVARCHAR(500) NULL,
            CheckpointName NVARCHAR(500) NULL,
            TaskName NVARCHAR(500) NULL,
            TaskCategory NVARCHAR(255) NULL,
            Priority NVARCHAR(255) NULL,
            ResultCategory NVARCHAR(100) NOT NULL,
            AiResult TINYINT NULL,
            ReviewDate DATETIME2(0) NULL,
            ReviewAIDate DATETIME2(0) NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL,
            ClassificationMethod NVARCHAR(50) NULL,
            ClassifierModelVersion NVARCHAR(100) NULL,
            ClassifierConfidence DECIMAL(9, 6) NULL,
            FailedEvidenceCount INT NOT NULL
            CONSTRAINT DF_factSmartauditsCategories_FailedEvidenceCount
            DEFAULT (0),
            EvidenceCount INT NOT NULL
            CONSTRAINT DF_factSmartauditsCategories_EvidenceCount
            DEFAULT (0),
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factSmartauditsCategories_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factSmartauditsCategories_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factSmartauditsCategories
            PRIMARY KEY CLUSTERED (PlanResultId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF OBJECT_ID(N'stg.factSmartauditsCategories', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factSmartauditsCategories
        (
            RunId NVARCHAR(100) NOT NULL,
            PlanResultId BIGINT NOT NULL,
            WorkPlanId BIGINT NOT NULL,
            EvidencePhotoId BIGINT NULL,
            LocationId NVARCHAR(100) NULL,
            SublocationId NVARCHAR(100) NULL,
            [Checkpoint] NVARCHAR(100) NULL,
            [Task] NVARCHAR(100) NULL,
            LocationName NVARCHAR(500) NULL,
            SublocationName NVARCHAR(500) NULL,
            CheckpointName NVARCHAR(500) NULL,
            TaskName NVARCHAR(500) NULL,
            TaskCategory NVARCHAR(255) NULL,
            Priority NVARCHAR(255) NULL,
            ResultCategory NVARCHAR(100) NOT NULL,
            AiResult TINYINT NULL,
            ReviewDate DATETIME2(0) NULL,
            ReviewAIDate DATETIME2(0) NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL,
            ClassificationMethod NVARCHAR(50) NULL,
            ClassifierModelVersion NVARCHAR(100) NULL,
            ClassifierConfidence DECIMAL(9, 6) NULL,
            FailedEvidenceCount INT NOT NULL,
            EvidenceCount INT NOT NULL
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
            AND o.name = N'factSmartauditsCategories'
            AND i.name = N'ix_factSmartauditsCategories_source_changed'
    )
    BEGIN
        CREATE INDEX ix_factSmartauditsCategories_source_changed
            ON dwh.factSmartauditsCategories (SourceLastChangedAt);
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
            AND o.name = N'factSmartauditsCategories'
            AND i.name = N'ix_stg_factSmartauditsCategories_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factSmartauditsCategories_run
            ON stg.factSmartauditsCategories (RunId, PlanResultId);
    END;
GO
