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

IF OBJECT_ID(N'ctl.SmartauditsAiCommentLookup', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SmartauditsAiCommentLookup
        (
            NormalizedCommentHash CHAR(64) NOT NULL,
            NormalizedComment NVARCHAR(2000) NOT NULL,
            AiResult TINYINT NOT NULL,
            ResultCategory NVARCHAR(100) NOT NULL,
            ClassificationMethod NVARCHAR(50) NOT NULL,
            ClassifierModelVersion NVARCHAR(100) NULL,
            ClassifierConfidence DECIMAL(9, 6) NULL,
            ClassifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentLookup_ClassifiedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentLookup_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_SmartauditsAiCommentLookup
            PRIMARY KEY CLUSTERED (NormalizedCommentHash, AiResult)
        );
    END;
GO
