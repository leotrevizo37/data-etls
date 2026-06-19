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

IF OBJECT_ID(N'ctl.SmartauditsAiCommentReviewQueue', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SmartauditsAiCommentReviewQueue
        (
            NormalizedCommentHash CHAR(64) NOT NULL,
            AiResult TINYINT NOT NULL,
            NormalizedComment NVARCHAR(2000) NOT NULL,
            SampleComment NVARCHAR(2000) NULL,
            CandidateCount INT NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentReviewQueue_CandidateCount
            DEFAULT (0),
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            LastPlanResultId BIGINT NULL,
            LastEvidencePhotoId BIGINT NULL,
            SuggestedCategory NVARCHAR(100) NULL,
            SuggestedMethod NVARCHAR(50) NULL,
            SuggestedConfidence DECIMAL(9, 6) NULL,
            ReviewStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentReviewQueue_ReviewStatus
            DEFAULT (N'PENDING'),
            ReviewedResultCategory NVARCHAR(100) NULL,
            ReviewedBy NVARCHAR(255) NULL,
            ReviewedAt DATETIME2(0) NULL,
            ReviewNotes NVARCHAR(1000) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentReviewQueue_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SmartauditsAiCommentReviewQueue_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_SmartauditsAiCommentReviewQueue
            PRIMARY KEY CLUSTERED (NormalizedCommentHash, AiResult)
        );
    END;
GO

IF OBJECT_ID(N'stg.SmartauditsAiCommentReviewQueue', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.SmartauditsAiCommentReviewQueue
        (
            RunId NVARCHAR(100) NOT NULL,
            NormalizedCommentHash CHAR(64) NOT NULL,
            AiResult TINYINT NOT NULL,
            NormalizedComment NVARCHAR(2000) NOT NULL,
            SampleComment NVARCHAR(2000) NULL,
            CandidateCount INT NOT NULL,
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            LastPlanResultId BIGINT NULL,
            LastEvidencePhotoId BIGINT NULL,
            SuggestedCategory NVARCHAR(100) NULL,
            SuggestedMethod NVARCHAR(50) NULL,
            SuggestedConfidence DECIMAL(9, 6) NULL
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
            AND o.name = N'SmartauditsAiCommentReviewQueue'
            AND i.name = N'ix_SmartauditsAiCommentReviewQueue_status'
    )
    BEGIN
        CREATE INDEX ix_SmartauditsAiCommentReviewQueue_status
            ON ctl.SmartauditsAiCommentReviewQueue (ReviewStatus, LastSeenAt);
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
            AND o.name = N'SmartauditsAiCommentReviewQueue'
            AND i.name = N'ix_stg_SmartauditsAiCommentReviewQueue_run'
    )
    BEGIN
        CREATE INDEX ix_stg_SmartauditsAiCommentReviewQueue_run
            ON stg.SmartauditsAiCommentReviewQueue (RunId, NormalizedCommentHash, AiResult);
    END;
GO
