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

IF OBJECT_ID(N'stg.MantizTextInsightReviewQueue', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.MantizTextInsightReviewQueue
        (
            RunId NVARCHAR(100) NOT NULL,
            InsightType NVARCHAR(30) NOT NULL,
            ConceptHash CHAR(64) NOT NULL,
            CanonicalCode NVARCHAR(100) NULL,
            CanonicalName NVARCHAR(255) NULL,
            RepresentativeText NVARCHAR(500) NULL,
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            CandidateCount INT NOT NULL,
            TicketCount INT NOT NULL,
            LastTicketId NVARCHAR(100) NULL,
            SuggestedByMethod NVARCHAR(50) NULL
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
            s.name = N'stg'
            AND o.name = N'MantizTextInsightReviewQueue'
            AND i.name = N'ix_stg_MantizTextInsightReviewQueue_run'
    )
    BEGIN
        CREATE INDEX ix_stg_MantizTextInsightReviewQueue_run
            ON stg.MantizTextInsightReviewQueue (RunId, InsightType, ConceptHash);
    END;
GO
