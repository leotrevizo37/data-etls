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

IF OBJECT_ID(N'ctl.MantizTextInsightReviewQueue', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.MantizTextInsightReviewQueue
        (
            InsightType NVARCHAR(30) NOT NULL,
            ConceptHash CHAR(64) NOT NULL,
            CanonicalCode NVARCHAR(100) NULL,
            CanonicalName NVARCHAR(255) NULL,
            RepresentativeText NVARCHAR(500) NULL,
            RepresentativeRawText NVARCHAR(MAX) NULL,
            ConceptStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_ConceptStatus
            DEFAULT (N'CANDIDATE'),
            CandidateCount INT NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_CandidateCount
            DEFAULT (0),
            TicketCount INT NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_TicketCount
            DEFAULT (0),
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            LastTicketId NVARCHAR(100) NULL,
            SuggestedByMethod NVARCHAR(50) NULL,
            ReviewStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_ReviewStatus
            DEFAULT (N'PENDING'),
            ReviewedCanonicalCode NVARCHAR(100) NULL,
            ReviewedCanonicalName NVARCHAR(255) NULL,
            ReviewedBy NVARCHAR(255) NULL,
            ReviewedAt DATETIME2(0) NULL,
            RejectionReason NVARCHAR(500) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_MantizTextInsightReviewQueue_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_MantizTextInsightReviewQueue
            PRIMARY KEY CLUSTERED (InsightType, ConceptHash)
        );
    END;
GO

IF
    OBJECT_ID(N'ctl.MantizTextInsightReviewQueue', N'U') IS NOT NULL
    AND COL_LENGTH(N'ctl.MantizTextInsightReviewQueue', N'RepresentativeRawText') IS NULL
    BEGIN
        ALTER TABLE ctl.MantizTextInsightReviewQueue
        ADD RepresentativeRawText NVARCHAR(MAX) NULL;
    END;
GO
