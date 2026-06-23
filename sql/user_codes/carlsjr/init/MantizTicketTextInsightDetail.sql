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

IF OBJECT_ID(N'stg.MantizTicketTextInsightDetail', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.MantizTicketTextInsightDetail
        (
            RunId NVARCHAR(100) NOT NULL,
            OrganizationId NVARCHAR(100) NOT NULL,
            TicketId NVARCHAR(100) NOT NULL,
            Folio NVARCHAR(191) NULL,
            BranchId NVARCHAR(100) NULL,
            EquipmentId NVARCHAR(100) NULL,
            EquipmentName NVARCHAR(255) NULL,
            SourceType NVARCHAR(50) NOT NULL,
            SourceId NVARCHAR(100) NOT NULL,
            SourceCreatedAt DATETIME2(0) NULL,
            SourceUpdatedAt DATETIME2(0) NULL,
            RawTextExcerpt NVARCHAR(2000) NULL,
            RawText NVARCHAR(MAX) NULL,
            NormalizedText NVARCHAR(2000) NULL,
            InsightType NVARCHAR(30) NOT NULL,
            InsightPhraseHash CHAR(64) NOT NULL,
            ExtractedEquipmentText NVARCHAR(255) NULL,
            ExtractedFailurePhrase NVARCHAR(500) NULL,
            NormalizedFailurePhrase NVARCHAR(500) NULL,
            FailurePhraseHash CHAR(64) NULL,
            FailureConceptHash CHAR(64) NULL,
            CanonicalFailureCode NVARCHAR(100) NULL,
            FailureConceptStatus NVARCHAR(20) NULL,
            ExtractedSolutionPhrase NVARCHAR(500) NULL,
            NormalizedSolutionPhrase NVARCHAR(500) NULL,
            SolutionPhraseHash CHAR(64) NULL,
            SolutionConceptHash CHAR(64) NULL,
            CanonicalSolutionCode NVARCHAR(100) NULL,
            SolutionConceptStatus NVARCHAR(20) NULL,
            ExtractedSolutionPartText NVARCHAR(255) NULL,
            NormalizedSolutionPartText NVARCHAR(255) NULL,
            SolutionPartHash CHAR(64) NULL,
            SolutionPartConceptHash CHAR(64) NULL,
            CanonicalSolutionPartName NVARCHAR(255) NULL,
            SolutionPartConceptStatus NVARCHAR(20) NULL,
            ExtractedInsightSubject NVARCHAR(255) NULL,
            ExtractedProblemQualifier NVARCHAR(100) NULL,
            NormalizedProblemQualifier NVARCHAR(100) NULL,
            ExtractionMethod NVARCHAR(50) NOT NULL,
            ExtractionConfidence DECIMAL(9, 6) NULL,
            IsNewConceptCandidate BIT NOT NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL
        );
    END;
GO

IF
    OBJECT_ID(N'stg.MantizTicketTextInsightDetail', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.MantizTicketTextInsightDetail', N'RawText') IS NULL
    BEGIN
        ALTER TABLE stg.MantizTicketTextInsightDetail
        ADD RawText NVARCHAR(MAX) NULL;
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
            AND o.name = N'MantizTicketTextInsightDetail'
            AND i.name = N'ix_stg_MantizTicketTextInsightDetail_run'
    )
    BEGIN
        CREATE INDEX ix_stg_MantizTicketTextInsightDetail_run
            ON stg.MantizTicketTextInsightDetail
            (
                RunId,
                OrganizationId,
                TicketId,
                InsightType,
                InsightPhraseHash
            );
    END;
GO
