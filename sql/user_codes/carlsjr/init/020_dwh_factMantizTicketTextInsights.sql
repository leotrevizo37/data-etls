IF OBJECT_ID(N'dwh.factMantizTicketTextInsights', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factMantizTicketTextInsights
        (
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
            ExtractionMethod NVARCHAR(50) NOT NULL,
            ExtractionConfidence DECIMAL(9, 6) NULL,
            IsNewConceptCandidate BIT NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_IsNewConceptCandidate
            DEFAULT (1),
            SourceLastChangedAt DATETIME2(0) NOT NULL,
            LoadedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_LoadedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factMantizTicketTextInsights
            PRIMARY KEY CLUSTERED
            (
                OrganizationId,
                TicketId,
                SourceType,
                SourceId,
                InsightType,
                InsightPhraseHash
            )
            WITH (IGNORE_DUP_KEY = ON)
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
            AND o.name = N'factMantizTicketTextInsights'
            AND i.name = N'ix_factMantizTicketTextInsights_ticket'
    )
    BEGIN
        CREATE INDEX ix_factMantizTicketTextInsights_ticket
            ON dwh.factMantizTicketTextInsights (OrganizationId, TicketId);
    END;
GO
