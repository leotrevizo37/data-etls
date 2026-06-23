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

IF
    OBJECT_ID(N'stg.factMantizTicketTextInsights', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factMantizTicketTextInsights
        (
            RunId NVARCHAR(100) NOT NULL,
            OrganizationId NVARCHAR(100) NOT NULL,
            TicketId NVARCHAR(100) NOT NULL,
            Folio NVARCHAR(191) NULL,
            BranchId NVARCHAR(100) NULL,
            EquipmentId NVARCHAR(100) NULL,
            EquipmentName NVARCHAR(255) NULL,
            FailureCode NVARCHAR(100) NULL,
            FailureSubject NVARCHAR(255) NULL,
            FailureProblem NVARCHAR(100) NULL,
            SolutionCode NVARCHAR(100) NULL,
            SolutionSubject NVARCHAR(255) NULL,
            SolutionPartName NVARCHAR(255) NULL,
            SparePartName NVARCHAR(255) NULL,
            SparePartUnitPrice DECIMAL(18, 4) NULL,
            SparePartCurrency NVARCHAR(10) NULL,
            InsightStatus NVARCHAR(30) NOT NULL,
            ClassificationMethod NVARCHAR(50) NOT NULL,
            ClassifierConfidence DECIMAL(9, 6) NULL,
            NeedsReview BIT NOT NULL,
            InsightsJson NVARCHAR(MAX) NULL,
            TextEvidenceJson NVARCHAR(MAX) NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL
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
            AND o.name = N'factMantizTicketTextInsights'
            AND i.name = N'ix_stg_factMantizTicketTextInsights_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factMantizTicketTextInsights_run
            ON stg.factMantizTicketTextInsights (RunId, OrganizationId, TicketId);
    END;
GO
