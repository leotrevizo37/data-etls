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
        WHERE name = N'ctl'
    )
    BEGIN
        EXEC ('CREATE SCHEMA ctl');
    END;
GO

IF
    OBJECT_ID(N'dwh.factMantizTicketTextInsights', N'U') IS NOT NULL
    AND (
        COL_LENGTH(N'dwh.factMantizTicketTextInsights', N'FailureCode') IS NULL
        OR COL_LENGTH(N'dwh.factMantizTicketTextInsights', N'PrimaryFailureCode') IS NOT NULL
        OR COL_LENGTH(N'dwh.factMantizTicketTextInsights', N'FailureConceptHash') IS NOT NULL
    )
    BEGIN
        IF OBJECT_ID(N'ctl.MantizTicketTextInsightLegacyDetail', N'U') IS NULL
            BEGIN
                SELECT *
                INTO ctl.MantizTicketTextInsightLegacyDetail
                FROM dwh.factMantizTicketTextInsights;
            END;

        DROP TABLE dwh.factMantizTicketTextInsights;
    END;
GO

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
            FailureCode NVARCHAR(100) NULL,
            FailureSubject NVARCHAR(255) NULL,
            FailureProblem NVARCHAR(100) NULL,
            SolutionCode NVARCHAR(100) NULL,
            SolutionSubject NVARCHAR(255) NULL,
            SolutionPartName NVARCHAR(255) NULL,
            SparePartName NVARCHAR(255) NULL,
            SparePartUnitPrice DECIMAL(18, 4) NULL,
            SparePartCurrency NVARCHAR(10) NULL,
            InsightStatus NVARCHAR(30) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_InsightStatus
            DEFAULT (N'SIN_CLASIFICAR'),
            ClassificationMethod NVARCHAR(50) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_ClassificationMethod
            DEFAULT (N'SIN_CLASIFICAR'),
            ClassifierConfidence DECIMAL(9, 6) NULL,
            NeedsReview BIT NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_NeedsReview
            DEFAULT (1),
            InsightsJson NVARCHAR(MAX) NULL,
            TextEvidenceJson NVARCHAR(MAX) NULL,
            SourceLastChangedAt DATETIME2(0) NOT NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factMantizTicketTextInsights_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factMantizTicketTextInsights
            PRIMARY KEY CLUSTERED (OrganizationId, TicketId)
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
            AND i.name = N'ix_factMantizTicketTextInsights_source_changed'
    )
    BEGIN
        CREATE INDEX ix_factMantizTicketTextInsights_source_changed
            ON dwh.factMantizTicketTextInsights (SourceLastChangedAt);
    END;
GO
