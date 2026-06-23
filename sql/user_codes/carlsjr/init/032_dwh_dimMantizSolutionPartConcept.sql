IF OBJECT_ID(N'dwh.dimMantizSolutionPartConcept', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimMantizSolutionPartConcept
        (
            SolutionPartConceptHash CHAR(64) NOT NULL,
            CanonicalSolutionPartName NVARCHAR(255) NOT NULL,
            RepresentativeText NVARCHAR(500) NULL,
            ConceptStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_dimMantizSolutionPartConcept_ConceptStatus
            DEFAULT (N'CANDIDATE'),
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            TicketCount INT NOT NULL
            CONSTRAINT DF_dimMantizSolutionPartConcept_TicketCount
            DEFAULT (0),
            LastTicketId NVARCHAR(100) NULL,
            CreatedByMethod NVARCHAR(50) NULL,
            LoadedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizSolutionPartConcept_LoadedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizSolutionPartConcept_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimMantizSolutionPartConcept
            PRIMARY KEY CLUSTERED (SolutionPartConceptHash)
        );
    END;
GO
