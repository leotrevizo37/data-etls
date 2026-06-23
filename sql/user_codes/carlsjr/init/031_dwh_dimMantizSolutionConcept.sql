IF OBJECT_ID(N'dwh.dimMantizSolutionConcept', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimMantizSolutionConcept
        (
            SolutionConceptHash CHAR(64) NOT NULL,
            CanonicalSolutionCode NVARCHAR(100) NOT NULL,
            CanonicalSolutionName NVARCHAR(255) NOT NULL,
            RepresentativeText NVARCHAR(500) NULL,
            ConceptStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_dimMantizSolutionConcept_ConceptStatus
            DEFAULT (N'CANDIDATE'),
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            TicketCount INT NOT NULL
            CONSTRAINT DF_dimMantizSolutionConcept_TicketCount
            DEFAULT (0),
            LastTicketId NVARCHAR(100) NULL,
            CreatedByMethod NVARCHAR(50) NULL,
            LoadedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizSolutionConcept_LoadedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizSolutionConcept_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimMantizSolutionConcept
            PRIMARY KEY CLUSTERED (SolutionConceptHash)
        );
    END;
GO
