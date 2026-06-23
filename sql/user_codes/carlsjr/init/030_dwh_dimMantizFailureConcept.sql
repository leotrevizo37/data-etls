IF OBJECT_ID(N'dwh.dimMantizFailureConcept', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimMantizFailureConcept
        (
            FailureConceptHash CHAR(64) NOT NULL,
            CanonicalFailureCode NVARCHAR(100) NOT NULL,
            CanonicalFailureName NVARCHAR(255) NOT NULL,
            RepresentativeText NVARCHAR(500) NULL,
            ConceptStatus NVARCHAR(20) NOT NULL
            CONSTRAINT DF_dimMantizFailureConcept_ConceptStatus
            DEFAULT (N'CANDIDATE'),
            FirstSeenAt DATETIME2(0) NULL,
            LastSeenAt DATETIME2(0) NULL,
            TicketCount INT NOT NULL
            CONSTRAINT DF_dimMantizFailureConcept_TicketCount
            DEFAULT (0),
            LastTicketId NVARCHAR(100) NULL,
            CreatedByMethod NVARCHAR(50) NULL,
            LoadedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizFailureConcept_LoadedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimMantizFailureConcept_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimMantizFailureConcept
            PRIMARY KEY CLUSTERED (FailureConceptHash)
        );
    END;
GO
