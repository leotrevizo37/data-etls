IF OBJECT_ID(N'dwh.dimIngestionFactLecturas', N'U') IS NULL
BEGIN
    CREATE TABLE dwh.dimIngestionFactLecturas
    (
        ControlIngestionId INT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_dimIngestionFactLecturas PRIMARY KEY,
        IngestionName SYSNAME NOT NULL
            CONSTRAINT UQ_dimIngestionFactLecturas_IngestionName UNIQUE,
        LastLoadedAt DATETIME2(3) NULL,
        LastRunStatus VARCHAR(20) NOT NULL
            CONSTRAINT DF_dimIngestionFactLecturas_LastRunStatus
            DEFAULT ('NEVER_RUN'),
        LastRunStartedAt DATETIME2(3) NULL,
        LastRunFinishedAt DATETIME2(3) NULL,
        LastRunError NVARCHAR(4000) NULL,
        IsActive BIT NOT NULL
            CONSTRAINT DF_dimIngestionFactLecturas_IsActive
            DEFAULT (1),
        CreatedAt DATETIME2(3) NOT NULL
            CONSTRAINT DF_dimIngestionFactLecturas_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
        UpdatedAt DATETIME2(3) NOT NULL
            CONSTRAINT DF_dimIngestionFactLecturas_UpdatedAt
            DEFAULT (SYSUTCDATETIME())
    );
END
GO
