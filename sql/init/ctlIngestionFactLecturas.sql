IF
    NOT EXISTS (
        SELECT
            1
        FROM sys.schemas
        WHERE name = N'dwh'
    )
    BEGIN
        EXEC ('CREATE SCHEMA dwh');
    END;
GO

IF OBJECT_ID(N'dwh.ctlIngestionFactLecturas', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.ctlIngestionFactLecturas (
            ControlIngestionId INT IDENTITY (1, 1) NOT NULL
                CONSTRAINT PK_ctlIngestionLecturas PRIMARY KEY,
            IngestionName SYSNAME NOT NULL
                CONSTRAINT UQ_ctlIngestionLecturas_IngestionName UNIQUE,
            LastLoadedAt DATETIME2(3) NULL,
            LastRunStatus VARCHAR(20) NOT NULL
                CONSTRAINT DF_ctlIngestionLecturas_LastRunStatus
                DEFAULT ('NEVER_RUN'),
            LastRunStartedAt DATETIME2(3) NULL,
            LastRunFinishedAt DATETIME2(3) NULL,
            LastRunError NVARCHAR(4000) NULL,
            IsActive BIT NOT NULL
                CONSTRAINT DF_ctlIngestionLecturas_IsActive
                DEFAULT (1),
            CreatedAt DATETIME2(3) NOT NULL
                CONSTRAINT DF_ctlIngestionLecturas_CreatedAt
                DEFAULT (SYSUTCDATETIME()),
            UpdatedAt DATETIME2(3) NOT NULL
                CONSTRAINT DF_ctlIngestionLecturas_UpdatedAt
                DEFAULT (SYSUTCDATETIME())
        );
    END;
GO
