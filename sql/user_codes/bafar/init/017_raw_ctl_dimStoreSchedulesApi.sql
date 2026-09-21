IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'raw'
    )
    BEGIN
        EXEC ('CREATE SCHEMA raw');
    END;
GO

IF OBJECT_ID(N'raw.StoreSchedulesApiPage', N'U') IS NULL
    BEGIN
        CREATE TABLE raw.StoreSchedulesApiPage
        (
            RunId NVARCHAR(100) NOT NULL,
            AttemptNumber INT NOT NULL,
            PageNumber INT NOT NULL,
            RequestCursor NVARCHAR(4000) NULL,
            PayloadJson NVARCHAR(MAX) NOT NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_StoreSchedulesApiPage_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_StoreSchedulesApiPage
            PRIMARY KEY CLUSTERED (RunId, AttemptNumber, PageNumber),
            CONSTRAINT CK_StoreSchedulesApiPage_PayloadJson
            CHECK (ISJSON(PayloadJson) = 1)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'DimStoreSchedules'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'DimStoreSchedules');
    END;
GO
