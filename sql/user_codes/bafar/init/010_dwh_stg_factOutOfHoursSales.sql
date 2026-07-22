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
        WHERE name = N'stg'
    )
    BEGIN
        EXEC ('CREATE SCHEMA stg');
    END;
GO

IF OBJECT_ID(N'dwh.factOutOfHoursSales', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factOutOfHoursSales
        (
            SaleId BIGINT NOT NULL,
            SaleNumber VARCHAR(50) NOT NULL,
            Amount DECIMAL(19, 4) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            UserName NVARCHAR(150) NULL,
            StoreId VARCHAR(4) NOT NULL,
            OutOfHoursType TINYINT NULL,
            DeviationMinutes INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factOutOfHoursSales_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factOutOfHoursSales
            PRIMARY KEY CLUSTERED (SaleId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF OBJECT_ID(N'stg.factOutOfHoursSales', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factOutOfHoursSales
        (
            RunId NVARCHAR(100) NOT NULL,
            SaleId BIGINT NOT NULL,
            SaleNumber VARCHAR(50) NOT NULL,
            Amount DECIMAL(19, 4) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            UserName NVARCHAR(150) NULL,
            StoreId VARCHAR(4) NOT NULL,
            OutOfHoursType TINYINT NULL,
            DeviationMinutes INT NULL
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
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'dwh'
            AND o.name = N'factOutOfHoursSales'
            AND i.name = N'ix_factOutOfHoursSales_localTimeSpan_storeId'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factOutOfHoursSales_localTimeSpan_storeId
            ON dwh.factOutOfHoursSales (LocalTimeSpan, StoreId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON i.object_id = o.object_id
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'stg'
            AND o.name = N'factOutOfHoursSales'
            AND i.name = N'ix_stg_factOutOfHoursSales_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factOutOfHoursSales_run
            ON stg.factOutOfHoursSales (RunId, SaleId);
    END;
GO
