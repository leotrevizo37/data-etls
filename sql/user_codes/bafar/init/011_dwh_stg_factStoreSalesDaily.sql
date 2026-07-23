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

IF OBJECT_ID(N'dwh.factStoreSalesDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factStoreSalesDaily
        (
            StoreId VARCHAR(5) NOT NULL,
            SalesDate DATE NOT NULL,
            SalesCount INT NOT NULL,
            SalesAmount DECIMAL(19, 4) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factStoreSalesDaily_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factStoreSalesDaily
            PRIMARY KEY CLUSTERED (StoreId, SalesDate)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF OBJECT_ID(N'stg.factStoreSalesDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factStoreSalesDaily
        (
            RunId NVARCHAR(100) NOT NULL,
            StoreId VARCHAR(5) NOT NULL,
            SalesDate DATE NOT NULL,
            SalesCount INT NOT NULL,
            SalesAmount DECIMAL(19, 4) NOT NULL
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
            AND o.name = N'factStoreSalesDaily'
            AND i.name = N'ix_factStoreSalesDaily_salesDate_storeId'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factStoreSalesDaily_salesDate_storeId
            ON dwh.factStoreSalesDaily (SalesDate, StoreId);
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
            AND o.name = N'factStoreSalesDaily'
            AND i.name = N'ix_stg_factStoreSalesDaily_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factStoreSalesDaily_run
            ON stg.factStoreSalesDaily (RunId, StoreId, SalesDate);
    END;
GO
