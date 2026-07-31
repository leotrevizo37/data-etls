IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'observability'
    )
    BEGIN
        EXEC ('CREATE SCHEMA observability');
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

IF OBJECT_ID(N'observability.factRedingsAudits', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factRedingsAudits
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            ReadingsCount INT NOT NULL,
            HasLateReadings BIT NOT NULL,
            IsConnectionLost BIT NOT NULL,
            LastReadingAt DATETIME2(0) NULL,
            ConnectionLostAt DATETIME2(0) NULL,
            MinutesWithoutReadings INT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factRedingsAudits_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factRedingsAudits
            PRIMARY KEY CLUSTERED (SensorId, TimeSpan)
        );
    END;
GO

IF OBJECT_ID(N'stg.factRedingsAudits', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factRedingsAudits
        (
            RunId NVARCHAR(100) NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            ReadingsCount INT NOT NULL,
            HasLateReadings BIT NOT NULL,
            IsConnectionLost BIT NOT NULL,
            LastReadingAt DATETIME2(0) NULL,
            ConnectionLostAt DATETIME2(0) NULL,
            MinutesWithoutReadings INT NULL
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
            s.name = N'observability'
            AND o.name = N'factRedingsAudits'
            AND i.name = N'ix_factRedingsAudits_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factRedingsAudits_timeSpan
            ON observability.factRedingsAudits (TimeSpan DESC, SensorId);
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
            AND o.name = N'factRedingsAudits'
            AND i.name = N'ix_stg_factRedingsAudits_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factRedingsAudits_run
            ON stg.factRedingsAudits (RunId, SensorId, TimeSpan);
    END;
GO
