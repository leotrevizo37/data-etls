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

IF OBJECT_ID(N'dwh.factLecturas', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factLecturas
        (
            ReadId UNIQUEIDENTIFIER NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            LocalTimeSpan DATETIME2(0) NULL,
            Value DECIMAL(19, 9) NOT NULL,
            CONSTRAINT PK_FactLecturas
            PRIMARY KEY CLUSTERED (ReadId)
            WITH (IGNORE_DUP_KEY = ON) -- noqa: PRS
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factLecturas'
          AND i.name = N'ix_factlecturas_sensor_time_span'
    )
    BEGIN
        CREATE INDEX ix_factlecturas_sensor_time_span
            ON dwh.factLecturas (SensorId, TimeSpan);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factLecturas'
          AND i.name = N'ix_factLecturas_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factLecturas_localTimeSpan_desc
            ON dwh.factLecturas (LocalTimeSpan DESC);
    END;
GO
