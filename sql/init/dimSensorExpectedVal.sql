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

IF OBJECT_ID(N'dwh.dimSensorExpectedVal', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimSensorExpectedVal
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            min_expected_value DATETIME2(0) NOT NULL,
            max_expected_value SMALLINT NOT NULL,
            StartTime NVARCHAR(255) NOT NULL,
            EndTime NVARCHAR(255) NOT NULL,
            scheduledDays NVARCHAR(255) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factReadingsMeasurement_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factReadingsMeasurement
            PRIMARY KEY CLUSTERED (SensorId, StartTime, scheduledDays)
            WITH (IGNORE_DUP_KEY = ON), -- noqa: PRS
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
            AND o.name = N'dimSensorExpectedVal'
            AND i.name = N'ix_dimSensorExpectedVal_DeviceId_SensorId'
    )
    BEGIN
        CREATE INDEX ix_dimSensorExpectedVal_DeviceId_SensorId
            ON dwh.dimSensorExpectedVal (DeviceId, SensorId);
    END;
GO
