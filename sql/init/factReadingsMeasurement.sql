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

IF OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factReadingsMeasurement
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            StatusCode SMALLINT NOT NULL,
            MeasurementValue DECIMAL(9, 4) NULL,
            Anomalies INT NULL,
            LastModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factReadingsMeasurement_LastModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factReadingsMeasurement
            PRIMARY KEY CLUSTERED (OperationId, SensorTyId)
            WITH (IGNORE_DUP_KEY = ON), -- noqa: PRS
            CONSTRAINT FK_factReadingsMeasurement_factHourlyDeviceStatus
            FOREIGN KEY (OperationId)
            REFERENCES dwh.factHourlyDeviceStatus (OperationId)
            ON DELETE CASCADE
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
            s.name = N'stg'
            AND o.name = N'factReadingsMeasurement'
            AND i.name = N'ix_factReadingsMeasurement_OperationId_SensorTyId'
    )
    BEGIN
        CREATE INDEX ix_factReadingsMeasurement_OperationId_SensorTyId
            ON stg.factReadingsMeasurement (OperationId, SensorTyId);
    END;
GO
