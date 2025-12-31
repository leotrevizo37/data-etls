SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

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

IF OBJECT_ID(N'dwh.factHourlyDeviceStatus', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factHourlyDeviceStatus
        (
            OperationId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            LocalTimeSpan DATETIME2(0) NOT NULL,
            StatusCode SMALLINT NOT NULL,
            Anomalies INT NULL,
            LastModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factHourlyDeviceStatus_LastModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            Fecha AS CONVERT(DATE, LocalTimeSpan) PERSISTED,
            Hora  AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED,
            CONSTRAINT PK_FactOperacionesMaquina
            PRIMARY KEY CLUSTERED (OperationId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factHourlyDeviceStatus'
          AND i.name = N'ix_factDeviceStatusEvent_OperationId_DeviceId'
    )
    BEGIN
        CREATE INDEX ix_factDeviceStatusEvent_OperationId_DeviceId
            ON dwh.factHourlyDeviceStatus (OperationId, DeviceId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factHourlyDeviceStatus'
          AND i.name = N'ix_factDeviceStatusEvent_OperationId_LocalTimeSpan'
    )
    BEGIN
        CREATE INDEX ix_factDeviceStatusEvent_OperationId_LocalTimeSpan
            ON dwh.factHourlyDeviceStatus (OperationId, LocalTimeSpan);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factHourlyDeviceStatus'
          AND i.name = N'ix_factDeviceStatusEvent_sensor_time_span'
    )
    BEGIN
        CREATE INDEX ix_factDeviceStatusEvent_sensor_time_span
            ON dwh.factHourlyDeviceStatus (DeviceId, LocalTimeSpan);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factHourlyDeviceStatus'
          AND i.name = N'ix_factDeviceStatusEvent_time_span_deviceid'
    )
    BEGIN
        CREATE INDEX ix_factDeviceStatusEvent_time_span_deviceid
            ON dwh.factHourlyDeviceStatus (LocalTimeSpan, DeviceId);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id = i.object_id
        JOIN sys.schemas s ON s.schema_id = o.schema_id
        WHERE s.name = N'dwh' AND o.name = N'factHourlyDeviceStatus'
          AND i.name = N'ix_factDeviceStatusEvent_localTimeSpan_desc'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factDeviceStatusEvent_localTimeSpan_desc
            ON dwh.factHourlyDeviceStatus (LocalTimeSpan DESC);
    END;
GO
