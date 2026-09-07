SET QUOTED_IDENTIFIER ON;
GO

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

IF OBJECT_ID(N'observability.factLecturasMqtt', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factLecturasMqtt
        (
            MqttId UNIQUEIDENTIFIER NOT NULL,
            FechaLocal DATETIME2(6) NOT NULL,
            TimeSpan DATETIME2(6) NOT NULL,
            DevEui NVARCHAR(64) NOT NULL,
            Tenant NVARCHAR(100) NOT NULL,
            Dispositivo NVARCHAR(255) NULL,
            Aplicacion NVARCHAR(255) NULL,
            Sucursal NVARCHAR(MAX) NULL,
            Topic NVARCHAR(MAX) NULL,
            Variable NVARCHAR(MAX) NULL,
            ValorJson NVARCHAR(MAX) NULL,
            ValorTexto NVARCHAR(MAX) NULL,
            MedicionesJson NVARCHAR(MAX) NOT NULL,
            FechaRecepcion NVARCHAR(100) NULL,
            MensajeRaw NVARCHAR(MAX) NOT NULL,
            ModifiedAt DATETIME2(6) NOT NULL
            CONSTRAINT DF_factLecturasMqtt_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factLecturasMqtt
            PRIMARY KEY CLUSTERED (MqttId)
        );
    END;
GO

IF OBJECT_ID(N'stg.factLecturasMqtt', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factLecturasMqtt
        (
            RunId NVARCHAR(100) NOT NULL,
            MqttId UNIQUEIDENTIFIER NOT NULL,
            FechaLocal DATETIME2(6) NOT NULL,
            TimeSpan DATETIME2(6) NOT NULL,
            DevEui NVARCHAR(64) NOT NULL,
            Tenant NVARCHAR(100) NOT NULL,
            Dispositivo NVARCHAR(255) NULL,
            Aplicacion NVARCHAR(255) NULL,
            Sucursal NVARCHAR(MAX) NULL,
            Topic NVARCHAR(MAX) NULL,
            Variable NVARCHAR(MAX) NULL,
            ValorJson NVARCHAR(MAX) NULL,
            ValorTexto NVARCHAR(MAX) NULL,
            MedicionesJson NVARCHAR(MAX) NOT NULL,
            FechaRecepcion NVARCHAR(100) NULL,
            MensajeRaw NVARCHAR(MAX) NOT NULL,
            CONSTRAINT PK_stg_factLecturasMqtt
            PRIMARY KEY CLUSTERED (RunId, MqttId)
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
            AND o.name = N'factLecturasMqtt'
            AND i.name = N'ix_factLecturasMqtt_timeSpan_devEui'
    )
    BEGIN
        CREATE INDEX ix_factLecturasMqtt_timeSpan_devEui
            ON observability.factLecturasMqtt (TimeSpan, DevEui);
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
            AND o.name = N'factLecturasMqtt'
            AND i.name = N'ix_factLecturasMqtt_fechaLocal_aplicacion_dispositivo'
    )
    BEGIN
        CREATE INDEX ix_factLecturasMqtt_fechaLocal_aplicacion_dispositivo
            ON observability.factLecturasMqtt
            (FechaLocal, Aplicacion, Dispositivo)
            INCLUDE (TimeSpan, DevEui);
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
            AND o.name = N'factLecturasMqtt'
            AND i.name = N'ix_factLecturasMqtt_aplicacion_fechaLocal_dispositivo'
    )
    BEGIN
        CREATE INDEX ix_factLecturasMqtt_aplicacion_fechaLocal_dispositivo
            ON observability.factLecturasMqtt
            (Aplicacion, FechaLocal, Dispositivo)
            INCLUDE (TimeSpan, DevEui)
            WHERE Aplicacion IS NOT NULL;
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
            AND o.name = N'factLecturasMqtt'
            AND i.name = N'ix_factLecturasMqtt_dispositivo_fechaLocal_aplicacion'
    )
    BEGIN
        CREATE INDEX ix_factLecturasMqtt_dispositivo_fechaLocal_aplicacion
            ON observability.factLecturasMqtt
            (Dispositivo, FechaLocal, Aplicacion)
            INCLUDE (TimeSpan, DevEui)
            WHERE Dispositivo IS NOT NULL;
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
            AND o.name = N'factLecturasMqtt'
            AND i.name = N'ix_factLecturasMqtt_devEui_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factLecturasMqtt_devEui_timeSpan
            ON observability.factLecturasMqtt (DevEui, TimeSpan);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'FactLecturasMqtt'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'FactLecturasMqtt');
    END;
GO
