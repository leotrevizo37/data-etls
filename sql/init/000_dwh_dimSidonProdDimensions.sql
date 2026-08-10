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

IF OBJECT_ID(N'dwh.dimSidonProdDimensions', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimSidonProdDimensions
        (
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            sensor_name NVARCHAR(255) NULL,
            sensor_type NVARCHAR(255) NULL,
            DeviceTyId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            Active BIT NOT NULL,
            device_name NVARCHAR(255) NULL,
            device_type NVARCHAR(255) NULL,
            SubLocationTyId UNIQUEIDENTIFIER NOT NULL,
            sublocation_name NVARCHAR(255) NULL,
            location_name NVARCHAR(255) NULL,
            city_name NVARCHAR(255) NULL,
            CityId UNIQUEIDENTIFIER NOT NULL,
            TimeZone NVARCHAR(255) NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            sublocation_type NVARCHAR(255) NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimSidonProdDimensions_ModifiedAt
            DEFAULT (SYSUTCDATETIME())
        );
    END;
GO

IF
    OBJECT_ID(N'dwh.dimSidonProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimSidonProdDimensions', N'TimeZone') IS NULL
    BEGIN
        ALTER TABLE dwh.dimSidonProdDimensions
        ADD TimeZone NVARCHAR(255) NULL;
    END;
GO

IF OBJECT_ID(N'stg.dimSidonProdDimensions', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.dimSidonProdDimensions
        (
            RunId NVARCHAR(100) NOT NULL,
            SensorId UNIQUEIDENTIFIER NOT NULL,
            SensorTyId UNIQUEIDENTIFIER NOT NULL,
            DeviceId UNIQUEIDENTIFIER NOT NULL,
            sensor_name NVARCHAR(255) NULL,
            sensor_type NVARCHAR(255) NULL,
            DeviceTyId UNIQUEIDENTIFIER NOT NULL,
            SubLocationId UNIQUEIDENTIFIER NOT NULL,
            Active BIT NOT NULL,
            device_name NVARCHAR(255) NULL,
            device_type NVARCHAR(255) NULL,
            SubLocationTyId UNIQUEIDENTIFIER NOT NULL,
            sublocation_name NVARCHAR(255) NULL,
            location_name NVARCHAR(255) NULL,
            city_name NVARCHAR(255) NULL,
            CityId UNIQUEIDENTIFIER NOT NULL,
            TimeZone NVARCHAR(255) NOT NULL,
            LocationId UNIQUEIDENTIFIER NOT NULL,
            sublocation_type NVARCHAR(255) NULL
        );
    END;
GO

IF
    OBJECT_ID(N'stg.dimSidonProdDimensions', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimSidonProdDimensions', N'TimeZone') IS NULL
    BEGIN
        ALTER TABLE stg.dimSidonProdDimensions
        ADD TimeZone NVARCHAR(255) NULL;
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
            AND o.name = N'dimSidonProdDimensions'
            AND i.name = N'ix_dimSidonProdDimensions_SensorId'
    )
    BEGIN
        CREATE UNIQUE INDEX ix_dimSidonProdDimensions_SensorId
            ON dwh.dimSidonProdDimensions (SensorId)
            WITH (IGNORE_DUP_KEY = ON);
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
            AND o.name = N'dimSidonProdDimensions'
            AND i.name = N'ix_stg_dimSidonProdDimensions_run'
    )
    BEGIN
        CREATE INDEX ix_stg_dimSidonProdDimensions_run
            ON stg.dimSidonProdDimensions (RunId, SensorId);
    END;
GO
