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

IF OBJECT_ID(N'dwh.factManagerAccessDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.factManagerAccessDaily
        (
            Fecha DATE NOT NULL,
            FechaCorte DATETIME2(0) NOT NULL,
            CorteHora TINYINT NOT NULL,
            DiasHistorial SMALLINT NOT NULL,
            IdUsuario INT NOT NULL,
            NoEmpleado NVARCHAR(50) NOT NULL,
            NombreGerente NVARCHAR(250) NOT NULL,
            NombreUsuario NVARCHAR(250) NOT NULL,
            Email NVARCHAR(320) NOT NULL,
            Puesto NVARCHAR(250) NOT NULL,
            Rol NVARCHAR(250) NOT NULL,
            Activo BIT NOT NULL,
            IdTienda INT NULL,
            ClaveTienda NVARCHAR(50) NOT NULL,
            NombreTienda NVARCHAR(250) NOT NULL,
            Zona NVARCHAR(150) NOT NULL,
            UltimoInicioSesion DATETIME2(0) NULL,
            IngresoAntesDelCorte BIT NOT NULL,
            DiasSinIngresar INT NULL,
            EstatusCorte NVARCHAR(50) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factManagerAccessDaily_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_factManagerAccessDaily
            PRIMARY KEY CLUSTERED (Fecha, IdUsuario, ClaveTienda)
        );
    END;
GO

IF COL_LENGTH(N'dwh.factManagerAccessDaily', N'DiasHistorial') IS NULL
    BEGIN
        ALTER TABLE dwh.factManagerAccessDaily
        ADD DiasHistorial SMALLINT NULL;
    END;
GO

IF COL_LENGTH(N'dwh.factManagerAccessDaily', N'NombreUsuario') IS NULL
    BEGIN
        ALTER TABLE dwh.factManagerAccessDaily
        ADD NombreUsuario NVARCHAR(250) NULL;
    END;
GO

IF OBJECT_ID(N'stg.factManagerAccessDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factManagerAccessDaily
        (
            RunId NVARCHAR(100) NOT NULL,
            Fecha DATE NOT NULL,
            FechaCorte DATETIME2(0) NOT NULL,
            CorteHora TINYINT NOT NULL,
            DiasHistorial SMALLINT NOT NULL,
            IdUsuario INT NOT NULL,
            NoEmpleado NVARCHAR(50) NOT NULL,
            NombreGerente NVARCHAR(250) NOT NULL,
            NombreUsuario NVARCHAR(250) NOT NULL,
            Email NVARCHAR(320) NOT NULL,
            Puesto NVARCHAR(250) NOT NULL,
            Rol NVARCHAR(250) NOT NULL,
            Activo BIT NOT NULL,
            IdTienda INT NULL,
            ClaveTienda NVARCHAR(50) NOT NULL,
            NombreTienda NVARCHAR(250) NOT NULL,
            Zona NVARCHAR(150) NOT NULL,
            UltimoInicioSesion DATETIME2(0) NULL,
            IngresoAntesDelCorte BIT NOT NULL,
            DiasSinIngresar INT NULL,
            EstatusCorte NVARCHAR(50) NOT NULL
        );
    END;
GO

IF COL_LENGTH(N'stg.factManagerAccessDaily', N'DiasHistorial') IS NULL
    BEGIN
        ALTER TABLE stg.factManagerAccessDaily
        ADD DiasHistorial SMALLINT NULL;
    END;
GO

IF COL_LENGTH(N'stg.factManagerAccessDaily', N'NombreUsuario') IS NULL
    BEGIN
        ALTER TABLE stg.factManagerAccessDaily
        ADD NombreUsuario NVARCHAR(250) NULL;
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
            AND o.name = N'factManagerAccessDaily'
            AND i.name = N'ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factManagerAccessDaily_fecha_ingresoAntesDelCorte
            ON dwh.factManagerAccessDaily (Fecha, IngresoAntesDelCorte)
            INCLUDE (IdUsuario, IdTienda, ClaveTienda, UltimoInicioSesion, DiasSinIngresar);
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
            AND o.name = N'factManagerAccessDaily'
            AND i.name = N'ix_factManagerAccessDaily_claveTienda_fecha'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX ix_factManagerAccessDaily_claveTienda_fecha
            ON dwh.factManagerAccessDaily (ClaveTienda, Fecha)
            INCLUDE (IdUsuario, IdTienda, IngresoAntesDelCorte, UltimoInicioSesion, DiasSinIngresar);
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
            AND o.name = N'factManagerAccessDaily'
            AND i.name = N'ix_stg_factManagerAccessDaily_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factManagerAccessDaily_run
            ON stg.factManagerAccessDaily (RunId, Fecha, IdUsuario, ClaveTienda);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'FactManagerAccessDaily'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'FactManagerAccessDaily');
    END;
GO
