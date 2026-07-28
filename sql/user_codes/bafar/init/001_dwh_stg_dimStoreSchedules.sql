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

IF OBJECT_ID(N'dwh.dimStoreSchedules', N'U') IS NULL
    BEGIN
        CREATE TABLE dwh.dimStoreSchedules
        (
            StoreId VARCHAR(4) NOT NULL,
            OpeningDate DATE NULL,
            ClosingDate DATE NULL,
            ReceptionHours TINYINT NOT NULL,
            MondayOpeningTime TIME(0) NOT NULL,
            MondayLunchStartTime TIME(0) NULL,
            MondayLunchEndTime TIME(0) NULL,
            MondayClosingTime TIME(0) NOT NULL,
            TuesdayOpeningTime TIME(0) NOT NULL,
            TuesdayLunchStartTime TIME(0) NULL,
            TuesdayLunchEndTime TIME(0) NULL,
            TuesdayClosingTime TIME(0) NOT NULL,
            WednesdayOpeningTime TIME(0) NOT NULL,
            WednesdayLunchStartTime TIME(0) NULL,
            WednesdayLunchEndTime TIME(0) NULL,
            WednesdayClosingTime TIME(0) NOT NULL,
            ThursdayOpeningTime TIME(0) NOT NULL,
            ThursdayLunchStartTime TIME(0) NULL,
            ThursdayLunchEndTime TIME(0) NULL,
            ThursdayClosingTime TIME(0) NOT NULL,
            FridayOpeningTime TIME(0) NOT NULL,
            FridayLunchStartTime TIME(0) NULL,
            FridayLunchEndTime TIME(0) NULL,
            FridayClosingTime TIME(0) NOT NULL,
            SaturdayOpeningTime TIME(0) NOT NULL,
            SaturdayLunchStartTime TIME(0) NULL,
            SaturdayLunchEndTime TIME(0) NULL,
            SaturdayClosingTime TIME(0) NOT NULL,
            SundayOpeningTime TIME(0) NOT NULL,
            SundayLunchStartTime TIME(0) NULL,
            SundayLunchEndTime TIME(0) NULL,
            SundayClosingTime TIME(0) NOT NULL,
            CreatedAt DATE NOT NULL,
            CreatedBy VARCHAR(12) NOT NULL,
            SalesOfficeId VARCHAR(4) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimStoreSchedules_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimStoreSchedules
            PRIMARY KEY CLUSTERED (StoreId)
            WITH (IGNORE_DUP_KEY = ON)
        );
    END;
GO

IF OBJECT_ID(N'stg.dimStoreSchedules', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.dimStoreSchedules
        (
            RunId NVARCHAR(100) NOT NULL,
            StoreId VARCHAR(4) NOT NULL,
            OpeningDate DATE NULL,
            ClosingDate DATE NULL,
            ReceptionHours TINYINT NOT NULL,
            MondayOpeningTime TIME(0) NOT NULL,
            MondayLunchStartTime TIME(0) NULL,
            MondayLunchEndTime TIME(0) NULL,
            MondayClosingTime TIME(0) NOT NULL,
            TuesdayOpeningTime TIME(0) NOT NULL,
            TuesdayLunchStartTime TIME(0) NULL,
            TuesdayLunchEndTime TIME(0) NULL,
            TuesdayClosingTime TIME(0) NOT NULL,
            WednesdayOpeningTime TIME(0) NOT NULL,
            WednesdayLunchStartTime TIME(0) NULL,
            WednesdayLunchEndTime TIME(0) NULL,
            WednesdayClosingTime TIME(0) NOT NULL,
            ThursdayOpeningTime TIME(0) NOT NULL,
            ThursdayLunchStartTime TIME(0) NULL,
            ThursdayLunchEndTime TIME(0) NULL,
            ThursdayClosingTime TIME(0) NOT NULL,
            FridayOpeningTime TIME(0) NOT NULL,
            FridayLunchStartTime TIME(0) NULL,
            FridayLunchEndTime TIME(0) NULL,
            FridayClosingTime TIME(0) NOT NULL,
            SaturdayOpeningTime TIME(0) NOT NULL,
            SaturdayLunchStartTime TIME(0) NULL,
            SaturdayLunchEndTime TIME(0) NULL,
            SaturdayClosingTime TIME(0) NOT NULL,
            SundayOpeningTime TIME(0) NOT NULL,
            SundayLunchStartTime TIME(0) NULL,
            SundayLunchEndTime TIME(0) NULL,
            SundayClosingTime TIME(0) NOT NULL,
            CreatedAt DATE NOT NULL,
            CreatedBy VARCHAR(12) NOT NULL,
            SalesOfficeId VARCHAR(4) NOT NULL
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
            AND o.name = N'dimStoreSchedules'
            AND i.name = N'ix_stg_dimStoreSchedules_run'
    )
    BEGIN
        CREATE INDEX ix_stg_dimStoreSchedules_run
            ON stg.dimStoreSchedules (RunId, StoreId);
    END;
GO
