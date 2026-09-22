SET NOCOUNT ON;
GO

IF
    OBJECT_ID(N'dwh.dimStoreSchedules', N'U') IS NOT NULL
    AND COL_LENGTH(N'dwh.dimStoreSchedules', N'DayNo') IS NULL
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;

            CREATE TABLE dwh.dimStoreSchedulesApiMigration
            (
                StoreId VARCHAR(4) NOT NULL,
                CustomerNo VARCHAR(20) NOT NULL,
                StoreName NVARCHAR(250) NOT NULL,
                OpeningDate DATE NULL,
                ClosingDate DATE NULL,
                IsBlocked BIT NOT NULL,
                BlockedFrom DATE NULL,
                BlockedTo DATE NULL,
                ReceivingId VARCHAR(20) NULL,
                DayCode CHAR(3) NOT NULL,
                DayNo TINYINT NOT NULL,
                IsClosed BIT NOT NULL,
                OpeningTime TIME(0) NULL,
                LunchStartTime TIME(0) NULL,
                LunchEndTime TIME(0) NULL,
                ClosingTime TIME(0) NULL,
                IsDeleted BIT NOT NULL,
                CreatedAt DATETIME2(0) NOT NULL,
                ModifiedAt DATETIME2(0) NOT NULL,
                CONSTRAINT PK_dimStoreSchedulesApiMigration
                PRIMARY KEY CLUSTERED (StoreId, DayNo),
                CONSTRAINT CK_dimStoreSchedulesApiMigration_DayNo
                CHECK (DayNo BETWEEN 1 AND 7),
                CONSTRAINT CK_dimStoreSchedulesApiMigration_DayCode
                CHECK (DayCode IN ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'))
            );

            EXEC sp_executesql N'
            INSERT INTO dwh.dimStoreSchedulesApiMigration
            (
                StoreId,
                CustomerNo,
                StoreName,
                OpeningDate,
                ClosingDate,
                IsBlocked,
                BlockedFrom,
                BlockedTo,
                ReceivingId,
                DayCode,
                DayNo,
                IsClosed,
                OpeningTime,
                LunchStartTime,
                LunchEndTime,
                ClosingTime,
                IsDeleted,
                CreatedAt,
                ModifiedAt
            )
            SELECT
                schedules.StoreId,
                schedules.StoreId,
                N'''',
                schedules.OpeningDate,
                schedules.ClosingDate,
                0,
                NULL,
                NULL,
                NULL,
                day_schedule.DayCode,
                day_schedule.DayNo,
                0,
                day_schedule.OpeningTime,
                day_schedule.LunchStartTime,
                day_schedule.LunchEndTime,
                day_schedule.ClosingTime,
                CASE
                    WHEN schedules.ClosingDate IS NOT NULL
                        AND schedules.ClosingDate < CONVERT(DATE, SYSUTCDATETIME())
                        THEN 1
                    ELSE 0
                END,
                schedules.CreatedAt,
                schedules.ModifiedAt
            FROM dwh.dimStoreSchedules AS schedules
            CROSS APPLY
            (
                VALUES
                    (''MON'', 1, schedules.MondayOpeningTime, schedules.MondayLunchStartTime, schedules.MondayLunchEndTime, schedules.MondayClosingTime),
                    (''TUE'', 2, schedules.TuesdayOpeningTime, schedules.TuesdayLunchStartTime, schedules.TuesdayLunchEndTime, schedules.TuesdayClosingTime),
                    (''WED'', 3, schedules.WednesdayOpeningTime, schedules.WednesdayLunchStartTime, schedules.WednesdayLunchEndTime, schedules.WednesdayClosingTime),
                    (''THU'', 4, schedules.ThursdayOpeningTime, schedules.ThursdayLunchStartTime, schedules.ThursdayLunchEndTime, schedules.ThursdayClosingTime),
                    (''FRI'', 5, schedules.FridayOpeningTime, schedules.FridayLunchStartTime, schedules.FridayLunchEndTime, schedules.FridayClosingTime),
                    (''SAT'', 6, schedules.SaturdayOpeningTime, schedules.SaturdayLunchStartTime, schedules.SaturdayLunchEndTime, schedules.SaturdayClosingTime),
                    (''SUN'', 7, schedules.SundayOpeningTime, schedules.SundayLunchStartTime, schedules.SundayLunchEndTime, schedules.SundayClosingTime)
            ) AS day_schedule (DayCode, DayNo, OpeningTime, LunchStartTime, LunchEndTime, ClosingTime);';

            DROP TABLE dwh.dimStoreSchedules;
            EXEC sp_rename N'dwh.dimStoreSchedulesApiMigration', N'dimStoreSchedules';

            COMMIT TRANSACTION;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            THROW;
        END CATCH;
    END;
GO

IF OBJECT_ID(N'dwh.PK_dimStoreSchedulesApiMigration', N'PK') IS NOT NULL
    BEGIN
        EXEC sp_rename N'dwh.PK_dimStoreSchedulesApiMigration', N'PK_dimStoreSchedules', N'OBJECT';
    END;
GO

IF OBJECT_ID(N'dwh.CK_dimStoreSchedulesApiMigration_DayNo', N'C') IS NOT NULL
    BEGIN
        EXEC sp_rename N'dwh.CK_dimStoreSchedulesApiMigration_DayNo', N'CK_dimStoreSchedules_DayNo', N'OBJECT';
    END;
GO

IF OBJECT_ID(N'dwh.CK_dimStoreSchedulesApiMigration_DayCode', N'C') IS NOT NULL
    BEGIN
        EXEC sp_rename N'dwh.CK_dimStoreSchedulesApiMigration_DayCode', N'CK_dimStoreSchedules_DayCode', N'OBJECT';
    END;
GO

IF
    COL_LENGTH(N'dwh.dimStoreSchedules', N'CreatedAt') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.default_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'dwh.dimStoreSchedules')
            AND parent_column_id = COLUMNPROPERTY(OBJECT_ID(N'dwh.dimStoreSchedules'), N'CreatedAt', 'ColumnId')
    )
    BEGIN
        ALTER TABLE dwh.dimStoreSchedules
        ADD CONSTRAINT DF_dimStoreSchedules_CreatedAt
        DEFAULT (SYSUTCDATETIME()) FOR CreatedAt;
    END;
GO

IF
    COL_LENGTH(N'dwh.dimStoreSchedules', N'ModifiedAt') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM sys.default_constraints
        WHERE
            parent_object_id = OBJECT_ID(N'dwh.dimStoreSchedules')
            AND parent_column_id = COLUMNPROPERTY(OBJECT_ID(N'dwh.dimStoreSchedules'), N'ModifiedAt', 'ColumnId')
    )
    BEGIN
        ALTER TABLE dwh.dimStoreSchedules
        ADD CONSTRAINT DF_dimStoreSchedules_ModifiedAt
        DEFAULT (SYSUTCDATETIME()) FOR ModifiedAt;
    END;
GO

IF
    OBJECT_ID(N'stg.dimStoreSchedules', N'U') IS NOT NULL
    AND COL_LENGTH(N'stg.dimStoreSchedules', N'DayNo') IS NULL
    BEGIN
        DROP TABLE stg.dimStoreSchedules;
    END;
GO

IF OBJECT_ID(N'stg.dimStoreSchedules', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.dimStoreSchedules
        (
            RunId NVARCHAR(100) NOT NULL,
            StoreId VARCHAR(4) NOT NULL,
            CustomerNo VARCHAR(20) NOT NULL,
            StoreName NVARCHAR(250) NOT NULL,
            OpeningDate DATE NULL,
            ClosingDate DATE NULL,
            IsBlocked BIT NOT NULL,
            BlockedFrom DATE NULL,
            BlockedTo DATE NULL,
            ReceivingId VARCHAR(20) NULL,
            DayCode CHAR(3) NOT NULL,
            DayNo TINYINT NOT NULL,
            IsClosed BIT NOT NULL,
            OpeningTime TIME(0) NULL,
            LunchStartTime TIME(0) NULL,
            LunchEndTime TIME(0) NULL,
            ClosingTime TIME(0) NULL,
            CONSTRAINT CK_stg_dimStoreSchedules_DayNo
            CHECK (DayNo BETWEEN 1 AND 7),
            CONSTRAINT CK_stg_dimStoreSchedules_DayCode
            CHECK (DayCode IN ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'))
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
            s.name = N'stg'
            AND o.name = N'dimStoreSchedules'
            AND i.name = N'ix_stg_dimStoreSchedules_run'
    )
    BEGIN
        CREATE UNIQUE INDEX ix_stg_dimStoreSchedules_run
            ON stg.dimStoreSchedules (RunId, StoreId, DayNo);
    END;
GO
