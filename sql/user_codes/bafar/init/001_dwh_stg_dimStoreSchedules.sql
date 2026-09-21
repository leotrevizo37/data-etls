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
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimStoreSchedules_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_dimStoreSchedules_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT PK_dimStoreSchedules
            PRIMARY KEY CLUSTERED (StoreId, DayNo),
            CONSTRAINT CK_dimStoreSchedules_DayNo
            CHECK (DayNo BETWEEN 1 AND 7),
            CONSTRAINT CK_dimStoreSchedules_DayCode
            CHECK (DayCode IN ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'))
        );
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
            ON o.schema_id = s.schema_iD
        WHERE
            s.name = N'stg'
            AND o.name = N'dimStoreSchedules'
            AND i.name = N'ix_stg_dimStoreSchedules_run'
    )
    AND COL_LENGTH(N'stg.dimStoreSchedules', N'DayNo') IS NOT NULL
    BEGIN
        CREATE UNIQUE INDEX ix_stg_dimStoreSchedules_run
            ON stg.dimStoreSchedules (RunId, StoreId, DayNo);
    END;
GO
