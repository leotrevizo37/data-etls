IF OBJECT_ID(N'observability.factChirpStackDeviceEventMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factChirpStackDeviceEventMetrics
        (
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            CapturedUplinkCount BIGINT NOT NULL,
            ObservedGatewayCount INT NOT NULL,
            MultiGatewayUplinkCount BIGINT NOT NULL,
            EventLogCountsJson NVARCHAR(MAX) NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factChirpStackDeviceEventMetrics_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factChirpStackDeviceEventMetrics_Counts
            CHECK (
                CapturedUplinkCount >= 0
                AND ObservedGatewayCount >= 0
                AND MultiGatewayUplinkCount >= 0
                AND MultiGatewayUplinkCount <= CapturedUplinkCount
            ),
            CONSTRAINT CK_factChirpStackDeviceEventMetrics_LogsJson
            CHECK (ISJSON(EventLogCountsJson) = 1),
            CONSTRAINT PK_factChirpStackDeviceEventMetrics
            PRIMARY KEY CLUSTERED (DevEui, TimeSpan)
        );
    END;
GO

IF OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics', N'U') IS NULL
    BEGIN
        CREATE TABLE stg.factChirpStackDeviceEventMetrics
        (
            RunId NVARCHAR(100) NOT NULL,
            DevEui NVARCHAR(16) NOT NULL,
            TimeSpan DATETIME2(0) NOT NULL,
            TenantId UNIQUEIDENTIFIER NOT NULL,
            CapturedUplinkCount BIGINT NOT NULL,
            ObservedGatewayCount INT NOT NULL,
            MultiGatewayUplinkCount BIGINT NOT NULL,
            EventLogCountsJson NVARCHAR(MAX) NOT NULL,
            ExtractedAt DATETIME2(0) NOT NULL
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(
                N'observability.factChirpStackDeviceEventMetrics'
            )
            AND name = N'ix_factChirpStackDeviceEventMetrics_timeSpan'
    )
    BEGIN
        CREATE INDEX ix_factChirpStackDeviceEventMetrics_timeSpan
            ON observability.factChirpStackDeviceEventMetrics
            (TimeSpan DESC, DevEui);
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE
            object_id = OBJECT_ID(N'stg.factChirpStackDeviceEventMetrics')
            AND name = N'ix_stg_factChirpStackDeviceEventMetrics_run'
    )
    BEGIN
        CREATE INDEX ix_stg_factChirpStackDeviceEventMetrics_run
            ON stg.factChirpStackDeviceEventMetrics
            (RunId, DevEui, TimeSpan);
    END;
GO
