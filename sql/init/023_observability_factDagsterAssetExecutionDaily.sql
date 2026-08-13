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

IF OBJECT_ID(N'observability.factDagsterAssetExecutionDaily', N'U') IS NULL
    BEGIN
        CREATE TABLE observability.factDagsterAssetExecutionDaily
        (
            UserCode VARCHAR(64) NOT NULL,
            AssetKey VARCHAR(500) NOT NULL,
            [Date] DATE NOT NULL,
            ExecutionCount INT NOT NULL,
            MaterializationCount INT NOT NULL,
            InsertedRowCount BIGINT NULL,
            MergedRowCount BIGINT NULL,
            ExecutionFailureCount INT NOT NULL,
            AssetCheckCount INT NOT NULL,
            AssetCheckFailureCount INT NOT NULL,
            AverageTimeToCompleteSeconds DECIMAL(18, 3) NULL,
            HasFailed BIT NOT NULL,
            NumberOfFailures INT NOT NULL,
            AverageQueuedTimeSeconds DECIMAL(18, 3) NULL,
            MinutesOnJob DECIMAL(18, 3) NOT NULL,
            TimeInJobPercentage DECIMAL(9, 4) NOT NULL,
            StepDetailsJson NVARCHAR(MAX) NOT NULL,
            AssetCheckDetailsJson NVARCHAR(MAX) NOT NULL,
            ModifiedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_factDagsterAssetExecutionDaily_ModifiedAt
            DEFAULT (SYSUTCDATETIME()),
            CONSTRAINT CK_factDagsterAssetExecutionDaily_StepDetailsJson
            CHECK (ISJSON(StepDetailsJson) = 1),
            CONSTRAINT CK_factDagsterAssetExecutionDaily_AssetCheckDetailsJson
            CHECK (ISJSON(AssetCheckDetailsJson) = 1),
            CONSTRAINT PK_factDagsterAssetExecutionDaily
            PRIMARY KEY CLUSTERED ([Date], UserCode, AssetKey)
        );
    END;
GO

IF
    NOT EXISTS (
        SELECT 1
        FROM ctl.IngestionControl
        WHERE IngestionName = N'factDagsterAssetExecutionDaily'
    )
    BEGIN
        INSERT INTO ctl.IngestionControl (IngestionName)
        VALUES (N'factDagsterAssetExecutionDaily');
    END;
GO
