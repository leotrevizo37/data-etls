IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'ctl'
    )
    BEGIN
        EXEC ('CREATE SCHEMA ctl');
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsClassifierModels', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.SmartauditsClassifierModels
        (
            ModelVersion NVARCHAR(100) NOT NULL
            CONSTRAINT PK_SmartauditsClassifierModels
            PRIMARY KEY,
            ArtifactPath NVARCHAR(1000) NOT NULL,
            Accuracy DECIMAL(9, 6) NULL,
            MacroF1 DECIMAL(9, 6) NULL,
            TrainingRows INT NOT NULL,
            IsActive BIT NOT NULL
            CONSTRAINT DF_SmartauditsClassifierModels_IsActive
            DEFAULT (0),
            PromotedAt DATETIME2(0) NULL,
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_SmartauditsClassifierModels_CreatedAt
            DEFAULT (SYSUTCDATETIME())
        );
    END;
GO
