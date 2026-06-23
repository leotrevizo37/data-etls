IF OBJECT_ID(N'ctl.MantizTextInsightClassifierModels', N'U') IS NULL
    BEGIN
        CREATE TABLE ctl.MantizTextInsightClassifierModels
        (
            InsightType NVARCHAR(30) NOT NULL,
            ModelVersion NVARCHAR(100) NOT NULL,
            ArtifactPath NVARCHAR(1000) NOT NULL,
            Accuracy DECIMAL(9, 6) NULL,
            MacroF1 DECIMAL(9, 6) NULL,
            TrainingRows INT NOT NULL,
            IsActive BIT NOT NULL
            CONSTRAINT DF_MantizTextInsightClassifierModels_IsActive
            DEFAULT (0),
            CreatedAt DATETIME2(0) NOT NULL
            CONSTRAINT DF_MantizTextInsightClassifierModels_CreatedAt
            DEFAULT (SYSUTCDATETIME()),
            PromotedAt DATETIME2(0) NULL,
            CONSTRAINT PK_MantizTextInsightClassifierModels
            PRIMARY KEY CLUSTERED (InsightType, ModelVersion)
        );
    END;
GO
