IF COL_LENGTH(N'dwh.factReadingsMeasurement', N'Sales') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD Sales INT NULL;
    END;
GO

IF COL_LENGTH(N'stg.factReadingsMeasurement', N'Sales') IS NULL
    BEGIN
        ALTER TABLE stg.factReadingsMeasurement
        ADD Sales INT NULL;
    END;
GO
