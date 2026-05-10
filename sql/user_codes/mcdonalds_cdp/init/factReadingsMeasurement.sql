IF COL_LENGTH(N'dwh.factReadingsMeasurement', N'sales') IS NULL
    BEGIN
        ALTER TABLE dwh.factReadingsMeasurement
        ADD sales INT NULL;
    END;
GO
