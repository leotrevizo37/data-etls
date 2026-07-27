IF OBJECT_ID(N'dwh.vwFactReadingsMeasurementDevice', N'V') IS NOT NULL
    BEGIN
        DROP VIEW dwh.vwFactReadingsMeasurementDevice;
    END;
GO

IF OBJECT_ID(N'stg.MantizTextInsightReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTextInsightReviewQueue;
    END;
GO

IF OBJECT_ID(N'stg.MantizTicketTextInsightDetail', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTicketTextInsightDetail;
    END;
GO

IF OBJECT_ID(N'stg.MantizTicketTextInsightChangedTicket', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.MantizTicketTextInsightChangedTicket;
    END;
GO

IF OBJECT_ID(N'stg.factMantizTicketTextInsights', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factMantizTicketTextInsights;
    END;
GO

IF OBJECT_ID(N'stg.factMantizTicketLifecycle', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factMantizTicketLifecycle;
    END;
GO

IF OBJECT_ID(N'stg.SmartauditsAiCommentReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.SmartauditsAiCommentReviewQueue;
    END;
GO

IF OBJECT_ID(N'stg.factSmartauditsCategories', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factSmartauditsCategories;
    END;
GO

IF OBJECT_ID(N'stg.factReadingsMeasurement', N'U') IS NOT NULL
    BEGIN
        DROP TABLE stg.factReadingsMeasurement;
    END;
GO

IF OBJECT_ID(N'ctl.MantizTextInsightReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.MantizTextInsightReviewQueue;
    END;
GO

IF OBJECT_ID(N'ctl.MantizTextInsightClassifierModels', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.MantizTextInsightClassifierModels;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsAiCommentReviewQueue', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsAiCommentReviewQueue;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsClassifierModels', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsClassifierModels;
    END;
GO

IF OBJECT_ID(N'ctl.SmartauditsAiCommentLookup', N'U') IS NOT NULL
    BEGIN
        DROP TABLE ctl.SmartauditsAiCommentLookup;
    END;
GO

IF OBJECT_ID(N'dwh.factMantizTicketTextInsights', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factMantizTicketTextInsights;
    END;
GO

IF OBJECT_ID(N'dwh.factMantizTicketLifecycle', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factMantizTicketLifecycle;
    END;
GO

IF OBJECT_ID(N'dwh.factSmartauditsCategories', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factSmartauditsCategories;
    END;
GO

IF OBJECT_ID(N'dwh.factSensorEvents', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factSensorEvents;
    END;
GO

IF OBJECT_ID(N'dwh.factReadingsMeasurement', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factReadingsMeasurement;
    END;
GO

IF OBJECT_ID(N'dwh.factLecturas', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factLecturas;
    END;
GO

IF OBJECT_ID(N'dwh.dimSensorExpectedVal', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimSensorExpectedVal;
    END;
GO

IF OBJECT_ID(N'dwh.dimSidonProdDimensions', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimSidonProdDimensions;
    END;
GO
