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
