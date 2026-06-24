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

IF OBJECT_ID(N'dwh.dimSmartauditsAiCommentLookup', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimSmartauditsAiCommentLookup;
    END;
GO

IF OBJECT_ID(N'dwh.factSmartauditsCategories', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.factSmartauditsCategories;
    END;
GO
