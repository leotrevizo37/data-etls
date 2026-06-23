IF OBJECT_ID(N'dwh.dimMantizSolutionPartConcept', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimMantizSolutionPartConcept;
    END;
GO

IF OBJECT_ID(N'dwh.dimMantizSolutionConcept', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimMantizSolutionConcept;
    END;
GO

IF OBJECT_ID(N'dwh.dimMantizFailureConcept', N'U') IS NOT NULL
    BEGIN
        DROP TABLE dwh.dimMantizFailureConcept;
    END;
GO
