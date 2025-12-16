USE mcdonalds;
GO

IF OBJECT_ID(N'dwh.usp_FactLecturas_MarkRunStarted', N'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_FactLecturas_MarkRunStarted;
GO

CREATE PROCEDURE dwh.usp_FactLecturas_MarkRunStarted
(
    @IngestionName SYSNAME
)
AS
    BEGIN
        SET NOCOUNT ON;

        DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

        UPDATE dwh.ctlIngestionFactLecturas WITH (ROWLOCK, UPDLOCK)
        SET
            LastRunStatus = 'RUNNING',
            LastRunStartedAt = @now,
            LastRunFinishedAt = NULL,
            LastRunError = NULL,
            UpdatedAt = @now
        WHERE
            IngestionName = @IngestionName
            AND IsActive = 1;

        IF @@ROWCOUNT = 0
            BEGIN
                RAISERROR (
                    'ctlIngestionFactLecturas: no row found for '
                    + 'IngestionName = %s or it is inactive.',
                    16,
                    1,
                    @IngestionName
                );
                RETURN;
            END;
    END;
GO
