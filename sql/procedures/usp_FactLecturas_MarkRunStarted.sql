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
            INSERT INTO dwh.ctlIngestionFactLecturas
            (
                IngestionName,
                LastRunStatus,
                LastRunStartedAt,
                LastRunFinishedAt,
                LastRunError,
                UpdatedAt
            )
            VALUES
            (
                @IngestionName,
                'RUNNING',
                @now,
                NULL,
                'ctlIngestionFactLecturas: no row found for IngestionName = %s or it is inactive. New one created',
                @now
            )
        END;
END;
GO
