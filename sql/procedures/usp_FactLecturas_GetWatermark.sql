USE mcdonalds;
GO

IF OBJECT_ID(N'dwh.usp_FactLecturas_GetWatermark', N'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_FactLecturas_GetWatermark;
GO

CREATE PROCEDURE dwh.usp_FactLecturas_GetWatermark
(
    @IngestionName SYSNAME
)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ControlIngestionId,
        IngestionName,
        LastLoadedAt,
        LastRunStatus,
        LastRunStartedAt,
        LastRunFinishedAt,
        LastRunError,
        IsActive,
        CreatedAt,
        UpdatedAt
    FROM dwh.ctlIngestionLecturas WITH (READCOMMITTED)
    WHERE IngestionName = @IngestionName
      AND IsActive = 1;

    IF @@ROWCOUNT = 0
    BEGIN
        RAISERROR('ctlIngestionLecturas: no active row found for IngestionName = %s.',
                  16, 1, @IngestionName);
        RETURN;
    END
END;
GO
