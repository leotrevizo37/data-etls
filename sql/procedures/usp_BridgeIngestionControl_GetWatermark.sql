IF OBJECT_ID(N'bridge.usp_BridgeIngestionControl_GetWatermark', N'P') IS NOT NULL
    DROP PROCEDURE bridge.usp_BridgeIngestionControl_GetWatermark;
GO

CREATE PROCEDURE bridge.usp_BridgeIngestionControl_GetWatermark
    (
        @IngestionName SYSNAME
    )
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        BridgeIngestionId,
        IngestionName,
        LastLoadedAt,
        LastRunStatus,
        LastRunStartedAt,
        LastRunFinishedAt,
        LastRunError,
        IsActive,
        CreatedAt,
        UpdatedAt
    FROM bridge.IngestionControl WITH (READCOMMITTED)
    WHERE
        IngestionName = @IngestionName
        AND IsActive = 1;

    IF @@ROWCOUNT = 0
        BEGIN
            DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

            INSERT INTO bridge.IngestionControl
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
                'BridgeIngestionControl: bootstrap row created automatically.',
                @now
            );
        END;
END;
GO
