IF OBJECT_ID(N'bridge.usp_BridgeIngestionControl_MarkRunStarted', N'P') IS NOT NULL
    DROP PROCEDURE bridge.usp_BridgeIngestionControl_MarkRunStarted;
GO

CREATE PROCEDURE bridge.usp_BridgeIngestionControl_MarkRunStarted
    (
        @IngestionName SYSNAME
    )
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

    UPDATE bridge.IngestionControl WITH (ROWLOCK, UPDLOCK)
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
                'BridgeIngestionControl: no row found for IngestionName = %s or it is inactive.',
                16,
                1,
                @IngestionName
            );
            RETURN;
        END;
END;
GO
