IF
    NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = N'observability'
    )
    BEGIN
        EXEC ('CREATE SCHEMA observability');
    END;
GO

CREATE OR ALTER VIEW observability.vwSenseDevEuiUplinkReadingsDaily
AS
WITH DeviceHourly AS (
    SELECT
        DevEui,
        CONVERT(DATE, LocalTimeSpan) AS LocalDate,
        TimeSpan,
        MAX(ReceivedUplinkCount) AS ReceivedUplinkCount,
        SUM(CONVERT(BIGINT, ReadingsCount)) AS ReadingsCount
    FROM serving.vwSenseSensorCommunicationHealthHourly
    WHERE DevEui IS NOT NULL
    GROUP BY
        DevEui,
        CONVERT(DATE, LocalTimeSpan),
        TimeSpan
),
DeviceSensors AS (
    SELECT
        LOWER(LTRIM(RTRIM(PhysicalIdentifier))) AS DevEui,
        COUNT_BIG(*) AS SensorIdCount
    FROM dwh.dimSidonProdDimensions
    WHERE PhysicalIdentifier IS NOT NULL
    GROUP BY LOWER(LTRIM(RTRIM(PhysicalIdentifier)))
)
SELECT
    hourly.DevEui,
    hourly.LocalDate,
    sensors.SensorIdCount,
    SUM(hourly.ReceivedUplinkCount) AS ReceivedUplinkCount,
    SUM(hourly.ReadingsCount) AS ReadingsCount
FROM DeviceHourly AS hourly
INNER JOIN DeviceSensors AS sensors
    ON hourly.DevEui = sensors.DevEui
GROUP BY
    hourly.DevEui,
    hourly.LocalDate,
    sensors.SensorIdCount;
GO
