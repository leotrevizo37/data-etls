CREATE TABLE dwh.factlecturas
(
    ReadId UNIQUEIDENTIFIER NOT NULL,
    SensorId UNIQUEIDENTIFIER NOT NULL,
    TimeSpan DATETIME2(0) NOT NULL,
    LocalTimeSpan DATETIME2(0) NULL,
    ReadingValue DECIMAL(19, 9) NOT NULL,
    Fecha AS CONVERT(DATE, LocalTimeSpan) PERSISTED,
    Hora AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED,
    CONSTRAINT pk_factlecturas PRIMARY KEY CLUSTERED (ReadId)
);
GO

CREATE INDEX ix_factlecturas_sensor_time_span
    ON dwh.factlecturas (SensorId, TimeSpan);
GO
