CREATE TABLE dwh.factLecturas
(
    ReadId UNIQUEIDENTIFIER NOT NULL,
    SensorId UNIQUEIDENTIFIER NOT NULL,
    TimeSpan DATETIME2(0) NOT NULL,
    LocalTimeSpan DATETIME2(0) NULL,
    Value DECIMAL(19, 9) NOT NULL,
    CONSTRAINT PK_FactLecturas
    PRIMARY KEY CLUSTERED (ReadId)
    WITH (IGNORE_DUP_KEY = ON) -- noqa: PRS
);
GO

CREATE INDEX ix_factlecturas_sensor_time_span
    ON dwh.factlecturas (SensorId, TimeSpan);
GO
