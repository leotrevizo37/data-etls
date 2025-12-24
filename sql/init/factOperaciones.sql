CREATE TABLE dwh.factOperaciones
(
    DeviceId UNIQUEIDENTIFIER NOT NULL,
    LocationId UNIQUEIDENTIFIER NOT NULL,
    SensorTyId UNIQUEIDENTIFIER NOT NULL,
    LocalTimeSpan DATETIME2(0) NOT NULL,
    Estatus SMALLINT NOT NULL,
    Valor DECIMAL(9, 4) NULL,
    Anomalias INTEGER NULL,
    Fecha AS CONVERT(DATE, LocalTimeSpan) PERSISTED,
    Hora AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED
);
GO

CREATE INDEX ix_factOperaciones_sensor_time_span
    ON dwh.factOperaciones (DeviceId, LocalTimeSpan);
CREATE INDEX ix_factOperaciones_time_span_location
    ON dwh.factOperaciones (LocalTimeSpan, LocationId);
CREATE INDEX ix_factOperaciones_sensorty_location
    ON dwh.factOperaciones (SensorTyId, LocationId);
CREATE UNIQUE INDEX UX_factOperaciones_Device_Sensor_Time
    ON mcdonalds.dwh.factOperaciones (DeviceId, SensorTyId, LocalTimeSpan)
    WITH (IGNORE_DUP_KEY = ON);
GO
