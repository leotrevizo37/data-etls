CREATE TABLE dwh.factOperacionesMaquina
(
    DeviceId UNIQUEIDENTIFIER NOT NULL,
    LocalTimeSpan DATETIME2(0) NOT NULL,
    Estatus SMALLINT NOT NULL,
    Anomalias INTEGER NULL,
    Fecha AS CONVERT(DATE, LocalTimeSpan) PERSISTED,
    Hora AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED
);
GO

CREATE INDEX ix_factOperacionesMaquinas_sensor_time_span
    ON dwh.factOperacionesMaquina (DeviceId, LocalTimeSpan);
CREATE INDEX ix_factOperacionesMaquinas_time_span_deviceid
    ON dwh.factOperacionesMaquina (LocalTimeSpan, DeviceId);
CREATE UNIQUE INDEX UX_factOperacionesMaquina_Device_Sensor_Time
    ON mcdonalds.dwh.factOperacionesMaquina (DeviceId, SensorTyId, LocalTimeSpan)
    WITH (IGNORE_DUP_KEY = ON);

GO
