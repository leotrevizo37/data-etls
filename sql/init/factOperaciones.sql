CREATE TABLE dwh.factOperaciones
(
    SensorId UNIQUEIDENTIFIER NOT NULL,
    LocationId UNIQUEIDENTIFIER NOT NULL,
    LocalTimeSpan DATETIME2(0) NOT NULL,
    Estatus SMALLINT NOT NULL,
    Valor DECIMAL(9, 4) NULL,
    Fecha AS CONVERT (DATE, LocalTimeSpan) PERSISTED,
    Hora AS CONVERT(TIME(0), LocalTimeSpan) PERSISTED,
);
GO

CREATE INDEX ix_factOperaciones_sensor_time_span
    ON dwh.factOperaciones (SensorId, LocalTimeSpan);
CREATE INDEX ix_factOperaciones_time_span_location
    ON dwh.factOperaciones (LocalTimeSpan, LocationId);
GO
