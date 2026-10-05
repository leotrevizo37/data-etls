# ADR 0009: Identidad temporal UTC para mediciones y eventos Sense

## Estado

Implementado en el código actual. Registra el contrato observado, sin acreditar migración ni despliegue productivo.

## Contexto

Una hora local puede repetirse al retroceder el reloj por DST. Usarla como identidad puede combinar dos intervalos físicos distintos. La validación histórica del [6 de agosto](../runbooks/fact_readings_measurement_timezone_validation.md) describe el problema del contrato anterior.

## Decisión implementada

En Gabinete, Carlsjr, Emerson y Valle, las mediciones persisten `TimeSpan` UTC y calculan `OperationId` mediante el hash de sensor y tiempo UTC. `LocalTimeSpan` se deriva mediante `TimeZone` para presentación. El `MERGE` sigue identificando mediciones por `OperationId`.

En Carlsjr, Emerson y Valle, los eventos persisten inicio UTC, conservan `ReadId` como referencia a la lectura y calculan `EventId` con el prefijo `event`, sensor e inicio UTC. Se escriben en staging por `RunId` y se consolidan por `EventId`.

## Compatibilidad y límites

- El DDL global de mediciones exige `TimeSpan` en tablas nuevas y agrega la columna nullable a las existentes; no reconstruye el UTC ni reidentifica automáticamente el histórico.
- Las migraciones específicas 016/017 de eventos agregan `TimeSpan`, renombran la columna de identidad anterior a `EventId` y agregan `ReadId` nullable. El merge elimina filas legacy sin tiempo UTC cuando coinciden con sensor e inicio local de una fila recibida.
- Cambiar la identidad no demuestra por sí solo la ausencia de duplicados históricos. La convivencia del histórico debe comprobarse en el entorno donde se aplique el contrato.
- Las vistas JSON de mediciones por dispositivo y grupo conservan agrupación por hora local; sus contratos no distinguen por sí solos las dos ocurrencias de una hora DST.

## Fuentes del contrato

- [DDL de mediciones](../../sql/init/011_dwh_stg_factReadingsMeasurement.sql).
- [Identidad de mediciones Carlsjr](../../dagster/carlsjr/src/carlsjr/defs/transformations/presencia.py).
- [Identidad de eventos](../../dagster/carlsjr/src/carlsjr/defs/transformations/sensor_events.py).
- [Migración temporal de eventos](../../sql/user_codes/carlsjr/init/016_dwh_factSensorEventsTimeSpan.sql) y [migración de identidad](../../sql/user_codes/carlsjr/init/017_dwh_factSensorEventsEventId.sql), con equivalentes en Emerson y Valle.

La revisión documental no ejecutó materializaciones ni pruebas DST nuevas. Los contratos de consumo están en los modelos de [mediciones](../data_model/sense/fact_readings_measurement.md) y [eventos](../data_model/sense/fact_sensor_events.md).
