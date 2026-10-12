# Pipeline `020_sense_operations`

## Proposito

Construir la tabla operacional horaria `dwh.factReadingsMeasurement` desde lecturas extendidas y dimensiones.

## Entradas

- Lecturas del data lake para la ventana UTC de operaciones basada en `TimeSpan`.
- `dwh.dimSidonProdDimensions`, incluyendo `TimeZone` por sensor.
- Watermark `factReadingsMeasurement` en `ctl.IngestionControl`.

## Salida

- `dwh.factReadingsMeasurement`.
- Metadatos de corrida en `ctl.IngestionControl`.

## Flujo

1. Resuelve la ventana y el watermark en UTC.
2. Lee dimensiones activas.
3. Lee lecturas por `TimeSpan` y las cruza con dimensiones.
4. Genera el scaffold, agrega y cruza las horas usando `TimeSpan`.
5. Limita filas a horas UTC cerradas y al ultimo `TimeSpan` con lecturas reales.
6. Convierte `TimeSpan` a `LocalTimeSpan` con el `TimeZone` del sensor.
7. Escribe a `stg.factReadingsMeasurement` con `RunId`.
8. Ejecuta `MERGE` hacia `dwh.factReadingsMeasurement` por `OperationId`, calculado con `SensorId` y `TimeSpan` UTC.
9. Limpia staging y actualiza watermark.

## Reglas de avance

- Si no hay lecturas reales, conserva el watermark.
- Si la corrida falla, conserva el watermark previo.
- Si hay datos reales, avanza al maximo `TimeSpan` procesado.

## Compatibilidad entre `user_codes`

El flujo existe en `ecosat`, `carlsjr`, `emerson` y `valledelencino`, con transformaciones específicas por cliente. McDonalds, CDP y Smartfit fueron retirados según [ADR 0005](../adr/0005-remove-code-locations.md). El DDL agrega `TimeSpan` nullable al histórico existente y lo exige en tablas nuevas; no reconstruye la identidad ni el tiempo del histórico. Véase [ADR 0009](../adr/0009-utc-fact-identities.md).
