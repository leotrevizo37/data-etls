# Pipeline `020_sense_operations`

## Proposito

Construir la tabla operacional horaria `dwh.factReadingsMeasurement` desde lecturas extendidas y dimensiones.

## Entradas

- Lecturas del data lake para la ventana local de operaciones.
- `dwh.dimSidonProdDimensions`.
- Watermark `factReadingsMeasurement` en `ctl.IngestionControl`.

## Salida

- `dwh.factReadingsMeasurement`.
- Metadatos de corrida en `ctl.IngestionControl`.

## Flujo

1. Resuelve ventana local con zona `America/Ciudad_Juarez`.
2. Lee dimensiones activas.
3. Lee lecturas extendidas y las cruza con dimensiones.
4. Ejecuta transformaciones operativas del `user_code`.
5. Limita filas a horas cerradas y a la ultima hora con lecturas reales.
6. Escribe a `stg.factReadingsMeasurement` con `RunId`.
7. Ejecuta `MERGE` hacia `dwh.factReadingsMeasurement` por `OperationId`.
8. Limpia staging y actualiza watermark.

## Reglas de avance

- Si no hay lecturas reales, conserva el watermark.
- Si la corrida falla, conserva el watermark previo.
- Si hay datos reales, avanza al maximo `LocalTimeSpan` materializado.

## Compatibilidad entre `user_codes`

El flujo es compartido, pero la lista de transformaciones puede variar por cliente. Los cambios de contrato deben sincronizar DDL, staging, writer Spark, merge SQL y tests en los `user_codes` que implementen el mismo contrato.

