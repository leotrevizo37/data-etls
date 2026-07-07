# Pipeline `010_sense_lecturas_ingestion`

## Proposito

Cargar lecturas de sensores desde parquets hacia `dwh.factLecturas` para corroboraciones especificas y consultas ad hoc.

## Entradas

- Parquets de lecturas en ADLS/Ozone.
- Watermark `FactLecturas` en `ctl.IngestionControl`.

## Salida

- `dwh.factLecturas`.

## Flujo

1. Lee el ultimo watermark de `FactLecturas`.
2. Define una ventana desde el watermark hasta `datetime.utcnow()`.
3. Borra el rango en `dwh.factLecturas` por `TimeSpan`.
4. Lee parquets del rango con Spark.
5. Escribe lecturas a SQL Server.
6. Avanza el watermark al maximo `TimeSpan` escrito, o al cierre de ventana si no hubo filas.

## Reintentos y fallas

- Tiene retry policy de 3 reintentos con espera de 60 segundos.
- Si falla una escritura, intenta limpiar filas parciales del rango.
- Si la corrida termina fallida, conserva el watermark original.

## Jobs relacionados

- `011_sense_lecturas_cleanup`
- `012_sense_lecturas_backfill_daily`
- `013_sense_lecturas_backfill_sensor`

## Limite del contrato

Ningun asset, job ni materializacion usa `dwh.factLecturas` como dependencia de entrada.
