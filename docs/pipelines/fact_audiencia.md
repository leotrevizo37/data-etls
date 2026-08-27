# Pipeline `014_sense_audiencia_ingestion`

## Proposito

Cargar eventos de audiencia de Sense desde parquets `audience/YYYY/M/D` hacia `dwh.factAudiencia` en el `user_code` CarlsJr.

## Entradas

- Parquets de audiencia en ADLS/Ozone.
- Watermark `FactAudiencia` en `ctl.IngestionControl`.

## Salida

- Asset `carlsjr/dwh_factaudiencia`.
- Tabla `dwh.factAudiencia`.

## Flujo

1. Lee el ultimo watermark de `FactAudiencia`.
2. Define una ventana desde el watermark hasta `datetime.utcnow()`.
3. Borra el rango en `dwh.factAudiencia` por `TimeSpan`.
4. Lee los directorios diarios bajo `audience`.
5. Deduplica por `EventId` y escribe en SQL Server.
6. Avanza el watermark al maximo `TimeSpan` escrito, o al cierre de ventana si no hubo filas.

## Reintentos y fallas

- Tiene retry policy de 3 reintentos con espera de 60 segundos.
- Si falla una escritura, intenta limpiar filas parciales del rango.
- Si la corrida termina fallida, conserva el watermark original.

## Asset checks

- `row_count_gt_0`.
- `original_last_loaded_at_lte_to_utc`.

## Backfill

- Job: `015_sense_audiencia_backfill_daily`.
- Procesa particiones diarias con ventana semiabierta `[from_utc, to_utc)`.
- Borra y recarga el rango completo.
- No admite filtrado por sensores.

## Schedules

- Cada hora y cada 6 horas, detenidos por defecto.
- El schedule `014_sense_audiencia_ingestion_every_w`, activo por defecto, ejecuta en `America/Ciudad_Juarez`:
  - Lunes a viernes cada 2 horas entre las 07:00 y las 17:00, inclusive.
  - Lunes a viernes una ejecucion adicional a las 23:00.
  - Sabado y domingo unicamente a las 11:00 y 23:00.
