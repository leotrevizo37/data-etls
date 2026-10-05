# Pipeline `020_bafar_out_of_hours_sales`

## Proposito

Materializar las ventas Bafar fuera de horario en `dwh.factOutOfHoursSales`.

## Entradas

- Ventas Bafar desde SQL Server.
- `dwh.dimStoreSchedules`, alimentada por el pipeline `010_bafar_store_schedules`.
- Watermark `FactOutOfHoursSales`.

## Salida

- `dwh.factOutOfHoursSales`.

## Programacion

- Job: `020_bafar_out_of_hours_sales`.
- Schedule: `020_bafar_out_of_hours_sales_every_1h`.
- Frecuencia: cada hora.
- Zona horaria: `America/Ciudad_Juarez`.
- Estado inicial: detenido.

## Flujo

1. Lee el watermark o usa la fecha inicial configurada.
2. Define el inicio de la ventana a las 00:00 del dia anterior en `America/Ciudad_Juarez`.
3. Lee las ventas Bafar desde el inicio de la ventana.
4. Lee los horarios de sucursales desde `dwh.dimStoreSchedules`.
5. Clasifica cada venta con `OutOfHoursType` y `DeviationMinutes`.
6. Escribe el resultado en `stg.factOutOfHoursSales` por `RunId`.
7. Ejecuta el `MERGE` hacia `dwh.factOutOfHoursSales` y limpia el staging.
8. Actualiza el watermark solo si la corrida termina correctamente.

## Reintentos

El pipeline permite hasta tres reintentos con una espera de 60 segundos. Si no se completa, conserva el watermark previo y registra el error en el control de ingestiones.

## Backfill diario

- Job: `022_bafar_out_of_hours_sales_backfill_daily`.
- Particiones: días completos desde `2026-05-01` en `America/Ciudad_Juarez`.
- Cada partición recibe una ventana `[from_value, to_value)` y no usa el watermark `FactOutOfHoursSales`.
- El flujo lee únicamente esa ventana, obtiene los horarios de sucursal, transforma las ventas, escribe en `stg.factOutOfHoursSales` y ejecuta el mismo `MERGE` por `SaleId`.
- El staging se limpia antes y después de cada corrida, incluso si la corrida falla.
- Usa hasta tres reintentos con espera de 60 segundos.
