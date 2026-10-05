# Pipeline `021_bafar_store_sales_daily`

## Alcance y programación

Exclusivo de Bafar. El asset `bafar/dwh_factstoresalesdaily` agrega ventas por sucursal y fecha local. El schedule `021_bafar_store_sales_daily_every_1d` declara `RUNNING` a las 03:10 en `America/Ciudad_Juarez`; ese default no acredita registro ni ejecución en un workspace.

## Flujo

1. Lee `FactStoreSalesDaily` en `ctl.IngestionControl`, con inicio alternativo `2026-05-01`.
2. Relee desde medianoche de la fecha del watermark hasta medianoche del día actual local; el día actual queda fuera de la ventana.
3. Lee ventas desde SQL Server origen mediante `dim_sqlserver`, deduplica por `SaleId` y agrega conteo e importe por sucursal y fecha.
4. Limpia y escribe `stg.factStoreSalesDaily` con `RunId`.
5. Consolida mediante `MERGE` por `(StoreId, SalesDate)` y limpia staging.
6. En éxito, avanza al máximo día de ventas recibido; sin ventas conserva el watermark. El fallo final conserva el watermark previo y registra el error.

Admite hasta tres reintentos con demora de 60 segundos. La metadata publica `evaluated_rows`, `affected_rows`, `from_sales_date` y `to_sales_date`; no es detalle de cada venta.

## Backfill

`023_bafar_store_sales_daily_backfill_daily` procesa días desde `2026-05-01` en la misma zona horaria. Recibe `from_value` y `to_value`, utiliza el mismo staging y merge y no modifica el watermark operativo. La carga no reconstruye sucursales sin ventas ni garantiza eliminación de agregados ausentes en una recarga.

Fuentes: [asset](../../dagster/bafar/src/bafar/defs/assets/dwh_factStoreSalesDaily.py), [transformación](../../dagster/bafar/src/bafar/defs/transformations/fact_store_sales_daily.py), [schedule](../../dagster/bafar/src/bafar/defs/schedules/factsalesdaily_scheduler.py) y [backfills](../../dagster/bafar/src/bafar/defs/jobs/sales_backfill_jobs.py). El contrato de consumo está en el [modelo](../data_model/out_of_hours_sales/fact_store_sales_daily.md).
