# Bafar ventas fuera de horario - Evidencia técnica del ETL - 2026-07-24

Estado: PENDIENTE DE API BAFAR PRODUCTIVA

## Objetivo cubierto

Desarrollo ETL para la alimentación del dashboard de ventas fuera de horario de ReporteAdministracion.

## Entregable

Pipeline en producción alimentando el dashboard de ventas fuera de horario.

## Valor

Habilita la integración del dato de ventas fuera de horario en BI solicitada por el cliente.

## Evidencia directa

| Área | Evidencia |
|---|---|
| Tabla principal | `dwh.factOutOfHoursSales` |
| Staging | `stg.factOutOfHoursSales` |
| Horarios | `dwh.dimStoreSchedules` |
| Catálogo de tipos | `dwh.dimOutOfHoursTypes` |
| Control incremental | `ctl.IngestionControl` con `IngestionName = 'FactOutOfHoursSales'` |
| Asset | `bafar/dwh_factoutofhourssales` |
| Job | `020_bafar_out_of_hours_sales` |
| Job de backfill | `022_bafar_out_of_hours_sales_backfill_daily` |
| Schedule | `020_bafar_out_of_hours_sales_every_1h` |
| Frecuencia | Cada hora, zona `America/Ciudad_Juarez` |
| Particiones de backfill | Días completos desde `2026-05-01` en `America/Ciudad_Juarez` |
| Fuente transaccional | Ventas Bafar desde SQL Server |
| Transformación | Clasificación por horario de sucursal, día de la semana y vigencia del horario |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | `dwh.dimStoreSchedules`, `stg.dimStoreSchedules` | `sql/user_codes/bafar/init/001_dwh_stg_dimStoreSchedules.sql` |
| DDL | `dwh.dimOutOfHoursTypes` | `sql/user_codes/bafar/init/002_dwh_dimOutOfHoursTypes.sql` |
| DDL | `dwh.factOutOfHoursSales`, `stg.factOutOfHoursSales` | `sql/user_codes/bafar/init/010_dwh_stg_factOutOfHoursSales.sql` |
| Asset | `dwh_factoutofhourssales` | `dagster/bafar/src/bafar/defs/assets/dwh_factOutOfHoursSales.py` |
| Job | `020_bafar_out_of_hours_sales` | `dagster/bafar/src/bafar/defs/jobs/factoutofhourssales_job.py` |
| Job de backfill | `022_bafar_out_of_hours_sales_backfill_daily` | `dagster/bafar/src/bafar/defs/jobs/sales_backfill_jobs.py` |
| Schedule | `020_bafar_out_of_hours_sales_every_1h` | `dagster/bafar/src/bafar/defs/schedules/factoutofhourssales_scheduler.py` |
| Transformación | Clasificación de ventas fuera de horario | `dagster/bafar/src/bafar/defs/transformations/fact_out_of_hours_sales.py` |
| Tests | Ventana, transformación, backfill y contrato `MERGE` | `dagster/bafar/tests/test_fact_out_of_hours_sales_window.py`, `dagster/bafar/tests/test_fact_out_of_hours_sales_merge_sql.py`, `dagster/bafar/tests/test_sales_backfill_jobs.py` |

## Flujo técnico

1. El asset obtiene el watermark de `FactOutOfHoursSales` y reprocesa desde las 00:00 del día anterior en `America/Ciudad_Juarez`.
2. Spark lee las ventas Bafar desde SQL Server y los horarios vigentes desde `dwh.dimStoreSchedules`.
3. La transformación identifica ventas antes de apertura, durante el horario de comida, después del cierre o sin horario aplicable.
4. El resultado se escribe en `stg.factOutOfHoursSales` con el `RunId` de la corrida.
5. SQL Server consolida en `dwh.factOutOfHoursSales` mediante `MERGE` por `SaleId`.
6. Las ventas que ya no están fuera de horario se eliminan de la tabla final durante el `MERGE`.
7. El staging se limpia y el watermark avanza únicamente cuando la corrida finaliza correctamente.

## Backfill diario

1. `022_bafar_out_of_hours_sales_backfill_daily` genera una corrida por día completo desde `2026-05-01` en `America/Ciudad_Juarez`.
2. La corrida recibe los límites `from_value` y `to_value` de su partición, sin consultar ni modificar `FactOutOfHoursSales` en `ctl.IngestionControl`.
3. Lee la ventana acotada de ventas, aplica los horarios vigentes y reutiliza el staging y `MERGE` del flujo incremental.
4. Limpia las filas de staging de su `RunId` antes y después de procesar; también intenta limpiarlas si ocurre un error.

## Reglas funcionales

| Tipo | Regla |
|---:|---|
| `1` | Venta antes de la apertura. |
| `2` | Venta durante el intervalo de comida, cuando existen ambas horas de comida. |
| `3` | Venta igual o posterior al cierre. |
| `4` | Venta sin horario aplicable para la sucursal, fecha o día de la semana. |

## Evidencia pendiente para el entregable

La evidencia técnica confirma que el pipeline incremental, el backfill diario y sus contratos están definidos en el `user_code` Bafar. Para declarar el entregable como productivo falta corroborar una materialización exitosa en el entorno de producción y la disponibilidad de `dwh.factOutOfHoursSales` en el dashboard de ReporteAdministracion.

## Relación con OKRs de julio

| Fecha | Objetivo | Relación con este release |
|---|---|---|
| 2026-07-24 | Desarrollo ETL para alimentación de dashboard de ventas fuera de horario | El pipeline Bafar implementa el contrato técnico que alimenta el dato de BI; queda pendiente la evidencia productiva del dashboard. |
