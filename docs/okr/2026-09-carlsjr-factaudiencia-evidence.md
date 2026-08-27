# CarlsJr factAudiencia Sense - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Incorporar en CarlsJr un asset de Sense para conservar eventos de audiencia con su granularidad original y habilitar su consulta en el DWH.

## Entregable

Pipeline incremental y backfill diario de `factAudiencia`, con contrato SQL Server, lectura de parquets, controles de calidad y documentacion para consumo analitico.

## Valor

Integra los eventos de audiencia al dominio Sense de CarlsJr y conserva atributos de edad, genero, emocion y sentimiento para analisis por sensor y tiempo. Cada registro representa una observacion por `EventId`, no necesariamente una persona unica.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Dominio | Sense, exclusivamente en el `user_code` CarlsJr |
| Fuente | Parquets bajo `audience/YYYY/M/D` |
| Tabla principal | `dwh.factAudiencia` |
| Asset | `carlsjr/dwh_factaudiencia` |
| Job incremental | `014_sense_audiencia_ingestion` |
| Job de backfill | `015_sense_audiencia_backfill_daily` |
| Control incremental | `ctl.IngestionControl` con `IngestionName = 'FactAudiencia'` |
| Llave | `EventId` |
| Asset checks | `row_count_gt_0`, `original_last_loaded_at_lte_to_utc` |
| Schedules | Cada hora y cada 6 horas detenidos por defecto; schedule `w` activo en `America/Ciudad_Juarez` cada 2 horas de 07:00 a 17:00 y a las 23:00 entre semana, y a las 11:00 y 23:00 en fin de semana |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | `dwh.factAudiencia` | `sql/user_codes/carlsjr/init/018_dwh_factAudiencia.sql` |
| Asset | `dwh_factaudiencia` | `dagster/carlsjr/src/carlsjr/defs/assets/dwh_factAudiencia.py` |
| Job | `014_sense_audiencia_ingestion` | `dagster/carlsjr/src/carlsjr/defs/jobs/factAudiencia_ingestion.py` |
| Job de backfill | `015_sense_audiencia_backfill_daily` | `dagster/carlsjr/src/carlsjr/defs/jobs/factaudiencia_backfill_job.py` |
| Schedule | Schedules de ingestion de audiencia | `dagster/carlsjr/src/carlsjr/defs/schedules/factaudiencia_scheduler.py` |
| Recurso Spark | Lectura de `audience` y escritura de `dwh.factAudiencia` | `dagster/carlsjr/src/carlsjr/defs/resources/spark.py` |
| Asset checks | Checks de volumen y watermark | `dagster/carlsjr/src/carlsjr/defs/asset_checks.py` |

## Flujo tecnico

1. El asset obtiene el watermark de `FactAudiencia`.
2. Define una ventana UTC desde el watermark hasta la hora actual.
3. Borra el rango correspondiente en `dwh.factAudiencia`.
4. Spark localiza parquets diarios bajo la raiz `audience` y filtra la ventana por `TimeSpan`.
5. Los eventos se deduplican por `EventId` antes de escribir en SQL Server.
6. El watermark avanza al maximo `TimeSpan` escrito, o al cierre de ventana cuando no existen filas.
7. Ante una falla, se intenta limpiar la carga parcial y se conserva el watermark original.

## Backfill diario

1. `015_sense_audiencia_backfill_daily` procesa una particion por dia.
2. Utiliza la ventana semiabierta `[from_utc, to_utc)` para evitar traslapes entre dias.
3. Borra y recarga el rango diario completo.
4. No declara ni admite filtrado por `SensorId`.
5. Reutiliza la lectura y escritura de `factAudiencia` sin modificar el watermark incremental.

## Contrato de datos

| Tipo | Columnas |
|---|---|
| Obligatorias | `EventId`, `SensorId`, `TimeSpan`, `LocalTimeSpan`, `Age`, `IsKid` |
| Opcionales | `PhysicalIdentifier`, `AgeGroup`, `Gender`, `EmotionRaw`, `Emotion`, `Sentiment` |

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/carlsjr/tests/test_factaudiencia_contract.py` | Contrato DDL, raiz `audience`, deduplicacion y backfill sin sensores |
| `dagster/carlsjr/tests/test_factaudiencia_backfill.py` | Ventanas UTC, particion diaria y configuracion del backfill |
| `dagster/carlsjr/tests/test_factaudiencia_parquet.py` | Rutas diarias, lectura valida, ausencia de archivos y Parquet invalido |
| `dagster/carlsjr/tests/test_factaudiencia_nullability.py` | Columnas obligatorias, opcionales, nulas y ausentes |
| `dagster/carlsjr/tests/test_factaudiencia_asset_checks.py` | Volumen, watermark y registro de asset checks |

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release                                                                                                                                                                      |
|---|---|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Septiembre 2026 | Incorporar datos de audiencia de CarlsJr al dominio Sense | El release define el contrato DWH, la carga incremental, el backfill diario sin sensores, los controles de calidad y la documentacion necesaria para consumir `factAudiencia` de manera cruda. |
