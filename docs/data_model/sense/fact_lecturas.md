# factLecturas

## Proposito

`dwh.factLecturas` almacena lecturas crudas de sensores en SQL Server para corroboraciones muy especificas, backfills acotados y consultas ad hoc. No es dependencia de otros assets, jobs ni materializaciones.

## Fuente de datos

- Fuente: parquets de lecturas en ADLS/Ozone leidos por Spark.
- Pipeline: `dwh_factlecturas`.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'FactLecturas'`.

## Granularidad

Un registro representa una lectura individual de un sensor.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `ReadId` | `UNIQUEIDENTIFIER` | Llave unica de la lectura |
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor que emitio la lectura |
| `TimeSpan` | `DATETIME2(0)` | Timestamp UTC de la lectura |
| `LocalTimeSpan` | `DATETIME2(0)` | Timestamp local de la lectura |
| `Value` | `DECIMAL(19,9)` | Valor registrado |

## Reglas de negocio

- La carga trabaja por ventana incremental y borra/reinserta el rango antes de escribir.
- El watermark avanza al maximo `TimeSpan` escrito; si no hay filas, avanza al cierre de ventana.
- En error, el pipeline intenta limpiar filas parciales del rango y conserva el watermark previo.

## Dependencias

- Spark resource para leer parquets.
- SQL Server resource para borrar ventana, escribir y cerrar control de ingestion.
- Indices sobre `(SensorId, TimeSpan)` y `LocalTimeSpan DESC`.

## Uso esperado

- Corroboraciones puntuales contra lecturas historicas.
- Backfills acotados de la propia tabla.
- Consultas ad hoc de validacion.
