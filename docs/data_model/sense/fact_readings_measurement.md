# factReadingsMeasurement

## Proposito

`dwh.factReadingsMeasurement` resume lecturas en una relacion operacional por sensor, dispositivo, ubicacion y hora UTC, con hora local para presentación. Es la tabla base para consumir estado horario sin recalcular desde lecturas crudas.

## Fuente de datos

- Fuente primaria: lecturas extendidas desde parquets y `dwh.dimSidonProdDimensions`, incluyendo `TimeZone`.
- Pipeline: `operations_assets`.
- Staging: `stg.factReadingsMeasurement`.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'factReadingsMeasurement'`.

## Granularidad

Un registro representa una combinacion `OperationId` calculada con `SensorId` y `TimeSpan` UTC. La hora local repetida durante DST no define la identidad. El histórico anterior puede conservar filas sin `TimeSpan`; agregar la columna no reconstruye automáticamente ese histórico.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `OperationId` | `UNIQUEIDENTIFIER` | Llave idempotente de la operacion horaria |
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor medido |
| `SensorTyId` | `UNIQUEIDENTIFIER` | Tipo de sensor |
| `DeviceId` | `UNIQUEIDENTIFIER` | Dispositivo asociado |
| `LocationId` | `UNIQUEIDENTIFIER` | Ubicacion o sucursal |
| `DeviceType` | `VARCHAR(255)` | Tipo funcional del dispositivo |
| `SensorType` | `VARCHAR(255)` | Tipo funcional del sensor |
| `LocalTimeSpan` | `DATETIME2(0)` | Hora local de medicion |
| `TimeSpan` | `DATETIME2(0)` | Hora UTC; obligatoria en tablas nuevas y nullable al migrar tablas anteriores |
| `MeasurementValue` | `DECIMAL(9,4)` | Valor agregado o calculado |
| `MeasurementStdDev` | `DECIMAL(9,4)` | Desviacion estandar si aplica |
| `Anomalies` | `INT` | Conteo de anomalias si aplica |
| `Min`, `Max` | `DECIMAL(9,4) NULL` | Extremos cuando la transformación los produce |
| `SensorName` | `NVARCHAR(255) NULL` | Nombre del sensor |
| `HasAnomalies` | `BIT NULL` | Indicador de anomalías |
| `CorrectReadingsJson`, `AnomalousReadingsJson` | `NVARCHAR(MAX) NULL` | Evidencia de lecturas cuando la transformación la produce |
| `ReadingsCount` | `INT` | Lecturas reales que alimentaron el registro |
| `ModifiedAt` | `DATETIME2(0)` | Fecha de ultima modificacion en DWH |

## Reglas de negocio

- Se escribe primero en `stg.factReadingsMeasurement` con `RunId`.
- La consolidacion a `dwh.factReadingsMeasurement` usa `MERGE` por `OperationId`.
- Solo se materializan horas cerradas y no se avanza el watermark si no hay lecturas reales.
- La ventana, el scaffold, las agregaciones, los cruces y el watermark usan `TimeSpan` UTC.
- `LocalTimeSpan` se calcula al final desde `TimeSpan` y el `TimeZone` del sensor.
- Las transformaciones disponibles dependen del `user_code`; CARLS, por ejemplo, usa `cisterna`, `cuartos_refrigeracion` y `presencia`.

## Dependencias

- Lecturas del data lake.
- `dwh.dimSidonProdDimensions`.
- `ctl.IngestionControl`.
- Spark profile `operaciones`.

## Uso esperado

- Vistas por dispositivo/hora.
- Dashboards de disponibilidad y comportamiento operativo.
- Base para deteccion de eventos y scores operativos derivados.

La identidad UTC y los límites de compatibilidad se documentan en [ADR 0009](../../adr/0009-utc-fact-identities.md). Las vistas JSON por hora local conservan su agrupación local; no deben usarse para distinguir por sí solas las dos ocurrencias de una hora DST.
