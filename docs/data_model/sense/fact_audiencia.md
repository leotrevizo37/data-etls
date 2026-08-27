# factAudiencia

## Proposito

`dwh.factAudiencia` pertenece a Sense y almacena eventos de audiencia detectados por sensores de Carl's Jr. Cada fila representa una observacion identificada por `EventId`; no representa necesariamente una persona unica.

## Fuente de datos

- Fuente: parquets en `audience/YYYY/M/D` leidos por Spark.
- Pipeline: `dwh_factaudiencia`.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'FactAudiencia'`.
- Alcance: exclusivamente el `user_code` CarlsJr.

## Granularidad

Un registro representa un evento de audiencia por `EventId`.

## Columnas

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `EventId` | `UNIQUEIDENTIFIER` | Llave unica del evento |
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor que genero la observacion |
| `PhysicalIdentifier` | `NVARCHAR(255)` | Identificador fisico reportado por la fuente |
| `TimeSpan` | `DATETIME2(7)` | Timestamp UTC del evento |
| `LocalTimeSpan` | `DATETIME2(7)` | Timestamp local del evento |
| `Age` | `INT` | Edad estimada |
| `AgeGroup` | `NVARCHAR(255)` | Grupo de edad estimado |
| `IsKid` | `BIT` | Indicador de menor de edad |
| `Gender` | `NVARCHAR(255)` | Genero estimado |
| `EmotionRaw` | `NVARCHAR(255)` | Emocion original reportada |
| `Emotion` | `NVARCHAR(255)` | Emocion normalizada |
| `Sentiment` | `NVARCHAR(255)` | Sentimiento asociado |

## Reglas de carga

- La carga incremental borra y reinserta la ventana por `TimeSpan`.
- Los registros se deduplican por `EventId` antes de escribir.
- El watermark avanza al maximo `TimeSpan` escrito; si no hay filas, avanza al cierre de ventana.
- En error, el pipeline intenta limpiar filas parciales y conserva el watermark previo.
- `EventId`, `SensorId`, `TimeSpan`, `LocalTimeSpan`, `Age` e `IsKid` son obligatorios.

## Uso esperado

- Analisis de eventos de audiencia por sensor y tiempo.
- Distribuciones de atributos estimados reportados por la fuente.
- Validacion puntual de eventos conservando la granularidad original.
