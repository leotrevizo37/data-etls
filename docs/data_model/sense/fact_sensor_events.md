# factSensorEvents

## Proposito

`dwh.factSensorEvents` almacena eventos derivados de lecturas de sensores, como cambios de presencia o periodos de temperatura fuera de rango.

## Fuente de datos

- Fuente: lecturas extendidas con dimension de sensores.
- Valores esperados: `dwh.dimSensorExpectedVal` para eventos de temperatura.
- Pipeline: `dwh_factsensorevents`.
- Control incremental: `ctl.IngestionControl` con `IngestionName = 'FactSensorEvents'`.

## Granularidad

Un registro representa un evento detectado para un sensor.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `ReadId` | `UNIQUEIDENTIFIER NULL` | Lectura que inicia el evento; nullable tras la migración de identidad |
| `EventId` | `UNIQUEIDENTIFIER` | Identidad del evento en Carlsjr, Emerson y Valle; se calcula con sensor e inicio UTC |
| `TimeSpan` | `DATETIME2(0) NULL` | Inicio UTC; el histórico anterior puede carecer de este dato |
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor asociado |
| `DeviceId` | `UNIQUEIDENTIFIER` | Dispositivo asociado |
| `LocationId` | `UNIQUEIDENTIFIER` | Ubicacion |
| `LocationName` | `VARCHAR(255)` | Nombre de ubicacion o sububicacion |
| `DeviceType` | `VARCHAR(255)` | Tipo de dispositivo |
| `SensorType` | `VARCHAR(255)` | Tipo de sensor |
| `SensorName` | `VARCHAR(255)` | Nombre del sensor |
| `LocalTimeSpan` | `DATETIME2(0)` | Inicio local del evento |
| `Value` | `DECIMAL(19,9)` | Valor que disparo el evento |
| `EventMinutes` | `DECIMAL(19,9)` | Duracion estimada del evento |
| `IsEventCompleted` | `BIT` | Indica si el evento tiene cierre detectado |

## Reglas de negocio

- Los eventos de presencia aceptan valores `0/1` para tipos de sensor que coincidan con `presencia` sin distinguir mayúsculas. Una interrupción de comunicación de al menos 60 minutos segmenta el evento y permite cerrarlo; la ausencia de lecturas no prolonga indefinidamente su duración.
- Los eventos de temperatura usan `Temperatura F`, valores esperados y agenda por dia/hora.
- Los eventos de temperatura se conservan cuando duran al menos 120 minutos fuera de rango.
- Los eventos abiertos se reevalúan desde el menor evento abierto para preservar continuidad.
- Carlsjr, Emerson y Valle escriben a `stg.factSensorEvents` por `RunId` y consolidan mediante `MERGE` por `EventId`. Sus migraciones 016/017 agregan `TimeSpan`, renombran la identidad anterior a `EventId` y conservan `ReadId` nullable como referencia a la lectura. No reconstruyen tiempos UTC históricos; el merge elimina filas legacy sin `TimeSpan` que coinciden con sensor e inicio local de un evento recibido.

## Dependencias

- `dwh.dimSidonProdDimensions`.
- `dwh.dimSensorExpectedVal`.
- Lecturas extendidas.
- SQL Server resource para limpieza por ventana y por tipo de sensor.

## Uso esperado

- Minutos en presencia o ausencia.
- Minutos fuera de rango por sensor/dispositivo.
- Alertas y analitica de comportamiento operacional.

