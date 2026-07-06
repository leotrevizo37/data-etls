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
| `ReadId` | `UNIQUEIDENTIFIER` | Lectura que inicia el evento |
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

- Los eventos de presencia se basan en cambios de valor `0/1` para sensores `Camara de presencia`.
- Los eventos de temperatura usan `Temperatura F`, valores esperados y agenda por dia/hora.
- Los eventos de temperatura se conservan cuando duran al menos 120 minutos fuera de rango.
- Los eventos abiertos se reevalúan desde el menor evento abierto para preservar continuidad.

## Dependencias

- `dwh.dimSidonProdDimensions`.
- `dwh.dimSensorExpectedVal`.
- Lecturas extendidas.
- SQL Server resource para limpieza por ventana y por tipo de sensor.

## Uso esperado

- Minutos en presencia o ausencia.
- Minutos fuera de rango por sensor/dispositivo.
- Alertas y analitica de comportamiento operacional.

