# vwFactReadingsMeasurementDevice

## Proposito

`dwh.vwFactReadingsMeasurementDevice` empaqueta las mediciones horarias por dispositivo y hora local, incluyendo los sensores como JSON para consumo por APIs, dashboards o agentes.

## Fuente de datos

- `dwh.factReadingsMeasurement`
- `dwh.dimSidonProdDimensions`

## Granularidad

Un registro representa un dispositivo en una ubicacion/sububicacion y hora local.

## Columnas

| Columna | Descripcion |
|---|---|
| `DeviceId` | Dispositivo |
| `LocationId` | Ubicacion |
| `LocationName` | Nombre de ubicacion |
| `SubLocationName` | Nombre de sububicacion |
| `DeviceType` | Tipo de dispositivo |
| `DeviceName` | Nombre del dispositivo |
| `LocalTimeSpan` | Hora local |
| `SensorsJson` | Arreglo JSON con sensores, valores y conteo de lecturas |

## JSON esperado

Cada elemento de `SensorsJson` contiene:

| Propiedad | Descripcion |
|---|---|
| `SensorType` | Tipo de sensor |
| `SensorName` | Nombre de sensor |
| `Value` | `MeasurementValue` |
| `ReadingsCount` | Lecturas reales que alimentaron la medicion |

## Reglas de negocio

- La vista agrupa por dispositivo, ubicacion, sububicacion y hora.
- Los sensores se ordenan por tipo, nombre y `SensorId`.
- `INCLUDE_NULL_VALUES` conserva valores nulos en JSON.
- La agrupación y el JSON utilizan `LocalTimeSpan`; no distinguen las dos ocurrencias UTC de una hora local repetida por DST. Para identidad temporal utilizar el contrato de [mediciones](fact_readings_measurement.md).

