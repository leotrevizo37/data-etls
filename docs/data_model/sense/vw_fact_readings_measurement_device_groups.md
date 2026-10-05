# vwFactReadingsMeasurementDeviceGroups

## Proposito

`dwh.vwFactReadingsMeasurementDeviceGroups` es una vista exclusiva de `carlsjr` que empaqueta mediciones horarias por grupo de dispositivo. Su salida agrupa dispositivos y sensores en JSON para consumo por grupo operacional.

## Fuente de datos

- `dwh.factReadingsMeasurement`
- `dwh.dimSidonProdDimensions`

## Granularidad

Un registro representa un `DeviceGroup` por `LocationId` y hora local.

## Columnas

| Columna | Descripcion |
|---|---|
| `LocationId` | Ubicacion |
| `LocationName` | Nombre de ubicacion |
| `DeviceGroup` | Grupo operacional derivado de `device_type` |
| `LocalTimeSpan` | Hora local de medicion |
| `DevicesJson` | Arreglo JSON de dispositivos del grupo en esa hora |

## Reglas de agrupacion

| Condicion sobre `device_type` | `DeviceGroup` |
|---|---|
| `Refrigerador%` | `Refrigeradores` |
| `Congelador%` | `Congeladores` |
| `Cámara congelación%` | `Congeladores` |
| `Camara congelacion%` | `Congeladores` |
| `Cámara fría%` | `Congeladores` |
| `Camara fria%` | `Congeladores` |
| `Monitoreo camara%` | `Presencia` |
| `Monitoreo cámara%` | `Presencia` |
| `Nivel de cisterna%` | `Cisterna` |
| Otro valor | `device_type` |

## JSON esperado

Cada elemento de `DevicesJson` contiene:

| Propiedad | Descripcion |
|---|---|
| `DeviceId` | Dispositivo |
| `SubLocationName` | Nombre de sububicacion |
| `DeviceType` | Tipo de dispositivo |
| `DeviceName` | Nombre de dispositivo |
| `Sensors` | Arreglo JSON de sensores del dispositivo |

Cada elemento de `Sensors` contiene:

| Propiedad | Descripcion |
|---|---|
| `SensorType` | Tipo de sensor |
| `SensorName` | Nombre de sensor |
| `Value` | `MeasurementValue` |
| `ReadingsCount` | Lecturas reales que alimentaron la medicion |

## Reglas de negocio

- La vista existe en `sql/user_codes/carlsjr/init/014_dwh_vwFactReadingsMeasurementDeviceGroups.sql`.
- No es un contrato compartido por los seis `user_codes`.
- La vista se define con `CREATE OR ALTER VIEW`.
- El resultado raiz agrupa por `LocationId`, `LocationName`, `DeviceGroup` y `LocalTimeSpan`.
- `DevicesJson` solo contiene datos del dispositivo; la ubicacion se expone en columnas raiz.
- La subconsulta de `DevicesJson` filtra por `LocationId` para evitar mezclar dispositivos de distintas sucursales dentro del mismo grupo y hora.
- Los dispositivos se ordenan por sububicacion, tipo, nombre y `DeviceId`.
- Los sensores se ordenan por tipo, nombre y `SensorId`.
- `INCLUDE_NULL_VALUES` conserva valores nulos en los JSON generados.
- La agrupación utiliza hora local y no distingue por sí sola horas repetidas por DST; la identidad UTC se conserva en el [hecho base](fact_readings_measurement.md).

