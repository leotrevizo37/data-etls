# shared_dimensions

## Proposito

Documenta las dimensiones compartidas que dan contexto a lecturas, sensores, dispositivos, sububicaciones y ubicaciones.

## Tablas

| Tabla | Proposito |
|---|---|
| `dwh.dimSidonProdDimensions` | Dimension denormalizada de sensor, dispositivo, sububicacion, ubicacion y ciudad |
| `dwh.dimSensorExpectedVal` | Valores esperados por sensor, horario y dias programados |

## `dwh.dimSidonProdDimensions`

Un registro representa un sensor activo o historico con su contexto operacional.

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `SensorId` | `UNIQUEIDENTIFIER` | Identificador del sensor |
| `SensorTyId` | `UNIQUEIDENTIFIER` | Tipo de sensor |
| `DeviceId` | `UNIQUEIDENTIFIER` | Dispositivo asociado |
| `DeviceTyId` | `UNIQUEIDENTIFIER` | Tipo de dispositivo |
| `SubLocationId` | `UNIQUEIDENTIFIER` | Sububicacion del dispositivo/sensor |
| `LocationId` | `UNIQUEIDENTIFIER` | Sucursal o ubicacion |
| `CityId` | `UNIQUEIDENTIFIER` | Ciudad |
| `ModelId`, `SensorHubId` | `UNIQUEIDENTIFIER NULL` | Modelo y hub del sensor |
| `PhysicalIdentifier` | `NVARCHAR(255) NULL` | Identificador físico; define el alcance DevEUI en ChirpStack |
| `PerEvent` | `BIT NULL` | Contexto de emisión por evento |
| `TimeZone` | `NVARCHAR(255)` | Zona horaria del sensor; obligatoria en tablas nuevas, nullable al agregarla a tablas existentes |
| `model_name`, `hub_model_name`, `hub_name` | `NVARCHAR(255) NULL` | Contexto legible de modelo y hub |
| `SubLocationTyId` | `UNIQUEIDENTIFIER` | Tipo de sububicación |
| `sublocation_type` | `NVARCHAR(255) NULL` | Tipo legible de sububicación |
| `sensor_name`, `sensor_type` | `NVARCHAR(255)` | Nombre y tipo funcional del sensor |
| `device_name`, `device_type` | `NVARCHAR(255)` | Nombre y tipo funcional del dispositivo |
| `sublocation_name`, `location_name`, `city_name` | `NVARCHAR(255)` | Jerarquia legible de ubicacion |
| `Active` | `BIT` | Marca de sensor activo |
| `ModifiedAt` | `DATETIME2(0)` | Fecha de modificacion en DWH |

## Reglas

- `SensorId` tiene indice unico con `IGNORE_DUP_KEY`.
- Los pipelines de operaciones filtran `Active = 1` para construir mediciones operativas.
- La dimension se usa como contrato de enriquecimiento, no como fuente de hechos.

## Dependencias

- Fuentes dimensionales del sistema Sidon.
- Asset de refresh de dimensiones en cada `user_code`.
- Pipelines Sense: `factReadingsMeasurement` y `factSensorEvents`.

## `dwh.dimSensorExpectedVal`

Un registro representa una regla de valores esperados para un sensor, dispositivo, horario y dias programados.

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor al que aplica la regla |
| `DeviceId` | `UNIQUEIDENTIFIER` | Dispositivo asociado |
| `min_expected_value` | `DECIMAL(5,3)` | Valor minimo esperado |
| `max_expected_value` | `DECIMAL(5,3)` | Valor maximo esperado |
| `StartTime` | `TIME(0)` | Hora inicial de vigencia diaria |
| `EndTime` | `TIME(0)` | Hora final de vigencia diaria |
| `scheduledDays` | `NVARCHAR(255)` | Dias programados para aplicar la regla |
| `ModifiedAt` | `DATETIME2(0)` | Fecha de modificacion en DWH |

## Reglas

- La llave primaria es `(SensorId, StartTime, scheduledDays)`.
- El indice `ix_dimSensorExpectedVal_DeviceId_SensorId` soporta consultas por dispositivo y sensor.
- `factSensorEvents` usa esta dimension para detectar eventos de temperatura fuera de rango.

## Dependencias

- Fuentes dimensionales del sistema Sidon para horarios y programas.
- El refresh depende de la implementación del tenant; la existencia del DDL global no demuestra que cada location tenga un asset de refresco. El contrato de consumo se precisa en [expected_values.md](sense/expected_values.md).
