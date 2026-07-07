# expected_values

## Proposito

`dwh.dimSensorExpectedVal` define rangos esperados por sensor y horario. Es el contrato que permite decidir si una lectura esta dentro o fuera de rango.

## Granularidad

Un registro representa un rango esperado para `SensorId`, hora de inicio y dias programados.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `SensorId` | `UNIQUEIDENTIFIER` | Sensor al que aplica la regla |
| `DeviceId` | `UNIQUEIDENTIFIER` | Dispositivo asociado |
| `min_expected_value` | `DECIMAL(5,3)` | Valor minimo permitido |
| `max_expected_value` | `DECIMAL(5,3)` | Valor maximo permitido |
| `StartTime` | `TIME(0)` | Inicio de ventana horaria |
| `EndTime` | `TIME(0)` | Fin de ventana horaria |
| `scheduledDays` | `NVARCHAR(255)` | Dias aplicables, por ejemplo rango o lista |
| `ModifiedAt` | `DATETIME2(0)` | Fecha de modificacion |

## Reglas de negocio

- La llave primaria es `(SensorId, StartTime, scheduledDays)`.
- Las reglas de temperatura unen por `SensorId` y `DeviceId`.
- Si el horario cruza medianoche, el filtro considera ambos lados del rango.
- `scheduledDays` acepta `*`, `1-7`, rangos simples o lista de dias.

## Uso esperado

- Validacion de temperatura fuera de rango.
- Configuracion de reglas por sensor.
- Features de cumplimiento horario y seguimiento de programas.

