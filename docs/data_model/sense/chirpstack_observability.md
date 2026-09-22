# Observabilidad ChirpStack

## Proposito

El modelo conserva evidencia horaria de comunicacion LoRaWAN y la integra con las lecturas de DATA sin modificar el contrato de `observability.factRedingsAudits`.

## Fuente de datos

- `dwh.dimSidonProdDimensions.PhysicalIdentifier`.
- APIs de devices, aplicaciones, perfiles y metricas de ChirpStack.
- APIs de inventario, detalle y metricas de gateways de ChirpStack.
- `observability.factRedingsAudits`.
- Pipeline `023_sense_chirpstack_metrics`.

Los endpoints, parametros y el recorrido desde la llamada HTTP hasta los hechos se documentan en el [pipeline de observabilidad ChirpStack](../../pipelines/chirpstack_observability.md).

## Alcance

El modelo esta habilitado en `carlsjr`, `emerson` y `valledelencino`. Los DDLs son globales para conservar un contrato SQL comun, por lo que `bafar_poc_gabinete` recibe los objetos pero no los materializa. El init especifico de `bafar` elimina las tablas, vistas y control de ingestion porque ese code location no ejecuta el pipeline.

## Tablas

| Tabla | Granularidad | Clave |
|---|---|---|
| `stg.factChirpStackDeviceMetrics` | corrida-device-hora | `RunId`, `DevEui`, `TimeSpan` |
| `observability.factChirpStackDeviceMetrics` | device-hora | `DevEui`, `TimeSpan` |
| `stg.factChirpStackGatewayMetrics` | corrida-gateway-hora | `RunId`, `GatewayId`, `TimeSpan` |
| `observability.factChirpStackGatewayMetrics` | gateway-hora | `GatewayId`, `TimeSpan` |

## Columnas principales de devices

| Columna | Descripcion |
|---|---|
| `DevEui` | Identificador fisico relacionado con `PhysicalIdentifier` |
| `TimeSpan` | Inicio UTC de la hora observada |
| `TenantId`, `ApplicationId`, `DeviceProfileId` | Contexto validado del device |
| `UplinkIntervalSeconds` | Intervalo configurado en el perfil |
| `ExpectedUplinkCount` | Uplinks esperados durante la hora |
| `ReceivedUplinkCount` | Uplinks recibidos por ChirpStack |
| `AverageRssi`, `AverageSnr` | Calidad agregada del enlace recibido |
| `LinkErrorCount` | Errores reportados para el bucket |
| `BatteryLevel`, `ExternalPowerSource` | Evidencia de bateria o alimentacion disponible |
| `BatteryLevelSource`, `BatteryObservedAt` | Origen y momento del valor de bateria |
| `MetricsAvailable` | Distingue bucket disponible de falta de evidencia |

## Columnas principales de gateways

| Columna | Descripcion |
|---|---|
| `GatewayId`, `GatewayName`, `TenantId` | Identidad y contexto del gateway |
| `TimeSpan` | Inicio UTC de la hora observada |
| `GatewayState` | Estado oficial `NEVER_SEEN`, `ONLINE` u `OFFLINE` |
| `GatewayLastSeenAt` | Ultimo contacto informado al extraer |
| `StatsIntervalSeconds` | Intervalo configurado de estadisticas |
| `ReceivedPacketCount`, `TransmittedPacketCount` | Actividad RX y TX |
| `TxOkCount`, `TxErrorCount` | Resultado agregado de transmisiones |
| `MetricsAvailable` | Disponibilidad del bucket horario |

## Vistas

| Vista | Granularidad | Uso |
|---|---|---|
| `observability.vwChirpStackTenantGatewayHealthHourly` | tenant-hora | Salud agregada de gateways |
| `serving.vwSenseGatewayHealthHourly` | gateway-hora | Mapa y detalle operativo de gateways |
| `serving.vwSenseSensorCommunicationHealthHourly` | sensor-hora | Diagnostico integrado DATA-ChirpStack-gateway |
| `serving.vwSenseSensorCommunicationHealthDaily` | sensor-device-dia | Tendencia diaria |
| `serving.vwSenseSensorCommunicationHealthWeekly` | sensor-device-semana | Tendencia semanal |

## Estados de comunicacion

| Estado | Evidencia |
|---|---|
| `OK` | DATA contiene lecturas |
| `DATA_INGESTION_PROBLEM` | ChirpStack recibio uplinks y DATA no contiene lecturas |
| `DEVICE_NOT_COMMUNICATING` | DATA no contiene lecturas, el sensor periodico debia transmitir, no hay uplinks recibidos y existe un gateway `ONLINE` |
| `GATEWAY_PROBLEM` | DATA no contiene lecturas, el sensor periodico debia transmitir, no hay uplinks, no existe gateway `ONLINE` y existe al menos uno `OFFLINE` |
| `UNKNOWN` | La evidencia no permite una clasificacion segura |

RSSI, SNR y bateria se exponen como valores crudos. El modelo no crea estados de mala señal o bateria baja sin umbrales aprobados.

## Reglas de calidad

- `ReceivedUplinkCount` cuenta recepcion en ChirpStack, no intentos fisicos de transmision.
- `ReadingsCount` puede no corresponder uno a uno con uplinks.
- Una hora ausente permanece `NULL` con `MetricsAvailable = 0`.
- `GatewayState` y `GatewayLastSeenAt` representan el snapshot observado en `ExtractedAt`.
- No se infiere una relacion device-gateway especifica sin evidencia del evento.
