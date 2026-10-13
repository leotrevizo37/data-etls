# Observabilidad ChirpStack

## Proposito

El modelo conserva evidencia horaria de comunicacion LoRaWAN y la integra con las lecturas de DATA sin modificar el contrato de `observability.factRedingsAudits`.

## Fuente de datos

- `dwh.dimSidonProdDimensions.PhysicalIdentifier`.
- APIs de devices, aplicaciones, perfiles y metricas de ChirpStack.
- APIs de inventario, detalle y metricas de gateways de ChirpStack.
- API de historial de eventos MQTT para `carlsjr`, `emerson` y `valledelencino`.
- `observability.factRedingsAudits`.
- Pipeline `023_sense_chirpstack_metrics`.

Los endpoints, parametros y el recorrido desde la llamada HTTP hasta los hechos se documentan en el [pipeline de observabilidad ChirpStack](../../pipelines/chirpstack_observability.md).

## Alcance

El modelo esta habilitado en `carlsjr`, `emerson` y `valledelencino`. Los DDLs son globales para conservar un contrato SQL comun, por lo que `ecosat` recibe los objetos pero no los materializa. El init especifico de `bafar` elimina las tablas, vistas y control de ingestion porque ese code location no ejecuta el pipeline.

## Tablas

| Tabla | Granularidad | Clave |
|---|---|---|
| `stg.factChirpStackDeviceMetrics` | corrida-device-hora | `RunId`, `DevEui`, `TimeSpan` |
| `observability.factChirpStackDeviceMetrics` | device-hora | `DevEui`, `TimeSpan` |
| `stg.factChirpStackGatewayMetrics` | corrida-gateway-hora | `RunId`, `GatewayId`, `TimeSpan` |
| `observability.factChirpStackGatewayMetrics` | gateway-hora | `GatewayId`, `TimeSpan` |
| `stg.factChirpStackDeviceEventMetrics` | corrida-device-hora | `RunId`, `DevEui`, `TimeSpan` |
| `observability.factChirpStackDeviceEventMetrics` | device-hora | `DevEui`, `TimeSpan` |
| `stg.factChirpStackDeviceGatewayMetrics` | corrida-device-gateway-hora | `RunId`, `DevEui`, `GatewayId`, `TimeSpan` |
| `observability.factChirpStackDeviceGatewayMetrics` | device-gateway-hora | `DevEui`, `GatewayId`, `TimeSpan` |
| `raw.MqttEventHistoryRejectedRow` | fila MQTT rechazada | `RejectedRowId` |

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
| `LinkErrorCountsJson` | Conteo por tipo de error del bucket; `NULL` si no existe evidencia |
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
| `TxPacketStatusCountsJson` | Conteo por estado de transmision del bucket |
| `MetricsAvailable` | Disponibilidad del bucket horario |

## Distribuciones MQTT por hora

`observability_chirpstackmqttmetrics` materializa los siguientes contratos en `carlsjr`, `emerson` y `valledelencino`, con staging equivalente por `RunId`:

| Tabla | Clave | Columnas JSON |
|---|---|---|
| `observability.factChirpStackDeviceEventMetrics` | `DevEui`, `TimeSpan` | `RadioProfileCountsJson`, `UplinkContextCountsJson`, `RadioFieldQualityJson`, `FrameCounterSummaryJson` |
| `observability.factChirpStackDeviceGatewayMetrics` | `DevEui`, `GatewayId`, `TimeSpan` | `RadioReceptionStatsJson`, `ReceptionFieldQualityJson` |

Las columnas son `NVARCHAR(MAX) NULL` con validacion `ISJSON`. `NULL` identifica historico sin este contrato materializado. Una hora procesada sin uplinks contiene arreglos `[]` y conteos de calidad en cero; no se crean recepciones de gateways inexistentes.

| Columna | Estructura de cada elemento o campo |
|---|---|
| `RadioProfileCountsJson` | Arreglo de combinaciones de `regionConfigId`, `adr`, `dr`, `spreadingFactor`, `bandwidthHz` y `count` |
| `UplinkContextCountsJson` | Arreglo de combinaciones de `devAddr`, `fPort` y `count` |
| `RadioFieldQualityJson` | Objeto por `regionConfigId`, `adr`, `dr`, `spreadingFactor`, `bandwidthHz`, `frequencyHz`, `devAddr`, `fPort` y `fCnt`; cada campo contiene `valid`, `missing`, `invalid` |
| `FrameCounterSummaryJson` | Arreglo por `devAddr` con `count`, `fCntCount`, `firstObservedAt`, `firstValues`, `lastObservedAt` y `lastValues` |
| `RadioReceptionStatsJson` | Arreglo por perfil radio, `frequencyHz`, `channel` y `rfChain`; cada grupo contiene `receptionCount`, `rssiCount`, `rssiSum`, `rssiMin`, `rssiMax`, `snrCount`, `snrSum`, `snrMin` y `snrMax` |
| `ReceptionFieldQualityJson` | Objeto por `channel`, `rfChain`, `rssi` y `snr`; cada campo contiene `valid`, `missing`, `invalid` |

Los conteos se calculan despues de deduplicar por `(DevEui, deduplicationId)`. La suma de `count` en cada arreglo del dispositivo equivale a `CapturedUplinkCount`; la suma de `receptionCount` equivale a `GatewayReceptionCount`. Cada campo de calidad suma el denominador de su tabla. Un uplink recibido por dos gateways cuenta una vez en el dispositivo y una vez en cada gateway.

`valid` indica valor explicito con tipo y rango admitidos; `missing` incluye clave ausente o `null`; `invalid` incluye tipo incorrecto, estructura anidada malformada o valor fuera del contrato. Las dimensiones ausentes o invalidas se representan como `null` en el grupo y se distinguen mediante sus conteos de calidad. La falta de un campo no se interpreta como un valor por defecto de Protobuf. `false`, DR0, puerto cero, canal cero y señal cero son valores explicitos validos. SF y BW no se infieren desde DR; sin `txInfo.modulation.lora` permanecen ausentes.

Se aceptan enteros DR de 0 a 15, SF de 5 a 12, `fPort` de 0 a 255 y `fCnt`, canal y cadena RF de 0 a 4294967295. Frecuencia y BW son enteros positivos hasta 4294967295 Hz. Estos limites validan el formato, no certifican compatibilidad con un plan regional. `devAddr` debe contener ocho digitos hexadecimales y se normaliza a minusculas. La region requiere texto no vacio sin espacios exteriores. Una señal invalida se excluye de sus estadisticos y aumenta `invalid`, sin eliminar una recepcion cuya identidad es valida.

Sumas, minimos y maximos de señal se serializan como texto decimal con seis decimales para evitar perdida de precision binaria; son `null` cuando su conteo es cero. Pueden consumirse con conversion decimal explicita. El promedio entre grupos u horas se calcula como suma de sumas dividida entre suma de conteos validos, nunca promediando promedios. No deben combinarse sumas entre gateways para contar uplinks unicos.

Los extremos de `fCnt` usan el tiempo UTC del evento y consideran solamente contadores validos. Si varios contadores coinciden en el timestamp extremo, `firstValues` o `lastValues` conserva todos los valores distintos ordenados; no se inventa un orden entre ellos. Sin contadores validos, los timestamps son `null` y los arreglos de valores estan vacios. Una direccion desconocida se agrupa como `null`. Este resumen no identifica sesiones, reinicios, retransmisiones ni perdidas, y no conserva los contadores intermedios.

La cardinalidad interna depende de las combinaciones observadas. No se guardan mensajes completos, `data`, `location`, `uplinkId`, `context`, tiempos de cada recepcion ni transiciones radio. Las distribuciones conservan asociaciones dentro de sus grupos, pero no el orden de mensajes ni asociaciones entre arreglos diferentes. Las observaciones de gateways distintos tampoco garantizan muestras emparejadas.

El reprocesamiento reemplaza los JSON del snapshot aceptado, sin concatenar arreglos ni sumar ejecuciones. Ambos hechos comparten la decision de aceptacion; se rechaza un snapshot que reduzca los conteos existentes o la cantidad de valores validos por campo. Esto protege evidencia acumulada, pero no demuestra que el conjunto de eventos sea completo. Los estadisticos escalares y JSON de una recepcion actualizada provienen del mismo snapshot; cero explicito se conserva como medicion valida.

## Procesamiento parcial y trazabilidad MQTT

La decision se registra en el [ADR 0008](../../adr/0008-chirpstack-partial-mqtt-evidence.md). Se aprovecha la evidencia utilizable de cada evento sin exigir que todos sus campos opcionales esten disponibles o sean validos. Esta tolerancia no elimina las validaciones de identidad, estructura y pertenencia al tenant.

| Situacion | Tratamiento actual |
|---|---|
| Campo opcional ausente o `null` | Incrementa `missing`; no se inventa un valor ni se rechaza el evento por esa ausencia |
| Campo radio o de recepcion invalido, con identidad y estructura aceptables | Incrementa `invalid`; excluye ese valor de sus estadisticos y conserva la evidencia valida del evento |
| RSSI invalido y SNR valido | Conserva el SNR, el conteo de recepciones y la evidencia temporal; no incorpora el RSSI invalido a suma, minimo, maximo ni promedio |
| Recepcion valida sin muestras de señal validas | Conserva la fila de gateway; sus escalares de señal pueden ser `NULL` |
| Error que impide aceptar la identidad o estructura del evento | Excluye el evento y lo incorpora a la coleccion de rechazos para su persistencia |
| Copia identica de un uplink ya procesado | No agrega otra observacion; un mismo identificador con payload diferente se rechaza |
| Dispositivo o timestamp fuera del alcance solicitado | Se omite; no se clasifica automaticamente como mensaje malformado |

Por tanto, la extension no es exclusivamente aditiva en columnas: aceptar eventos antes descartados por RSSI/SNR invalidos puede cambiar `CapturedUplinkCount`, `ObservedGatewayCount`, `GatewayReceptionCount`, `ReceptionShare`, los estadisticos de señal y los extremos temporales. Puede crear una fila dispositivo-gateway-hora antes inexistente, sin cambiar su granularidad. No se garantiza igualdad con los valores anteriores cuando cambia el conjunto de eventos aceptados.

`raw.MqttEventHistoryRejectedRow` recibe los rechazos de extraccion y transformacion a traves del asset. Conserva `RunId`, tenant, ventana solicitada, limite solicitado, `FailureReason`, `RowJson`, `FetchedAt` y `CreatedAt`. Actualmente los campos marcados `invalid` de un evento parcialmente aceptado se reflejan en los JSON de calidad, pero no generan por si solos una fila en ese historico. Su ausencia en el historico no demuestra que todos sus campos fueran validos.

El historico tiene retencion configurable y no es un archivo permanente ni una copia integra garantizada: el motivo se limita a 1000 caracteres y la representacion del mensaje a 4000. Una representacion truncada puede no ser JSON parseable. Los fallos de transporte o del contrato global de respuesta tampoco equivalen necesariamente a un rechazo individual persistido.

Los objetos `RadioFieldQualityJson` y `ReceptionFieldQualityJson` mantienen respectivamente nueve y cuatro campos, cada uno con tres contadores. Su estructura es fija bajo este contrato; los bytes cambian con los digitos de los conteos. Los arreglos de distribuciones tienen cardinalidad variable. `ISJSON` valida sintaxis, no todas las relaciones semanticas entre columnas. La aceptacion compara conteos validos de calidad, no cada grupo de `RadioReceptionStatsJson`.

## Columnas de eventos MQTT

`factChirpStackDeviceEventMetrics` conserva uplinks capturados, gateways observados, uplinks recibidos por mas de un gateway y conteos de logs por codigo. `factChirpStackDeviceGatewayMetrics` conserva recepciones, participacion sobre los uplinks del device, RSSI, SNR y primera/ultima recepcion para cada relacion observada. RSSI y SNR se agregan de forma independiente y permanecen `NULL` cuando ninguna recepcion del bucket aporta la medicion.

`raw.MqttEventHistoryRejectedRow` conserva el JSON de la fila recibida truncado a 4000 caracteres, el motivo del rechazo, los parametros exactos de la consulta, `RunId`, `FetchedAt` y `CreatedAt`. Su retencion se aplica por `CreatedAt` durante la ejecucion del pipeline.

## Vistas

| Vista | Granularidad | Uso |
|---|---|---|
| `observability.vwChirpStackTenantGatewayHealthHourly` | tenant-hora | Salud agregada de gateways |
| `serving.vwSenseGatewayHealthHourly` | gateway-hora | Mapa y detalle operativo de gateways |
| `serving.vwSenseSensorCommunicationHealthHourly` | sensor-hora | Diagnostico integrado DATA-ChirpStack-gateway |
| `serving.vwSenseSensorCommunicationHealthDaily` | sensor-device-dia | Tendencia diaria |
| `serving.vwSenseSensorCommunicationHealthWeekly` | sensor-device-semana | Tendencia semanal |
| `observability.vwSenseDevEuiUplinkReadingsDaily` | DevEUI-día local | Compara uplinks y lecturas sin sumar el mismo uplink por cada sensor |

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
- La relacion device-gateway solo se materializa cuando `rxInfo[]` del evento MQTT contiene el `GatewayId`; no se infiere desde metricas agregadas.
- La indisponibilidad de la API MQTT no se representa como cero y no reemplaza hechos MQTT previamente persistidos.
- En replays MQTT con `ReceptionFieldQualityJson`, un cero explícito es válido y los escalares de señal se reemplazan con el snapshot aceptado conjuntamente. La rama legacy, sin ese contrato de calidad, conserva una terna RSSI/SNR previa completa si la nueva contiene algún `NULL` o sus tres valores son cero. La aceptación sigue protegiendo conteos y señal previa frente a su desaparición; no demuestra completitud del conjunto de eventos.
