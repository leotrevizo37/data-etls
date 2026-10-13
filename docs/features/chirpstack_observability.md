# Observabilidad ChirpStack

## Proposito

Agregar evidencia de ChirpStack al diagnostico horario de sensores para distinguir una ausencia real de comunicacion de un problema de gateway o de ingestion hacia DATA.

La feature complementa `observability.factRedingsAudits`; no sustituye el frente de lecturas ya existente ni modifica su contrato.

## Alcance

| User code | Asset Dagster | Contratos SQL |
|---|---|---|
| `carlsjr` | habilitado | globales |
| `emerson` | habilitado | globales |
| `valledelencino` | habilitado | globales |
| `ecosat` | no habilitado | globales, sin materializacion |
| `bafar` | no aplica | eliminados por su inicializacion especifica |

## Diagnostico que habilita

```text
DATA no contiene lecturas
        ↓
¿ChirpStack recibio uplinks?
        ↓
¿Como estaba el enlace RSSI/SNR?
        ↓
¿Los gateways del tenant estaban disponibles?
        ↓
¿Cuantos uplinks se esperaban?
        ↓
Causa probable con evidencia
```

La comparacion se realiza sobre la misma hora UTC cerrada y conserva por separado:

- lecturas presentes en DATA;
- uplinks recibidos por ChirpStack;
- expectativa derivada del intervalo del device profile;
- RSSI, SNR y errores disponibles;
- estado oficial y actividad de gateways;
- bateria para devices cuyo perfil declare esa medicion.

La API de historial MQTT agrega evidencia de eventos individuales: conteos de logs, uplinks capturados y relaciones device-gateway observadas directamente en `rxInfo[]`.

## Experiencia de consumo

La vista horaria permite enriquecer el mapa de calor de lecturas por sensor. Para cada celda se pueden mostrar las lecturas DATA junto con la recepcion en ChirpStack, calidad del enlace, evidencia de bateria y salud agregada de gateways.

El consumidor puede alternar la metrica visual entre cantidad de lecturas y calidad de señal sin perder el contexto horario. `serving.vwSenseGatewayHealthHourly` permite construir un mapa independiente de gateways con estado oficial, ultimo contacto y actividad RX/TX.

Las vistas diaria y semanal resumen continuidad, cobertura y horas clasificadas sin reemplazar la evidencia horaria utilizada por el tooltip.

## Estados de comunicacion

| Estado | Interpretacion |
|---|---|
| `OK` | DATA contiene lecturas para la hora. |
| `DATA_INGESTION_PROBLEM` | ChirpStack recibio uplinks, pero DATA no contiene lecturas. |
| `DEVICE_NOT_COMMUNICATING` | El device periodico no produjo lecturas ni uplinks y existe al menos un gateway `ONLINE`. |
| `GATEWAY_PROBLEM` | El device periodico no produjo lecturas ni uplinks, no existe gateway `ONLINE` y existe al menos uno `OFFLINE`. |
| `UNKNOWN` | La expectativa o la evidencia disponible no permiten una atribucion segura. |

RSSI, SNR y bateria se exponen como valores crudos. La feature no clasifica `POOR_SIGNAL` ni `DEVICE_BATTERY_LOW` sin umbrales aprobados.

## Reglas de interpretacion

- `ReceivedUplinkCount` representa paquetes recibidos por ChirpStack, no intentos fisicos de transmision del sensor.
- `ExpectedUplinkCount` es una expectativa basada en `uplinkInterval`, no prueba de que el firmware transmitio.
- `ReadingsCount` y `ReceivedUplinkCount` no mantienen necesariamente una relacion uno a uno.
- RSSI y SNR solo existen para paquetes recibidos.
- Cero significa una medicion disponible sin eventos; `NULL` significa que no existe evidencia suficiente.
- El estado y `lastSeenAt` del gateway son snapshots observados al extraer, no un historico previo proporcionado por ChirpStack.
- Solo se atribuye un device a un gateway cuando `rxInfo[]` aporta evidencia explicita del evento.

## Historia e idempotencia

La feature comienza a construir su propio historico desde la primera ejecucion. No implementa backfill porque las APIs consultadas no ofrecen el historico horario necesario para reconstruir periodos anteriores con la misma calidad.

Cada corrida procesa la ultima hora UTC cerrada. Una ventana movil configurable vuelve a consultar eventos MQTT recientes para compensar entregas tardias. Las claves naturales `(DevEui, TimeSpan)`, `(GatewayId, TimeSpan)` y `(DevEui, GatewayId, TimeSpan)` evitan duplicados, y una falla de la API MQTT no reemplaza evidencia valida ya almacenada.

## Documentacion relacionada

- [Pipeline y endpoints de ChirpStack](../pipelines/chirpstack_observability.md).
- [Modelo de datos de observabilidad ChirpStack](../data_model/sense/chirpstack_observability.md).
