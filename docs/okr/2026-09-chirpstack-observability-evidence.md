# Observabilidad ChirpStack multi-tenant - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Incorporar evidencia horaria de ChirpStack al dominio Sense para distinguir falta de comunicacion del device, degradacion del enlace, problemas de gateway y fallas de ingestion posteriores a ChirpStack.

## Entregable

Pipeline de observabilidad para `carlsjr`, `emerson` y `valledelencino`, con extraccion desde ChirpStack, hechos horarios de devices y gateways, persistencia idempotente y vistas de serving que integran esa evidencia con `observability.factRedingsAudits`.

## Valor

Permite analizar una hora sin lecturas DATA utilizando evidencia independiente de recepcion LoRaWAN, calidad RSSI/SNR, errores, bateria disponible y estado oficial de gateways. Esto evita atribuir automaticamente toda ausencia de lecturas a una falla del sensor.

Las vistas resultantes soportan el tooltip del mapa de calor, la comparacion entre lecturas y calidad de señal y un analisis independiente de gateways, conservando la diferencia entre cero real y ausencia de evidencia.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Alcance | `carlsjr`, `emerson` y `valledelencino` |
| Relacion DATA-ChirpStack | `dwh.dimSidonProdDimensions.PhysicalIdentifier = DevEui` |
| Asset | `observability_chirpstackmetrics` |
| Job | `023_sense_chirpstack_metrics` |
| Schedule | `023_sense_chirpstack_metrics_hourly_at_30`, una vez por hora al minuto 30 |
| Ventana | Ultima hora UTC cerrada con agregacion `HOUR` |
| Hecho de devices | `observability.factChirpStackDeviceMetrics`, granularidad `DevEui`-hora |
| Hecho de gateways | `observability.factChirpStackGatewayMetrics`, granularidad `GatewayId`-hora |
| Control incremental | `ctl.IngestionControl` con `IngestionName = 'factChirpStackMetrics'` |
| Idempotencia | Staging por `RunId` y `MERGE` por las claves naturales de ambos hechos |
| Serving | Salud horaria de gateways y comunicacion de sensores horaria, diaria y semanal |
| Backfill | No soportado; el historico se construye desde la primera ejecucion |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| DDL | Hechos staging y observability de devices | `sql/init/024_observability_factChirpStackDeviceMetrics.sql` |
| DDL | Hechos staging y observability de gateways | `sql/init/025_observability_factChirpStackGatewayMetrics.sql` |
| DDL | Vistas horarias de gateways | `sql/init/026_observability_chirpstackGatewayViews.sql` |
| DDL | Vistas de comunicacion horaria, diaria y semanal | `sql/init/027_serving_sensorCommunicationHealthViews.sql` |
| Asset | Extraccion, DataFrames y orquestacion de persistencia | `dagster/<user_code>/src/<user_code>/defs/assets/observability_chirpstack_metrics.py` |
| Recurso | Cliente HTTP validado de ChirpStack | `dagster/<user_code>/src/<user_code>/defs/resources/chirpstack.py` |
| Transformacion | Normalizacion de devices y gateways | `dagster/<user_code>/src/<user_code>/defs/transformations/chirpstack_metrics.py` |
| Job | Seleccion del asset de observabilidad | `dagster/<user_code>/src/<user_code>/defs/jobs/chirpstack_metrics_job.py` |
| Schedule | Ejecucion cada 30 minutos | `dagster/<user_code>/src/<user_code>/defs/schedules/chirpstack_metrics_scheduler.py` |
| Pruebas | Contratos HTTP, transformaciones, SQL y paridad multi-tenant | `dagster/carlsjr/tests/test_chirpstack_observability.py` |

`<user_code>` representa `carlsjr`, `emerson` o `valledelencino`. Las tres implementaciones conservan el mismo contrato sin introducir una abstraccion compartida nueva.

## Flujo tecnico

1. El asset calcula la ultima hora UTC cerrada.
2. Obtiene y deduplica los `DevEui` de sensores activos desde `dwh.dimSidonProdDimensions`.
3. Consulta detalle, aplicacion, perfil, link metrics y bateria disponible por device.
4. Resuelve `TenantId` desde las aplicaciones y perfiles validados.
5. Consulta inventario, detalle, estado oficial y metricas horarias de gateways por tenant.
6. Normaliza las respuestas en `device_df` y `gateway_df` con esquemas Spark explicitos.
7. Escribe ambos DataFrames en staging con el `RunId` de Dagster.
8. Consolida ambos hechos mediante `MERGE` dentro de una misma transaccion SQL.
9. Limpia staging y actualiza el watermark solamente cuando la corrida termina correctamente.
10. Las vistas integran los hechos ChirpStack con `factRedingsAudits` para exponer el diagnostico al serving layer.

## Capacidades entregadas

| Frente | Evidencia disponible | Uso diagnostico |
|---|---|---|
| Recepcion en ChirpStack | `ReceivedUplinkCount` y `MetricsAvailable` | Distinguir ausencia de DATA de ausencia de uplinks |
| Calidad del enlace | `AverageRssi`, `AverageSnr` y `LinkErrorCount` | Relacionar degradacion de señal con perdida de lecturas |
| Frecuencia esperada | `UplinkIntervalSeconds` y `ExpectedUplinkCount` | Comparar recepcion observada contra periodicidad configurada |
| Bateria | `BatteryLevel`, `ExternalPowerSource`, fuente y timestamp | Exponer evidencia de energia cuando el device la soporta |
| Salud de gateways | Estado oficial, `GatewayLastSeenAt`, RX/TX y errores TX | Separar problemas de infraestructura de problemas del device |
| Ingestion hacia DATA | Uplinks ChirpStack contra `ReadingsCount` | Identificar evidencia compatible con un problema posterior a ChirpStack |

## Estados expuestos

| Estado | Evidencia principal |
|---|---|
| `OK` | DATA contiene lecturas |
| `DATA_INGESTION_PROBLEM` | ChirpStack recibio uplinks y DATA no contiene lecturas |
| `DEVICE_NOT_COMMUNICATING` | Device periodico sin lecturas ni uplinks y con gateway `ONLINE` |
| `GATEWAY_PROBLEM` | Device periodico sin lecturas ni uplinks, sin gateway `ONLINE` y con gateway `OFFLINE` |
| `UNKNOWN` | Evidencia insuficiente o expectativa no aplicable |

RSSI, SNR y bateria permanecen como valores crudos. No se generan estados `POOR_SIGNAL` o `DEVICE_BATTERY_LOW` sin umbrales aprobados.

## Controles de calidad

- Validacion de identidad y tenant entre device, aplicacion, perfil e inventario de gateways.
- Validacion de tipos, rangos SQL, timestamps y alineacion de series metricas.
- Paginacion de gateways con control de `totalCount` y deteccion de paginas incompletas.
- Uso del estado oficial de gateway expuesto por ChirpStack.
- `NULL` y `MetricsAvailable = 0` cuando no existe bucket, sin fabricar conteos de cero.
- Reejecucion sin bucket incapaz de reemplazar una observacion valida ya persistida.
- Fallo de la extraccion ChirpStack aislado de la materializacion de `factRedingsAudits`.
- Eliminacion de estos contratos durante la inicializacion especifica de `bafar`, que no ejecuta el pipeline.

## Validaciones disponibles

`dagster/carlsjr/tests/test_chirpstack_observability.py` cubre:

- preservacion de ceros reales y bateria;
- ausencia de bucket como `NULL`;
- rechazo de series desalineadas y valores fuera del contrato SQL;
- estado oficial y metricas de gateways;
- paginacion, autenticacion y ventana horaria de las requests;
- claves horarias y contratos DDL;
- registro exclusivo en las code locations soportadas;
- paridad de logica entre Carl's Jr., Emerson y Valle del Encino;
- staging y `MERGE` delimitados por `RunId`.

## Evidencia pendiente

La estructura, contratos y pruebas del repositorio no sustituyen una validacion contra cada instancia privada. Para acreditar operacion completa en los entornos objetivo falta corroborar:

- configuracion efectiva de URL, token, timeout y paginacion en cada code location;
- respuestas reales compatibles de las instancias ChirpStack desplegadas;
- disponibilidad de bateria en los perfiles y devices que la soporten;
- materializacion exitosa del asset con datos reales;
- creacion de los hechos y vistas en cada SQL Server objetivo;
- correspondencia operativa entre uplinks, lecturas DATA y gateways durante una incidencia real.

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release |
|---|---|---|
| Septiembre 2026 | Incorporar trazabilidad de comunicacion LoRaWAN al monitoreo de lecturas Sense | El release agrega evidencia horaria de devices y gateways, la integra con DATA y habilita diagnosticos diferenciados para sensores, enlace, gateways e ingestion. |

## Documentacion base

- `docs/features/chirpstack_observability.md`.
- `docs/pipelines/chirpstack_observability.md`.
- `docs/data_model/sense/chirpstack_observability.md`.
