# Deteccion de señales de alerta por tenant - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Detectar de forma aislada por tenant el inicio y fin comprobable de condiciones de sensores, ingestiones, corridas de Dagster y tendencias de volumen, y persistir señales que un sistema externo pueda consumir sin asignar a Dagster el ciclo de vida ni la entrega de alertas.

## Entregable

Las code locations `bafar`, `bafar_poc_gabinete`, `carlsjr`, `emerson` y `valledelencino` registran dos sensores locales. En `carlsjr` y `emerson` comienzan en estado `RUNNING`; en los demás tenants permanecen `STOPPED`. Cada copia consulta únicamente la base SQL Server de su tenant y persiste incidencias y señales `DETECTED` o `CLEARED`. El sistema externo de alertamiento es responsable del workflow, correo, tickets y otros canales.

La code location `alerting` conserva ambos nombres de sensor solo por compatibilidad operativa. Sus sensores permanecen `STOPPED`, no solicitan recursos y siempre devuelven `SkipReason`; no realizan detección, persistencia ni entrega.

## Valor

La deteccion queda junto a los datos y la configuración que pertenecen a cada tenant, sin lecturas multitenant ni estado de entrega dentro de los pipelines. Las señales conservan evidencia y correlación suficientes para que el sistema externo administre reconocimiento, resolución administrativa y acciones sin inventar estados cuando todavía no existen eventos de workflow o de acción.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Alcance | `bafar`, `bafar_poc_gabinete`, `carlsjr`, `emerson` y `valledelencino` |
| Deteccion | Copias locales de `operational_alerts_sensor` y `asset_row_count_trend_sensor` |
| Estado inicial | Ambos sensores están `RUNNING` en `carlsjr` y `emerson`; permanecen `STOPPED` en los demás tenants |
| Recurso | `sqlserver` de la code location correspondiente |
| Sensor operativo | Intervalo minimo de 30 segundos; la cadencia efectiva de cada regla se obtiene de la configuración activa |
| Sensor de tendencias | Intervalo minimo de 21600 segundos, consistente con `capture_interval_seconds` |
| Persistencia | Esquema `alerting` en la base SQL Server de cada tenant |
| Serving | `serving.vwEstadoAlertas` y `serving.vwValidacionConfiguracionesAlerta` |
| Acciones externas | Dagster no envia correo, no crea tickets y no escribe estados de workflow |
| Compatibilidad | Code location `alerting` con dos sensores inertes, sin recursos ni registro en `workspace.yaml` |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| Arquitectura | Separacion entre deteccion, workflow y acciones | `docs/architecture/alerting.md` |
| ADR vigente | Deteccion de señales en cada tenant | `docs/adr/0007-tenant-alert-detection.md` |
| Sensor | Deteccion operativa tenant-specific | `dagster/<user_code>/src/<user_code>/defs/sensors/operational_alerts_sensor.py` |
| Sensor | Captura y evaluacion de tendencias tenant-specific | `dagster/<user_code>/src/<user_code>/defs/sensors/asset_row_count_trend_sensor.py` |
| Compatibilidad | Sensores centrales inertes | `dagster/alerting/src/alerting/defs/sensors/` |
| Configuracion | Versiones, reglas default y overrides exactos | `sql/init/028_alerting_config_model.sql` |
| Incidencias | Identidad y evidencia raiz | `sql/init/029_alerting_event_model.sql` |
| Serving | Estado consolidado y validacion de configuración | `sql/init/030_serving_alert_config_validation.sql` y `sql/init/031_serving_alert_state.sql` |
| Protecciones | Inmutabilidad, referencias y activacion transaccional | `sql/init/032_alerting_state_guards.sql` y `sql/init/033_alerting_catalog_guards.sql` |
| Provisionamiento | Sin configuracion por defecto; cada version se crea y activa explícitamente por tenant | `docs/runbooks/alerting_sql_configuration.md` |

## Señales cubiertas

| Tipo | Fuente | Regla principal |
|---|---|---|
| Sensor desconectado | `observability.factRedingsAudits` | Tiempo sin lecturas por encima del umbral configurado |
| Lecturas retrasadas | `observability.factRedingsAudits` | Retraso sin perdida total de comunicacion, cuando la regla esta habilitada |
| Ingestion fallida | `ctl.IngestionControl` | Ultima ejecucion terminal con `LastRunStatus = 'FAILED'` |
| Pipeline atrasado | `ctl.IngestionControl` | Ultima ejecucion exitosa fuera de la cadencia configurada |
| Corrida fallida | Event log de Dagster | Paso fallido relacionado con assets comprobables de la misma code location |
| Tendencia de volumen | Event log de Dagster | Caida simultanea contra promedios de 7 y 28 dias en metadata `row_count` |

Las condiciones continuas solo se cierran cuando existe evidencia posterior a su deteccion. Para sensores se requiere una lectura nueva; para ingestiones, una ejecución exitosa posterior; para fallos Dagster, una materializacion posterior del mismo asset en la misma code location; y para tendencias, una materializacion posterior con volumen saludable. La ausencia de datos, una fuente obsoleta o una regla deshabilitada no producen `CLEARED`.

Cada fallo Dagster se deduplica por `run_id` y asset afectado. El detector recorre las dependencias downstream de un paso fallido, excluye assets materializados por la misma corrida y no emite una señal cuando no puede demostrar el asset afectado.

## Tendencias de lecturas

1. El sensor consume incrementalmente eventos `ASSET_MATERIALIZATION` mediante cursor.
2. Solo registra materializaciones con metadata `row_count` valida.
3. Agrega las observaciones en `alerting.asset_row_count_daily` de la base del tenant por dia UTC, code location, asset y clave de metadata.
4. La captura queda habilitada al transcurrir al menos 21600 segundos y conserva paginacion pendiente mediante el cursor del sensor.
5. La comparacion usa dias UTC cerrados y exige la cobertura configurada para las ventanas de 7 y 28 dias.
6. Abre `WARNING` cuando ambas comparaciones alcanzan la caida configurada y `CRITICAL` cuando alcanzan el umbral critico.
7. Una observacion posterior saludable puede producir la señal `CLEARED` de la incidencia activa.

## Persistencia y responsabilidades

| Objeto | Responsabilidad |
|---|---|
| `alerting.alert_incident` | Identidad, evidencia raiz, deteccion y fin comprobable de la condicion |
| `alerting.alert_signal_event` | Eventos append-only `DETECTED` y `CLEARED` emitidos por Dagster |
| `alerting.alert_workflow_event` | Estados del ciclo de vida escritos exclusivamente por el sistema externo |
| `alerting.alert_action_event` | Intentos y resultados de correo, tickets u otras acciones externas |
| `alerting.asset_row_count_daily` | Historia diaria utilizada para tendencias de volumen |
| `serving.vwEstadoAlertas` | Vista de reporteo por incidencia, sin inventar workflow ni acciones ausentes |

La configuracion sigue el ciclo `DRAFT` → `ACTIVE` → `RETIRED`. Cada versión valida contiene una regla default por cada tipo y puede incluir overrides por `entity_key`. La activacion valida errores bloqueantes, retira la version activa anterior y activa la nueva dentro de una transaccion.

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/<user_code>/tests/test_alerting_sensors.py` | Configuracion, deduplicacion, deteccion, recuperacion y contratos SQL de cada copia tenant-specific |
| `dagster/alerting/tests/test_alerting.py` | Registro exclusivo de los dos sensores centrales inertes, sin recursos y en estado `STOPPED` |
| `dagster/alerting/tests/test_asset_row_count_trends.py` | Paridad de las copias tenant-specific y contratos de captura y tendencias |

## Evidencia pendiente

La evidencia del repositorio demuestra la topologia, los contratos SQL, la configuracion inicial, la deteccion y las pruebas automatizadas. Para acreditar operacion en un entorno real todavía se requiere comprobar, por tenant:

- Ejecucion completa de los scripts SQL y activacion valida de la configuracion.
- Activacion manual de los sensores despues de validar DDL y datos de referencia.
- Lecturas y escrituras autorizadas contra SQL Server.
- Emision real de señales `DETECTED` y `CLEARED` con evidencia operacional.
- Consumo de esas señales y escritura de workflow o acciones por el sistema externo.

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release |
|---|---|---|
| Septiembre 2026 | Separar la deteccion de condiciones del ciclo de vida y la entrega de alertas | El release incorpora deteccion aislada por tenant, configuración versionada, evidencia persistente y un contrato de señales consumible por un sistema externo, sin correo, tickets ni workflow dentro de Dagster. |
