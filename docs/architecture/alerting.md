# Code location `alerting`

## Proposito

`alerting` es una code location de Dagster dedicada al alertamiento de sensores, ingestiones, corridas de Dagster y tendencias de volumen. No ejecuta pipelines de datos: contiene dos sensores y los recursos necesarios para consultar señales, conservar el estado de cada incidencia y enviar correos.

## Integracion con Dagster

| Elemento | Configuracion actual |
|---|---|
| Servicio Compose | `dagster-user-code-alerting` |
| Contenedor | `alerting-dagster-user-code` |
| Location name | `alerting` |
| Puerto gRPC interno | `4009` |
| Puerto publicado | `ALERTING_USER_CODE_PORT`, con `4009` como valor por defecto |
| Modulo de definiciones | `alerting.definitions` |
| Comando | `dagster code-server start -h 0.0.0.0 -p 4009 -m alerting.definitions` |
| Healthcheck | `dagster api grpc-health-check -p 4009 -h 127.0.0.1` |

El servicio usa el mismo `dagster/Dockerfile` que las demas code locations. Espera que `dagster-postgres` este saludable y que `sql-init` termine correctamente. El `dagster-daemon` espera a su vez que `dagster-user-code-alerting` este saludable antes de iniciar.

La instancia cargada por Compose usa el `workspace.yaml` de la raiz, donde `alerting-dagster-user-code:4009` esta registrado como `alerting`.

## Definiciones

La code location registra estos recursos:

| Recurso | Responsabilidad |
|---|---|
| `mssql_reader` | Consultas de solo lectura a las bases SQL Server de cada tenant |
| `alert_state` | Estado de entrega e historial diario de `row_count` en PostgreSQL de Dagster |
| `email_notifier` | Construccion y envio de correo HTML mediante SMTP |

Tambien registra dos sensores, ambos habilitados por defecto.

## `operational_alerts_sensor`

Se evalua con un intervalo minimo de 300 segundos. Consulta las condiciones configuradas para cada `user_code` y consolida el envio por tenant.

| Tipo interno | Fuente | Condicion actual |
|---|---|---|
| `connection_lost` | `observability.factRedingsAudits` | Ultimo estado del sensor dentro del lookback con `IsConnectionLost = 1` |
| `late_readings` | `observability.factRedingsAudits` | `HasLateReadings = 1` e `IsConnectionLost = 0` dentro del lookback |
| `pipeline_failure` | `ctl.IngestionControl` | Ingestion activa con `LastRunStatus = 'FAILED'` |
| `pipeline_stale` | `ctl.IngestionControl` | Ingestion activa, terminada y no fallida cuyo ultimo termino excede su cadencia configurada |
| `dagster_run_failure` | Tabla `runs` de PostgreSQL de Dagster | Corrida con estado `FAILURE` durante las ultimas dos horas y code location monitoreada |

Las alertas de sensores se enriquecen, cuando existe coincidencia, con sucursal, sububicacion, dispositivo y sensor desde `dwh.dimSidonProdDimensions`.

Una desconexion no se considera resuelta solo porque desaparezca del lookback. La recuperacion requiere que el ultimo audit indique comunicacion activa y que exista una lectura en `dwh.factReadingsMeasurement` posterior a `ConnectionLostAt`. El correo puede incluir `CommunicationResumedAt` y `LastReadingAt` como evidencia.

## `asset_row_count_trend_sensor`

Se evalua con un intervalo minimo de 21600 segundos, equivalente a seis horas. Lee incrementalmente eventos `ASSET_MATERIALIZATION` del event log de Dagster y procesa solamente materializaciones que incluyan metadata `row_count`.

El cursor guarda el ultimo `storage_id` procesado. En ausencia de cursor, el backfill actual cubre 28 dias. Cada tick procesa 500 eventos y limita el valor configurable a un maximo de 1000. Si quedan eventos pendientes, registra el avance y difiere la evaluacion de alertas hasta completar el backlog.

Las observaciones se agregan por dia, code location, asset y clave de metadata. La evaluacion usa solamente dias UTC cerrados; el dia actual no participa.

Una condicion de tendencia requiere simultaneamente:

- 7 dias observados en la ventana de 7 dias.
- 28 dias observados en la ventana de 28 dias.
- Caida contra ambos promedios de al menos 20% para `WARNING`.
- Caida contra ambos promedios de al menos 40% para `CRITICAL`.

Este sensor no envia correo directamente. Una incidencia nueva se conserva con `notification_count = 0` y solo se incorpora cuando el tenant ya tiene un recordatorio normal que enviar.

## Configuracion monitoreada

La configuracion vive en `dagster/alerting/src/alerting/alert_config.yaml`. Actualmente contiene cinco tenants:

| User code | Base por defecto | Alertas de conexion | Ingestiones con cadencia explicita |
|---|---|---|---|
| `bafar_poc_gabinete` | `cdp` | Deshabilitadas | `FactLecturas`: 3600 min |
| `carlsjr` | `carlsjr` | Habilitadas | `factRedingsAudits`: 45 min; `FactLecturas`: 1500 min; `FactSensorEvents`: 400 min; `factReadingsMeasurement`: 400 min; `factSmartauditsCategories`: 1500 min; `factUrlAvailabilityDaily`: 140 min |
| `valledelencino` | `valledelencino` | Habilitadas | `factRedingsAudits`: 60 min; `FactSensorEvents`: 400 min; `factReadingsMeasurement`: 400 min |
| `emerson` | `emerson` | Habilitadas | `factRedingsAudits`: 45 min; `FactSensorEvents`: 400 min; `factReadingsMeasurement`: 400 min |
| `bafar` | `bafar` | Deshabilitadas | `FactOutOfHoursSales`: 900 min; `FactStoreSalesDaily`: 1700 min; `FactManagerAccessDaily`: 1460 min |

`late_readings` esta deshabilitado por defecto para todos. La deteccion de fallos de `ctl.IngestionControl`, corridas fallidas de Dagster y pipelines atrasados esta habilitada por defecto. Las ingestiones sin cadencia explicita usan 720 minutos.

La base de cada tenant puede reemplazarse con su variable `database_env`. La conexion SQL Server tambien admite variables con prefijo por tenant y usa las variables `MSSQL_*` como fallback.

## Ciclo de vida

El estado se conserva en PostgreSQL de Dagster bajo el esquema `alerting`.

| Estado | Significado |
|---|---|
| `OPEN` | Incidencia activa; las tendencias pueden permanecer en este estado sin haber enviado correo |
| `REMINDER` | Incidencia activa incluida en un recordatorio aceptado |
| `RESOLVED_PENDING` | Condicion cerrada, pendiente de comunicar junto con otro envio valido |
| `RESOLVED` | Resolucion incluida en un correo aceptado |

Cada nueva ocurrencia usa una clave formada por la condicion y un UUID. Una incidencia resuelta no se reutiliza si la misma condicion reaparece. Un indice unico parcial permite solamente una incidencia `OPEN` o `REMINDER` por combinacion de tenant, tipo y entidad.

En el sensor central, las aperturas, recordatorios y resoluciones notificadas se actualizan despues de que el servidor SMTP acepta el correo. Si el envio falla, permanecen disponibles para una evaluacion posterior. El sensor de tendencias persiste aperturas con `notification_count = 0` y resoluciones pendientes sin realizar un envio directo.

## Agrupacion de correos

- Las aperturas nuevas del sensor central se envian inmediatamente y no quedan bloqueadas por las horas silenciosas.
- Los recordatorios se agrupan en un solo correo por tenant y dia.
- La hora configurada actualmente es `07:30` en `America/Mexico_City`.
- Las horas silenciosas actuales son de `18:00` a `08:00` en `America/Mexico_City`; solo suprimen recordatorios.
- Una tendencia nunca genera un correo independiente. Solo acompana un recordatorio normal del mismo tenant.
- Una resolucion nunca genera un correo independiente. Puede acompanar una apertura nueva o el recordatorio diario del tenant.
- Si una apertura y un recordatorio coinciden para el mismo tenant, la resolucion pendiente se incluye una sola vez.

Los asuntos implementados dependen del contenido:

| Contenido | Prefijo |
|---|---|
| Sensores | `[ALERTAS DE SENSORES]` |
| Fallos de ingesta | `[MONITOREO INGESTA DE DATOS]` |
| Pipelines atrasados | `[ALERTAS PIPELINES DE DATOS]` |
| Corridas Dagster fallidas | `[ALERTAS DAGSTER]` |
| Tendencias | `[TENDENCIAS DE LECTURAS]` |
| Tipos mezclados | `[MONITOREO DE DATOS]` |
| Recordatorio diario | `[MONITOREO OPERATIVO]` |

El HTML actual no incorpora emojis ni iconos por tipo de alerta.

## Destinatarios y SMTP

El recurso SMTP usa `ALERT_SMTP_HOST`, `ALERT_SMTP_PORT`, `ALERT_SMTP_FROM` y `ALERT_SMTP_USE_TLS`. La autenticacion se realiza cuando existen `ALERT_SMTP_USER` y `ALERT_SMTP_PASSWORD`.

La resolucion de destinatarios soporta esta precedencia:

1. `ALERT_SMTP_TO_<TENANT>_<TIPO>`.
2. `ALERT_SMTP_TO_<TENANT>`.
3. `ALERT_SMTP_TO_<TIPO>`.
4. `ALERT_SMTP_TO`.

En el flujo consolidado actual, `operational_alerts_sensor` no establece `recipient_alert_type`; por lo tanto, la seleccion efectiva es `ALERT_SMTP_TO_<TENANT>` y despues `ALERT_SMTP_TO`. Las variables por tipo siguen disponibles en el recurso, pero no son seleccionadas por este sensor.

Los nombres y valores de ejemplo permitidos estan en `compose-data_etls/.env.example`. Los archivos `.env` reales no forman parte de la documentacion.

## Persistencia

`alert_state` crea de forma idempotente el esquema y sus objetos al inicializarse:

| Objeto | Uso |
|---|---|
| `alerting.alert_delivery_state` | Ciclo de vida, detalle, timestamps y contador de notificaciones |
| `alerting.asset_row_count_daily` | Agregados diarios, promedios 1/7/28 dias, cambios porcentuales y cursor de materializacion |
| `ix_alert_state_active` | Consulta de incidencias activas |
| `ux_alert_state_active_condition` | Una sola incidencia activa por condicion |
| `ix_asset_row_count_daily_asset` | Consulta historica por code location, asset y metadata |

## Validacion

Desde `dagster/alerting`, las validaciones focalizadas son:

```text
PYTHONPATH=src python -m pytest tests -q --import-mode=importlib
PYTHONPATH=src python -m dagster definitions validate -d src -m alerting.definitions
ruff check src/alerting tests
```

La suite actual contiene una prueba SMTP real deshabilitada por defecto. Solo se ejecuta cuando `RUN_LIVE_SMTP_TEST=true` y requiere `ALERT_SMTP_TEST_TO`, ademas de la configuracion SMTP normal. Las pruebas unitarias y la validacion de definiciones no demuestran conectividad real con SMTP, SQL Server, PostgreSQL ni el arranque completo de Compose.

## Archivos principales

| Archivo | Responsabilidad |
|---|---|
| `dagster/alerting/src/alerting/defs/__init__.py` | Registro de sensores y recursos |
| `dagster/alerting/src/alerting/defs/sensors/operational_alerts_sensor.py` | Deteccion, reconciliacion y envio consolidado |
| `dagster/alerting/src/alerting/defs/sensors/asset_row_count_trend_sensor.py` | Captura y evaluacion de tendencias `row_count` |
| `dagster/alerting/src/alerting/defs/resources/mssql_reader.py` | Consultas SQL Server de solo lectura |
| `dagster/alerting/src/alerting/defs/resources/alert_state_store.py` | Persistencia PostgreSQL |
| `dagster/alerting/src/alerting/defs/resources/email_notifier.py` | Asuntos, HTML, destinatarios y SMTP |
| `dagster/alerting/src/alerting/alert_config.yaml` | Tenants, señales, cadencias, recordatorios y umbrales |
| `compose-data_etls/docker-compose.yml` | Servicio, dependencias, variables, volumenes y healthcheck |
| `workspace.yaml` | Registro gRPC de la code location usada por Compose |
