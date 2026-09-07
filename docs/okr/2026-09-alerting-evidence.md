# Alertamiento centralizado multi-tenant - Evidencia tecnica - Septiembre 2026

Estado: EVIDENCIA TECNICA

## Objetivo cubierto

Centralizar la deteccion y entrega de alertas de sensores, ingestiones, corridas de Dagster y tendencias de volumen para los tenants monitoreados, sin incorporar esta responsabilidad en sus pipelines de datos.

## Entregable

Code location `alerting` con dos sensores habilitados por defecto, consultas de solo lectura a SQL Server, estado de incidencias en PostgreSQL de Dagster, recordatorios agrupados por tenant y envio de correos HTML mediante SMTP.

## Valor

Concentra las reglas de alertamiento multi-tenant en una sola implementacion y conserva la trazabilidad de cada incidencia desde su apertura hasta su resolucion. La agrupacion diaria de recordatorios limita correos repetitivos, mientras que las aperturas nuevas mantienen un envio inmediato.

La separacion en una code location dedicada evita asignar el monitoreo global a un tenant especifico y mantiene los recursos SMTP y el estado de entrega fuera de los pipelines que producen datos.

## Evidencia directa

| Area | Evidencia |
|---|---|
| Alcance | `bafar_poc_gabinete`, `carlsjr`, `valledelencino`, `emerson` y `bafar` |
| Code location | `alerting` |
| Servicio Compose | `dagster-user-code-alerting` |
| Puerto gRPC | `4009`, publicado mediante `ALERTING_USER_CODE_PORT` |
| Arranque | `dagster code-server start -h 0.0.0.0 -p 4009 -m alerting.definitions` |
| Sensor central | `operational_alerts_sensor`, intervalo minimo de 300 segundos |
| Sensor de tendencias | `asset_row_count_trend_sensor`, intervalo minimo de 21600 segundos |
| Estado | Esquema `alerting` en PostgreSQL de Dagster |
| Correo | SMTP con destinatarios configurables y HTML sin emojis ni iconos por tipo |
| Recordatorios | Un correo agrupado por tenant y dia, sujeto a horario configurado y horas silenciosas |

## Objetos del release

| Tipo | Objeto | Archivo de referencia |
|---|---|---|
| Servicio | Code location gRPC `alerting` | `compose-data_etls/docker-compose.yml` |
| Workspace | Registro `alerting-dagster-user-code:4009` | `workspace.yaml` |
| Definiciones | Dos sensores y tres recursos | `dagster/alerting/src/alerting/defs/__init__.py` |
| Sensor | `operational_alerts_sensor` | `dagster/alerting/src/alerting/defs/sensors/operational_alerts_sensor.py` |
| Sensor | `asset_row_count_trend_sensor` | `dagster/alerting/src/alerting/defs/sensors/asset_row_count_trend_sensor.py` |
| Recurso | Consultas SQL Server de solo lectura | `dagster/alerting/src/alerting/defs/resources/mssql_reader.py` |
| Recurso | Estado de incidencias e historial diario | `dagster/alerting/src/alerting/defs/resources/alert_state_store.py` |
| Recurso | Construccion y envio de correos | `dagster/alerting/src/alerting/defs/resources/email_notifier.py` |
| Configuracion | Tenants, cadencias, umbrales y horarios | `dagster/alerting/src/alerting/alert_config.yaml` |
| Pruebas | Alertas centrales, correo y SMTP opt-in | `dagster/alerting/tests/test_alerting.py` |
| Pruebas | Captura, agregacion y tendencias `row_count` | `dagster/alerting/tests/test_asset_row_count_trends.py` |
| ADR | Justificacion de la code location independiente | `docs/adr/0006-dedicated-alerting-code-location.md` |

## Señales cubiertas

| Tipo | Fuente | Regla principal |
|---|---|---|
| Sensor desconectado | `observability.factRedingsAudits` | Ultimo estado con `IsConnectionLost = 1` dentro del lookback |
| Lecturas retrasadas | `observability.factRedingsAudits` | `HasLateReadings = 1` sin desconexion; deshabilitado por defecto actualmente |
| Ingestion fallida | `ctl.IngestionControl` | Ingestion activa con `LastRunStatus = 'FAILED'` |
| Pipeline atrasado | `ctl.IngestionControl` | Ultima ejecucion terminada fuera de la cadencia configurada |
| Corrida fallida | PostgreSQL de Dagster | Run `FAILURE` reciente de una code location monitoreada |
| Tendencia de volumen | Event log de Dagster | Caida simultanea contra promedios de 7 y 28 dias en metadata `row_count` |

La recuperacion de un sensor desconectado requiere evidencia posterior a `ConnectionLostAt` en `dwh.factReadingsMeasurement`. La ausencia de una condicion dentro del lookback no se usa por si sola para marcarla como resuelta.

## Tendencias de lecturas

1. El sensor consume incrementalmente eventos `ASSET_MATERIALIZATION` mediante cursor.
2. Solo registra materializaciones que contienen metadata `row_count`.
3. Agrega las observaciones en `alerting.asset_row_count_daily` por dia, code location, asset y clave de metadata.
4. Evalua solamente dias UTC cerrados y requiere 7 de 7 dias y 28 de 28 dias observados.
5. Abre `WARNING` cuando ambas comparaciones caen al menos 20% y `CRITICAL` cuando ambas caen al menos 40%.
6. Conserva la incidencia con `notification_count = 0` sin enviar un correo independiente.
7. La tendencia se incorpora solamente cuando el tenant ya tiene un recordatorio normal que enviar.

## Ciclo de entrega

1. El sensor central recolecta las condiciones activas de los tenants configurados y las corridas fallidas de Dagster.
2. Compara las condiciones actuales con `alerting.alert_workflow_state`.
3. Cada ocurrencia nueva recibe una clave con UUID; una incidencia resuelta no se reutiliza si la condicion reaparece.
4. Las aperturas del sensor central se envian inmediatamente, incluso durante las horas silenciosas.
5. Los recordatorios se agrupan una vez al dia por tenant. La configuracion actual usa `07:30` en `America/Mexico_City`.
6. Las horas silenciosas de `18:00` a `08:00` suprimen solamente recordatorios.
7. Las tendencias no generan correos independientes y solo acompañan un recordatorio normal. Las resoluciones tampoco se envian solas: acompañan una apertura nueva o un recordatorio valido.
8. Las transiciones notificadas se guardan despues de que SMTP acepta el correo; un fallo de envio conserva la accion para una evaluacion posterior.

## Persistencia de incidencias

| Objeto | Evidencia |
|---|---|
| `alerting.alert_workflow_state` | Estado, detalle, timestamps y contador de notificaciones por incidencia |
| `alerting.asset_row_count_daily` | Agregados diarios, promedios 1/7/28 dias y cambios porcentuales |
| `ux_alert_state_active_condition` | Limita a una incidencia activa por tenant, tipo y entidad |
| Estados | `OPEN`, `REMINDER`, `RESOLVED_PENDING`, `RESOLVED` |

`RESOLVED_PENDING` permite conservar una resolucion hasta que pueda acompañar un correo valido. Solo despues de ese envio pasa a `RESOLVED`.

## Validaciones existentes

| Archivo | Cobertura |
|---|---|
| `dagster/alerting/tests/test_alerting.py` | Aperturas, recordatorios, horas silenciosas, agrupacion por tenant, resoluciones pendientes, asuntos, destinatarios, HTML sin emojis y prueba SMTP opt-in |
| `dagster/alerting/tests/test_asset_row_count_trends.py` | Cursor, backfill, deduplicacion, agregados diarios, dias cerrados, umbrales, historial completo, incidencias nuevas y entrega agrupada |

La suite actual registra 30 pruebas: 29 exitosas y una omitida porque el envio SMTP real requiere habilitacion explicita. La validacion de definiciones de `alerting.definitions` finalizo correctamente.

## Evidencia pendiente

La evidencia tecnica confirma la estructura Compose, las definiciones, reglas, persistencia, formato de correo y pruebas del repositorio. Para acreditar operacion en un entorno real falta corroborar:

- Arranque completo del Compose con la code location saludable.
- Consultas exitosas contra SQL Server y PostgreSQL autorizados.
- Creacion de los objetos del esquema `alerting` en la instancia objetivo.
- Entrega SMTP real al destinatario de prueba mediante el test opt-in.
- Evaluacion de una condicion real desde su apertura hasta su resolucion notificada.

## Relacion con OKRs de septiembre

| Periodo | Objetivo | Relacion con este release |
|---|---|---|
| Septiembre 2026 | Centralizar el alertamiento multi-tenant de sensores, pipelines y calidad de lecturas | El release incorpora una code location independiente, deteccion sobre fuentes verificables, estado persistente de incidencias, recordatorios agrupados y entrega SMTP sin acoplar estas responsabilidades a los pipelines de cada tenant. |
