# Señales y estado de alertas

## Responsabilidades

El flujo separa tres responsabilidades:

1. Dagster detecta el inicio o fin comprobable de una condición.
2. El sistema externo de alertamiento administra el ciclo de vida.
3. El sistema externo ejecuta correo, tickets y otros canales.

Dagster solo escribe eventos DETECTED y CLEARED en
alerting.alert_signal_event. No crea tickets, no envía correo y no asigna
estados de workflow.

## Topología

Cada code location de bafar, bafar_poc_gabinete, carlsjr, emerson y
valledelencino registra copias independientes de:

- operational_alerts_sensor.
- asset_row_count_trend_sensor.

Ambos usan exclusivamente el recurso sqlserver del tenant. El detector operativo
conserva `RUNNING` en `carlsjr`, `emerson` y `valledelencino`, y `STOPPED` en
`bafar` y `bafar_poc_gabinete`. Los estados del sensor de tendencias no cambian.

`operational_alerts_sensor` consulta la configuración, descubre entidades y
solicita `operational_alerts_job`. El asset `<tenant>/operational_alerts` evalúa
una entidad y un tipo por ejecución, vuelve a comprobar la configuración y
regla solicitadas y utiliza los escritores existentes de incidencias y señales.
Estos escritores compartidos permanecen en el módulo original porque también
los utilizan SIDON y tendencias; la función del sensor no los ejecuta.

La unidad de evaluación es `(tipo, SensorId)` para lecturas, `(tipo, IngestionName)`
para ingestas, `(run_id, asset_key)` para detectar fallos de Dagster e `alert_id`
para comprobar su recuperación. Las lecturas, umbrales e incidencias se consultan
de nuevo dentro del job; el cursor conserva identificadores, no copias de evidencia.

El cursor conserva los `checks` anteriores y agrega los pendientes y la solicitud
en vuelo. Cada tick solicita como máximo un job y espera su estado terminal antes
de solicitar otro. Solo `SUCCESS` confirma la evaluación; errores o cancelaciones
conservan el pendiente y respetan `evaluation_interval_seconds` para reintentar.
Si todavía no existe el run, se repite la misma `run_key`. Los reintentos automáticos
de runs se deshabilitan en este job: el sensor controla el nuevo intento. Las
consultas de runs incluyen el tenant para evitar confirmaciones cruzadas.

Los pendientes se ordenan por su última evaluación para dar paso a otras entidades.
El intervalo es un mínimo; la frecuencia efectiva depende de la cola y duración de
los jobs. Los fallos de Dagster ya descubiertos permanecen pendientes aunque salgan
del lookback: el job consulta el run específico. `operational_alerts_job` se excluye
de esa detección para impedir que un fallo de su propio evaluador genere otra
evaluación de sí mismo; el fallo sigue visible en Dagster.

Cada materialización expone entidad, tipo, versiones, regla, severidad, run de
evaluación, evidencia y cantidades reales `detected_count` / `cleared_count`.
`outcome` distingue detección, recuperación, ambas transiciones, deduplicación,
condición activa o ausente y falta de evidencia válida. Una ingesta atrasada puede
confirmar la recuperación anterior y emitir una nueva detección en la misma
evaluación, conservando la semántica existente.

El check `evaluation_conclusive`, de severidad `WARN`, informa si la evidencia
permitió concluir la evaluación; no afirma que la entidad esté sana. Una fuente
ausente, incompleta, obsoleta o futura produce metadata sin señales. Los errores
técnicos de consulta, escritura o commit fallan el job sin materialización exitosa.
Los errores SQL del sensor ahora fallan el tick en lugar de ocultarse como un skip.

La recuperación de ingestas rechaza fechas de finalización futuras. Se corrigió
una condición anterior que permitía emitir `CLEARED` con una ejecución `SUCCESS`
cuya finalización todavía no había ocurrido. Las reglas y versiones también se
revalidan antes de persistir, incluida la recuperación paginada de fallos Dagster.

Las pruebas de `test_operational_alerts_job.py` cubren los cinco tenants con SQL
simulado y ejecuciones Dagster efímeras: selección de entidad, cola, intervalos,
aislamiento, metadata, checks, recuperación, reapertura y fallos de lectura,
persistencia o commit. Se mantienen las regresiones de dependencias de assets,
recuperación paginada, SIDON y tendencias. Esta validación no ejecuta escrituras
sobre los warehouses reales ni confirma la recarga de las code locations.

La code location alerting conserva esos dos nombres para compatibilidad. Sus
sensores también están STOPPED, no solicitan recursos y siempre devuelven
SkipReason. La ruta central no contiene entrega por correo, estado mutable ni
lectura multitenant.

## Modelo SQL

Cada base tenant conserva su propio modelo:

| Objeto | Responsabilidad |
|---|---|
| alerting.alert_incident | Identidad y evidencia raíz de una incidencia |
| alerting.alert_signal_event | Transiciones de detección append-only |
| alerting.alert_workflow_event | Cambios de ciclo de vida del sistema externo |
| alerting.alert_action_event | Intentos y resultados de acciones externas |
| alerting.asset_row_count_daily | Historia diaria de metadata row_count |
| alerting.alert_config | Versiones de configuración por tenant |
| alerting.alert_rule | Reglas default y overrides exactos |
| serving.vwEstadoAlertas | Una fila de reporteo por incidencia |
| serving.vwValidacionConfiguracionesAlerta | Errores bloqueantes de configuración |

Los catálogos usan GUID explícitos y estables. Sus códigos e identificadores
no se eliminan ni cambian; nombre, descripción e indicador de activo sí
pueden corregirse. Las bitácoras no admiten actualización ni eliminación.

serving.vwEstadoAlertas tiene nombre y columnas en español PascalCase. Sin un
evento de workflow conserva esas columnas en NULL; sin acciones conserva IDs,
fechas y estados en NULL, y contadores en cero.

## Configuración

Una versión sigue DRAFT → ACTIVE → RETIRED. Existe como máximo una versión
activa por tenant. alerting.usp_AlertConfig_Activate valida el borrador, retira
la versión vigente y activa la nueva dentro de una transacción.

Cada versión válida contiene exactamente una regla default por cada uno de
los seis tipos. Puede agregar overrides completos por entity_key; la
coincidencia exacta gana y, si no existe, se usa el default. En
connection_lost y late_readings, entity_key es el SensorId canónico.

Las políticas de entrega, reintentos, tickets y horarios silenciosos no forman
parte de la configuración nueva. Permanecen preservadas en las tablas
*_legacy.

El soporte de destinatarios, rutas de correo, etapas de escalamiento y
decisiones por destinatario se mantiene por separado mediante
`038_alerting_email_recipients.sql`. El sistema externo administra y consume
estas tablas; Dagster no interactúa con ellas. Su contrato de mantenimiento,
selección, recuperación y reenvío está en
[Destinatarios y rutas de correo](../data_model/alerting_email.md).

## Semántica de señales

Las condiciones continuas emiten solo transiciones. La apertura crea una
incidencia y un evento DETECTED con GUID distintos y correlacionados. La
recuperación actualiza cleared_at y agrega un evento CLEARED únicamente cuando
existe evidencia posterior a la detección:

- Una lectura nueva para sensores.
- Una ejecución terminal exitosa posterior para ingestiones.
- Una materialización posterior con volumen saludable para tendencias.

La ausencia de datos, una fuente obsoleta, un cambio de configuración o una
regla deshabilitada no producen CLEARED.

dagster_run_failure identifica cada ocurrencia por run y asset. El detector usa
las APIs públicas de Dagster, exige el origen de la code location y recorre las
dependencias del execution plan desde los pasos fallidos, excluyendo assets ya
materializados y ramas independientes. Sin asset afectado comprobable no produce
señal. En este tipo, entity_id es el run_id y entity_key es el asset afectado.
Una materialización posterior del mismo asset en el mismo tenant permite emitir
CLEARED para cada incidencia anterior, manteniendo su evidencia individual.

## Códigos de alerta

alert_code es una etiqueta histórica no única, en minúsculas ASCII y
kebab-case:

- sin-senal-{tipo-sensor}-{sublocacion}
- lecturas-retrasadas-{tipo-sensor}-{sublocacion}
- fallo-ingesta-{entity-key}
- ingesta-atrasada-{entity-key}
- fallo-dagster-{asset-key}
- caida-volumen-{asset-key}

Si no existe metadata descriptiva, se usa entity_key.

## Migración

Los scripts 026 a 029 archivan las tablas anteriores como
alert_workflow_state_legacy, alert_config_legacy y alert_rule_legacy, y crean
catálogos, configuración, incidencias y eventos. Los scripts 030 a 033 crean
las vistas, protecciones y el procedimiento de activación. Los scripts 034 y
035 materializan la configuración efectiva anterior y migran únicamente
evidencia histórica demostrable.

Los scripts son repetibles. La inicialización no crea una configuración de
alertas por defecto; cada tenant permanece sin configuración activa hasta que
se prepare y active explícitamente una versión válida.

`029_alerting_event_model.sql` debe terminar antes de crear
`serving.vwEstadoAlertas` en `031`. La migración usa `SET NOCOUNT ON` porque el
cliente antiguo de `mssql-tools` puede emitir `SqlState 24000, Invalid cursor
state` después del `COMMIT`, omitir los siguientes lotes y devolver código cero.
El inicializador también detecta esos errores en la salida del cliente y detiene
el arranque en el script que falló. No se deben adelantar las vistas ni borrar
las bases para recuperar una inicialización incompleta: se vuelve a ejecutar
el inicializador corregido.
