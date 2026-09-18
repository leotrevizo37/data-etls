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

Ambos usan exclusivamente el recurso sqlserver del tenant y se registran en
estado inicial STOPPED.

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

dagster_run_failure es puntual y solo produce DETECTED. El detector usa las
APIs públicas de Dagster, exige el origen de la code location y relaciona cada
paso fallido con los assets declarados en el execution plan. Un paso
multi-asset produce una incidencia por asset; sin asset comprobable no produce
señal. En este tipo, entity_id es el run_id y entity_key es el asset afectado.

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

Los scripts son repetibles. Los seeds 999_zz_alerting_alert_config.sql solo
crean una primera versión cuando alerting.alert_config está vacía y nunca
sobrescriben una configuración existente.
