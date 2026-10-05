# Modelo de detección y estado de alertas

## Alcance

Cada base tenant mantiene sus propios objetos. Dagster escribe incidencias y señales; el sistema externo escribe workflow y acciones. No existe una vista cross-database. Los GUID de catálogo son compartidos y estables; las identidades de incidencias y eventos pertenecen al tenant.

## Contratos principales

| Objeto | Granularidad y responsabilidad | DDL |
|---|---|---|
| `alerting.dim_alert_type` y demás catálogos | Identidad y código técnico estable por valor | [026](../../sql/init/026_alerting_catalog_tables.sql), [027](../../sql/init/027_alerting_catalog_values.sql) |
| `alerting.alert_config` | Versión por `user_code`; `DRAFT → ACTIVE → RETIRED` | [028](../../sql/init/028_alerting_config_model.sql) |
| `alerting.alert_rule` | Regla por versión, tipo y entidad nullable | [028](../../sql/init/028_alerting_config_model.sql) |
| Extensiones y configuración técnica | Parámetros de ausencia de lecturas, atraso y tendencia; configuración de detectores | [028](../../sql/init/028_alerting_config_model.sql) |
| `alerting.alert_incident` | Una incidencia con configuración, regla y evidencia de detección | [029](../../sql/init/029_alerting_event_model.sql) |
| `alerting.alert_signal_event` | Transición append-only `DETECTED` o `CLEARED` | [029](../../sql/init/029_alerting_event_model.sql) |
| `alerting.alert_workflow_event` | Cambio administrativo append-only del sistema externo | [029](../../sql/init/029_alerting_event_model.sql) |
| `alerting.alert_action_event` | Intento o resultado append-only de una acción externa | [029](../../sql/init/029_alerting_event_model.sql) |
| `alerting.asset_row_count_daily` | Historia diaria de volumen por location, asset y metadata | [024](../../sql/init/024_alerting_asset_row_count_daily.sql) |
| `serving.vwValidacionConfiguracionesAlerta` | Errores por versión; `EsBloqueante` determina rechazo | [030](../../sql/init/030_serving_alert_config_validation.sql) |
| `serving.vwEstadoAlertas` | Una fila de reporte por incidencia | [031](../../sql/init/031_serving_alert_state.sql) |

## Reglas de consumo

Una versión activa o retirada y sus reglas son inmutables. La activación se realiza con `alerting.usp_AlertConfig_Activate`, bajo las protecciones de [032](../../sql/init/032_alerting_state_guards.sql) y [033](../../sql/init/033_alerting_catalog_guards.sql). La validación exige una regla default por cada tipo activo; actualmente son ocho, incluidos los dos tipos SIDON. Un override exacto por `entity_key` prevalece sin herencia parcial.

`alert_code` es descriptivo e histórico, no una llave única. La incidencia conserva los GUID de configuración y regla vigentes al detectarse. `CLEARED` confirma fin comprobable de la condición; no equivale a resolución administrativa. La ausencia de workflow y acciones permanece `NULL` en la vista, con contadores de intentos en cero.

Las bitácoras son append-only. `event_sequence` no debe tratarse como offset global de commits ni como prueba de entrega al consumidor; la recepción durable, deduplicación y reconciliación corresponden al [sistema externo](../../ESPECIFICACION_SISTEMA_ALERTAMIENTO.md). La vista resumida no sustituye el historial de eventos.

## Configuración inicial y correo

El init global no crea reglas por defecto. Carlsjr, Emerson y Valle disponen de scripts específicos `020_alerting_default_config.sql` que crean y activan una versión solo si no existe historial de configuración del tenant. No reemplazan versiones previas. El [runbook](../runbooks/alerting_sql_configuration.md) documenta preparación y validación.

Destinatarios, rutas, etapas y decisiones inmutables de correo están en el [contrato de correo](alerting_email.md). Dagster no administra esas tablas ni ejecuta los envíos. Las políticas legacy no se convierten automáticamente en las nuevas rutas.
