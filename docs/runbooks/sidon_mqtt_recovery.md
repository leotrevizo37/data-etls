# Diagnóstico y recuperación de SIDON y MQTT

## Alcance

Procedimiento de diagnóstico para Carlsjr, Emerson y Valle. Antes de ejecutar una acción operativa se requiere autorización para el entorno. No se editan secretos, se borran datos, se reinician servicios ni se publican artefactos como parte de la revisión documental.

## SIDON

1. Identificar tenant, entidad, tick y run de `sidon_configuration_job`.
2. Si existe `SkipReason`, comprobar que haya una única configuración activa y que la vista de validación no reporte errores bloqueantes. Los dos tipos SIDON requieren reglas default cuando están activos.
3. Si existe un run solicitado, pendiente o activo, el cursor conserva la entidad en vuelo; no solicitar otra evaluación manual equivalente sin revisar ese estado.
4. Un fallo técnico de lectura, IA, persistencia o commit mantiene la entidad pendiente y habilita reintento según la regla. Revisar el error del run sin copiar payloads ni credenciales a servicios externos.
5. Una materialización inconclusa no es un fallo técnico: revisar `evaluation`, `signal`, `outcome` y el check `configuration_consistent`. El check `WARN` puede fallar con run exitoso.
6. Para recuperación, comprobar evidencia completa y diferente, política y modelo compatibles y resultado consistente. Eliminar o desactivar una entidad, o cambiar el prompt/modelo, no confirma `CLEARED`.

No borrar ni reiniciar el cursor como solución genérica: controla deduplicación, pendientes y confirmación de runs. Las marcas `MAX(ModifiedAt)` y conteos no garantizan detectar toda modificación compensada. El [pipeline SIDON](../pipelines/sidon_configuration_signals.md) conserva el procedimiento de configuración y los límites exactos.

## MQTT y ChirpStack

1. Separar el resultado de `observability_chirpstackmetrics` del de `observability_chirpstackmqttmetrics`; las métricas oficiales y el historial MQTT son fuentes distintas.
2. Revisar ventana UTC, lookback y resultado HTTP. Un fallo de la fuente complementaria no equivale a cero uplinks ni autoriza sustituir evidencia existente.
3. Ante `DEFERRED_DST`, revisar `resume_after_utc`: la ventana local repetida debe terminar antes de recuperar ambas ocurrencias. El scheduler retoma la captura; no mantener un worker esperando ni forzar el watermark.
4. Para filas rechazadas, consultar de forma autorizada `raw.MqttEventHistoryRejectedRow` por tenant, ventana y `RunId`. Sus textos están truncados y tienen retención; no son un archivo íntegro de mensajes.
5. Un campo `invalid` en un evento parcialmente aceptado puede aparecer solo en los JSON de calidad. No encontrar un rechazo individual no demuestra validez de todos los campos.
6. Si un replay no reemplaza datos, comprobar la aceptación conjunta de los dos hechos por dispositivo-hora. Una reducción de conteos o calidad puede rechazar ese snapshot para conservar evidencia; no flexibilizar protecciones para forzar una recarga.

No existe job de backfill de ChirpStack. El lookback MQTT recupera eventos tardíos dentro de su rango configurado, sin demostrar completitud histórica. La aceptación conjunta evita nuevas decisiones incoherentes, pero no repara automáticamente inconsistencias anteriores. Véanse el [pipeline](../pipelines/chirpstack_observability.md) y [ADR 0008](../adr/0008-chirpstack-partial-mqtt-evidence.md).
