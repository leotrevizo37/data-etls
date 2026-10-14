# Pipeline de señales de configuración SIDON

## Propósito

Detectar problemas estructurales y posibles incongruencias semánticas en la
configuración de dispositivos y alertas SIDON, conservando evidencia por entidad
para su revisión humana.

`sidon_configuration_sensor` consulta cambios y entidades pendientes y dispara
`sidon_configuration_job`. El asset `<user_code>/sidon_configuration` evalúa una
entidad por ejecución y escribe incidentes y eventos `DETECTED` / `CLEARED`
mediante los escritores existentes. No modifica configuraciones SIDON ni envía
notificaciones.

## Alcance

El mismo flujo existe en los siguientes `user_codes`, con assets, recursos,
cursores y consultas de runs separados por tenant:

| User code | Asset Dagster | Estado inicial del sensor |
| --- | --- | --- |
| `carlsjr` | `carlsjr/sidon_configuration` | `RUNNING` |
| `emerson` | `emerson/sidon_configuration` | `RUNNING` |
| `valledelencino` | `valledelencino/sidon_configuration` | `STOPPED` |
| `ecosat` | `ecosat/sidon_configuration` | `RUNNING` |

Las variables y los ejemplos usan `<PREFIJO>` como marcador. Sustituirlo por
el prefijo del tenant antes de configurar su proceso:

| User code | `<PREFIJO>` |
| --- | --- |
| `carlsjr` | `CARLSJR` |
| `emerson` | `EMERSON` |
| `valledelencino` | `VALLEDELENCINO` |
| `ecosat` | `ECOSAT` |

### Entidades evaluadas

| Tipo | Entidad | Evidencia |
| --- | --- | --- |
| `sidon_device_configuration` | `device:<DeviceId>` | Dispositivo, tipo y sensores activos con sus tipos y unidades. |
| `sidon_alert_configuration` | `alert:<AlertId>` | Condiciones activas, dispositivos aplicables y rangos esperados de sus sensores. |

Una condición huérfana utiliza `condition:<AlertConditionId>`. Los dispositivos
con sensores activos se revisan incluso si el dispositivo está inactivo o falta.
Las alertas inactivas y las exclusiones activas de `AlertStoppedOnDevices` no se
evalúan. Una alerta sin dispositivos aplicables tiene evidencia insuficiente.

## Entradas

La fuente es la conexión de dimensiones del tenant: `Devices`, `DeviceTys`,
`Sensors`, `SensorTys`, `Alerts`, `AlertConditions` y `AlertStoppedOnDevices`.
La lectura utiliza ODBC Driver 18 sin modificar el recurso de dimensiones.
`dwh.dimSensorExpectedVal` se consulta en el warehouse del mismo tenant mediante
la conexión `sqlserver`, relacionando tanto `SensorId` como `DeviceId`.

La configuración activa y las reglas se leen de `alerting.alert_config` y
`alerting.alert_rule`, mediante el cargador y la validación SQL existentes.

## Salidas

- Incidentes en `alerting.alert_incident`.
- Eventos `DETECTED` y `CLEARED` en `alerting.alert_signal_event`.
- Materialización del asset con evidencia, evaluación y resultado de escritura.
- Asset check `configuration_consistent` con severidad `WARN`.

## Evaluación

### Evidencia y contrato de IA

Las referencias inexistentes/inactivas, condiciones sin sensores aplicables,
operadores desconocidos, umbrales inválidos y rangos invertidos producen hallazgos
estructurales sin consultar IA. La congruencia semántica se evalúa con Azure
OpenAI o un servidor local compatible y un esquema JSON estricto. Se transmiten
nombres de dispositivos, sensores y alertas, nombres de sus tipos, unidades,
operadores, umbrales y rangos con todos sus horarios/días. El payload incluye
`device_id` para dispositivos y `alert_id` y `alert_condition_id` para alertas.
La lista `source_ids` permanece en la evidencia local. No se incluyen
ubicaciones, identificadores físicos, fórmulas, tópicos ni datos de notificación.
Los nombres son datos no confiables dentro del prompt, nunca instrucciones.

El modelo devuelve `verdict`, `confidence`, `reasons` y `reasoning.analysis`.
El recurso valida una confianza numérica finita entre 0 y 1, entre una y tres
razones no vacías de hasta 2000 caracteres cada una y un análisis no vacío.
Después descarta `reasoning`, incorpora el modelo informado por el servidor y
convierte el veredicto al estado interno que consume el asset:

| `verdict` de IA | `status` interno |
| --- | --- |
| `problem` | `suspicious` |
| `no_problem_found` | `consistent` |
| `insufficient_evidence` | `insufficient_evidence` |

El prompt comprueba por separado la relación entre el nombre del dispositivo y
su `device_type`; sensores coherentes no subsanan una contradicción entre ambos.
Para alertas, considera si el umbral avisa antes de que el equipo pierda su
función física, incluso sin rangos esperados, interpretando el operador y las
unidades °C/°F. Para tipos de dos estados comprueba si la condición puede
activarse con 0 o 1, sin inventar su codificación. Solicita razones breves en
español, sin recomendaciones. Estas instrucciones no garantizan la corrección
semántica del resultado.

### Reglas de emisión

Una evaluación `suspicious` con confianza inferior a 0.9 o
`insufficient_evidence` con confianza de al menos 0.9 emite `WARNING`, conservando
la lógica existente. Los demás resultados con confianza inferior a 0.9 o
`insufficient_evidence` no emiten señales. La confianza declarada no representa
una precisión estadística medida. Los límites no necesitan ser iguales a los rangos
esperados. No se aplica una tolerancia numérica universal ni se inventan unidades
o relaciones booleanas entre condiciones. Se conservan límites nulos y ceros.
La ausencia de rangos esperados permite la evaluación semántica y el cierre de
un incidente de configuración de alerta cuando se cumplen las condiciones de recuperación.

## Programación y flujo

- Sensor: `sidon_configuration_sensor`, con intervalo mínimo de 30 segundos.
- Job: `sidon_configuration_job`, solicitado para una entidad por tick.
- Evaluación y persistencia: asset `<user_code>/sidon_configuration`.
- Reintentos técnicos: administrados por el sensor según el intervalo de la regla.

### Detección de cambios

El cursor conserva `MAX(ModifiedAt)` y `COUNT_BIG(*)` por cada una de las ocho
tablas consultadas, incluyendo filas inactivas. Las marcas se comparan por tabla;
no se utiliza un único máximo global. Si no hay cambios ni entidades pendientes,
se omiten la lectura completa y la evaluación de IA. Los agregados SQL se siguen
consultando. El conteo permite detectar eliminaciones físicas que no cambien el
máximo; no detecta una eliminación compensada por una inserción si tampoco cambia
el máximo. Los soft deletes requieren que la fuente actualice `ModifiedAt`.

La primera ejecución o un cambio de marcas, configuración/reglas del detector o
política de IA inicia una nueva revisión de las entidades habilitadas. La huella
de política incluye endpoint, deployment/modelo configurado, prompt y esquema.
El sensor tiene un intervalo mínimo de 30 segundos. Cada tick solicita como
máximo un job para una entidad; el sensor solo consulta marcas,
identificadores, habilitación, intervalos y estado del run. El job lee la evidencia
completa, evalúa y persiste. El cursor anterior sigue siendo compatible.

### Confirmación y reintentos

El cursor conserva la entidad pendiente y la solicitud en vuelo hasta que Dagster
registre `SUCCESS`. Mientras el run está pendiente o activo no se solicita otro.
Si todavía no existe un run para la solicitud, se repite su misma `run_key` para
que Dagster deduplique el lanzamiento. Un fallo o cancelación conserva la entidad
pendiente y permite un nuevo intento con otra clave al cumplirse
`evaluation_interval_seconds`; las otras entidades pendientes pueden continuar.
Los reintentos automáticos de runs se deshabilitan para este job para mantener un
solo responsable de los reintentos: el sensor.

Las respuestas válidas inconclusas y la falta de evidencia completa producen una
materialización sin señal y esperan un nuevo cambio para reevaluarse. Los errores
de lectura, IA, persistencia o commit fallan el job sin materialización exitosa.
Los errores de consulta SQL desde el sensor fallan el tick. Una configuración
activa ausente, no única o con errores bloqueantes produce `SkipReason`.
No se mantienen transacciones SQL abiertas durante la llamada a IA.

### Consistencia antes de escribir

Antes de evaluar, el job comprueba la configuración, regla y política de la
solicitud. Antes de escribir vuelve a leer la evidencia y la configuración activa,
incluida la regla de la entidad, aunque no haya cambiado la versión de
`alert_config`. Un cambio detectado falla el job sin señales.

## Observabilidad y recuperación

Cada materialización identifica la entidad, el run, la regla, la política, el
modelo, la confianza, la evidencia y su huella. `signal` indica exclusivamente la
señal que el escritor confirmó: `DETECTED`, `CLEARED` o `NONE`. `outcome` distingue:

| Resultado | Significado |
| --- | --- |
| `detected` | El escritor confirmó la inserción de una detección. |
| `cleared` | El escritor confirmó la recuperación. |
| `deduplicated` | Se intentó emitir una señal y el escritor no insertó otra. |
| `consistent` | Evaluación consistente sin incidente abierto. |
| `recovery_not_confirmed` | Existe un incidente, pero no se cumplen las condiciones de recuperación. |
| `inconclusive` | Evaluación sin señal por evidencia o confianza insuficiente. |

El check `configuration_consistent` pasa únicamente ante `consistent` con
confianza de al menos 0.9. Su severidad `WARN` permite que una evaluación completada
con un hallazgo o evidencia insuficiente tenga run exitoso y quede registrada sin
provocar reintentos técnicos. Las condiciones de recuperación comprobadas quedan
en `evaluation.recovery_conditions`; los fallos técnicos quedan en el run y su
entidad permanece pendiente.

Los metadatos locales conservan evidencia, identificadores, resultado, modelo y
huellas de evidencia y política. `CLEARED` requiere evaluación consistente con confianza de al menos 0.9,
evidencia completa y diferente de la detección, misma política y mismo modelo
(o una detección originalmente estructural). `expected_ranges_available` se conserva
en los metadatos como dato informativo y no condiciona el cierre. Una eliminación, desactivación,
cambio de modelo o de prompt no cierra por sí solo un incidente. Los escritores
existentes deduplican las señales. La configuración permanece bajo revisión
humana; la calidad semántica requiere validación con casos etiquetados del dominio.

## Configuración y activación

1. En el warehouse del tenant, aplicar la versión actual de
   [catálogo de alertamiento](../../sql/init/027_alerting_catalog_values.sql)
   sobre el esquema de alertamiento existente y luego la
   [validación de configuración](../../sql/init/030_serving_alert_config_validation.sql).
   Los dos tipos SIDON forman parte del catálogo compartido. La validación
   exige una regla default (`entity_key IS NULL`) para cada tipo activo del
   catálogo, incluidos ambos tipos SIDON. Su ausencia es bloqueante, también
   para configuraciones previas.
2. Crear una nueva versión `DRAFT` siguiendo
   el [runbook de configuración SQL](../runbooks/alerting_sql_configuration.md), conservar sus reglas y
   configuraciones técnicas e incluir reglas default para ambos tipos SIDON
   cuando estén activos en el catálogo. `is_enabled` permite decidir cuáles
   evaluar; una regla por entidad prevalece sobre la default de su tipo.
   Estas reglas usan únicamente `alerting.alert_rule`: severidad, habilitación e
   intervalo. No tienen tabla adicional de parámetros. Activar la versión con el
   procedimiento existente, sin alterar reglas de una versión `ACTIVE`.
3. Seleccionar `<PREFIJO>_SIDON_AI_PROVIDER=azure` (valor predeterminado) o `local`.
   Para Azure, proporcionar al proceso del tenant estas variables, sin incluir
   sus valores privados en archivos versionados:

   - `<PREFIJO>_SIDON_AZURE_OPENAI_ENDPOINT`: origen HTTPS de Azure OpenAI, sin ruta.
   - `<PREFIJO>_SIDON_AZURE_OPENAI_DEPLOYMENT`: nombre del deployment compatible con
     salidas estructuradas, por ejemplo un deployment de GPT-4.1-mini.
   - `<PREFIJO>_SIDON_AZURE_OPENAI_API_KEY`: clave del recurso Azure.

   Para un servidor local, utilizar la configuración de la siguiente sección.
   `<PREFIJO>_SIDON_AI_TIMEOUT_SECONDS` controla el timeout HTTP, con valor
   predeterminado de 20 segundos y rango permitido de 1 a 300. El timeout debe
   contemplarse dentro del tiempo disponible para el job, incluyendo las lecturas
   SQL; la llamada a IA ya no se ejecuta dentro del tick del sensor.

   El Compose existente incorpora estas variables mediante su `env_file`.
   `.env.example` contiene únicamente la estructura de configuración.
4. Iniciar `sidon_configuration_sensor` en el tenant correspondiente.

La implementación utiliza Chat Completions con respuesta estructurada y un
máximo de 4096 tokens. Para Azure envía `store=false` y `max_completion_tokens`;
para local envía `max_tokens` y `stream=false`. No sigue redirecciones ni utiliza
Azure como respaldo de local. Limita el JSON de entrada a 64 KB y la respuesta a
256 KiB; superar un límite falla el job y deja la entidad pendiente de reintento.

## Desarrollo local con LM Studio

Descargar y cargar un modelo que admita salidas estructuradas e iniciar el
servidor desde Developer → Start server. Configurar el proceso del tenant con:

```text
<PREFIJO>_SIDON_AI_PROVIDER=local
<PREFIJO>_SIDON_LOCAL_ENDPOINT=http://localhost:1234
<PREFIJO>_SIDON_LOCAL_MODEL=identificador-del-modelo-cargado
<PREFIJO>_SIDON_AI_TIMEOUT_SECONDS=40
```

`<PREFIJO>_SIDON_LOCAL_MODEL` debe ser el identificador real que publica el servidor
en `GET /v1/models`. La URL se configura sin `/v1` ni otra ruta: el recurso agrega
`/v1/chat/completions`. Se permiten `localhost`, `127.0.0.1`, `::1` y
`host.docker.internal`, con puerto configurable y HTTP o HTTPS.

Cuando Dagster corre en Docker Desktop y LM Studio en el host, utilizar
`http://host.docker.internal:1234`; el servidor debe aceptar conexiones desde el
contenedor. `localhost` dentro del contenedor corresponde al propio contenedor.

No se requieren ni se consultan variables de credenciales Azure al seleccionar
`local`. Si el servidor local exige autenticación, proporcionar
`<PREFIJO>_SIDON_LOCAL_API_KEY`; se envía como `Authorization: Bearer`. En caso
contrario puede quedar vacía. El prompt, esquema de respuesta, validaciones y
escritores de señales son los mismos para ambos proveedores.

El servidor local cambia dónde se ejecuta la IA. El detector sigue escribiendo
en el SQL configurado para el tenant y requiere el catálogo, las reglas y su
activación descritos arriba. No constituye un modo de simulación de señales.

Referencias: [API y parámetros de LM Studio](https://lmstudio.ai/docs/developer/openai-compat/chat-completions),
[salidas estructuradas](https://lmstudio.ai/docs/developer/openai-compat/structured-output),
[autenticación local](https://lmstudio.ai/docs/developer/core/authentication).

## Validación local

Las pruebas de [Carlsjr](../../dagster/carlsjr/tests/test_sidon_configuration.py),
[Emerson](../../dagster/emerson/tests/test_sidon_configuration.py) y
[Valle del Encino](../../dagster/valledelencino/tests/test_sidon_configuration.py)
usan datos sintéticos y dobles
de SQL/HTTP. Cubren el payload con identificadores de entidad, rangos y horarios,
referencias inválidas, exclusiones, errores de IA, cambios durante la evaluación, recuperación, cursor
por tabla, traducción de veredictos, validación de una a tres razones y contratos
Azure/local. También ejecutan el job en una instancia Dagster efímera para
comprobar materializaciones, checks, aislamiento de runs por
tenant y confirmación del cursor; simulan errores de lectura, IA, escritura y
commit. No invocan un modelo real ni ejecutan migraciones sobre el warehouse.

Las pruebas de [Ecosat](../../dagster/ecosat/tests/test_sidon_configuration.py)
replican la cobertura de Carlsjr con la identidad y el prefijo `ECOSAT_` del tenant.

## Documentación relacionada

- [Índice de documentación](../README.md).
- [Catálogo de pipelines](README.md).
- [Arquitectura de señales y estado de alertas](../architecture/alerting.md).
- [Configuración SQL de alertamiento](../runbooks/alerting_sql_configuration.md).
- [Decisión de detección de señales por tenant](../adr/0007-tenant-alert-detection.md).
- [Contrato de rangos esperados](../data_model/sense/expected_values.md).
