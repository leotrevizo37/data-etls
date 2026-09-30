# Destinatarios y rutas de correo para alertamiento

## Alcance e instalación

`sql/init/038_alerting_email_recipients.sql` mantiene cinco tablas en el esquema
`alerting` de cada base tenant. Se aplica después de los catálogos, configuración
y eventos existentes mediante el inicializador SQL global. Es repetible, no
carga destinatarios reales ni habilita rutas por defecto.

El proyecto mantiene el DDL y este contrato. El sistema externo administra los
destinatarios, configura las rutas, decide y ejecuta los envíos. Dagster no lee
ni escribe estas tablas. No son assets ejecutables ni una cola de workers.
No cambian las reglas de detección, su activación o las tablas `*_legacy`.

El backend resuelve el tenant autorizado y su base antes de cualquier operación.
Estas tablas no repiten `user_code`: pertenecen a la base tenant. Los GUID son
identidades locales de registros, no códigos de catálogo compartidos. Si el
consumidor centraliza su persistencia, debe incluir fuente y tenant en sus claves.

Referencia del DDL: [038_alerting_email_recipients.sql](../../sql/init/038_alerting_email_recipients.sql).

## Tablas y relaciones

| Tabla | Unidad de registro | Relaciones |
|---|---|---|
| `alerting.dim_email_recipient` | Un destinatario y su correo actual | Referenciado por asignaciones y acciones |
| `alerting.alert_email_route` | Una política de correo por tipo, severidad opcional y entidad opcional | FK a `dim_alert_type` y `dim_severity`; independiente de `alert_rule` |
| `alerting.alert_email_route_step` | Una etapa de la ruta | FK a ruta; número único dentro de ella |
| `alerting.alert_email_route_recipient` | Un destinatario asignado a una etapa | PK `(email_route_step_id, recipient_id)` y FK a ambos registros |
| `alerting.alert_email_action` | Una decisión de correo para una incidencia y un destinatario | FK a incidencia, señal, destinatario, etapa/ruta y acción padre de la misma incidencia |

### Destinatarios

| Campo | Tipo SQL | Contrato |
|---|---|---|
| `recipient_id` | `UNIQUEIDENTIFIER` | PK estable generada por el backend |
| `display_name` | `NVARCHAR(250) NULL` | Nombre visible opcional |
| `email_address` | `NVARCHAR(320)` | Un único buzón, obligatorio y único en esta base |
| `is_active` | `BIT` | Arranca en cero; habilitación explícita |
| `created_at`, `updated_at` | `DATETIME2(3)` | UTC; el escritor actualiza `updated_at` al editar |
| `version` | `ROWVERSION` | Token de concurrencia, no fecha ni número de versión funcional |

El backend valida y almacena una dirección sin nombre incorporado, listas,
espacios ni caracteres de control; conserva la parte local y convierte el
dominio a minúsculas. La comparación es binaria: no convertir todo el buzón a
minúsculas ni combinar direcciones que solo difieren en la parte local. Las
restricciones SQL comprueban la forma básica y rechazan separadores y saltos de
línea; no sustituyen la validación de sintaxis del backend ni prueban que el
buzón exista o acepte correo.

Para guardar los destinatarios indicados, el backend valida cada dirección,
resuelve la existente por su forma canónica o crea un GUID nuevo y guarda las
asignaciones en una transacción. Una coincidencia no reactiva automáticamente
un destinatario deshabilitado. No extraer correos de evidencia del detector ni
copiar usuarios o correos de otras fuentes por defecto.

Las ediciones usan `version` para detectar conflictos. Para retirar un
destinatario, establecer `is_active = 0`; conservar su identidad e historial.
Las FK impiden eliminar destinatarios utilizados. Cambiar su dirección no
actualiza las acciones históricas. Una dirección modificada requiere una nueva
decisión explícita para futuros envíos; no sustituye el buzón de un trabajo ya
planificado.

### Rutas y etapas

`alert_email_route` contiene `email_route_id` (PK), `alert_type_id`,
`severity_id NULL`, `entity_key NVARCHAR(450) NULL`, `is_enabled`,
`send_recovery`, `stop_on_acknowledged`, fechas UTC y `version ROWVERSION`.
La combinación `(alert_type_id, severity_id, entity_key)` es única. Una
severidad nula cubre todas; una entidad nula es el default del tipo.
`is_enabled` y `send_recovery` arrancan en cero; `stop_on_acknowledged`, en uno.

Para una incidencia, el consumidor busca únicamente rutas de su tipo, con esta
precedencia:

1. Entidad exacta y severidad exacta.
2. Entidad exacta y cualquier severidad.
3. Entidad default y severidad exacta.
4. Entidad default y cualquier severidad.

Primero seleccionar la ruta, después comprobar si está habilitada. Una ruta
específica deshabilitada bloquea la entrega; no permite caer al default.
No acumular destinatarios de varias rutas ni heredar campos parcialmente.
La `entity_key` conserva exactamente la identidad de la incidencia; para
sensores, usar el GUID canónico del `SensorId`.

Cada etapa contiene `email_route_step_id` (PK), `email_route_id`,
`step_number SMALLINT`, `delay_minutes INT` e `is_enabled BIT`, inicialmente
cero. La etapa 0 es la detección inicial y exige demora 0. Las etapas positivas
son escalamientos y exigen demora positiva. La demora es absoluta desde el
`occurred_at` de `DETECTED`, no desde el correo anterior ni desde su recepción
por el consumidor. Los números y demoras deben ser crecientes; se permiten
huecos en los números.

Para habilitar una ruta, el backend comprueba en la misma transacción que los
catálogos estén activos, exista una etapa 0 habilitada con destinatarios
activos y que cada escalamiento habilitado tenga destinatarios activos y una
demora mayor que la etapa anterior. SQL garantiza FK, unicidad y demoras
válidas por fila; la coherencia del conjunto y esta habilitación corresponden
al consumidor, no a `usp_AlertConfig_Activate`.

Toda edición de una ruta o sus etapas/asignaciones debe bloquear o actualizar
primero la cabecera, comprobar su `version`, guardar el conjunto y actualizar
`updated_at` para generar un nuevo token, dentro de una transacción. La lectura
para planificar usa el mismo protocolo y conserva una instantánea coherente.
Los GUID y el ámbito de una ruta usada se conservan; para otro ámbito, crear
otra ruta. No mover etapas existentes entre rutas ni reutilizar sus GUID.

## Decisiones de correo e historial

| Campo de `alert_email_action` | Tipo SQL | Uso |
|---|---|---|
| `action_id` | `UNIQUEIDENTIFIER` | PK; mismo GUID usado en `alert_action_event` |
| `alert_id`, `source_signal_event_id` | `UNIQUEIDENTIFIER` | Incidencia y señal de origen de la misma incidencia |
| `recipient_id`, `email_address` | `UNIQUEIDENTIFIER`, `NVARCHAR(320)` | Identidad y copia inmutable del correo seleccionado |
| `purpose` | `NVARCHAR(20)` | `DETECTION`, `ESCALATION`, `RECOVERY` o `FORWARD` |
| `email_route_id`, `email_route_step_id` | `UNIQUEIDENTIFIER NULL` | Ruta/etapa aplicada; ambas nulas en un reenvío manual |
| `route_version` | `BINARY(8) NULL` | Copia del token de ruta al decidir; no token de edición de esta acción |
| `parent_action_id` | `UNIQUEIDENTIFIER NULL` | Obligatorio en recuperación y reenvío; FK a una acción de la misma incidencia |
| `idempotency_key` | `NVARCHAR(200)` | Identidad lógica única persistida antes del intento |
| `not_before` | `DATETIME2(3)` | Instante UTC más temprano para intentar el correo |
| `requested_by`, `request_reason` | `NVARCHAR(250) NULL`, `NVARCHAR(1000) NULL` | Actor autorizado y motivo, obligatorios para reenvío manual |
| `created_at` | `DATETIME2(3)` | Momento UTC de registro SQL |

La tabla es inmutable: registrar la decisión antes de ejecutar el correo y
conservarla aunque se cancele o falle. Las nuevas decisiones requieren un
destinatario activo y el correo actual coincidente. La detección y el
escalamiento requieren una ruta/etapa habilitada y vigente, un destinatario
asignado y ámbito compatible con la incidencia. La recuperación copia la
ruta, etapa y token de la acción padre, incluso si después cambió la ruta;
para recuperar un reenvío manual conserva esos tres campos nulos.

La clave de idempotencia se deriva de fuente, tenant, incidencia, señal,
propósito, destinatario y etapa para detección/escalamiento. Para recuperación
identifica una sola notificación por señal y destinatario, aunque este haya
recibido varias etapas. Para un reenvío incluye además el identificador durable
de la solicitud humana. Usar una representación estable o su hash que quepa
en 200 caracteres. No incluir el token de ruta para generar automáticamente
otro correo al editar una política. Ante clave existente, reutilizar la acción
y comprobar que su contenido coincide; una colisión no equivale a éxito.

Registrar la decisión y su evento inicial `EMAIL/SEND/PENDING`, intento 0,
en una transacción cuando ambos residan en esta base. Los intentos y resultados
se insertan en `alert_action_event` con ese `action_id`, `alert_id` y señal.
No hay FK inversa desde la bitácora existente: continúa admitiendo acciones
históricas y otros canales. El nuevo escritor debe mantener la correlación
`EMAIL/SEND` y consultar el detalle uniendo ambas tablas por `action_id`.
El token de ruta no permite consultar una versión histórica de la política;
para reconstruirla, guardar en el `metadata_json` del evento inicial el número
y demora de etapa, `send_recovery`, `stop_on_acknowledged` y la versión de
plantilla aplicados. Ese evento conserva la evidencia aunque se edite la ruta.
Esta extensión no modifica el resumen de `serving.vwEstadoAlertas`, que sigue
mostrando la última acción de correo, no todos sus destinatarios.

### Escalamiento, recuperación y reenvío

- **Escalamiento:** cada etapa positiva genera una decisión nueva por
  destinatario, con propósito `ESCALATION`, señal `DETECTED` y `not_before`
  igual a la detección más su demora. Antes de decidir cada etapa, volver a
  seleccionar la ruta vigente y comprobar que la etapa corresponde a ella;
  un cambio de ruta no reinicia el reloj ni repite decisiones existentes. Si
  hay `CLEARED`, workflow `RESOLVED`/`CLOSED`, ruta/etapa deshabilitada o
  destinatario revocado, cancelar lo pendiente. Si `stop_on_acknowledged = 1`,
  detener también ante `ACKNOWLEDGED` o `IN_PROGRESS`. No depender de que el
  intento de la etapa anterior falle para avanzar. Un fallo de correo no es
  una nueva señal del detector.
- **Recuperación:** usar `CLEARED` y referenciar una acción previa de detección,
  escalamiento o reenvío de detección aceptada/entregada para ese mismo
  destinatario y dirección. Exige `send_recovery` habilitado en la ruta y
  autorización vigente. La recuperación de un reenvío sin ruta requiere una
  política explícita del consumidor; no se habilita automáticamente. No
  notificar a destinatarios nuevos ni duplicar recuperaciones por etapa.
- **Reenvío manual:** registrar `FORWARD`, una acción padre, actor, motivo y
  una solicitud autorizada durable. Los destinatarios indicados se guardan
  primero en el catálogo del mismo tenant; cada uno obtiene su propia acción
  con ruta/etapa/token nulos. Conservar la señal de la acción padre y la
  evidencia de recuperación disponible; no presentar como vigente una
  condición ya recuperada. Repetir la petición HTTP con la misma identidad
  no genera otra acción. Una solicitud nueva sí puede reenviar al mismo buzón.
- **Reintento técnico:** conserva acción, destinatario y propósito; agrega
  eventos de intento con el contador correspondiente. No crea otra fila en
  `alert_email_action` ni otra solicitud de reenvío.

Antes de cada intento, verificar de nuevo autorización, estado activo y
coincidencia del correo con la instantánea. Una revocación o cambio de correo
cancela el trabajo, no borra su decisión ni redirige el mensaje. Las acciones
pendientes, el bloqueo entre workers, los límites de reintentos y la
reconciliación con el proveedor permanecen en el consumidor. `not_before` no
es un disparador SQL ni una garantía de envío a esa hora. Un resultado incierto
requiere reconciliación antes de intentar otro envío.

## Restricciones y permisos

| Error | Significado |
|---|---|
| `51040` | Se intenta modificar o eliminar una decisión de correo |
| `51041` | La señal pertenece a otra incidencia o no corresponde al propósito |
| `51042` | Destinatario inactivo o dirección distinta a la seleccionada |
| `51043` | Recuperación sin padre de detección al mismo destinatario/dirección y con la misma instantánea de ruta, o reenvío con señal distinta a la de su padre |
| `51044` | Ruta/etapa deshabilitada, token obsoleto, destinatario no asignado o ámbito/etapa incompatible |

Las restricciones nativas también rechazan referencias inexistentes,
asignaciones repetidas, ámbitos duplicados, demoras inválidas, claves de
idempotencia repetidas y referencias parciales de ruta. El backend revierte
la transacción ante un rechazo; no oculta el error ni devuelve éxito.

La base no decide precedencia, comprueba aceptación del proveedor, ejecuta
escalamientos ni acredita permisos humanos. El consumidor valida estas
condiciones y la unicidad funcional antes de escribir, bajo exclusión mutua
por incidencia. El trigger comprueba integridad local, no la entrega.

El administrador de destinatarios/rutas necesita las lecturas y escrituras
necesarias sobre esas cuatro tablas. El worker necesita lectura e `INSERT`
en decisiones y bitácoras de su responsabilidad; no `UPDATE`/`DELETE` en
decisiones, incidencias o señales. Este script no concede permisos ni instala
credenciales. El proveedor de correo no recibe acceso SQL directo.
