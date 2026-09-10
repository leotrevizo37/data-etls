# Configuración de detección de alertas en SQL Server

## Alcance

Dagster únicamente detecta condiciones y escribe señales `DETECTED` o
`CLEARED`. El ciclo de vida, correo, tickets, reintentos y demás acciones son
responsabilidad del sistema externo de alertamiento.

Cada tenant mantiene su configuración, incidencias y eventos en su propia base
SQL Server. No existe una vista cross-database.

## Instalación y migración

Los scripts globales se aplican en orden numérico a cada base tenant:

1. `023_alerting_alert_workflow_state.sql` conserva la compatibilidad previa.
2. `024_alerting_asset_row_count_daily.sql` conserva la historia de volumen.
3. `025_alerting_alert_config.sql` conserva el modelo de configuración previo.
4. `026_alerting_signal_model.sql` archiva las tablas previas como `*_legacy` y
   crea catálogos, configuración, incidencias y bitácoras.
5. `027_serving_alert_views.sql` crea las vistas, protecciones y el procedimiento
   de activación.
6. `028_alerting_legacy_migration.sql` migra únicamente evidencia demostrable.

Después se ejecuta
`sql/user_codes/<user_code>/init/999_zz_alerting_alert_config.sql`. Cada seed
crea una primera versión solo cuando no existe configuración y nunca reemplaza
una versión creada previamente. Los scripts globales y los seeds son
repetibles.

Las políticas antiguas de correo, tickets, reintentos y horarios silenciosos
permanecen preservadas en las tablas `*_legacy`; no se migran a la
configuración de detección.

## Catálogos

Los identificadores de los catálogos son GUID explícitos y compartidos por
todas las bases tenant. El identificador y el código técnico no se eliminan ni
se reutilizan. Solo pueden corregirse el nombre, la descripción o el indicador
de activo.

Las reglas usan los catálogos de tipo de alerta, severidad y estado de
configuración. Las incidencias y eventos también referencian tipo de señal,
fuente, estado de alerta, tipo de cierre, tipo de acción, operación y estado de
acción.

## Versiones de configuración

Una configuración sigue `DRAFT → ACTIVE → RETIRED`. Solo puede existir una
versión `ACTIVE` por `user_code`. Una versión activa o retirada y sus reglas son
inmutables.

Para preparar una versión:

1. Insertar `alerting.alert_config` con un GUID nuevo, el siguiente
   `version_number` y el estado `DRAFT`.
2. Insertar exactamente una regla default con `entity_key = NULL` para cada uno
   de los seis tipos activos.
3. Insertar, cuando aplique, overrides completos con una `entity_key` exacta.
4. Insertar los detalles 1:1 requeridos en
   `alerting.alert_rule_no_readings`,
   `alerting.alert_rule_pipeline_stale` y
   `alerting.alert_rule_row_count_trend`.
5. Insertar una fila en `alerting.alert_detector_operational_config` y otra en
   `alerting.alert_detector_row_count_config`.
6. Consultar `serving.vwValidacionConfiguracionesAlerta` para el GUID de la
   versión.
7. Activar con `alerting.usp_AlertConfig_Activate`.

```sql
SELECT
    CodigoValidacion,
    DescripcionValidacion,
    EsBloqueante
FROM serving.vwValidacionConfiguracionesAlerta
WHERE ConfiguracionAlertaId = @ConfiguracionAlertaId;

EXEC alerting.usp_AlertConfig_Activate
    @alert_config_id = @ConfiguracionAlertaId;
```

El procedimiento rechaza sin activar una versión que no sea `DRAFT` o que
tenga errores bloqueantes. Cuando es válida, retira la versión vigente y activa
la nueva dentro de la misma transacción. Las incidencias existentes conservan
los GUID de la configuración y regla que estaban activas al detectarse; no se
repuntan retroactivamente.

La validación bloquea antes de activar:

- códigos de usuario vacíos, distintos del historial del tenant o con versiones
  anteriores a una activa o retirada;
- reglas default faltantes y extensiones requeridas ausentes;
- extensiones asociadas a un tipo de alerta incompatible;
- clave de metadata de tendencia distinta de `row_count`;
- tipos de alerta o severidades inactivos;
- severidad crítica de tendencia distinta de `CRITICAL` activa;
- overrides de `connection_lost` o `late_readings` cuya `entity_key` no sea el
  GUID canónico en minúsculas del sensor.

La base también rechaza configuraciones creadas fuera de `DRAFT`, cambios al
usuario, número de versión o fecha de creación, transiciones directas de
`DRAFT` a `RETIRED` y timestamps incompatibles con el estado. La aplicación
debe mostrar al usuario todas las filas de
`serving.vwValidacionConfiguracionesAlerta` y activar únicamente mediante
`alerting.usp_AlertConfig_Activate`.

## Selección de reglas

La coincidencia exacta por `entity_key` tiene precedencia. Si no existe, se usa
la regla default del mismo tipo. No hay herencia parcial entre ambas reglas.

Para `connection_lost` y `late_readings`, `entity_key` es exactamente el
`SensorId`. Por ello, un override de 20 minutos para un sensor no afecta a otro
que use el default de 200 minutos. Para ingestiones y tendencias se usa la
clave exacta de la entidad monitoreada. Para `dagster_run_failure`, la
incidencia usa `entity_id = run_id` y `entity_key = asset_key`.

## Operación

Los dos sensores de cada tenant se registran inicialmente en estado `STOPPED` y
usan únicamente su recurso SQL Server local. Deben activarse manualmente
después de aplicar y validar el DDL y la configuración del tenant.

La code location central conserva los mismos nombres de sensores en estado
`STOPPED`, pero siempre devuelve `SkipReason`, no solicita recursos y no escribe
en tablas legacy.

`serving.vwEstadoAlertas` entrega una fila por incidencia. Si el sistema externo
no ha escrito eventos de workflow o acciones, las columnas correspondientes
permanecen `NULL`; los contadores de intentos permanecen en cero. La vista no
inventa el estado `NEW`.
