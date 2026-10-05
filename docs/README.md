# Documentacion del serving layer

Este directorio concentra la documentacion operativa y funcional del proyecto: arquitectura, flujo de datos, contratos del modelo, pipelines, features, runbooks y decisiones tecnicas.

## Alcance

El repositorio contiene cinco implementaciones tenant y una location de compatibilidad `alerting`. Sense aplica a `bafar_poc_gabinete`, `valledelencino`, `carlsjr` y `emerson`; `bafar` implementa los flujos comerciales. Los puertos y alcances se definen así:

| User code | Puerto gRPC | Alcance principal |
|---|---:|---|
| `bafar_poc_gabinete` | 4004 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `valledelencino` | 4005 | Sense, observabilidad ChirpStack, dimensiones, lecturas, operaciones, eventos de sensores y mantenimiento |
| `carlsjr` | 4006 | Sense, observabilidad ChirpStack, eventos de sensores, SmartAudits y mantenimiento; Mantiz desactivado |
| `emerson` | 4007 | Sense, observabilidad ChirpStack, dimensiones, lecturas, operaciones, eventos de sensores y mantenimiento |
| `bafar` | Configuración externa | Ventas diarias, ventas fuera de horario y acceso de gerentes |
| `alerting` | No registrada | Sensores inertes conservados por compatibilidad operativa |

## Indice

- [Especificación de configuración de alertas](../ESPECIFICACION_CONFIGURACION_ALERTAS.md)
- [Especificación del sistema externo de alertamiento](../ESPECIFICACION_SISTEMA_ALERTAMIENTO.md)
- [Especificación de correos y escalamiento](../ESPECIFICACION_CONFIGURACION_CORREOS_ESCALAMIENTO.md)
- [Modelo de detección de alertas](data_model/alerting.md)
- [Destinatarios, rutas y decisiones de correo](data_model/alerting_email.md)
- [Concurrencia de ejecución](architecture/infrastructure.md#concurrencia)
- [PoC configurable, experimental y sin procesamiento real](../dagster/configurable_poc/README.md)
- [Especificación de consumo de datos Sense para aplicaciones y agentes de IA](../ESPECIFICACION_CONSUMO_DATOS_SENSE.md)
- [Arquitectura](architecture/overview.md)
- [Flujo de datos](architecture/data_flow.md)
- [Infraestructura](architecture/infrastructure.md)
- [Code location de alerting](architecture/alerting.md)
- [Modelo Sense](data_model/sense/fact_lecturas.md)
- [Modelo de observabilidad ChirpStack](data_model/sense/chirpstack_observability.md)
- [Vista Sense agrupada CARLS](data_model/sense/vw_fact_readings_measurement_device_groups.md)
- [Modelo SmartAudits](data_model/smartaudits/fact_smartaudits_categories.md)
- [Modelo Mantiz, desactivado](data_model/mantiz/ticket_lifecycle.md)
- [Modelo de horarios de sucursales Bafar](data_model/out_of_hours_sales/dim_store_schedules.md)
- [Modelo de ventas fuera de horario](data_model/out_of_hours_sales/fact_out_of_hours_sales.md)
- [Modelo de acceso de gerentes Bafar](data_model/manager_access/fact_manager_access_daily.md)
- [Pipelines](pipelines/fact_lecturas.md)
- [Pipeline de observabilidad ChirpStack](pipelines/chirpstack_observability.md)
- [Pipeline de señales de configuración SIDON](pipelines/sidon_configuration_signals.md)
- [Pipeline de horarios de sucursales Bafar](pipelines/dim_store_schedules.md)
- [Pipeline de ventas fuera de horario](pipelines/fact_out_of_hours_sales.md)
- [Pipeline de acceso de gerentes Bafar](pipelines/fact_manager_access_daily.md)
- [Features](features/operational_score.md)
- [Feature de observabilidad ChirpStack](features/chirpstack_observability.md)
- [Feature de ventas fuera de horario](features/out_of_hours_sales.md)
- [Runbooks](runbooks/failed_jobs.md)
- [OKR SmartAudits AI](okr/2026-07-duma-smartaudits-ai-evidence.md)
- [OKR disponibilidad y frescura DUMA](okr/2026-07-duma-data-freshness-evidence.md)
- [OKR ventas fuera de horario Bafar](okr/2026-07-bafar-out-of-hours-sales-evidence.md)
- [OKR horarios de sucursales Bafar](okr/2026-09-bafar-store-schedules-api-evidence.md)
- [OKR acceso diario de gerentes Bafar](okr/2026-09-bafar-manager-access-daily-evidence.md)
- [OKR factAudiencia Sense CarlsJr](okr/2026-09-carlsjr-factaudiencia-evidence.md)
- [OKR observabilidad ChirpStack multi-tenant](okr/2026-09-chirpstack-observability-evidence.md)
- [OKR deteccion de señales de alerta por tenant](okr/2026-09-alerting-evidence.md)
- [ADRs](adr/0001-use-dagster.md)
- [ADR de deteccion de señales por tenant](adr/0007-tenant-alert-detection.md)
- [ADR de evidencia MQTT parcial](adr/0008-chirpstack-partial-mqtt-evidence.md)
- [ADR de identidad UTC](adr/0009-utc-fact-identities.md)
- [ADR de evaluación SIDON mediante IA](adr/0010-sidon-ai-evaluation.md)
- [Contratos de vistas Sense](data_model/sense/serving_views.md)
- [Pipelines de observabilidad](pipelines/observability.md)
- [Ventas diarias Bafar](pipelines/fact_store_sales_daily.md)
- [Recuperación de SIDON y MQTT](runbooks/sidon_mqtt_recovery.md)

## Convenciones

- Los contratos fisicos viven en `sql/init`.
- Los contratos y migraciones específicos de tenant viven en `sql/user_codes/<user_code>/init`.
- Los procedimientos operativos viven en `sql/procedures`.
- Los assets, jobs, schedules y resources viven dentro de cada `dagster/<user_code>/src/<user_code>/defs`.
- La documentacion no debe incluir secretos ni valores reales de entorno. Para configuracion se toma como referencia solo `compose-data_etls/.env.example`.
