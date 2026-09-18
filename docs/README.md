# Documentacion del serving layer

Este directorio concentra la documentacion operativa y funcional del proyecto: arquitectura, flujo de datos, contratos del modelo, pipelines, features, runbooks y decisiones tecnicas.

## Alcance

El repositorio contiene varios `user_codes` Dagster. Todos comparte el módulo sense, pero su implementación es completamente distinta, y sus puertos y alcances se definen así:

| User code | Puerto gRPC | Alcance principal |
|---|---:|---|
| `bafar_poc_gabinete` | 4004 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `valledelencino` | 4005 | Sense, observabilidad ChirpStack, dimensiones, lecturas, operaciones, eventos de sensores y mantenimiento |
| `carlsjr` | 4006 | Sense, observabilidad ChirpStack, eventos de sensores, SmartAudits, Mantiz y mantenimiento |
| `emerson` | 4007 | Sense, observabilidad ChirpStack, dimensiones, lecturas, operaciones, eventos de sensores y mantenimiento |
| `bafar` | Configuración externa | Ventas diarias, ventas fuera de horario y acceso de gerentes |
| `alerting` | 4009 | Sensores de alertamiento, estado de incidencias y correo SMTP |

## Indice

- [Arquitectura](architecture/overview.md)
- [Flujo de datos](architecture/data_flow.md)
- [Infraestructura](architecture/infrastructure.md)
- [Code location de alerting](architecture/alerting.md)
- [Modelo Sense](data_model/sense/fact_lecturas.md)
- [Modelo de observabilidad ChirpStack](data_model/sense/chirpstack_observability.md)
- [Vista Sense agrupada CARLS](data_model/sense/vw_fact_readings_measurement_device_groups.md)
- [Modelo SmartAudits](data_model/smartaudits/fact_smartaudits_categories.md)
- [Modelo Mantiz](data_model/mantiz/ticket_lifecycle.md)
- [Modelo de horarios de sucursales Bafar](data_model/out_of_hours_sales/dim_store_schedules.md)
- [Modelo de ventas fuera de horario](data_model/out_of_hours_sales/fact_out_of_hours_sales.md)
- [Modelo de acceso de gerentes Bafar](data_model/manager_access/fact_manager_access_daily.md)
- [Pipelines](pipelines/fact_lecturas.md)
- [Pipeline de observabilidad ChirpStack](pipelines/chirpstack_observability.md)
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
- [OKR alertamiento centralizado multi-tenant](okr/2026-09-alerting-evidence.md)
- [ADRs](adr/0001-use-dagster.md)
- [ADR de la code location dedicada para alerting](adr/0006-dedicated-alerting-code-location.md)

## Convenciones

- Los contratos fisicos viven en `sql/init`.
- Los procedimientos operativos viven en `sql/procedures`.
- Los assets, jobs, schedules y resources viven dentro de cada `dagster/<user_code>/src/<user_code>/defs`.
- La documentacion no debe incluir secretos ni valores reales de entorno. Para configuracion se toma como referencia solo `compose-data_etls/.env.example`.
