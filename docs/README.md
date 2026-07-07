# Documentacion del serving layer

Este directorio concentra la documentacion operativa y funcional del proyecto: arquitectura, flujo de datos, contratos del modelo, pipelines, features, runbooks y decisiones tecnicas.

## Alcance

El repositorio contiene seis `user_codes` Dagster. Todos comparte el módulo sense, pero su implementación es completamente distinta, y sus puertos y alcances se definen así:

| User code | Puerto gRPC | Alcance principal |
|---|---:|---|
| `mcdonalds` | 4001 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `smartfit` | 4002 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `mcdonalds_cdp` | 4003 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `bafar_poc_gabinete` | 4004 | Sense, dimensiones, lecturas, operaciones y mantenimiento |
| `valledelencino` | 4005 | Sense, dimensiones, lecturas, operaciones, eventos de sensores y mantenimiento |
| `carlsjr` | 4006 | Sense, eventos de sensores, SmartAudits, Mantiz y mantenimiento |

## Indice

- [Arquitectura](architecture/overview.md)
- [Flujo de datos](architecture/data_flow.md)
- [Infraestructura](architecture/infrastructure.md)
- [Modelo Sense](data_model/sense/fact_lecturas.md)
- [Vista Sense agrupada CARLS](data_model/sense/vw_fact_readings_measurement_device_groups.md)
- [Modelo SmartAudits](data_model/smartaudits/fact_smartaudits_categories.md)
- [Modelo Mantiz](data_model/mantiz/ticket_lifecycle.md)
- [Pipelines](pipelines/fact_lecturas.md)
- [Features](features/operational_score.md)
- [Runbooks](runbooks/failed_jobs.md)
- [OKR SmartAudits AI](okr/2026-07-duma-smartaudits-ai-evidence.md)
- [OKR disponibilidad y frescura DUMA](okr/2026-07-duma-data-freshness-evidence.md)
- [ADRs](adr/0001-use-dagster.md)

## Convenciones

- Los contratos fisicos viven en `sql/init`.
- Los procedimientos operativos viven en `sql/procedures`.
- Los assets, jobs, schedules y resources viven dentro de cada `dagster/<user_code>/src/<user_code>/defs`.
- La documentacion no debe incluir secretos ni valores reales de entorno. Para configuracion se toma como referencia solo `compose-data_etls/.env.example`.
