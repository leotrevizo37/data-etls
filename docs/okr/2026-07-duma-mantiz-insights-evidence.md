# Mantiz Insights - Diseno inicial DUMA - 2026-07-23

Estado: DESARROLLO INICIAL

## Objetivo cubierto

Iniciar el desarrollo de insights para Mantiz dentro de DUMA.

## Entregable

Diseno inicial del flujo, exploracion de fuentes, tablas base y estructura para futuros insights.

## Valor

Deja preparados los dataframes y contratos de datos para integrar Mantiz cuando exista mayor volumen de datos productivos.

## Evidencia directa

| Area | Base preparada | Evidencia |
|---|---|---|
| Fuentes transaccionales | Tickets y contexto operativo de sucursal, cliente, equipo, catalogo, falla y proveedor | MySQL Mantiz: `Ticket`, `Branch`, `Client`, `EquipmentInstance`, `EquipmentCatalog`, `FailureType`, `Supplier` |
| Fuentes textuales | Titulo, descripcion, notas del ciclo y comentarios del ticket | Fuentes normalizadas para extraer fallas, soluciones y refacciones |
| Ciclo de vida | Estado, duraciones, aging y calidad de captura por ticket | `dwh.factMantizTicketLifecycle` |
| Detalle de extraccion | Una fila por fuente, frase, tipo de insight y hash durante cada corrida | `stg.MantizTicketTextInsightDetail` |
| Agregado por ticket | Falla, solucion, refaccion, evidencia y estado de clasificacion | `dwh.factMantizTicketTextInsights` |
| Revision humana | Conceptos nuevos o ambiguos pendientes de aprobacion | `ctl.MantizTextInsightReviewQueue` |
| Modelos futuros | Registro versionado de clasificadores por tipo de insight | `ctl.MantizTextInsightClassifierModels` |

## Fuentes exploradas

| Grupo | Datos disponibles | Uso inicial |
|---|---|---|
| Ticket | Identificadores, folio, estado, prioridad, tipo, origen y fechas del ciclo | Construir la llave, el estado operativo y la ventana incremental |
| Sucursal y cliente | `Branch`, `Client` | Ubicar cada ticket en su contexto organizacional |
| Equipo | `EquipmentInstance`, `EquipmentCatalog` | Relacionar fallas y soluciones con el activo atendido |
| Falla y proveedor | `FailureType`, `Supplier` | Conservar clasificaciones fuente y responsables del servicio |
| Texto del ticket | Titulo, descripcion, agenda, resolucion, aprobacion y cancelacion | Extraer conceptos de falla y solucion |
| Comentarios | Comentarios creados o modificados | Incorporar evidencia textual adicional y detectar cambios |
| Refacciones | Nombre, precio y moneda de la refaccion | Relacionar soluciones con partes y costo operativo |

## Diseno inicial del flujo

El diseno separa dos salidas complementarias.

### Ciclo de vida del ticket

1. Seleccionar tickets cambiados por `COALESCE(updatedAt, createdAt)`.
2. Cruzar el ticket con sucursal, cliente, equipo, catalogo, falla y proveedor.
3. Calcular duraciones entre hitos, aging, estado y banderas de calidad de captura.
4. Escribir la corrida en `stg.factMantizTicketLifecycle`.
5. Consolidar en `dwh.factMantizTicketLifecycle` por `(OrganizationId, TicketId)`.
6. Actualizar el watermark de `ctl.IngestionControl`.

### Insights de texto

1. Seleccionar tickets cambiados o con comentarios cambiados.
2. Construir el contexto del ticket y reunir sus fuentes textuales.
3. Normalizar los textos.
4. Extraer detalle de `FALLA`, `SOLUCION` y `REFACCION_SOLUCION`.
5. Aplicar modelos activos cuando existan.
6. Agregar el detalle a una fila por ticket.
7. Registrar conceptos nuevos en `ctl.MantizTextInsightReviewQueue`.
8. Reemplazar en `dwh.factMantizTicketTextInsights` solo las llaves cambiadas.

## Estructuras de dataframe preparadas

| Estructura logica | Contenido | Destino o uso |
|---|---|---|
| Tickets cambiados | Llaves de tickets modificados por datos del ticket o comentarios | Delimitar el procesamiento incremental |
| Contexto del ticket | Sucursal, cliente, equipo, falla, proveedor y refacciones | Enriquecer ciclo de vida e insights |
| Ciclo de vida | Fechas, duraciones, estado y calidad de captura | `stg.factMantizTicketLifecycle` y `dwh.factMantizTicketLifecycle` |
| Fuentes normalizadas | Texto original, extracto, texto normalizado, origen y fecha de cambio | Entrada para extraccion por reglas o modelos |
| Detalle de insights | Frase, hash, tipo, canonico, metodo, confianza y candidato nuevo | `stg.MantizTicketTextInsightDetail` |
| Insights por ticket | Falla, solucion, refaccion, evidencia, metodo, confianza y revision | `stg.factMantizTicketTextInsights` y `dwh.factMantizTicketTextInsights` |

## Tablas base

| Esquema | Tabla | Funcion |
|---|---|---|
| `stg` | `factMantizTicketLifecycle` | Recibir la corrida incremental del ciclo de vida |
| `dwh` | `factMantizTicketLifecycle` | Conservar una fila por organizacion y ticket |
| `stg` | `MantizTicketTextInsightChangedTicket` | Delimitar las llaves textuales que deben reemplazarse |
| `stg` | `MantizTicketTextInsightDetail` | Conservar el detalle de extraccion por `RunId` |
| `stg` | `factMantizTicketTextInsights` | Preparar el agregado textual por ticket |
| `dwh` | `factMantizTicketTextInsights` | Exponer insights consolidados para consumo |
| `ctl` | `MantizTextInsightReviewQueue` | Acumular conceptos candidatos para revision |
| `ctl` | `MantizTextInsightClassifierModels` | Registrar y activar modelos por tipo de insight |

## Estructura para futuros insights

La salida por ticket ya contempla:

- `FailureCode`, `FailureSubject` y `FailureProblem`.
- `SolutionCode`, `SolutionSubject` y `SolutionPartName`.
- Refaccion fuente, precio y moneda.
- `InsightStatus`, `ClassificationMethod` y `ClassifierConfidence`.
- `NeedsReview`, `InsightsJson` y `TextEvidenceJson`.

La cola de revision permite acumular frecuencia, tickets y ejemplos representativos de conceptos nuevos. Los conceptos aprobados pueden alimentar modelos versionados por `InsightType`, conservando metricas, artefacto y estado activo.

## Alcance del OKR

Este OKR deja definido el flujo incremental, las fuentes, la granularidad, las tablas de staging, las salidas DWH y los puntos de extension para revision humana y modelos. La estructura permite comenzar con reglas y datos limitados sin cambiar el contrato final cuando aumente el volumen productivo.

El alcance no supone que exista volumen suficiente para validar representatividad, calidad del clasificador o rendimiento a escala. Esas capacidades quedan soportadas por la cola de revision, el registro de modelos y la separacion entre detalle de extraccion y agregado por ticket.
El futuro desarrollo y puesta en produccion del feature seguirá, por tanto, sujeta a posibles cambios tanto en lógica y transformaciones, como en los propios dataframes.

## Documentacion base

- `docs/features/mantiz_text_insights.md`
- `docs/pipelines/mantiz_ticket_lifecycle.md`
- `docs/pipelines/mantiz_ticket_text_insights.md`
- `docs/data_model/mantiz/ticket_lifecycle.md`
- `docs/data_model/mantiz/ticket_text_insights.md`
- `docs/data_model/mantiz/text_insight_detail.md`
- `docs/data_model/mantiz/text_insight_review_queue.md`
- `docs/data_model/mantiz/text_classifier_models.md`

