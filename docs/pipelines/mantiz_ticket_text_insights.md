# Pipeline `100_mantiz_tickets` - text insights

## Proposito

Extraer conceptos medibles desde textos Mantiz y consolidarlos en `dwh.factMantizTicketTextInsights`.

## Entradas

- Tickets cambiados o con comentarios cambiados.
- Comentarios y notas del ticket.
- Contexto de sucursal, cliente, equipo, falla, proveedor y refacciones.
- Modelos activos en `ctl.MantizTextInsightClassifierModels`.

## Salidas

- `dwh.factMantizTicketTextInsights`.
- `stg.MantizTicketTextInsightDetail`.
- `ctl.MantizTextInsightReviewQueue`.

## Flujo

1. Selecciona tickets cambiados y comentarios cambiados.
2. Construye contexto del ticket.
3. Normaliza textos fuente.
4. Extrae detalle de fallas, soluciones y refacciones.
5. Aplica modelos activos cuando existen.
6. Agrega el detalle a una fila por ticket.
7. Registra conceptos candidatos en la cola de revision.
8. Reemplaza en DWH los tickets cambiados.

## Reglas

- El reemplazo se limita a llaves cambiadas en staging.
- `NeedsReview` queda activo si no hay clasificacion o hay conceptos nuevos.
- `InsightsJson` y `TextEvidenceJson` conservan evidencia estructurada para auditoria.

## Jobs relacionados

- `101_mantiz_text_classifier_training`
- `102_mantiz_text_insight_review_promotion`

