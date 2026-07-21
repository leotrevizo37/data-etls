# Mantiz Text Insights

## Proposito

Transformar texto libre de tickets Mantiz en fallas, soluciones y refacciones medibles.

## Fuentes textuales

- Titulo del ticket.
- Descripcion.
- Notas de agenda.
- Notas de resolucion.
- Notas de aprobacion.
- Notas de cancelacion.
- Comentarios del ticket.

## Salidas

| Salida | Descripcion |
|---|---|
| `FailureCode` | Codigo canonico de falla |
| `SolutionCode` | Codigo canonico de solucion |
| `SolutionPartName` | Refaccion o parte inferida |
| `InsightsJson` | Evidencia estructurada |
| `NeedsReview` | Indica si requiere revision |

## Ciclo de mejora

1. El pipeline extrae conceptos por reglas o ML.
2. Los conceptos nuevos entran a `ctl.MantizTextInsightReviewQueue`.
3. La revision humana aprueba o rechaza canonicos.
4. Los modelos se reentrenan con conceptos aprobados.

