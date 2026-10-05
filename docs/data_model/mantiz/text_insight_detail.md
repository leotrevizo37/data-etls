# MantizTicketTextInsightDetail

Estado: Mantiz desactivado. El contrato existente se conserva como referencia.

## Proposito

`stg.MantizTicketTextInsightDetail` conserva el detalle de cada frase o concepto extraido desde textos Mantiz durante una corrida.

## Granularidad

Un registro representa una extraccion textual por ticket, fuente de texto, tipo de insight y hash de frase.

## Columnas clave

| Columna | Descripcion |
|---|---|
| `RunId` | Corrida Dagster |
| `OrganizationId`, `TicketId` | Llave del ticket |
| `SourceType`, `SourceId` | Origen textual, por ejemplo comentario o nota |
| `RawText`, `RawTextExcerpt`, `NormalizedText` | Texto original y normalizado |
| `InsightType` | `FALLA`, `SOLUCION` o `REFACCION_SOLUCION` |
| `InsightPhraseHash` | Hash de la frase extraida |
| `CanonicalFailureCode`, `CanonicalSolutionCode`, `CanonicalSolutionPartName` | Canonicos derivados |
| `ExtractionMethod` | Metodo de extraccion |
| `ExtractionConfidence` | Confianza |
| `IsNewConceptCandidate` | Indica candidato nuevo para revision |
| `SourceLastChangedAt` | Fecha de cambio fuente |

## Reglas de negocio

- Es staging por `RunId`; no es tabla final de consumo.
- Alimenta `dwh.factMantizTicketTextInsights`.
- Alimenta `ctl.MantizTextInsightReviewQueue` mediante agregacion por concepto.

