# MantizTextInsightClassifierModels

## Proposito

`ctl.MantizTextInsightClassifierModels` registra modelos de clasificacion textual Mantiz por tipo de insight.

## Granularidad

Un registro representa una version de modelo para un `InsightType`.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `InsightType` | `NVARCHAR(30)` | Tipo de insight clasificado |
| `ModelVersion` | `NVARCHAR(100)` | Version del modelo |
| `ArtifactPath` | `NVARCHAR(1000)` | Ruta del artefacto |
| `Accuracy` | `DECIMAL(9,6)` | Accuracy |
| `MacroF1` | `DECIMAL(9,6)` | Macro F1 |
| `TrainingRows` | `INT` | Filas de entrenamiento |
| `IsActive` | `BIT` | Modelo activo |
| `CreatedAt`, `PromotedAt` | `DATETIME2(0)` | Fechas de control |

## Reglas de negocio

- Puede existir un modelo activo por `InsightType`.
- Los artefactos se leen desde almacenamiento remoto mediante Spark.
- La promocion desactiva modelos activos previos del mismo `InsightType`.

