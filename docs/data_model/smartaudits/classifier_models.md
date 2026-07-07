# SmartAuditsClassifierModels

## Proposito

`ctl.SmartauditsClassifierModels` registra artefactos de modelos de clasificacion SmartAudits y controla cual esta activo.

## Granularidad

Un registro representa una version de modelo.

## Columnas principales

| Columna | Tipo SQL | Descripcion |
|---|---|---|
| `ModelVersion` | `NVARCHAR(100)` | Version del modelo |
| `ArtifactPath` | `NVARCHAR(1000)` | Ruta del artefacto |
| `Accuracy` | `DECIMAL(9,6)` | Accuracy de evaluacion |
| `MacroF1` | `DECIMAL(9,6)` | Macro F1 de evaluacion |
| `TrainingRows` | `INT` | Filas usadas para entrenar |
| `IsActive` | `BIT` | Modelo activo para prediccion |
| `PromotedAt`, `CreatedAt` | `DATETIME2(0)` | Fechas de control |

## Reglas de negocio

- El asset SmartAudits consulta el modelo activo antes de predecir.
- El artefacto se lee desde almacenamiento remoto mediante Spark.
- La promocion desactiva versiones previas cuando aplica.

