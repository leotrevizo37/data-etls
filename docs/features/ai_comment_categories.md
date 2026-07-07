# AI Comment Categories

## Proposito

Convertir comentarios SmartAudits generados por revision IA en categorias operativas medibles.

## Contratos

| Objeto | Uso |
|---|---|
| `dwh.factSmartauditsCategories` | Resultado clasificado por `PlanResultId` |
| `ctl.SmartauditsAiCommentLookup` | Comentarios ya clasificados |
| `ctl.SmartauditsAiCommentReviewQueue` | Comentarios pendientes de revision |
| `ctl.SmartauditsClassifierModels` | Modelos activos o historicos |

## Metodos

| Metodo | Descripcion |
|---|---|
| `RULE` | Reglas deterministas por palabras clave y cumplimiento |
| `ML` | Modelo supervisado activo |
| `HUMAN` | Revision aprobada y promovida desde cola |
| `SIN_CLASIFICAR` | Comentario pendiente o insuficiente confianza |

## Uso esperado

- Detectar problemas recurrentes de evidencia.
- Relacionar fallas por tarea, checkpoint o sucursal.
- Alimentar ciclos de mejora del clasificador.
- Clasificar ejecucion de las tareas de smartaudits.
- Errores más recurrentes por sucursales.
- Planes de accion para elevar calificacion de sucursal.
- Planes de accion para elevar calificacion del pilar de cultura.
- Porcentajes de cumplimiento de tareas por sucursal.
- Se puede obtener la calificacion directamente para el ICOS.
- Se puede sacar una relación de la criticidad y prioridad de las tareas con su recurrencia y tipos de fallas.
- Por ejemplo, se puede obtener el dato de que X sucursal NO esta tomando bien sus evidencias.

