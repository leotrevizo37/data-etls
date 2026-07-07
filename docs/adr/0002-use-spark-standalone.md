# ADR 0002: Usar Spark standalone para transformaciones

## Estado

R&D. Deliberando

## Contexto

Las lecturas y fuentes operativas pueden ser voluminosas, y varias transformaciones requieren joins, agregaciones, ventanas y escritura JDBC particionada.

## Decision

Usar Spark standalone como motor de transformacion distribuida.

## Consecuencias

- Los pipelines pueden procesar lecturas, dimensiones y fuentes MySQL con el mismo motor.
- El recurso Spark controla perfiles por tipo de job.
- La disponibilidad de workers y memoria pasa a ser una dependencia operacional.
- Los DataFrames intermedios se materializan con `spark_df_io_manager`.

