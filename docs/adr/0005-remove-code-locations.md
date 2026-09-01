# ADR 0005: Eliminacion de code locations de tenants que ya no reciben las soluciones

## Estado

Aceptado. 

## Contexto

Se recibe la instrucción de apagar completamente los procesos que alimentan soluciones de tenants que ya no recibirán soporte.
Se apagan los procesos, y para no mantener código muerto, y bajo los principios yagni, se elimina el código.

## Decision

Eliminar uno a uno los code locations de mcdonalds, mcdonalds centro-de-postres y smartfit. Así como todas sus referencias y usos.

## Consecuencias

- Los procesos se apagan.
- El code base se reduce, mejorando su mantenibilidad.
- El user-code ya no vive en memoria del host del proceso, liberando recursos para otros procesos.
- Volver a "encender" los procesos no séra un "toggle-switch", requerirá refactorización grande.

