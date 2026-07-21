# Pipeline `100_mantiz_tickets` - lifecycle

## Proposito

Construir `dwh.factMantizTicketLifecycle` con estado y duraciones del ciclo de vida de tickets Mantiz.

## Entradas

- MySQL Mantiz: `Ticket`, `Branch`, `Client`, `EquipmentInstance`, `EquipmentCatalog`, `FailureType`, `Supplier`.
- Variable de organizacion Mantiz.
- Watermark `factMantizTicketLifecycle`.

## Salida

- `dwh.factMantizTicketLifecycle`.

## Flujo

1. Selecciona tickets cambiados por `COALESCE(updatedAt, createdAt)`.
2. Cruza contexto de sucursal, cliente, equipo, catalogo, falla y proveedor.
3. Calcula duraciones entre hitos del ciclo.
4. Deriva banderas de estado y calidad de captura.
5. Escribe a `stg.factMantizTicketLifecycle`.
6. Ejecuta `MERGE` por `(OrganizationId, TicketId)`.
7. Actualiza watermark.

## Uso operativo

- Aging de tickets abiertos.
- Seguimiento por proveedor/equipo.
- Evaluacion de tickets con foto, equipo y tipo de falla.

