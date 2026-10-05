# factStoreSalesDaily

## Contrato

`dwh.factStoreSalesDaily` es un agregado Bafar por sucursal y fecha local, con llave primaria `(StoreId, SalesDate)`. No representa ventas individuales ni ventas exclusivamente fuera de horario.

| Columna | Tipo | Semántica |
|---|---|---|
| `StoreId` | `VARCHAR(5) NOT NULL` | Clave de sucursal de la fuente |
| `SalesDate` | `DATE NOT NULL` | Fecha de las ventas |
| `SalesCount` | `INT NOT NULL` | Conteo después de deduplicar por identificador de venta |
| `SalesAmount` | `DECIMAL(19,4) NOT NULL` | Suma de importes |
| `ModifiedAt` | `DATETIME2(0) NOT NULL` | Momento UTC de escritura en warehouse |

`stg.factStoreSalesDaily` contiene los campos de negocio y `RunId`, sin `ModifiedAt`. El índice `(SalesDate, StoreId)` soporta consulta temporal; staging tiene índice por corrida y llave. El [DDL](../../../sql/user_codes/bafar/init/011_dwh_stg_factStoreSalesDaily.sql) es el contrato físico.

El merge actualiza o inserta por la llave; no es un reemplazo completo de todos los agregados históricos. No se debe usar `ModifiedAt` como fecha de venta. El [pipeline](../../pipelines/fact_store_sales_daily.md) cubre programación, ventana incremental y backfill.
