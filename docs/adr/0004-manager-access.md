# ADR 0004: Scrapeo de datos de API /api/gerentes/ultimo-acceso 

## Estado

Aceptado.

## Contexto

Se espera desarrollar una interfaz web para observar los datos de accesos de los gerentes a plataformas.
El proyecto se esperaba hacer sin nuestro apoyo, pero de último momento nos incorporaron al flujo para
que fuéramos nosotros los que muestran los datos y métricas solicitadas, para los cuales se espera poder 
seleccionar y comparar rangos de fechas que pueden variar de días hasta cuartos, esto puede ser caro para 
la API y tardado para el usuario.

## Decision

Consultar la api de manera sistemática y calendarizada, y almacenar los datos en `dwh`, en la propia warehouse
destinada para bafar para que funga como una caché de datos en calientes incluyendo histórico, para no saturar 
la API al momento de hacer comparaciones o consultas de periodos grandes. 

## Consecuencias

- Los consumidores consultan directamente la tabla en dwh.factManagerAccessDaily, en vez de hacer multiples llamadas a la API con cada consulta.
- La API se mantiene protegida de consultas masivas.
- Los datos se mantienen responsivos y rápidos para ser consultados.
- Se duplica en cierta medida la información, sin embargo, es despreciable en comparación con el cómputo ahorrado y disponibilidad para hacer reportes.

