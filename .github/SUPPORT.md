# Support

Este repositorio contiene componentes de la plataforma de datos, incluyendo pipelines, procesamiento, almacenamiento, infraestructura, automatización y documentación.

Los problemas, dudas y solicitudes deben registrarse mediante **GitHub Issues**, utilizando la categoría correspondiente.

## Antes de abrir un Issue

Antes de crear uno nuevo:

1. revisar si ya existe un Issue relacionado;
2. confirmar que el problema corresponde a este repositorio;
3. recopilar la información necesaria para reproducir o analizar el caso;
4. evitar incluir credenciales, secretos o información sensible.

 Para información sobre desarrollo, configuración del entorno y ejecución local, consultar la Wiki **[Development](https://github.com/leotrevizo37/data-etls/wiki/Development)**
## Bugs

Crear un Issue de tipo bug cuando exista un comportamiento incorrecto o inesperado en alguno de los componentes de la plataforma.

Ejemplos:

- fallos en pipelines o assets;
- errores en jobs o schedules;
- transformaciones incorrectas;
- problemas en procesos Spark;
- errores en scripts SQL;
- fallos de integración;
- errores de configuración;
- problemas en CI/CD;
- documentación incorrecta o desactualizada.

El reporte debe incluir, cuando aplique:

```text
Componente:
Ambiente:
Issue relacionado:
Comportamiento esperado:
Comportamiento observado:
Pasos para reproducir:
Logs relevantes:
Fecha/hora aproximada:
```

Para problemas relacionados con datos, agregar también:

```text
Fuente:
Dataset o tabla:
Periodo afectado:
Identificador relevante:
Valor esperado:
Valor observado:
```

## Nuevas funcionalidades o mejoras

Crear un Issue de mejora o feature cuando se requiera:

- un nuevo pipeline;
- una nueva integración;
- nuevos assets;
- nuevas transformaciones;
- automatización adicional;
- cambios de infraestructura;
- mejoras de rendimiento;
- nuevas validaciones;
- mejoras de observabilidad;
- cambios en documentación o procesos.

El Issue debe describir:

- problema o necesidad;
- solución o comportamiento esperado;
- componentes afectados;
- impacto esperado;
- dependencias conocidas.

## Problemas de infraestructura

Para problemas relacionados con componentes como:

- Dagster;
- Spark;
- Docker;
- bases de datos;
- almacenamiento;
- GitHub Actions;
- runners;
- servicios externos;

incluir información suficiente para identificar el componente afectado y el ambiente donde ocurrió.

Cuando sea relevante, adjuntar logs o mensajes de error completos, eliminando previamente cualquier información sensible.

## Seguridad

No utilizar Issues públicos para reportar:

- vulnerabilidades;
- credenciales expuestas;
- tokens comprometidos;
- accesos no autorizados;
- configuraciones que puedan comprometer infraestructura o datos.

Estos casos deben seguir el procedimiento definido en **[SECURITY.md](SECURITY.md)**.

## Dudas de desarrollo

Las dudas relacionadas con:

- configuración del entorno;
- ejecución local;
- pruebas;
- herramientas;
- convenciones de desarrollo;
- estructura del proyecto;

deben resolverse primero consultando:

**Wiki → [[Home]](https://github.com/leotrevizo37/data-etls/wiki)**

Si la documentación no cubre el caso, crear un Issue de documentación o soporte para que la respuesta quede disponible para futuras contribuciones.

## Regla general

Si el problema puede afectar código, datos, infraestructura, documentación o el proceso de desarrollo del repositorio, debe quedar registrado en un Issue para mantener trazabilidad.