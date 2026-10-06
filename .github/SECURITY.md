# Security Policy

Este documento define cómo reportar y gestionar problemas de seguridad relacionados con este repositorio y los componentes de la plataforma de datos.

## Reportar una vulnerabilidad

No crear Issues públicos para reportar vulnerabilidades o incidentes de seguridad.

Esto incluye, entre otros:

- credenciales, tokens o secretos expuestos;
- accesos no autorizados;
- permisos incorrectos;
- connection strings comprometidas;
- vulnerabilidades en dependencias;
- imágenes Docker vulnerables;
- configuraciones inseguras;
- problemas en GitHub Actions o runners;
- exposición de datos sensibles;
- ejecución de código no autorizada;
- configuraciones que permitan acceso indebido a infraestructura o servicios.

El reporte debe realizarse mediante el canal privado de seguridad definido por la organización o mediante los mecanismos privados de seguridad del repositorio cuando estén disponibles.

## Información del reporte

Incluir, cuando sea posible:

```text
Componente afectado:
Versión o commit:
Ambiente:
Descripción:
Impacto potencial:
Pasos para reproducir:
Evidencia relevante:
Mitigación propuesta:
```

Evitar compartir secretos completos salvo que sea estrictamente necesario para investigar el incidente.

## Secretos expuestos

Si una credencial aparece en:

- código;
- commits;
- Pull Requests;
- logs;
- GitHub Actions;
- artefactos;
- imágenes Docker;
- configuración;
- documentación;

debe considerarse comprometida.

Eliminar el secreto del repositorio no es suficiente.

Se debe:

1. revocar o rotar la credencial;
2. identificar dónde fue expuesta;
3. revisar el periodo de exposición;
4. evaluar posibles usos no autorizados;
5. eliminar cualquier referencia persistente cuando corresponda;
6. corregir la causa que permitió la exposición.

## Gestión de secretos

No deben almacenarse directamente en el repositorio:

- passwords;
- API keys;
- access tokens;
- private keys;
- certificados privados;
- connection strings con credenciales;
- secretos de bases de datos;
- secretos de servicios externos.

Los secretos deben proporcionarse mediante los mecanismos autorizados para cada ambiente.

Ejemplos de valores que no deben versionarse:

```text
password = "..."
token = "..."
api_key = "..."
connection_string = "...password=..."
```

## Validaciones automáticas

El repositorio utiliza validaciones automáticas para reducir el riesgo de introducir secretos o dependencias vulnerables.

Entre ellas pueden incluirse:

- Gitleaks;
- Dependabot;
- validaciones de GitHub Actions;
- análisis de dependencias;
- validaciones adicionales definidas en CI/CD.

Estas validaciones complementan la revisión manual y no sustituyen las buenas prácticas de seguridad.

## Dependencias

Las actualizaciones de dependencias deben revisarse antes de integrarse, especialmente cuando afecten:

- Dagster;
- Spark;
- drivers de bases de datos;
- librerías de almacenamiento;
- integraciones externas;
- imágenes Docker;
- GitHub Actions.

Los Pull Requests generados automáticamente por Dependabot deben cumplir las mismas validaciones que cualquier otro cambio.

## GitHub Actions

Los workflows deben utilizar únicamente los permisos necesarios.

Ejemplo:

```yaml
permissions:
  contents: read
```

Cualquier permiso adicional debe estar justificado por las acciones que realiza el workflow.

También se debe evitar:

- exponer secretos en logs;
- utilizar credenciales innecesariamente;
- ejecutar código no confiable con acceso a secretos;
- otorgar permisos de escritura cuando solo se requiere lectura.

## Alcance

Esta política aplica a los componentes versionados o administrados desde este repositorio, incluyendo:

```text
Código
SQL
Pipelines
Dagster
Spark
Docker
CI/CD
Configuración
Infraestructura asociada
Documentación técnica
```

Para información sobre el flujo general de desarrollo y contribución, consultar **[CONTRIBUTING.md](CONTRIBUTING.md)**.