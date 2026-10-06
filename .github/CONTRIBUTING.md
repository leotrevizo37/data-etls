# Contributing to data-etls

Este repositorio contiene los componentes de la plataforma de datos. Las contribuciones deben realizarse mediante Issues, ramas independientes y Pull Requests.

Para información detallada sobre configuración del entorno, desarrollo local, pruebas, arquitectura y herramientas, consultar la Wiki **[Development](https://github.com/leotrevizo37/data-etls/wiki/Development)**

## Contribución y Git

Todo cambio debe estar asociado a un Issue y desarrollarse en una rama propia creada a partir de `development`.

No se deben realizar cambios directamente sobre `development`, `stage` o `main`.

El flujo general es:

```text
Issue
  │
  ▼
development
  │
  └── <tipo>/<issue>-<descripcion-corta>
          │
          ├── desarrollo
          ├── tests
          ├── documentación
          ├── actualización con development
          │
          └── Pull Request → development
```

### Crear una rama

Antes de comenzar un cambio, actualizar `development`:

```bash
git checkout development
git pull origin development
```

Después, crear una rama utilizando el número del Issue correspondiente:

```bash
git checkout -b <tipo>/<issue>-<descripcion-corta>
```

Por ejemplo, para el Issue `#438`:

```bash
git checkout -b feat/438-feature-x
```

La rama debe originarse siempre desde una versión actualizada de `development`.

## Convención de ramas

Las ramas deben seguir el formato:

```text
<tipo>/<issue>-<descripcion-corta>
```

Ejemplos:

```text
feat/438-feature-x
fix/512-correct-watermark
refactor/601-spark-writer
docs/620-update-wiki
```

Tipos recomendados:

- `feat/`: nuevas funcionalidades, pipelines, assets o capacidades.
- `fix/`: correcciones de bugs.
- `refactor/`: cambios internos sin modificación funcional esperada.
- `docs/`: documentación.
- `test/`: cambios exclusivamente de pruebas.
- `chore/`: mantenimiento, dependencias, configuración o tooling.
- `ci/`: cambios de CI/CD.

La descripción debe ser corta, descriptiva y utilizar `kebab-case`.

Durante el desarrollo, la rama debe mantenerse actualizada con `development`, especialmente antes de abrir o mergear el Pull Request.

## Commits

Los commits deben seguir **Conventional Commits**:

```text
<tipo>(<scope>): <descripción>
```

Ejemplos:

```text
feat(dagster): add daily sales asset
fix(spark): correct partition filtering
refactor(sql): simplify staging merge
test(data-quality): add duplicate validation
docs(wiki): document backfill workflow
chore(deps): update dagster dependencies
ci(github): add pull request validation
```

Tipos principales:

- `feat`: nueva funcionalidad o capacidad.
- `fix`: corrección de comportamiento.
- `refactor`: cambio interno sin alterar funcionalidad esperada.
- `test`: creación o modificación de pruebas.
- `docs`: documentación.
- `chore`: mantenimiento o configuración.
- `ci`: cambios relacionados con CI/CD.
- `perf`: mejoras de rendimiento.
- `build`: cambios en el sistema de construcción o empaquetado.

Cada commit debe representar un cambio coherente y evitar mezclar modificaciones no relacionadas.

## Pull Requests

Una vez terminado el desarrollo, crear un Pull Request hacia `development`.

Antes de abrirlo:

1. actualizar la rama con los cambios recientes de `development`;
2. resolver conflictos;
3. ejecutar las validaciones y pruebas aplicables;
4. actualizar la documentación cuando corresponda.

El Pull Request debe indicar el Issue relacionado:

```text
Closes #438
```

y describir, cuando aplique:

- cambios principales;
- beneficios;
- posibles breaking changes.

El Pull Request debe pasar las validaciones automáticas configuradas en el repositorio antes de integrarse.

## Más información

Para el flujo completo de desarrollo, configuración del entorno, ejecución local, testing y herramientas utilizadas por la plataforma, consultar:

**Wiki → [[Development]](https://github.com/leotrevizo37/data-etls/wiki/Development)**