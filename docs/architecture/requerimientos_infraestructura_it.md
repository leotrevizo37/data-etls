# Propuesta de infraestructura - Data&AISolutions (Proyecto DUMA)

La propuesta separa la orquestación ETL en una VM, el cómputo Spark en AKS con Spark on Kubernetes y los sistemas web y agentes en Azure Container Apps. SQL se mantiene como capa de consulta para aplicaciones, dashboards y agentes. Es una arquitectura objetivo; este documento no implica que la migración esté implementada.

El dimensionamiento contempla un crecimiento con escalamiento pensado desde el diseño. La VM de orquestación se reduce de **8 vCPU / 32 GiB a 4 vCPU / 16 GiB**, y el cómputo Spark pasa a nodos AKS de **4 vCPU / 16 GiB**, con escalamiento de **1 a 16 nodos**. Se incluyen operaciones-duma, duma-web y los repositorios de agentes. Los tamaños siguientes constituyen la propuesta inicial de aprovisionamiento; los agentes se dimensionan a partir de su VM prevista, sin asumir una revisión de sus repositorios. Los demás proyectos que actualmente comparten infraestructura quedan fuera de esta propuesta.

## Componentes

| Proyecto o finalidad | SKU o servicio de referencia | Dimensiones y capacidades |
|---|---|---|
| ETL — orquestación Dagster (data) | Azure Standard_D4as_v5 | 1 VM: 4 vCPU y 16 GiB de RAM para webserver, daemon, servicios de código, PostgreSQL y procesos de orquestación; sin drivers ni executors Spark |
| AKS — servicios del clúster (data) | AKS Standard; pool de sistema Standard_F4s_v2 | 2 nodos fijos de 4 vCPU / 8 GiB cada uno; capacidad bruta total de 8 vCPU / 16 GiB, reservada a servicios del clúster |
| AKS — cómputo distribuido Spark (data) | Pool de usuario Standard_D4ads_v5 | Inicial/mínimo: 1 nodo; máximo: 16. Cada nodo: 4 vCPU / 16 GiB y 150 GiB de SSD temporal. Capacidad bruta: 4–64 vCPU / 16–256 GiB |
| Aplicación agéntica (agentes) | Azure Container Apps — Consumption | 4 vCPU / 8 GiB por réplica; 0–4 réplicas por servicio de agentes. Máximo por servicio: 16 vCPU / 32 GiB |
| Plataforma web — Front, BFF y Data API (web) | Azure Container Apps — Consumption | Presupuesto conjunto por réplica: 3 vCPU / 6 GiB; 2–4 réplicas. Base: 6 vCPU / 12 GiB; máximo: 12 vCPU / 24 GiB |
| Registro de imágenes de contenedor (web, agentes, data) | Azure Container Registry — Standard | Imágenes de producción. La compilación ocurre en CI; VM, AKS y Container Apps ejecutan imágenes ya construidas |
| Secretos y material criptográfico (web) | Azure Key Vault — Standard | Clave de firma JWT, keyring de sesión del BFF, clave privada RS256 interna, clave de cifrado de fuentes por tenant y credenciales de las bases de los clientes |
| Warehouse — servicio de consultas (data) | Azure SQL Database Elastic Pool Standard | Cuatro bases independientes. Objetivo solicitado: 150 eDTU compartidas; Standard no ofrece ese tamaño. Se propone 200 eDTU como siguiente tamaño disponible, pendiente de aprobación |
| Históricos e intermedios (data, web, agentes) | Blob Storage / ADLS Gen2 | Capacidad ampliable. Retención indefinida de históricos y limpieza de intermedios según su uso |
| Base de datos de plataforma (web) | Azure SQL Database — Standard S1 | Base `platform`: identidad, tenants y catálogo de módulos. 20 DTU, 250 GB incluidos |
| Persistencia y recuperación de la VM (data) | Azure Managed Disks y respaldo de PostgreSQL | Metadatos de Dagster en disco persistente; copias fuera de la VM y restauración verificada. El disco por sí solo no constituye un respaldo |
| Persistencia de aplicaciones (web, agentes) | Bases de datos externas y Blob Storage / ADLS Gen2 | Estado, conversaciones y archivos fuera del almacenamiento efímero de Container Apps. El destino del MSSQL de agentes requiere validar compatibilidad con sus responsables |
| Respaldo de largo plazo de la warehouse (data) | Azure SQL Database — LTR, RA-GZRS | Una copia mensual por base, conservada durante 12 meses. Referencia de costo: 100 GB acumulados entre todas las copias |

Para un poco de contexto, las cinco bases que actualmente se manteinen son de 3GiB, pero la de mcdonalds con data de octubre 2025 - junio 2026 terminó pesando 25GiB. El escenario de crecimiento corresponde a unos **60 GB**, antes de considerar espacio adicional para índices, staging y operación.

La distribución propuesta de ejecución y persistencia es:

```
VM ETL — Dagster — D4as_v5 — 4 vCPU / 16 GiB
│
├── P6 — 64 GiB — Disco del sistema operativo
│   └── Linux, Docker, imágenes y aplicación
│
└── P10 — 128 GiB — Montado en /data/etl
    └── PostgreSQL de Dagster y estado persistente


AKS — Spark on Kubernetes
│
├── Pool de sistema — 2 × F4s_v2 — 8 vCPU / 16 GiB totales
│
└── Pool Spark — 1–16 × D4ads_v5 — 4–64 vCPU / 16–256 GiB totales
    ├── 150 GiB de SSD temporal por nodo para shuffle y spill
    └── Datos duraderos y event logs en Blob Storage / ADLS Gen2


AZURE CONTAINER APPS — WEB Y AGENTES
│
├── Plataforma web — 3 vCPU / 6 GiB por réplica — 2–4 réplicas
├── Agentes — 4 vCPU / 8 GiB por réplica — 0–4 réplicas por servicio
└── Estado persistente en bases de datos y almacenamiento externos
```

## Separación de orquestación y cómputo

Se recomienda mantener inicialmente la orquestación en la VM y ejecutar en AKS tanto el driver como los executors de cada aplicación Spark. Dagster debe enviar y supervisar las ejecuciones, registrar sus resultados y propagar sus fallos. Llevar también los servicios de Dagster a AKS es una alternativa si se necesita gestionar toda la plataforma en Kubernetes; el crecimiento de datos por sí solo no exige ese traslado. Una VM única sigue siendo un punto de fallo de la orquestación y no proporciona alta disponibilidad.

El código actual crea sesiones Spark desde recursos de Dagster y utiliza referencias a Spark Standalone en las implementaciones revisadas de bafar, ecosat, carlsjr, emerson y valledelencino. La arquitectura objetivo requiere adaptar el lanzamiento y seguimiento de trabajos, las imágenes y dependencias, los permisos y el acceso al almacenamiento; no se obtiene únicamente cambiando la dirección del master. Deben conservarse los contratos y particularidades de cada user_code afectado. Estos cambios de código quedan fuera de esta actualización documental.

VM, AKS, Container Apps, SQL y almacenamiento requieren conectividad privada y permisos acotados a cada servicio. El pool de Spark debe tener límites de concurrencia y recursos para evitar saturar SQL con lecturas y escrituras simultáneas. El almacenamiento temporal de los nodos no debe contener la única copia de datos o resultados.

## Costo y dimensionamiento

### Container Apps

Se propone un entorno con workload profiles y perfil **Consumption**, que admite réplicas de 4 vCPU / 8 GiB. Los tamaños de la tabla son presupuestos totales por réplica, incluidos sus contenedores auxiliares, y no asignaciones adicionales para cada contenedor. Si Front, BFF o Data API se despliegan como aplicaciones independientes, se debe repartir el presupuesto indicado entre ellas.

| Aplicación | Mínimo / inicial / máximo de réplicas | Regla inicial de escalamiento |
|---|---|---|
| Agentes, por servicio | 0 / 0 / 4 | HTTP: objetivo de 15 solicitudes concurrentes por réplica. Para consumidores asíncronos, usar el escalador de su cola existente con objetivo de 1 mensaje pendiente por réplica |
| Plataforma web | 2 / 2 / 4 | HTTP: objetivo de 50 solicitudes concurrentes por réplica |

Las reglas HTTP y de cola son alternativas según el tipo de servicio; un agente sin ingress HTTP necesita un evento o cola que lo active desde cero. Se acepta el arranque en frío de los agentes y se mantienen dos réplicas de la plataforma web para atender tráfico durante reinicios. Los umbrales son valores iniciales de escalamiento, no límites de admisión ni rendimientos medidos. Las sesiones y conversaciones deben persistirse fuera de las réplicas.

La VM web original de 4 vCPU / 16 GiB se sustituye por una base conjunta de **6 vCPU / 12 GiB**, ampliable a **12 vCPU / 24 GiB**. Se asigna más CPU al conjunto de réplicas y se reduce la RAM base al externalizar persistencia y observabilidad; Front, BFF y Data API comparten el presupuesto de la plataforma web, con 1 vCPU / 2 GiB para Front/BFF y 2 vCPU / 4 GiB para backend/Data API por réplica. La propuesta agrupa su despliegue y escalamiento; no implica fusionar los repositorios. Este presupuesto no incluye bases de datos ni backends de observabilidad dentro de las réplicas. Cada réplica de agentes conserva los **4 vCPU / 8 GiB** de su VM original; el máximo de cuatro réplicas se aplica por servicio, por lo que el total depende del número de servicios desplegados. Los agentes consumen modelos mediante APIs/endpoints externos, según el alcance confirmado; estas réplicas no hospedan modelos de inferencia.

### VM de orquestación y AKS

La **D4as_v5 de 4 vCPU / 16 GiB** reduce a la mitad CPU y RAM de la VM ETL original y elimina la necesidad de SSD temporal para Spark en esa VM. Mantiene margen para los servicios de código de los user_codes y PostgreSQL. La reducción requiere que tanto drivers como executors se ejecuten en AKS; los procesos locales de Dagster quedan limitados a orquestación y tareas ligeras.

Se propone **AKS Standard**, con **2 nodos F4s_v2 de sistema** para la base productiva y un pool exclusivo de Spark con Cluster Autoscaler entre **1 y 16 nodos D4ads_v5**. El mínimo de un nodo Spark mantiene capacidad de arranque entre ciclos de 15–30 minutos; las cargas que excedan sus recursos esperan el aprovisionamiento de nodos adicionales. Se mantienen nodos regulares, sin Spot, para esta capacidad inicial. Cada nodo tendrá un disco de sistema administrado de **128 GiB**; en los nodos Spark se montará además el SSD temporal de **150 GiB** como espacio de shuffle/spill, sin utilizarlo como almacenamiento duradero.

| Recurso Spark | Propuesta inicial |
|---|---|
| Driver por aplicación | Solicitud de CPU: 0.5 vCPU; límite: 1 vCPU. Memoria Spark: 1 GiB + 1 GiB de overhead = 2 GiB por pod |
| Executor | 1 vCPU; 3 GiB de memoria Spark + 1 GiB de overhead = 4 GiB por pod |
| Executors por aplicación | Asignación dinámica: mínimo 1, inicial 1, máximo 6; habilitar seguimiento de shuffle compatible con la versión desplegada |
| Concurrencia | Máximo 2 aplicaciones Spark activas en el conjunto de user_codes; las demás esperan en Dagster |
| Máximo agregado de aplicaciones | 2 drivers + 12 executors: 13 vCPU / 52 GiB solicitados, dentro de los 64 vCPU / 256 GiB brutos del pool máximo |

Una ejecución inicial solicita **1.5 vCPU / 6 GiB** entre driver y un executor. El nodo de 4 vCPU / 16 GiB deja 2.5 vCPU / 10 GiB brutos tras esa asignación, antes de reservas y servicios del nodo. Se prevén hasta tres executors por nodo, o un driver y dos executors con mayor margen para servicios del nodo; la colocación efectiva depende de los recursos asignables. Se mantienen los tamaños de drivers y executors, por lo que el aumento del nodo permite alojar más pods sin cambiar la memoria por tarea Spark. Los nuevos pods pendientes por falta de capacidad disparan el crecimiento del pool; al finalizar las cargas se retiran nodos ociosos hasta el mínimo de uno. Los recursos asignables son inferiores a los brutos por las reservas de Kubernetes. Los máximos propuestos contemplan ese margen y la distribución de pods entre nodos.

El clúster arranca con **3 nodos en total: 2 de sistema y 1 de Spark**, equivalentes a **12 vCPU / 32 GiB brutos**, y puede alcanzar **18 nodos: 2 de sistema y 16 de Spark**, equivalentes a **72 vCPU / 272 GiB brutos**. Los nodos de sistema conservan 4 vCPU, mínimo admitido por AKS, y reducen su RAM a 8 GiB. Dos nodos cumplen el mínimo de redundancia; ofrecen menos margen ante fallos o mantenimiento que tres. El pool de sistema no se cuenta como capacidad Spark. Esta base permite presupuestar explícitamente el costo fijo de Kubernetes, además de la VM de Dagster.

### Configuración y compatibilidad de AKS

La propuesta utiliza pools Linux: `System` con dos F4s_v2 y `User` con D4ads_v5. El pool de sistema cumple el mínimo documentado de dos nodos y 4 vCPU por nodo; el de usuario cumple el mínimo de 2 vCPU. Configurar `maxPods=30` en ambos pools, el taint `CriticalAddonsOnly=true:NoSchedule` en el de sistema y un selector de nodo para que drivers y executors se programen en el pool Spark. Ambos pools tienen 4 vCPU por nodo; los de Spark disponen de 16 GiB de RAM frente a los 8 GiB de los de sistema.

Configurar explícitamente `osDiskType=Managed` y disco de sistema de 128 GiB. En el pool Spark, usar `kubeletDiskType=Temporary` y volúmenes `emptyDir` respaldados por disco para los directorios locales de Spark. Los 150 GiB temporales se comparten con imágenes, logs y datos de kubelet; no son 150 GiB exclusivos de shuffle. No usar `emptyDir` en memoria; mantener el disco de sistema administrado para no consumir el SSD temporal con el sistema operativo.

Parámetros propuestos para Spark 4.0.1 en modo `cluster`, que deben incorporarse al mecanismo de envío de trabajos:

| Parámetro | Valor |
|---|---|
| `spark.kubernetes.driver.request.cores` / `spark.kubernetes.driver.limit.cores` | `0.5` / `1` |
| `spark.driver.memory` / `spark.driver.memoryOverhead` | `1g` / `1g` |
| `spark.executor.cores` | `1` |
| `spark.kubernetes.executor.request.cores` / `spark.kubernetes.executor.limit.cores` | `1` / `1` |
| `spark.executor.memory` / `spark.executor.memoryOverhead` | `3g` / `1g` |
| `spark.dynamicAllocation.enabled` | `true` |
| `spark.dynamicAllocation.minExecutors` / `initialExecutors` / `maxExecutors` | `1` / `1` / `6`, con el prefijo `spark.dynamicAllocation.` en los tres parámetros |
| `spark.dynamicAllocation.shuffleTracking.enabled` | `true` |
| `spark.shuffle.service.enabled` | `false` |

La solicitud de 0.5 vCPU del driver está soportada; no se debe trasladar ese valor a `spark.driver.cores`. Los procesos Python comparten el overhead presupuestado: no añadir una reserva independiente de PySpark sin recalcular la memoria del pod. La asignación dinámica crea o elimina executors; Cluster Autoscaler ajusta nodos según pods pendientes y posibilidades de desalojo. Los nodos con trabajos o shuffle activo pueden permanecer ocupados hasta que se liberen los pods, por lo que el descenso al mínimo no es inmediato ni garantizado durante una ejecución.

La comprobación realizada es documental y de dimensionamiento, no un despliegue probado. Antes del aprovisionamiento se debe confirmar disponibilidad regional de ambos SKU, cuotas para el máximo de 72 vCPU del clúster más margen de actualización, espacio de red y recursos asignables después de los DaemonSets. El límite de 16 nodos del pool Spark es de operación normal; las actualizaciones pueden necesitar nodos temporales adicionales. El envío desde Dagster requiere acceso al API de Kubernetes, una service account con RBAC para Spark e imágenes accesibles desde AKS.

Fuentes: [mínimos de los pools](https://learn.microsoft.com/en-us/azure/aks/use-system-pools), [restricciones y disponibilidad de SKU](https://learn.microsoft.com/en-us/azure/aks/quotas-skus-regions), [reservas de recursos](https://learn.microsoft.com/en-us/azure/aks/node-resource-reservations), [ubicación del almacenamiento temporal](https://techcommunity.microsoft.com/blog/fasttrackforazureblog/everything-you-want-to-know-about-ephemeral-os-disks-and-azure-kubernetes-servic/3565605), [Spark 4.0.1 en Kubernetes](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html) y [configuración de Spark 4.0.1](https://spark.apache.org/docs/4.0.1/configuration.html).

Los tamaños y límites anteriores son la propuesta para aprovisionar. Las pruebas de carga deben comprobar tiempos de ejecución, memoria, shuffle/spill y presión de consultas y escrituras sobre SQL, ajustando estos valores con evidencia. AKS aporta crecimiento independiente del cómputo; el ahorro frente a una VM única no está demostrado y debe comparar también nodos de sistema, red, discos, observabilidad y operación. La propuesta de Elastic Pool Standard de **200 eDTU** se mantiene como sustituto del objetivo de 150 eDTU, tamaño no disponible en Standard.

Referencias oficiales consultadas: [Spark on Kubernetes](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html), [costos de AKS](https://learn.microsoft.com/en-us/azure/aks/best-practices-cost), [pools de sistema de AKS](https://learn.microsoft.com/en-us/azure/aks/use-system-pools), [planes de Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/plans) y [capacidades del Elastic Pool Standard](https://learn.microsoft.com/en-us/azure/azure-sql/database/resource-limits-dtu-elastic-pools?view=azuresql). El repositorio utiliza Spark/PySpark 4.0.1. Se debe seleccionar una versión de Kubernetes soportada por AKS en la región y comprobar su compatibilidad con el cliente Kubernetes incluido en esa imagen mediante una ejecución de integración.

Referencias de los tamaños y reglas propuestos: [recursos por réplica de Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/containers), [escalamiento de Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/scale-app), [serie Fsv2](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/compute-optimized/fsv2-series), [serie Dasv5](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dasv5-series) y [serie Dadsv5](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dadsv5-series).


## Alcance de capacidad

El alcance de cuatro bases del pool es una propuesta y no un inventario verificado. La referencia anterior a cinco bases mantenidas corresponde al contexto de dimensionamiento; este documento no identifica su correspondencia con las cuatro propuestas. Esa asignación y las estimaciones de crecimiento y frescura quedan pendientes de validación de capacidad en el entorno objetivo.

El objetivo de capacidad es trabajar pensando en escalamiento desde el inicio, con **20 veces los datos actuales** y un tráfico **30 veces mayor al actual** como base operativa, conservando los tiempos de respuesta de las consultas, una frescura de **15 minutos** para los datos prioritarios y escalamiento frente a más tráfico o datos. Es una meta pendiente de pruebas de carga, no una capacidad validada con los tamaños de referencia.

Esto contempla el procesamiento incremental, las consultas de los agentes y el trabajo ordinario de respaldo y mantenimiento. La capacidad de cada componente podrá ampliarse por separado conforme aumente el uso.


