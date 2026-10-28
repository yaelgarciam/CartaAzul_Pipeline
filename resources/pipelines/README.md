# Operación de Carta Azul

El pipeline procesa el histórico cerrado de Applebee's desde los JSON originales en `/Volumes/workspace/applebees/raw_files/data/Dataset`. Silver conserva una fila canónica por ticket, eventos independientes de pagos y descuentos y referencias históricas. Gold publica métricas, resúmenes, casos explicables y calidad. Los archivos originales se preservan.

Los recursos ya existen en tu [workspace](https://dbc-37e1517c-bfc3.cloud.databricks.com):

| Recurso | Identificador o ubicación |
| --- | --- |
| [Pipeline](https://dbc-37e1517c-bfc3.cloud.databricks.com/pipelines/869fc958-69e2-4f3b-ae58-616b27696816) | `869fc958-69e2-4f3b-ae58-616b27696816` |
| [Job](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410) | `245292025266410` |
| Código en Workspace Files | `/Workspace/Users/a01352461@tec.mx/CartaAzul` |
| Tablas publicadas | `workspace.applebees.silver_*` y `workspace.applebees.gold_*` |

El Job se ejecuta manualmente: concurrencia máxima de una ejecución, un reintento y timeout de cuatro horas. El pipeline es triggered, serverless, edición ADVANCED. La configuración del histórico completo comprende del **28 de abril al 20 de agosto de 2026**, ambos inclusive, y exige 115 archivos diarios. No hay calendario diario.

## Actualizar código y configuración

Subir `src/transformations/` a la misma ubicación privada de Workspace Files, conservando las extensiones `.py` y `.sql` y sus rutas. Incluir también `silver/_common.py` y `silver/_schemas.py`: son módulos importados, no entradas adicionales de `libraries`. El `root_path` termina en `src/transformations` y permite importar `silver`. Compartir únicamente con el equipo autorizado; no subir los JSON privados al repositorio.

Los YAML de esta carpeta y de `resources/jobs/` describen directamente los cuerpos de las API. **No constituyen un bundle autónomo** y no se aplican mediante `databricks bundle deploy`. El pipeline usa Pipelines REST create/update y el Job usa Jobs REST create/reset. En este workspace ya existen ambos: actualizar sus identificadores actuales evita crear recursos duplicados.

El siguiente ejemplo aplica la configuración a los recursos existentes, una vez terminadas las ejecuciones activas y subidos los archivos. Requiere Databricks SDK, PyYAML y autenticación configurada para este workspace; no guarda credenciales en el código. Ejecutarlo desde un entorno donde exista el repositorio local, o ajustar `repo` a su ubicación accesible.

```python
from pathlib import Path
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines

repo = Path("/Users/yaelgarciam/CartaAzul_Repo")
pipeline_id = "869fc958-69e2-4f3b-ae58-616b27696816"
job_id = 245292025266410
w = WorkspaceClient(host="https://dbc-37e1517c-bfc3.cloud.databricks.com")

pipeline_spec = yaml.safe_load(
    (repo / "resources/pipelines/carta_azul_pipeline.yml").read_text()
)
pipeline_spec["libraries"] = [
    pipelines.PipelineLibrary.from_dict(library)
    for library in pipeline_spec["libraries"]
]
# Comprobar que existe; no crear un segundo pipeline.
w.pipelines.get(pipeline_id=pipeline_id)
w.pipelines.update(pipeline_id=pipeline_id, **pipeline_spec)

job_spec = yaml.safe_load(
    (repo / "resources/jobs/carta_azul_job.yml").read_text()
)
settings = jobs.JobSettings.from_dict(job_spec)
assert len(settings.tasks) == 1
assert settings.tasks[0].pipeline_task.pipeline_id == pipeline_id
w.jobs.get(job_id=job_id)
w.jobs.reset(job_id=job_id, new_settings=settings)
```

`PipelineLibrary.from_dict` convierte las bibliotecas al tipo que espera el SDK; `JobSettings.from_dict` convierte también las tareas anidadas. `jobs.reset` sustituye la configuración del Job por la definición completa del YAML. [SDK de Pipelines](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/pipelines/pipelines.html), [SDK de Jobs](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html).

Para un piloto acotado, cambiar conjuntamente `start_date`, `end_date`, `expected_start_date`, `expected_end_date` y `expected_source_files` en la configuración `carta_azul.*`. El piloto y el histórico usan el mismo pipeline y los mismos nombres de tablas: una actualización acotada reemplaza las salidas por ese corte; no debe confundirse con el histórico completo. El prefijo de tablas debe mantenerse alineado entre Python y SQL.

## Ejecutar y comprobar

1. Abrir el Job y pulsar **Run now**. Revisar que la tarea `historical_pipeline` y el Job terminen en **SUCCESS**.
2. Consultar la fila de `gold_pipeline_quality` y verificar cobertura, conteos y diferencias de conciliación. Las expectations bloquean las discrepancias estructurales; los problemas de negocio se conservan para revisión.
3. Consultar las incidencias y casos. Un ticket puede tener varios motivos: contar tickets distintos para medir la población, y no sumar poblaciones de reglas superpuestas.

```sql
SELECT * FROM workspace.applebees.gold_pipeline_quality;

SELECT issue_code, severity, COUNT(*) AS issue_rows,
       COUNT(DISTINCT ticket_key) AS affected_tickets
FROM workspace.applebees.silver_quality_issues
GROUP BY issue_code, severity;

SELECT case_category, reason_code, COUNT(*) AS cases,
       COUNT(DISTINCT ticket_key) AS tickets
FROM workspace.applebees.gold_review_cases
GROUP BY case_category, reason_code;
```

La fila de calidad representa el snapshot actual; el historial del Job y del pipeline registra las ejecuciones. Consumir las tablas después de una actualización exitosa: un fallo no revierte automáticamente todas las tablas como una transacción global.

Repetir las mismas entradas debe conservar los resultados y evitar duplicar el histórico. `full_refresh: false` no garantiza refresco incremental ni evita releer JSON. Las materialized views pueden recalcularse completamente; las que tienen expectations requieren refresco completo.

## Definiciones e interpretación

```text
venta bruta     = sum(ItemsSold.GrossPrice)
venta neta      = sum(ItemsSold.SoldPrice) - sum(Discounts.Amount del ticket)
propina         = -sum(Payments.Gratuity)
pago conciliable = sum(Payments.Amount) + sum(Payments.Gratuity)
saldo esperado  = venta neta + sum(Tax.Amount) + ServiceCharge
diferencia      = pago conciliable - saldo esperado
```

`SoldPrice` ya incorpora los descuentos de artículos; no se vuelven a restar ni se multiplican los precios por `Quantity`. La tolerancia empírica de conciliación es 0.05. Se preservan los signos originales y operaciones especiales como pagos negativos `GC Sold`. Los catálogos son históricos: `employees.Deleted` no excluye automáticamente una referencia.

Los casos `DATA_QUALITY` requieren resolver la calidad del dato. Los casos `EXPLORATORY_COMPARISON` muestran observaciones, umbrales, soporte y definición de sus cohortes. El percentil 0.99 y el mínimo de 50 observaciones son parámetros exploratorios para análisis de sensibilidad; no son políticas oficiales ni una clasificación de fraude. Los identificadores de actor/aprobador no prueban quién recibió el beneficio. Se analizan **propinas registradas**; puede faltar información sobre propinas en efectivo. Los denominadores diarios se repiten por etiqueta de descuento y no son sumables entre etiquetas.

## Evidencia de aceptación

La ejecución del [histórico completo, run `438408340438199`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/438408340438199) y su [reejecución, run `939543104005830`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/939543104005830), terminaron en **SUCCESS**. La aceptación exportada tiene estado **PASS**. Pasaron las **28 aserciones sintéticas** y se verificó por SHA-256 la correspondencia de los **11 archivos fuente** locales y desplegados.

| Resultado del histórico completo | Valor |
| --- | --- |
| Cobertura | 115 archivos y 115 días, del 28 de abril al 20 de agosto de 2026 |
| Tickets | 2,142,846 |
| Eventos de pago | 2,232,092 |
| Eventos de descuento | 516,690 |
| Materialized views | 11 |
| Llaves de ticket inválidas / duplicadas | 0 / 0 |

Las huellas de las 11 tablas coincidieron entre ambas ejecuciones: conteos, llaves distintas, mínimos/máximos SHA y cuatro sumas de segmentos del hash. Es una comprobación agregada de estabilidad; no una prueba exhaustiva de igualdad fila por fila.

La calidad es **`COMPLETE_WITH_ISSUES_TO_REVIEW`**, con cero incidencias `ERROR`. Se conservaron para revisión 59 diferencias de conciliación de pagos, 2 diferencias de descuentos de artículos, 3 tickets con propina original positiva y referencias de actor no resueltas en 2 eventos de descuento y 179 pagos. Los conteos de reglas pueden solaparse. Las comparaciones generaron **424 casos exploratorios**: 144 por propinas registradas y 280 por proporción de descuento; son motivos de revisión humana, no determinaciones de fraude o incumplimiento.

La ejecución real requirió dos ajustes: `Voids.Quantity` usa decimal para preservar cantidades fraccionarias válidas del JSON, y la lectura inicial aplica `coalesce(1)` para contener la presión de buffers de Photon con estos archivos diarios no divisibles. La lectura se serializa y puede tardar más; es una decisión de recursos para este histórico en Free Edition, no una promesa de mejor rendimiento general.

La [evidencia detallada privada](</Users/yaelgarciam/Documents/Databricks/inspection/acceptance_evidence.json>) permanece fuera del repositorio Git. También existe en Workspace Files, en `/Workspace/Users/a01352461@tec.mx/CartaAzul/resources/pipelines/acceptance_evidence.json`. Mantener ese acceso limitado al equipo; no copiar los datos ni las salidas privadas al repositorio.


## Repetir pruebas de transformaciones

`test_pipeline_spark.py` usa datos sintéticos y vistas temporales. Ejecuta los cuerpos reales de las funciones Silver y las consultas Gold; el pipeline real valida además las declaraciones y expectations. El adaptador de pruebas suministra configuración explícita porque los notebooks serverless restringen las claves Spark personalizadas fuera de un pipeline. No cambia los JSON ni las tablas persistentes.

Después de subir también este archivo a la ruta equivalente de Workspace Files, ejecutar en un notebook conectado a Spark:

```python
from pathlib import Path
PIPELINE_TRANSFORMATIONS_ROOT = "/Workspace/Users/a01352461@tec.mx/CartaAzul/src/transformations"
exec(Path("/Workspace/Users/a01352461@tec.mx/CartaAzul/resources/pipelines/test_pipeline_spark.py").read_text())
```

La batería final contiene 28 aserciones, incluidos descuentos con etiqueta desconocida y pagos sin indicador de tarjeta o categoría. Esos tickets conservan sus importes conciliables y quedan fuera de las cohortes comparativas.
