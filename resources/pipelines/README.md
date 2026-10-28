# Carta Azul pipeline: operation and delivery guide

Carta Azul processes the closed Applebee's transaction history into 7 Silver and 4 Gold materialized views. It preserves the original JSON, applies consistent monetary definitions and data-quality checks, and produces explained cases for an authorized business review. Employee-discount policies and confirmed outcome labels were not supplied; the outputs are exploratory indicators.

## Start here

| Material | What it explains |
| --- | --- |
| [Architecture and table catalog](ARCHITECTURE.md) | Source-to-output graph, dependencies, grain and purpose of all 11 tables. |
| [Engineering decisions](ENGINEERING_DECISIONS.md) | Batch versus streaming, materialized views, expectations, liquid-clustering evaluation and reliability tradeoffs. |
| [Findings and presentation notes](ANALYSIS_AND_PRESENTATION.md) | Verified results, denominators, business interpretation, queries and a six-part presentation outline. |
| [Requirements traceability](REQUIREMENTS_TRACEABILITY.md) | Where each Phase 2 delivery criterion is addressed. |
| [Source-validation notebook](01_source_validation.ipynb) | Complete file inventory and a bounded, read-only source diagnostic using the production schema. |
| [Pipeline-validation notebook](02_pipeline_validation.ipynb) | Existing synthetic tests, accepted run status, full-history coverage, row counts, keys and quality. |
| [Gold-analysis notebook](03_gold_analysis.ipynb) | Review queue, chart, monthly context, location-support context and correctly defined frequencies. |

The repository is the implementation source of record. `pipeline_stage` in the local working directory is an earlier preparation copy, not an input to Databricks. Supporting documentation and notebooks stay in the existing `resources/pipelines/` directory to preserve the requested four-folder structure. The notebooks are curated, reproducible companions to the private exploration/deployment notebooks; they are not unedited exports. Notebook outputs, execution counts, personal data and repair/deployment payloads are excluded from Git.

## Existing deployment and prerequisites

| Resource | Current identifier or location |
| --- | --- |
| [Workspace](https://dbc-37e1517c-bfc3.cloud.databricks.com) | Authorized challenge workspace |
| [Pipeline](https://dbc-37e1517c-bfc3.cloud.databricks.com/pipelines/869fc958-69e2-4f3b-ae58-616b27696816) | `869fc958-69e2-4f3b-ae58-616b27696816` |
| [Job](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410) | `245292025266410` |
| Private project root | `/Workspace/Users/a01352461@tec.mx/CartaAzul` |
| Source JSON | `/Volumes/workspace/applebees/raw_files/data/Dataset` |
| Output namespace | `workspace.applebees` |
| Historical scope | April 28–August 20, 2026 inclusive; 115 daily transaction files |

An authorized user needs read access to the Volume and published tables, access to the project Workspace Files, and permission to run the existing Job. Changing configuration additionally requires ownership or suitable management permission on those resources. Use native Databricks notebook authentication or an authenticated SDK environment for this workspace. Never store credentials in the repository. SDK version observed at acceptance: `0.122.0`.

The Job has one `historical_pipeline` task, concurrency 1, one retry after at least 60 seconds, no retry on timeout, a four-hour task/Job timeout and queuing enabled. It runs manually without a schedule. Its pipeline is triggered, serverless, edition `ADVANCED`, channel `current`, with `development: false`. Dataset reads define the dependency graph inside the pipeline; a list of independent Job tasks is not required for each table.

## Run and inspect

1. Open the Job and select **Run now** when the pipeline is idle. Require both the Job and task to finish successfully.
2. Read `gold_pipeline_quality`. Confirm complete coverage and preserved event counts. Structural failures stop acceptance; source-quality warnings remain visible.
3. Inspect `silver_quality_issues` for data issues and `gold_review_cases` for explained quality or exploratory reasons. Count distinct tickets alongside case rows because reasons can overlap.
4. Import the three supporting `.ipynb` files into the private workspace using Databricks notebook import, then run their cells in order in a Python/Spark session. Keep them outside the pipeline's explicit `libraries` list. Their first code cells contain editable paths and scope settings. The analysis notebook uses the standard Matplotlib notebook library for its aggregate chart.

```sql
SELECT * FROM workspace.applebees.gold_pipeline_quality;

SELECT issue_code, severity, COUNT(*) AS issue_rows,
       COUNT(DISTINCT ticket_key) AS affected_tickets
FROM workspace.applebees.silver_quality_issues
GROUP BY issue_code, severity;

SELECT case_category, reason_code, COUNT(*) AS case_rows,
       COUNT(DISTINCT ticket_key) AS distinct_tickets
FROM workspace.applebees.gold_review_cases
GROUP BY case_category, reason_code;
```

Consume tables after a successful update. Lakeflow does not provide a single transaction across all output tables; a failed update can leave tables at different successful refresh points. The one-row quality output describes the current snapshot, while Job/pipeline history records executions.

## Update the existing deployment

The unchanged configuration files are [the pipeline specification](carta_azul_pipeline.yml) and [the Job settings](../jobs/carta_azul_job.yml). They are API-shaped YAML, not a standalone bundle; `databricks bundle deploy` is not their deployment command.

Upload `src/transformations/` to the private project root while preserving paths and `.py`/`.sql` extensions. Include `_common.py` and `_schemas.py`; they are imported helpers rather than additional library entries. Also upload the existing `test_pipeline_spark.py` to `resources/pipelines/`. The configured `root_path` makes `silver` importable. The supporting notebooks and Markdown do not belong in pipeline libraries.

After active updates have finished, the following example applies configuration to the **existing** resource IDs. It requires the Databricks SDK and PyYAML, an authenticated workspace session, and a local path to this repository. Edit `repo` for the machine executing it. In a native notebook, local Mac files are not automatically visible: use an accessible repository checkout or Workspace Files copy instead.

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

`jobs.reset` replaces the full Job settings. Preserve the current IDs to avoid duplicate resources. A different workspace needs accessible source/code paths, an output namespace, permissions and its own resource IDs; this example updates the supplied workspace rather than provisioning an independent environment. [Pipeline SDK reference](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/pipelines/pipelines.html), [Jobs SDK reference](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html).

For an intentionally bounded pilot, change `start_date`, `end_date`, `expected_start_date`, `expected_end_date` and `expected_source_files` together. The pilot uses the same output names and replaces their snapshot; restore the full scope before claiming historical acceptance. Keep the Python/SQL table prefix aligned.

## Failure handling and reruns

For a transient infrastructure error, allow the configured retry and inspect the terminal Job/task status. For schema, permission, resource or structural-quality errors, inspect the failed dataset and event log, correct the cause, and rerun after the pipeline is idle. Retrying alone is not a schema repair. Do not accept a cancelled/failed attempt or a partial pilot as the complete history.

The original full-history failures were Photon JSON-reader buffer pressure and a declared integer quantity that encountered valid fractional void quantities. The accepted implementation serializes the initial JSON scan with `coalesce(1)` and reads `Voids.Quantity` as `DECIMAL(18,4)`. Serial reading reduces parallelism. It is a bounded Free Edition choice verified on this history, not a general scalability optimization. Both fixes are already part of the accepted source code.

Reruns recompute the historical snapshot without appending duplicate records. `full_refresh: false` does not guarantee an incremental refresh or avoid JSON reads. The accepted run UI showed full recomputation. JSON source scans require batch work; downstream Delta-based materialized views are planned according to supported query features. Expectations alone do not universally force every materialized view to refresh fully. See the [engineering decisions](ENGINEERING_DECISIONS.md) and [official refresh documentation](https://docs.databricks.com/aws/en/ldp/incremental-refresh).

## Accepted results and interpretation

The complete-history [run `438408340438199`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/438408340438199) and [repeat run `939543104005830`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/939543104005830) succeeded on October 27, 2026. Acceptance included 28 passing synthetic assertions, all 115 daily files, 2,142,846 canonical tickets, 2,232,092 payment events, 516,690 discount events and matching aggregate fingerprints across all 11 materialized views. Aggregate fingerprints are a practical stability check, not a formal row-by-row proof.

The snapshot is `COMPLETE_WITH_ISSUES_TO_REVIEW`, with zero `ERROR` issues. It preserves 59 settlement differences, 2 item-discount differences, 3 tickets with unexpected positive raw gratuity, and unresolved actor references in 2 discount events and 179 payments. The exploratory rules produced 424 distinct review tickets: 144 recorded-tip cases and 280 discount-share cases among 41,475 eligible employee-label candidates. These results describe a review queue, not confirmed wrongdoing. See the [findings](ANALYSIS_AND_PRESENTATION.md) for denominators and review actions.

```text
recorded gross sales = sum(ItemsSold.GrossPrice)
recorded net sales   = sum(ItemsSold.SoldPrice) - sum(ticket-level Discounts.Amount)
recorded tip         = -sum(Payments.Gratuity)
actual settlement   = sum(Payments.Amount) + sum(Payments.Gratuity)
expected settlement = recorded net sales + sum(Tax.Amount) + ServiceCharge
```

`SoldPrice` already incorporates item discounts. Do not subtract them again or multiply recorded line amounts by quantity. Tolerance 0.05, percentile 0.99 and minimum cohort support 50 are versioned empirical/exploratory settings, not official benefit policy. Actor/approver IDs do not establish a beneficiary. Recorded cash tips may be incomplete. Historical `employees.Deleted` records remain reference context. Daily denominators repeat by discount label and are not additive across labels.

Private detailed evidence is stored at the project Workspace Files path `resources/pipelines/acceptance_evidence.json`; the local working copy is outside Git. Reviewers need authorized workspace access for that evidence and the run links. The Git deliverable contains reproducible, output-free notebooks and code rather than private source data, event-level exports or employee details.

To run only the existing synthetic harness after uploading it and the source files:

```python
from pathlib import Path
PIPELINE_TRANSFORMATIONS_ROOT = "/Workspace/Users/a01352461@tec.mx/CartaAzul/src/transformations"
exec(Path("/Workspace/Users/a01352461@tec.mx/CartaAzul/resources/pipelines/test_pipeline_spark.py").read_text())
```

The [pipeline-validation notebook](02_pipeline_validation.ipynb) additionally reloads the schema before these tests and checks the accepted historical tables. The test adapter supplies explicit synthetic configuration because serverless notebooks restrict custom Spark configuration outside a pipeline.

## Supporting-notebook verification

On October 28, 2026, all 19 code cells across the three companion notebooks executed in order, each in its own namespace inside a native Databricks Spark session. The existing 28 synthetic assertions passed again. Notebook JSON was validated with `nbformat`, and imported source cells were checked against the local files. The private imported copies have cleared outputs:

- [01_source_validation](https://dbc-37e1517c-bfc3.cloud.databricks.com/editor/notebooks/2523613708789701?o=7474648099639044)
- [02_pipeline_validation](https://dbc-37e1517c-bfc3.cloud.databricks.com/editor/notebooks/2523613708789702?o=7474648099639044)
- [03_gold_analysis](https://dbc-37e1517c-bfc3.cloud.databricks.com/editor/notebooks/2523613708789703?o=7474648099639044)

Validation confirmed unchanged SHA-256 values for all 14 production source/configuration/test-harness files before and after execution, as well as unchanged pipeline specifications and Job settings. It did not trigger another historical pipeline run. Private evidence: `resources/pipelines/documentation_validation.json` under the project Workspace Files root. These execution checks complement the original two-run historical acceptance rather than replacing it.
