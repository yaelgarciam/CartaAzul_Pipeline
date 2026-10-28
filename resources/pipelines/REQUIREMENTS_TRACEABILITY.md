# Phase 2 requirements traceability

This index connects the submitted repository to the two delivery guides reviewed on October 28, 2026:

- [Basic Databricks GitHub Repository Template](https://sunholdings-shared-services-bi.atlassian.net/wiki/external/ZDcwNTQ3MTUzMjY0NGU3MWFiZThmNGZkYTM0YjU5MmU).
- [Phase 2: Pipeline Presentation](https://sunholdings-shared-services-bi.atlassian.net/wiki/external/MTAzZDI5NzhmYjM4NGZmNzkwYTg3ZWQyODM5NWI5ZGI).

The source guides were last updated on October 13, 2026. The reference filenames in the template are examples. The implementation retains the required division of orchestration/configuration and Silver/Gold table logic. Supporting notebooks and English documentation are located in `resources/pipelines/` without adding new top-level folders.

## Repository structure

| Required location | Actual content |
| --- | --- |
| `resources/jobs/` | [Manual historical Job settings](../jobs/carta_azul_job.yml). |
| `resources/pipelines/` | [Pipeline specification](carta_azul_pipeline.yml), [operation guide](README.md), supporting documents, three reproducible notebooks and the existing synthetic test harness. |
| `src/transformations/silver/` | Seven materialized views through five entry-point files, plus schema and expression helpers. |
| `src/transformations/gold/` | Four SQL materialized views for ticket features, daily discounts, review cases and quality. |

## Deliverable coverage

| Phase 2 deliverable | Evidence and demonstration |
| --- | --- |
| Complete, organized GitHub repository | The four required folders, code/configuration, linked English documentation and output-free notebooks. Start at [README](README.md). The local implementation is the source of record; local edits require a normal Git commit/push before they appear on GitHub. |
| Pipeline Job | [Job settings](../jobs/carta_azul_job.yml) and [operation guide](README.md): manual triggering, one task calling the declarative pipeline, queuing/concurrency, retry/timeout, failure diagnosis and repeat-run behavior. The task graph inside the pipeline is explained in [architecture](ARCHITECTURE.md). |
| Pipeline overview | [Architecture](ARCHITECTURE.md): original JSON Volume, canonical tickets, independent events and references, four Gold outputs and the actual dependency graph. |
| Transformations and table value | [Architecture table catalog](ARCHITECTURE.md) describes grain, purpose and dependencies for every output. Source code implements strict parsing, normalization, safe expansion, joins, eligibility and aggregation. [Findings](ANALYSIS_AND_PRESENTATION.md) explain how those outputs support review. |
| Data quality | The unchanged source expectations, explained `silver_quality_issues`, structural `FAIL UPDATE` rules in `gold_pipeline_quality`, [source notebook](01_source_validation.ipynb), and [pipeline-validation notebook](02_pipeline_validation.ipynb). Completeness, validity, types, uniqueness, references, money and cardinality are covered. |
| Technical best practices | [Engineering decisions](ENGINEERING_DECISIONS.md) explicitly explain the batch choice after evaluating streaming, implemented materialized views/expectations, qualitative liquid-clustering assessment and current decision, full-refresh limits, memory tradeoffs and reproducibility. No unmeasured speedup or unimplemented optimization is presented as a result. |
| Analysis and business value | [Findings and presentation notes](ANALYSIS_AND_PRESENTATION.md) explain verified metrics, candidate denominators, review actions and limits. [Gold analysis](03_gold_analysis.ipynb) adds an executable aggregate chart and period/location/discount context. Additional query results must be inspected before describing them as findings. |

## Recommended presentation sequence

The [six-part outline](ANALYSIS_AND_PRESENTATION.md) covers the objective, source/architecture, transformations, data quality, final analysis and review actions. Use the implemented graph, accepted metrics and live aggregate notebook outputs as supporting evidence. Technical communication and the actual presentation remain the team's work; the repository supplies the material rather than claiming an assessment score.

The guides permit explaining the use **or evaluation** of streaming and liquid clustering. This historical project documents both decisions without adding continuous ingestion or changing the accepted table layout. A dashboard, a new Bronze table, a multi-task Job and a Databricks Asset Bundle are not required by the provided template.

## Evidence boundaries

- **Accepted runtime (October 27, 2026):** full run `438408340438199` and repeat run `939543104005830`, both successful; 115 daily files, 2,142,846 tickets; 28 passing synthetic assertions; matching aggregate fingerprints across 11 tables.
- **Documentation change:** English narrative and three curated supporting notebooks; production Python/SQL, YAML and the existing regression harness are kept unchanged.
- **Notebook execution:** the files committed to Git have no outputs. Private execution status and original acceptance evidence remain in the authorized Databricks workspace. The one-day source profile does not replace full-history validation; the current validation notebook does not rerun the full Job twice.
- **Confidential material:** raw JSON, transaction/employee rows, original unsanitized notebook outputs and private detailed evidence stay outside Git. Native notebook authentication is used without embedded credentials.
- **Interpretation:** statistical flags are exploratory review signals. No official policy, beneficiary inference, loss estimate or confirmed wrongdoing is asserted. All ratios identify their population and overlapping reasons are counted distinctly.

## Documentation verification completed on October 28, 2026

The three companion notebooks passed native Databricks execution: 7 source-validation code cells, 6 pipeline-validation code cells and 6 Gold-analysis code cells, including a fresh PASS of the existing 28 synthetic assertions. Standard notebook-schema validation and imported-cell round-trip checks passed. The period/location aggregates documented in the findings were collected from this execution; the optional extended SQL examples remain clearly identified as queries to run before claiming their results.

All 14 production source/configuration/harness files matched their original SHA-256 values before and after execution. The Job settings and pipeline specification also remained unchanged. Detailed evidence is private in project Workspace Files at `resources/pipelines/documentation_validation.json`; reproducible code and English explanations are included here without private notebook outputs.
