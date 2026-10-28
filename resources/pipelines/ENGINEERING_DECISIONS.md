# Carta Azul engineering decisions

These decisions describe the implemented historical pipeline and distinguish deployed capabilities from evaluated options. They introduce no runtime or YAML changes. The [architecture](ARCHITECTURE.md), [pipeline specification](carta_azul_pipeline.yml) and [operating guide](README.md) identify the actual artifacts.

| Capability | Decision for this delivery | Evidence or limitation |
| --- | --- | --- |
| Streaming | Evaluated; use batch reads and triggered execution. | Fixed 115-day source history, no expected new data and no continuous freshness requirement. |
| Materialized views | Implemented for all 11 datasets. | Declarative dependencies and reusable results; full recomputation remains a supported operational cost. |
| Expectations | Implemented with warning metrics and structural fail-update gates. | Preserve explainable source exceptions while blocking broken dataset structure. |
| Liquid clustering | Evaluated qualitatively and deferred; project code does not explicitly configure it. | No controlled query benchmark or measured maintenance/payback case exists yet. |

## Batch and triggered execution

The source is a closed historical set, not an arrival stream. Batch semantics support reconstructing complete snapshots after a rule or schema change and inspecting exceptions against the same inputs. A continuous service would remain active without a corresponding business freshness requirement. A streaming-table design could process arrivals incrementally, but checkpoint/arrival semantics add little value to this fixed source and would need reconsideration if corrections or future data were introduced.

Pipeline mode and dataset type are independent: a streaming table can run in triggered mode, and a materialized view can participate in either mode. Triggered mode stops after refreshing available data. This decision therefore does not claim that streaming requires continuous compute. [Databricks pipeline modes](https://docs.databricks.com/aws/en/ldp/concepts/pipeline-mode).

The Job is manually triggered because no future daily input is expected. A schedule or event trigger can be added if the actual data contract changes; it is not needed to reproduce this historical delivery.

## Eleven materialized views and refresh cost

Each Python definition returns a batch DataFrame, and each Gold SQL file declares a materialized view. Lakeflow owns the dependency graph and output refreshes. `silver_checks` materializes the parsed transaction base once per applicable refresh so child transformations use managed results rather than independently reading the original daily files. This is reuse by design, not a measured performance benchmark. [Databricks materialized views](https://docs.databricks.com/aws/en/ldp/concepts/materialized-views).

Correctness takes precedence over assuming an incremental refresh. Raw JSON in a volume does not supply an incrementally supported source for these materialized views. Downstream refresh choices depend on their sources and query plans; they can be incremental, a full recompute, or no change. `full_refresh: false` does not guarantee an incremental scan. Current Databricks documentation supports incremental refresh for some views with expectations, so expectations alone do not prove that every dataset must fully recompute. Inspect the actual incrementalization information before claiming a strategy. [Incremental refresh requirements and limitations](https://docs.databricks.com/aws/en/ldp/incremental-refresh).

Keeping the source history intact makes recomputation possible. The cost is additional reading, storage and refresh time, acceptable for the tested closed history but not an unlimited-scale claim. Removing or archiving source data would require reevaluating this contract.

## Expectations: preserve observations, fail structural damage

The implementation uses `dp.expect_all` for record-level quality metrics, keeping rows that fail checks. The explicitly built `silver_quality_issues` table records what failed and why. Negative money, a missing actor reference or a reconciliation exception is preserved for contextual review rather than dropped indiscriminately.

Gold applies `ON VIOLATION FAIL UPDATE` to structural invariants: required keys, nonempty historical input, expected source coverage, one feature per canonical check and child-event counts matching their source arrays. Failures such as an incompatible input schema also stop processing through strict parsing. Databricks supports warning, dropping and failing actions; dropping is not the chosen default here. [Expectation actions and tracking](https://docs.databricks.com/aws/en/ldp/expectations).

An issue severity such as `ERROR` is an analytical label in the issue table; it does not automatically convert a warning expectation into a fail-update expectation. Duplicate source keys, for example, remain inspectable with their counts and are excluded from eligible analysis. Consumers inspect quality and the overall successful run, not just the existence of output tables. A failed dataset update does not provide global rollback of every table in the graph.

This separates reproducibility from the claim that all source observations are normal. The accepted snapshot has `COMPLETE_WITH_ISSUES_TO_REVIEW`: operational acceptance coexists with retained exceptions.

## Liquid clustering: evaluation and decision to defer

Databricks recommends liquid clustering for new tables, including materialized views. It organizes data for query skipping, supports evolving keys and replaces physical partitioning/`ZORDER`; it is not equivalent to the Hive-style date folders of the original JSON. Candidate keys should follow observed query filters. [Official liquid-clustering guidance](https://docs.databricks.com/aws/en/tables/clustering).

For this delivery, the available workload is a fixed 2.14-million-check history on constrained Free Edition compute. The operational problem observed was raw JSON reader memory, which clustering downstream results would not solve. Consumer query frequency, selectivity, physical table sizes and maintenance benefit have not been benchmarked. We therefore defer explicit clustering until there is a measured consumption workload; this is not a claim that clustering is unavailable, ineffective or forbidden on Free Edition. Platform-managed defaults and the current physical clustering state have not been inspected, so no claim is made that every output is physically unclustered. No query speedup, cost reduction or clustering coverage is asserted.

A follow-up experiment would:

1. Establish repeated, representative date/location filters and aggregation queries on `gold_check_features` and the larger event tables; record table/file sizes and query profiles under comparable cache/compute conditions.
2. Evaluate a small key set such as business date and location, or supported automatic selection, through the pipeline definition. Keep any experiment separate from accepted outputs.
3. Check declared keys and actual historical layout/optimization coverage using supported metadata and maintenance history. Merely declaring keys is not proof that existing files have been reorganized.
4. Compare files/bytes scanned, elapsed-time distributions and refresh/maintenance work. Adopt the setting only with repeatable workload benefit and no unexplained regression.
5. Rerun data-quality checks and content-equivalence checks to confirm that layout changes do not alter analytical results.

The list is an evaluation plan, not an experiment already executed. This documentation change leaves all clustering/runtime settings untouched.

## Source memory and fractional quantities

The first full parallel read failed when Photon's `SimdJsonReader` could not reserve a 768 MiB buffer for daily multiline JSON. The accepted implementation applies DataFrame `.coalesce(1)` immediately after the check-source read and before hashing/window transformations. It narrows the source scan so multiple unsplittable daily files are not parsed concurrently in that stage. Spark documents the narrow dependency of `coalesce`; `repartition(1)` would retain parallel upstream execution before its shuffle. [Spark DataFrame.coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html).

Both accepted full-history runs completed with this change. It is a memory tradeoff that serializes reading and can reduce throughput; it is not a general performance optimization or evidence that larger sources will always fit. No unsupported Photon disablement or enlarged compute setting was introduced.

The next full-history failure exposed a source-schema mismatch on May 23: `Voids[].Quantity` contained a fraction while the initial schema expected an integer. The final schema uses `DECIMAL(18,4)`. A regression assertion in the 28-test suite strictly parses a quantity of 0.5 and amount of 1.25 as exact decimal values. This was a type-contract correction, not evidence that valid source JSON was corrupt. Monetary sums remain decimal, prices remain line totals, and item discounts are not deducted twice.

## Orchestration, failure handling and reproducibility

One Job task launches one pipeline. Lakeflow schedules its dataset DAG; the task count does not need to equal the table count. The Job allows a single concurrent run, queues later runs, has one retry with a minimum 60-second interval and a four-hour timeout. A retry may recover a transient failure; it cannot repair a deterministic schema or data-contract defect. Inspect the failed dataset/error, correct the cause, ensure the owned run has stopped, then rerun. Source changes should not be deployed during an active update.

Two accepted runs, `438408340438199` and `939543104005830`, both succeeded. Acceptance verified 115 source files, 2,142,846 source/canonical checks, exact event cardinality and stable signatures across all 11 views. The 28 synthetic assertions cover money, list semantics, duplicates, references, unknown cohort context and fractional void quantities. Source files deployed to the workspace were checked against 11 local code files by SHA-256.

The signature comparison uses counts, distinct primary keys where applicable, minimum/maximum row SHA-256 and four hash-limb sums. It is a strong practical aggregate check, not exhaustive row-by-row equality or proof against hash collisions. No refresh timestamp is inserted into analytical rows. The quality dataset is a current snapshot; Job/pipeline history is the execution log.

The resource YAMLs are API request specifications for the existing workspace, not a standalone bundle. Reproduction requires authorized data access, the specified workspace source paths and aligned pipeline/Job identifiers. There is no embedded token. Synthetic tests adapt configuration outside the pipeline without replacing transformation logic; a fresh session or schema-module reload is needed after changing an imported schema. Private acceptance details remain access-controlled as described in the operating guide.
