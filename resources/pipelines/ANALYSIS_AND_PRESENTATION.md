# Verified findings and presentation notes

This document explains what the accepted Carta Azul pipeline establishes, how its outputs support a business review, and how to present that evidence for Phase 2. It documents the existing runtime; it does not change transformations, thresholds, resources, or data.

## 1. Business question and analytical value

**Which tickets with employee-labeled discounts warrant further review because their recorded tips or discount share are unusual relative to documented historical peers?**

The pipeline turns nested transaction JSON into consistent ticket metrics, independent payment and discount events, historical references, and explained review cases. Its practical value is a reproducible review queue with traceable reasons and denominators. It also identifies data-quality issues that must be understood before a reviewer interprets transaction behavior.

The output does not identify confirmed improper benefits, monetary loss, or fraud. No official employee-discount policies or confirmed outcome labels were provided. Discount actors and approvers are operational roles; neither field establishes who received a benefit. An employee-related label describes the recorded discount, not a verified beneficiary.

## 2. Accepted findings and their denominators

The full historical Job run [`438408340438199`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/438408340438199) and repeat run [`939543104005830`](https://dbc-37e1517c-bfc3.cloud.databricks.com/jobs/245292025266410/runs/939543104005830) both completed successfully. The private acceptance evidence reports `PASS`, 28 passing synthetic assertions, matching SHA-256 values for the 11 deployed source files, and stable aggregate fingerprints for all 11 materialized views. Fingerprints are a practical repeatability check, not an exhaustive row-by-row proof.

| Verified result | Scope or denominator | Business interpretation |
| --- | --- | --- |
| 2,142,846 source and canonical tickets | 115 files and 115 business days, April 28–August 20, 2026; 127 locations with transactions | The accepted results cover the supplied transaction history rather than a sample. |
| 0 invalid or duplicate ticket keys | All 2,142,846 canonical tickets | The accepted ticket identity did not require ambiguous records to enter the analytical population. |
| 2,128,584 tickets eligible for monetary analysis | Tickets passing the implemented key, date, completeness, uniqueness, settlement, and item-discount checks | Financial summaries have an explicit eligible population; they do not silently treat all source tickets as comparable sales. |
| 2,021,005 tickets eligible for ordinary comparisons | Tickets additionally passing the implemented reference, sign, discount-classification, payment-context, positive-denominator, and no-void filters | Statistical comparisons deliberately use a narrower population than the complete historical dataset. |
| 42,460 tickets with an observed employee discount label | All tickets with at least one such label | This is label presence, not a count of employees or beneficiaries. |
| 41,475 eligible employee-label candidates | Ordinary-comparison tickets with at least one observed employee discount label | This is the denominator for the review-queue rates below. It is not a count of confirmed eligible benefit recipients. |
| 424 distinct tickets with exploratory cases, or **1.0223%** | 424 / 41,475 eligible employee-label candidates | The current rules narrow the review workload; this percentage is not an estimated misconduct rate. |
| 144 high recorded-tip cases, or **0.3472%** | 144 / 41,475 eligible employee-label candidates | Both recorded tip amount and tip/gross exceed the selected peer quantiles. |
| 280 high discount-share cases, or **0.6751%** | 280 / 41,475 eligible employee-label candidates | Employee-labeled discount/gross exceeds the selected quantile for peers with the same normalized label set. |
| 59 settlement exceptions, or **0.002772%** | 59 / 2,128,645 settlement-evaluable tickets; the corresponding pass rate is **99.997228%** | The empirical settlement identity reconciles almost all evaluable tickets, while preserving exceptions for review. This is not an error rate for all data fields. |
| 2 item-discount reconciliation exceptions | 2 / 2,131,338 item-discount-evaluable tickets | These exceptions require inspection of the recorded gross, sold, and item-discount relationship. |
| 3 tickets with unexpected positive raw gratuity | All historical tickets; this counts tickets, not payment events | The observed tip-sign convention has exceptions; applying an absolute value would conceal them. |
| 0 `ERROR` issue rows; status `COMPLETE_WITH_ISSUES_TO_REVIEW` | The current quality snapshot | Structural acceptance does not mean that every transaction is suitable for every analysis. Warnings and contextual observations remain visible. |

The two exploratory reasons currently total 424 cases and 424 distinct tickets. Continue counting distinct tickets when combining reasons: future settings or datasets may produce multiple cases for one ticket. Quality cases form a separate reason category and can overlap the same tickets; they are not confirmed adverse outcomes.

These whole-period figures describe the accepted history. The additional descriptive period/location breakdown below was executed on October 28, 2026. No location misconduct ranking, predictive trend, cohort mean, recovered-revenue amount or causal explanation is established. A small review queue demonstrates manageable triage, not the accuracy of a wrongdoing detector.

Private supporting evidence is retained outside Git at `/Users/yaelgarciam/Documents/Databricks/inspection/acceptance_evidence.json` and in Workspace Files at `/Workspace/Users/a01352461@tec.mx/CartaAzul/resources/pipelines/acceptance_evidence.json`. Do not copy private transaction exports or employee data into the repository or presentation.

### Executed period and location context

All 19 code cells in the three companion notebooks completed successfully in a native Databricks Spark session on October 28, 2026. The existing 28 synthetic assertions passed again. The Gold-analysis notebook produced the following descriptive breakdown directly from the published tables:

| Observed business period | Eligible employee-label candidates | Distinct flagged tickets | Candidate flag percentage |
| --- | ---: | ---: | ---: |
| April 28–30, 2026 | 1,038 | 11 | 1.0597% |
| May 2026 | 11,032 | 135 | 1.2237% |
| June 2026 | 10,484 | 122 | 1.1637% |
| July 2026 | 11,274 | 90 | 0.7983% |
| August 1–20, 2026 | 7,647 | 66 | 0.8631% |

The complete-month candidate flag percentage was 1.2237% in May and 0.7983% in July. This supports planning the historical review workload with an explicit denominator; it does not establish improving conduct, a prospective trend or a policy-compliance change. The rules were fitted to the complete history, and channel, location, label and basket mix can affect these summaries. April and August have partial coverage.

The location-support aggregate found 126 locations with at least 50 employee-label candidates, containing 41,454 candidates and all 424 flagged tickets. Flags appeared across 107 of those locations. One additional location had 21 candidates and no flags. This indicates that the review queue spans many locations; it does not establish a location ranking or prove that an unflagged location is normal. The site-level candidate counts here are descriptive and do not replace each rule's more specific cohort-support requirement.

The [Gold-analysis notebook](03_gold_analysis.ipynb) reproduces these summaries and an aggregate reason-count chart. Its Git copy has no saved outputs. Private validation evidence is stored in the project Workspace Files as `resources/pipelines/documentation_validation.json`; it records each successful code-cell sequence, aggregate outputs and unchanged production files/configuration.

## 3. What the rules measure

The accepted monetary definitions are:

```text
recorded gross sales = sum(ItemsSold.GrossPrice)
recorded net sales   = sum(ItemsSold.SoldPrice) - sum(ticket-level Discounts.Amount)
recorded tip         = -sum(Payments.Gratuity)
actual settlement    = sum(Payments.Amount) + sum(Payments.Gratuity)
expected settlement  = recorded net sales + sum(Tax.Amount) + ServiceCharge
settlement difference = actual settlement - expected settlement
```

`SoldPrice` already incorporates item discounts in the accepted empirical model. The pipeline does not subtract them again or multiply recorded line amounts by `Quantity`. Reconciliation uses a configured tolerance of 0.05. Gross and net tip ratios use positive denominators and retain distinct names; the recorded-tip signal uses the gross ratio so that a reduced net-sales denominator is not its only explanation.

Both signals use ordinary-comparison tickets and configurable exploratory defaults: percentile **0.99**, minimum cohort support **50**, and rule version `v1_exploratory`. These settings define descriptive historical comparisons, not official policy limits or calibrated probabilities.

| Reason | Peer population and rule | Evidence attached to each case |
| --- | --- | --- |
| `EMPLOYEE_LABEL_HIGH_RECORDED_TIP` | Compare an employee-label candidate with ordinary tickets without employee labels, grouped by brand, location, channel, weekday, and presence of a credit-card payment. If local support is insufficient, broaden across locations while retaining the other fields. Both recorded tip amount and tip/gross must strictly exceed their peer quantiles. | Observed amount and ratio, both thresholds, cohort support, selected cohort definition, percentile, minimum support, and rule version. |
| `EMPLOYEE_LABEL_HIGH_DISCOUNT_SHARE` | Compare employee-labeled discount/gross with employee-label peers sharing the same normalized label set, brand, location, channel, and weekday. The fallback broadens across locations. The historical comparison includes the candidate itself and requires the ratio to strictly exceed its peer quantile. | Observed ratio, threshold, support, local/fallback definition, explicit in-sample method, and rule version. |

Cohorts below the minimum support do not generate a statistical case. An unflagged ticket is therefore not a declaration of ordinary behavior or policy compliance. No period holdout, predictive validation, sensitivity result, or confirmed business outcome is claimed. Those are possible later analyses, not findings from the accepted execution.

Unknown discount labels/IDs and missing payment-card/category context remain visible as data; those tickets cannot silently become clean non-employee peers. Negative payments, unexpected gratuity signs, negative items, and voided activity are preserved but excluded from these ordinary comparisons. For example, a negative `GC Sold` payment needs its category context; negativity alone does not make it an invalid sale. The observed 11,467 `ManagerId = 0` events are unresolved-reference `INFO` observations, not proof of missing authorization. Historical employee records marked `Deleted` remain available as references; their current marker does not establish employment status on the transaction date.

## 4. Read-only queries for additional context

The queries below produce aggregate context from the accepted Gold outputs. They were not executed as part of writing this document and do not establish additional findings. Run them only after a successful Job update, inspect the results, and verify any statement before adding it to a presentation. They return no employee identifiers or individual-ticket details.

### Period context: candidate workload and distinct flagged tickets

```sql
WITH flagged AS (
  SELECT ticket_key,
         MAX(CASE WHEN reason_code = 'EMPLOYEE_LABEL_HIGH_RECORDED_TIP'
                  THEN 1 ELSE 0 END) AS tip_flag,
         MAX(CASE WHEN reason_code = 'EMPLOYEE_LABEL_HIGH_DISCOUNT_SHARE'
                  THEN 1 ELSE 0 END) AS discount_flag
  FROM workspace.applebees.gold_review_cases
  WHERE case_category = 'EXPLORATORY_COMPARISON'
  GROUP BY ticket_key
), population AS (
  SELECT f.*,
         ordinary_comparison_eligible AND has_employee_discount AS is_candidate,
         COALESCE(q.tip_flag, 0) AS tip_flag,
         COALESCE(q.discount_flag, 0) AS discount_flag
  FROM workspace.applebees.gold_check_features f
  LEFT JOIN flagged q ON f.ticket_key = q.ticket_key
)
SELECT CAST(DATE_TRUNC('MONTH', business_date) AS DATE) AS period_month,
       MIN(business_date) AS first_observed_day,
       MAX(business_date) AS last_observed_day,
       COUNT(*) AS source_ticket_count,
       COUNT_IF(is_candidate) AS employee_candidate_ticket_count,
       COUNT_IF(is_candidate AND (tip_flag = 1 OR discount_flag = 1))
         AS flagged_candidate_ticket_count,
       ROUND(100.0 * TRY_DIVIDE(
         COUNT_IF(is_candidate AND (tip_flag = 1 OR discount_flag = 1)),
         COUNT_IF(is_candidate)), 4) AS flagged_candidate_pct
FROM population
GROUP BY CAST(DATE_TRUNC('MONTH', business_date) AS DATE)
ORDER BY period_month;
```

April and August are partial months in this dataset. Compare the displayed observed coverage and denominators before interpreting counts as a time trend. A change in the candidate flag rate can also reflect channel, location, or label mix; it does not establish a change in improper behavior.

### Location context: review rate with its candidate denominator

```sql
WITH flagged AS (
  SELECT DISTINCT ticket_key
  FROM workspace.applebees.gold_review_cases
  WHERE case_category = 'EXPLORATORY_COMPARISON'
)
SELECT f.location_id,
       COUNT(*) AS source_ticket_count,
       COUNT_IF(f.ordinary_comparison_eligible AND f.has_employee_discount)
         AS employee_candidate_ticket_count,
       COUNT_IF(f.ordinary_comparison_eligible AND f.has_employee_discount
                AND q.ticket_key IS NOT NULL) AS flagged_candidate_ticket_count,
       ROUND(100.0 * TRY_DIVIDE(
         COUNT_IF(f.ordinary_comparison_eligible AND f.has_employee_discount
                  AND q.ticket_key IS NOT NULL),
         COUNT_IF(f.ordinary_comparison_eligible AND f.has_employee_discount)), 4)
         AS flagged_candidate_pct
FROM workspace.applebees.gold_check_features f
LEFT JOIN flagged q ON f.ticket_key = q.ticket_key
GROUP BY f.location_id
ORDER BY employee_candidate_ticket_count DESC, f.location_id;
```

This describes review workload normalized by each location's eligible employee-label candidates. It does not rank employee conduct or remove all differences in basket, channel, or label mix. A zero denominator yields a null percentage, not a measured zero rate. Show candidate counts alongside percentages; do not interpret a small population as strong evidence of a location-level pattern.

### Discount context: label-specific frequency and additive event amounts

```sql
WITH population AS (
  SELECT brand, CAST(DATE_TRUNC('MONTH', business_date) AS DATE) AS period_month,
         COUNT(*) AS eligible_ticket_denominator,
         SUM(gross_sales_amount) AS eligible_gross_sales_denominator
  FROM workspace.applebees.gold_check_features
  WHERE monetary_analysis_eligible
  GROUP BY brand, CAST(DATE_TRUNC('MONTH', business_date) AS DATE)
), labels AS (
  SELECT brand, CAST(DATE_TRUNC('MONTH', business_date) AS DATE) AS period_month,
         discount_id, discount_name, is_employee_discount,
         SUM(discounted_ticket_count) AS tickets_with_this_label,
         SUM(discount_event_count) AS discount_event_count,
         SUM(ticket_discount_amount) AS ticket_discount_amount,
         SUM(item_discount_amount) AS item_discount_amount,
         SUM(discount_amount) AS discount_amount
  FROM workspace.applebees.gold_discount_daily
  GROUP BY brand, CAST(DATE_TRUNC('MONTH', business_date) AS DATE),
           discount_id, discount_name, is_employee_discount
)
SELECT l.*,
       p.eligible_ticket_denominator, p.eligible_gross_sales_denominator,
       ROUND(100.0 * TRY_DIVIDE(l.tickets_with_this_label,
                              p.eligible_ticket_denominator), 4)
         AS label_ticket_frequency_pct,
       ROUND(100.0 * TRY_DIVIDE(l.discount_amount,
                              p.eligible_gross_sales_denominator), 4)
         AS label_amount_share_of_population_gross_pct
FROM labels l
JOIN population p ON l.brand = p.brand AND l.period_month = p.period_month
ORDER BY l.period_month, l.discount_amount DESC;
```

The daily groups are disjoint for a given label and ticket, so their ticket counts can be summed for that same label. A ticket with different labels appears in multiple label rows: do not sum their ticket frequencies. Monetary event amounts remain additive across labels and scopes. The population denominator is calculated independently from eligible tickets; it repeats across output labels and must not be summed over those rows. A label's amount share of population gross is not its nominal discount percentage or a policy limit.

## 5. Authorized review workflow and limits

1. Check Job success and `gold_pipeline_quality` first. Separate structural acceptance from quality warnings and preserve the complete source context.
2. Review `DATA_QUALITY` reasons using the ticket's file lineage, original values, and independently expanded events. Resolve or explain monetary and reference issues before drawing conclusions from them. Keep these observations distinct from exploratory reasons.
3. For an exploratory case, inspect its observed values, both denominators where applicable, reference thresholds, support, local/fallback cohort, discount labels, payment categories, signs, and void context. A threshold exceedance is a starting point for a question.
4. Ask an authorized business reviewer for the actual benefit rules and relevant operational explanation. Confirm beneficiary identity and approval meaning from appropriate business records; do not infer them from actor or approver IDs.
5. Record an explained review disposition through the team's authorized process. A benign explanation, incomplete evidence, and a concern requiring more investigation are different outcomes. The current pipeline does not automate a final determination.

The main business result is an auditable route from a manageable candidate queue back to the source transaction. It supports targeted investigation and consistent definitions. It does not quantify losses prevented, establish intent, confirm policy violations, or measure detection precision. Missing recorded cash tips and excluded or unsupported comparison populations limit what can be concluded.

## 6. Six-part Phase 2 presentation outline

The presentation structure below is a suggested speaking sequence. It maps the implementation and evidence to the delivery criteria without requiring a dashboard.

| Part | Speaking notes and evidence | Phase 2 criteria addressed |
| --- | --- | --- |
| 1. Objective and scope | “We built a governed historical data foundation and a review queue for employee-labeled discounts and recorded tips.” State the 115 days, 127 transaction locations, and absence of official policies or confirmed outcome labels. | Clear objective, business context, and communication. |
| 2. Architecture and table value | Show the implemented JSON → Silver → Gold graph. Explain ticket grain, independent event expansion, reference tables, per-ticket features, daily discount context, review cases, and the current quality snapshot. | Architecture; transformations and purpose of each table; repository organization. |
| 3. Definitions and data quality | Walk through one synthetic monetary example from the test suite. Explain why line amounts are not multiplied by quantity, why item discounts are not subtracted twice, and why the original gratuity sign is preserved. Show the 59/2,128,645 settlement exception count and its denominator. | Correct transformations; null, duplicate, invalid-value and format controls; expectations. |
| 4. Operation and engineering decisions | Show the manual Job, successful full run and repeat run, and current quality record. Explain triggered batch for a closed history, materialized views, expectations, and the documented use/evaluation of streaming and liquid clustering. Describe the serial-scan memory tradeoff without claiming unlimited capacity or guaranteed incremental refresh. | Job trigger, dependencies, failure handling and reruns; supported evidence for technical decisions. |
| 5. Verified findings and comparisons | Show the 41,475-candidate → 424-distinct-ticket review queue and the two reason counts. Explain an actual case's threshold, support and selected cohort privately, or use synthetic data in a shareable demonstration. Add location/period summaries only after running and verifying the aggregate queries above. | Accurate, meaningful findings, metrics/comparisons, evidence and business value. |
| 6. Review action and boundaries | “These are indicators for an authorized business review, not fraud decisions.” Explain the review workflow, unknown beneficiary/policy information, missing cash-tip context, and what further evidence would be needed to validate outcomes. | Interpretation, responsible conclusions and next steps. |

Useful visuals are the actual pipeline graph, a compact quality table, and the candidate-to-review-queue counts. Keep synthetic demonstrations clearly labeled and private transaction details out of public material. Publish the code, configuration, reproducible instructions, and English project documentation to the intended authorized GitHub repository when that delivery step is performed; workspace execution alone does not establish that publication.
