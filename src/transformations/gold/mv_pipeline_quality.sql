-- This is a deterministic snapshot of the current historical refresh, not an
-- append-only run log. Databricks pipeline/job history supplies execution status.
CREATE OR REFRESH MATERIALIZED VIEW `${carta_azul.table_prefix}gold_pipeline_quality`
(
  CONSTRAINT historical_input_not_empty EXPECT (canonical_ticket_count > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT source_coverage_complete EXPECT (
    source_file_count = expected_source_file_count
    AND first_partition_date = expected_start_date
    AND last_partition_date = expected_end_date
  ) ON VIOLATION FAIL UPDATE,
  CONSTRAINT one_feature_per_canonical_ticket EXPECT (
    feature_ticket_count = canonical_ticket_count
    AND distinct_feature_ticket_count = canonical_ticket_count
  ) ON VIOLATION FAIL UPDATE,
  CONSTRAINT payment_array_count_preserved EXPECT (payment_event_count = expected_payment_event_count) ON VIOLATION FAIL UPDATE,
  CONSTRAINT discount_array_count_preserved EXPECT (discount_event_count = expected_discount_event_count) ON VIOLATION FAIL UPDATE
)
COMMENT 'One deterministic current-snapshot quality record: fixed-historical coverage, canonical versus raw counts, array/event invariants, reference quality, empirical reconciliation and review population. Execution history lives in Lakeflow.'
AS
WITH checks AS (
  SELECT
    COUNT(*) AS canonical_ticket_count,
    COALESCE(SUM(source_duplicate_count), 0) AS source_record_count,
    COUNT_IF(source_duplicate_count > 1) AS duplicate_ticket_key_count,
    COUNT_IF(NOT key_is_valid) AS invalid_ticket_key_count,
    COUNT(DISTINCT source_file) AS source_file_count,
    COUNT(DISTINCT partition_date) AS partition_date_count,
    MIN(partition_date) AS first_partition_date,
    MAX(partition_date) AS last_partition_date,
    MIN(business_date) AS first_business_date,
    MAX(business_date) AS last_business_date,
    COUNT(DISTINCT business_date) AS business_date_count,
    COUNT(DISTINCT location_id) AS transaction_location_count,
    COUNT_IF(NOT business_date_matches_partition) AS business_partition_mismatch_count,
    COUNT_IF(NOT monetary_fields_complete) AS monetary_incomplete_ticket_count,
    COUNT_IF(item_count = 0) AS tickets_without_items,
    COUNT_IF(payment_count = 0) AS tickets_without_payments,
    COUNT_IF(negative_item_count > 0) AS tickets_with_negative_items,
    COUNT_IF(negative_payment_count > 0) AS tickets_with_negative_payments,
    COUNT_IF(positive_gratuity_count > 0) AS tickets_with_unexpected_positive_raw_gratuity,
    COUNT_IF(negative_discount_count > 0) AS tickets_with_negative_discounts,
    COUNT_IF(void_count > 0) AS tickets_with_voids,
    COUNT_IF(reconciliation_evaluable) AS reconciliation_evaluable_count,
    COUNT_IF(reconciliation_evaluable AND reconciliation_pass) AS reconciliation_pass_count,
    COUNT_IF(reconciliation_evaluable AND NOT reconciliation_pass) AS reconciliation_exception_count,
    COUNT_IF(item_discount_reconciliation_evaluable) AS item_discount_reconciliation_evaluable_count,
    COUNT_IF(item_discount_reconciliation_evaluable AND NOT item_discount_reconciliation_pass) AS item_discount_reconciliation_exception_count,
    MAX(ABS(reconciliation_difference)) AS maximum_absolute_settlement_difference,
    COALESCE(SUM(payment_count), 0) AS expected_payment_event_count,
    COALESCE(SUM(check_discount_count + item_discount_count), 0) AS expected_discount_event_count
  FROM `${carta_azul.table_prefix}silver_checks`
), features AS (
  SELECT
    COUNT(*) AS feature_ticket_count,
    COUNT(DISTINCT ticket_key) AS distinct_feature_ticket_count,
    COUNT_IF(monetary_analysis_eligible) AS monetary_analysis_eligible_ticket_count,
    COUNT_IF(ordinary_comparison_eligible) AS ordinary_comparison_eligible_ticket_count,
    COUNT_IF(ordinary_comparison_eligible AND has_employee_discount) AS exploratory_employee_ticket_candidate_count,
    COUNT_IF(has_employee_discount) AS employee_labeled_ticket_count,
    COUNT_IF(NOT location_reference_resolved) AS unresolved_location_ticket_count,
    COUNT_IF(NOT revenue_center_reference_resolved) AS unresolved_channel_ticket_count,
    COALESCE(SUM(unresolved_discount_actor_count), 0) AS unresolved_discount_actor_event_count,
    COALESCE(SUM(unresolved_discount_approver_count), 0) AS unresolved_discount_approver_event_count,
    COALESCE(SUM(unresolved_payment_actor_count), 0) AS unresolved_payment_actor_event_count
  FROM `${carta_azul.table_prefix}gold_check_features`
), payments AS (
  SELECT COUNT(*) AS payment_event_count
  FROM `${carta_azul.table_prefix}silver_payments`
), discounts AS (
  SELECT COUNT(*) AS discount_event_count
  FROM `${carta_azul.table_prefix}silver_discounts`
), issues AS (
  SELECT
    COUNT(*) AS quality_issue_row_count,
    COUNT(DISTINCT ticket_key) AS tickets_with_quality_issues,
    COUNT_IF(severity = 'ERROR') AS error_issue_row_count,
    COUNT_IF(severity = 'WARNING') AS warning_issue_row_count,
    COUNT_IF(ticket_key IS NULL) AS reference_or_dataset_issue_row_count
  FROM `${carta_azul.table_prefix}silver_quality_issues`
), issue_rules AS (
  SELECT issue_code, severity,
    COUNT(*) AS issue_rows,
    COUNT(DISTINCT ticket_key) AS affected_canonical_tickets,
    SUM(affected_source_rows) AS affected_entity_source_rows
  FROM `${carta_azul.table_prefix}silver_quality_issues`
  GROUP BY issue_code, severity
), issue_summary AS (
  SELECT SORT_ARRAY(COLLECT_LIST(NAMED_STRUCT(
    'issue_code', issue_code, 'severity', severity, 'issue_rows', issue_rows,
    'affected_canonical_tickets', affected_canonical_tickets,
    'affected_entity_source_rows', affected_entity_source_rows
  ))) AS quality_rules
  FROM issue_rules
), review AS (
  SELECT
    COUNT(*) AS review_case_count,
    COUNT(DISTINCT ticket_key) AS review_ticket_count,
    COUNT_IF(case_category = 'DATA_QUALITY') AS quality_review_case_count,
    COUNT_IF(case_category = 'EXPLORATORY_COMPARISON') AS exploratory_review_case_count,
    COUNT(DISTINCT CASE WHEN case_category = 'EXPLORATORY_COMPARISON' THEN ticket_key END) AS exploratory_review_ticket_count,
    COUNT_IF(reason_code = 'EMPLOYEE_LABEL_HIGH_RECORDED_TIP') AS high_recorded_tip_case_count,
    COUNT_IF(reason_code = 'EMPLOYEE_LABEL_HIGH_DISCOUNT_SHARE') AS high_discount_share_case_count
  FROM `${carta_azul.table_prefix}gold_review_cases`
), snapshot AS (
  SELECT
    c.*, f.*, p.*, d.*, i.*, s.*, r.*,
    CAST('${carta_azul.expected_start_date}' AS DATE) AS expected_start_date,
    CAST('${carta_azul.expected_end_date}' AS DATE) AS expected_end_date,
    CAST(${carta_azul.expected_source_files} AS BIGINT) AS expected_source_file_count,
    CAST(${carta_azul.reconciliation_tolerance} AS DECIMAL(18,4)) AS reconciliation_tolerance,
    CAST(${carta_azul.review_percentile} AS DOUBLE) AS exploratory_reference_percentile,
    CAST(${carta_azul.review_min_cohort} AS BIGINT) AS exploratory_minimum_cohort_support,
    '${carta_azul.rule_version}' AS rule_version
  FROM checks c CROSS JOIN features f CROSS JOIN payments p CROSS JOIN discounts d
  CROSS JOIN issues i CROSS JOIN issue_summary s CROSS JOIN review r
)
SELECT
  snapshot.*,
  TRY_DIVIDE(reconciliation_pass_count, reconciliation_evaluable_count) AS empirical_reconciliation_pass_rate,
  TRY_DIVIDE(exploratory_review_ticket_count, exploratory_employee_ticket_candidate_count) AS exploratory_candidate_flag_rate,
  CASE
    WHEN canonical_ticket_count = 0 OR feature_ticket_count <> canonical_ticket_count
      OR distinct_feature_ticket_count <> canonical_ticket_count
      OR payment_event_count <> expected_payment_event_count
      OR discount_event_count <> expected_discount_event_count
      OR source_file_count <> expected_source_file_count
      OR NOT (first_partition_date <=> expected_start_date)
      OR NOT (last_partition_date <=> expected_end_date)
    THEN 'STRUCTURAL_FAILURE'
    WHEN error_issue_row_count > 0 OR warning_issue_row_count > 0 THEN 'COMPLETE_WITH_ISSUES_TO_REVIEW'
    ELSE 'COMPLETE'
  END AS snapshot_quality_status,
  'Current snapshot, not a global transaction across tables. Consume after a successful pipeline update; issue counts can overlap and are not additive populations.' AS consumption_note,
  'Exploratory quantile/minimum-support settings require sensitivity review; no official discount policy supplied.' AS threshold_interpretation
FROM snapshot;
