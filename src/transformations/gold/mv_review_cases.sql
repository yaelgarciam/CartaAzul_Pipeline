-- Exploratory, in-sample comparisons over a closed historical dataset. Quantile
-- and minimum-support settings are candidates for sensitivity analysis, not
-- employee-benefit policies or a probability of wrongdoing.
CREATE OR REFRESH MATERIALIZED VIEW `${carta_azul.table_prefix}gold_review_cases`
(
  CONSTRAINT review_key_present EXPECT (case_key IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'One ticket/reason per review case. Quality exceptions and exploratory comparisons retain provenance, support and threshold; no fraud or policy-compliance classification.'
AS
WITH ordinary AS (
  SELECT * FROM `${carta_azul.table_prefix}gold_check_features`
  WHERE ordinary_comparison_eligible
), tip_local AS (
  SELECT
    brand, location_id, revenue_center_id, business_day_of_week, has_credit_card_payment,
    COUNT(*) AS support,
    PERCENTILE_APPROX(tip_amount, ${carta_azul.review_percentile}, 10000) AS amount_threshold,
    PERCENTILE_APPROX(recorded_tip_rate_gross, ${carta_azul.review_percentile}, 10000) AS rate_threshold
  FROM ordinary WHERE NOT has_employee_discount
  GROUP BY brand, location_id, revenue_center_id, business_day_of_week, has_credit_card_payment
), tip_broad AS (
  SELECT
    brand, revenue_center_id, business_day_of_week, has_credit_card_payment,
    COUNT(*) AS support,
    PERCENTILE_APPROX(tip_amount, ${carta_azul.review_percentile}, 10000) AS amount_threshold,
    PERCENTILE_APPROX(recorded_tip_rate_gross, ${carta_azul.review_percentile}, 10000) AS rate_threshold
  FROM ordinary WHERE NOT has_employee_discount
  GROUP BY brand, revenue_center_id, business_day_of_week, has_credit_card_payment
), employee_local AS (
  SELECT
    brand, location_id, revenue_center_id, business_day_of_week, employee_discount_label_group,
    COUNT(*) AS support,
    PERCENTILE_APPROX(employee_discount_rate_gross, ${carta_azul.review_percentile}, 10000) AS rate_threshold
  FROM ordinary WHERE has_employee_discount
  GROUP BY brand, location_id, revenue_center_id, business_day_of_week, employee_discount_label_group
), employee_broad AS (
  SELECT
    brand, revenue_center_id, business_day_of_week, employee_discount_label_group,
    COUNT(*) AS support,
    PERCENTILE_APPROX(employee_discount_rate_gross, ${carta_azul.review_percentile}, 10000) AS rate_threshold
  FROM ordinary WHERE has_employee_discount
  GROUP BY brand, revenue_center_id, business_day_of_week, employee_discount_label_group
), comparisons AS (
  SELECT
    f.ticket_key, f.tip_amount, f.recorded_tip_rate_gross, f.employee_discount_rate_gross,
    CASE WHEN tl.support >= ${carta_azul.review_min_cohort} THEN tl.support ELSE tb.support END AS tip_cohort_support,
    CASE WHEN tl.support >= ${carta_azul.review_min_cohort} THEN tl.amount_threshold ELSE tb.amount_threshold END AS tip_amount_threshold,
    CASE WHEN tl.support >= ${carta_azul.review_min_cohort} THEN tl.rate_threshold ELSE tb.rate_threshold END AS tip_rate_threshold,
    CASE WHEN tl.support >= ${carta_azul.review_min_cohort}
      THEN 'non_employee_label_peers:brand/location/channel/day_of_week/card_flag'
      ELSE 'non_employee_label_peers:brand/channel/day_of_week/card_flag;broader_location_fallback'
    END AS tip_cohort_definition,
    CASE WHEN el.support >= ${carta_azul.review_min_cohort} THEN el.support ELSE eb.support END AS discount_cohort_support,
    CASE WHEN el.support >= ${carta_azul.review_min_cohort} THEN el.rate_threshold ELSE eb.rate_threshold END AS discount_rate_threshold,
    CASE WHEN el.support >= ${carta_azul.review_min_cohort}
      THEN 'employee_label_peers_including_self:brand/location/channel/day_of_week/exact_normalized_label_set'
      ELSE 'employee_label_peers_including_self:brand/channel/day_of_week/exact_normalized_label_set;broader_location_fallback'
    END AS discount_cohort_definition
  FROM ordinary f
  LEFT JOIN tip_local tl
    ON f.brand = tl.brand AND f.location_id = tl.location_id
    AND f.revenue_center_id = tl.revenue_center_id
    AND f.business_day_of_week = tl.business_day_of_week
    AND f.has_credit_card_payment = tl.has_credit_card_payment
  LEFT JOIN tip_broad tb
    ON f.brand = tb.brand AND f.revenue_center_id = tb.revenue_center_id
    AND f.business_day_of_week = tb.business_day_of_week
    AND f.has_credit_card_payment = tb.has_credit_card_payment
  LEFT JOIN employee_local el
    ON f.brand = el.brand AND f.location_id = el.location_id
    AND f.revenue_center_id = el.revenue_center_id
    AND f.business_day_of_week = el.business_day_of_week
    AND f.employee_discount_label_group = el.employee_discount_label_group
  LEFT JOIN employee_broad eb
    ON f.brand = eb.brand AND f.revenue_center_id = eb.revenue_center_id
    AND f.business_day_of_week = eb.business_day_of_week
    AND f.employee_discount_label_group = eb.employee_discount_label_group
  WHERE f.has_employee_discount
), statistical_cases AS (
  SELECT
    ticket_key,
    'EMPLOYEE_LABEL_HIGH_RECORDED_TIP' AS reason_code,
    'Recorded tip amount AND recorded tip/gross exceed exploratory historical peer quantiles. Recorded cash tips may be unavailable; employee labels do not identify the beneficiary.' AS reason_detail,
    'EXPLORATORY_COMPARISON' AS case_category,
    'recorded_tip_rate_gross_and_amount' AS metric_name,
    CAST(recorded_tip_rate_gross AS DOUBLE) AS observed_value,
    CAST(tip_rate_threshold AS DOUBLE) AS reference_threshold,
    CAST(tip_amount AS DOUBLE) AS secondary_observed_value,
    CAST(tip_amount_threshold AS DOUBLE) AS secondary_reference_threshold,
    tip_cohort_support AS cohort_support,
    tip_cohort_definition AS cohort_definition,
    CAST(${carta_azul.review_percentile} AS DOUBLE) AS reference_percentile,
    CAST(${carta_azul.review_min_cohort} AS BIGINT) AS minimum_cohort_support,
    'approx_percentile_accuracy_10000;strict_greater_than_both_thresholds;historical_exploratory' AS comparison_method
  FROM comparisons
  WHERE tip_cohort_support >= ${carta_azul.review_min_cohort}
    AND tip_amount > tip_amount_threshold AND recorded_tip_rate_gross > tip_rate_threshold
  UNION ALL
  SELECT
    ticket_key,
    'EMPLOYEE_LABEL_HIGH_DISCOUNT_SHARE',
    'Employee-labeled discount/gross exceeds an exploratory quantile for the same normalized label set. Basket eligibility and benefit policies are unknown; this is not a policy violation.',
    'EXPLORATORY_COMPARISON', 'employee_discount_rate_gross',
    CAST(employee_discount_rate_gross AS DOUBLE), CAST(discount_rate_threshold AS DOUBLE),
    CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
    discount_cohort_support, discount_cohort_definition,
    CAST(${carta_azul.review_percentile} AS DOUBLE), CAST(${carta_azul.review_min_cohort} AS BIGINT),
    'approx_percentile_accuracy_10000;strict_greater_than_threshold;historical_in_sample_includes_self'
  FROM comparisons
  WHERE discount_cohort_support >= ${carta_azul.review_min_cohort}
    AND employee_discount_rate_gross > discount_rate_threshold
), quality_cases AS (
  SELECT
    ticket_key, CONCAT('QUALITY_', issue_code) AS reason_code,
    CONCAT('Resolve data quality before interpreting behavior. ', MIN(issue_detail)) AS reason_detail,
    'DATA_QUALITY' AS case_category, 'reported_quality_issue_rows' AS metric_name,
    CAST(COUNT(*) AS DOUBLE) AS observed_value,
    CAST(0 AS DOUBLE) AS reference_threshold,
    CAST(NULL AS DOUBLE) AS secondary_observed_value,
    CAST(NULL AS DOUBLE) AS secondary_reference_threshold,
    CAST(NULL AS BIGINT) AS cohort_support,
    CAST(NULL AS STRING) AS cohort_definition,
    CAST(NULL AS DOUBLE) AS reference_percentile,
    CAST(NULL AS BIGINT) AS minimum_cohort_support,
    'silver_quality_rule;no_statistical_or_policy_inference' AS comparison_method
  FROM `${carta_azul.table_prefix}silver_quality_issues`
  WHERE ticket_key IS NOT NULL AND severity IN ('WARNING', 'ERROR')
  GROUP BY ticket_key, issue_code
), cases AS (
  SELECT * FROM statistical_cases
  UNION ALL
  SELECT * FROM quality_cases
)
SELECT
  SHA2(CONCAT(c.ticket_key, '|', c.reason_code), 256) AS case_key,
  c.*,
  f.brand, f.business_date, f.location_id, f.location_name,
  f.revenue_center_id, f.revenue_center_name, f.check_id,
  f.gross_sales_amount, f.net_sales_amount, f.employee_discount_amount,
  f.total_discount_amount, f.tip_amount, f.recorded_tip_rate_gross, f.recorded_tip_rate_net,
  f.employee_discount_labels, f.discount_names, f.payment_category_names,
  f.discount_actor_ids, f.discount_approver_ids,
  f.unresolved_discount_actor_count, f.unresolved_discount_approver_count,
  f.unresolved_payment_actor_count,
  f.location_reference_resolved, f.revenue_center_reference_resolved,
  f.monetary_analysis_eligible,
  f.reconciliation_difference, f.source_duplicate_count, f.ordinary_comparison_eligible,
  f.source_file, f.source_record_hash,
  '${carta_azul.rule_version}' AS rule_version,
  'Indicators for authorized business review; no confirmed wrongdoing or benefit-policy determination.' AS interpretation_limit
FROM cases c
INNER JOIN `${carta_azul.table_prefix}gold_check_features` f ON c.ticket_key = f.ticket_key;
