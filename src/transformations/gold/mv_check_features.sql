-- One canonical source ticket per row. Nested collections are aggregated separately in
-- Silver; joining their children directly would multiply monetary values.
CREATE OR REFRESH MATERIALIZED VIEW `${carta_azul.table_prefix}gold_check_features`
(
  CONSTRAINT ticket_key_present EXPECT (ticket_key IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT source_record_count_valid EXPECT (source_duplicate_count >= 1) ON VIOLATION FAIL UPDATE
)
COMMENT 'One row per canonical check; empirical monetary definitions, recorded tips, reference quality and exploratory comparison eligibility. Actor identifiers are not beneficiary identifiers.'
AS
WITH discount_context AS (
  SELECT
    ticket_key,
    COUNT(*) AS discount_event_count,
    COUNT_IF(is_employee_discount) AS employee_discount_event_count,
    COUNT_IF(amount IS NULL) AS discount_amount_missing_count,
    COUNT_IF(NOT event_is_complete) AS incomplete_discount_event_count,
    SUM(CASE WHEN is_employee_discount THEN amount ELSE CAST(0 AS DECIMAL(18,4)) END) AS employee_discount_amount,
    COUNT_IF(NOT actor_reference_resolved) AS unresolved_discount_actor_count,
    COUNT_IF(NOT approver_reference_resolved) AS unresolved_discount_approver_count,
    SORT_ARRAY(COLLECT_SET(discount_name)) AS discount_names,
    SORT_ARRAY(COLLECT_SET(CASE WHEN is_employee_discount THEN
      REGEXP_REPLACE(UPPER(TRIM(discount_name)), '\\s+', ' ') END)) AS employee_discount_labels,
    SORT_ARRAY(COLLECT_SET(actor_employee_id)) AS discount_actor_ids,
    SORT_ARRAY(COLLECT_SET(approver_employee_id)) AS discount_approver_ids
  FROM `${carta_azul.table_prefix}silver_discounts`
  GROUP BY ticket_key
), payment_context AS (
  SELECT
    ticket_key,
    COUNT(*) AS payment_event_count,
    COUNT_IF(NOT actor_reference_resolved) AS unresolved_payment_actor_count,
    COUNT_IF(is_credit_card IS NULL) AS payment_credit_card_flag_missing_count,
    COUNT_IF(payment_category_name IS NULL OR LENGTH(TRIM(payment_category_name)) = 0) AS payment_category_missing_count,
    COUNT_IF(is_credit_card) > 0 AS has_credit_card_payment,
    COUNT_IF(is_cash_debit) > 0 AS has_cash_debit_payment,
    SORT_ARRAY(COLLECT_SET(payment_category_name)) AS payment_category_names
  FROM `${carta_azul.table_prefix}silver_payments`
  GROUP BY ticket_key
), features AS (
  SELECT
    c.ticket_key, c.brand, c.business_date, c.partition_date, c.check_id,
    c.location_id, l.location_name, l.time_zone_name,
    c.revenue_center_id, r.revenue_center_name,
    DAYOFWEEK(c.business_date) AS business_day_of_week,
    c.business_date_raw, c.open_time_raw, c.close_time_raw,
    c.source_file, c.source_record_hash, c.source_duplicate_count,
    c.key_is_valid, c.business_date_matches_partition, c.monetary_fields_complete,
    COALESCE(l.reference_duplicate_count = 1 AND l.key_is_valid, FALSE) AS location_reference_resolved,
    COALESCE(r.reference_duplicate_count = 1 AND r.key_is_valid, FALSE) AS revenue_center_reference_resolved,
    c.item_count, c.payment_count, c.check_discount_count, c.item_discount_count,
    c.void_count, c.negative_item_count, c.negative_discount_count,
    c.negative_payment_count, c.positive_gratuity_count,
    c.gross_sales_amount, c.sold_sales_amount, c.check_discount_amount,
    c.item_discount_amount, c.check_discount_amount + c.item_discount_amount AS total_discount_amount,
    c.net_sales_amount, c.tax_amount, c.service_charge_amount,
    c.payment_amount, c.gratuity_raw_amount, c.tip_amount,
    c.expected_settlement_amount, c.actual_settlement_amount,
    c.reconciliation_difference, c.reconciliation_evaluable, c.reconciliation_pass,
    c.item_discount_reconciliation_evaluable,
    c.item_discount_reconciliation_difference, c.item_discount_reconciliation_pass,
    c.analytic_eligible AS monetary_analysis_eligible,
    COALESCE(d.discount_event_count, 0) AS discount_event_count,
    COALESCE(d.employee_discount_event_count, 0) AS employee_discount_event_count,
    COALESCE(d.discount_amount_missing_count, 0) AS discount_amount_missing_count,
    COALESCE(d.incomplete_discount_event_count, 0) AS incomplete_discount_event_count,
    COALESCE(d.incomplete_discount_event_count, 0) = 0 AS discount_classification_complete,
    COALESCE(d.employee_discount_event_count, 0) > 0 AS has_employee_discount,
    -- Unknown labels or IDs prevent a complete employee-discount classification.
    -- They must not silently produce a zero employee-discount amount or enter
    -- the non-employee peer population. Monetary source totals remain intact.
    CASE WHEN COALESCE(d.incomplete_discount_event_count, 0) > 0 THEN CAST(NULL AS DECIMAL(18,4))
      WHEN COALESCE(d.employee_discount_event_count, 0) = 0 THEN CAST(0 AS DECIMAL(18,4))
      WHEN d.discount_amount_missing_count = 0 THEN d.employee_discount_amount END AS employee_discount_amount,
    COALESCE(d.unresolved_discount_actor_count, 0) AS unresolved_discount_actor_count,
    COALESCE(d.unresolved_discount_approver_count, 0) AS unresolved_discount_approver_count,
    COALESCE(p.unresolved_payment_actor_count, 0) AS unresolved_payment_actor_count,
    COALESCE(p.payment_event_count, 0) AS payment_event_count,
    COALESCE(p.payment_credit_card_flag_missing_count, 0) AS payment_credit_card_flag_missing_count,
    COALESCE(p.payment_category_missing_count, 0) AS payment_category_missing_count,
    COALESCE(p.has_credit_card_payment, FALSE) AS has_credit_card_payment,
    COALESCE(p.has_cash_debit_payment, FALSE) AS has_cash_debit_payment,
    COALESCE(d.discount_names, ARRAY()) AS discount_names,
    COALESCE(d.employee_discount_labels, ARRAY()) AS employee_discount_labels,
    TO_JSON(COALESCE(d.employee_discount_labels, ARRAY())) AS employee_discount_label_group,
    COALESCE(d.discount_actor_ids, ARRAY()) AS discount_actor_ids,
    COALESCE(d.discount_approver_ids, ARRAY()) AS discount_approver_ids,
    COALESCE(p.payment_category_names, ARRAY()) AS payment_category_names
  FROM `${carta_azul.table_prefix}silver_checks` c
  LEFT JOIN discount_context d ON c.ticket_key = d.ticket_key
  LEFT JOIN payment_context p ON c.ticket_key = p.ticket_key
  LEFT JOIN `${carta_azul.table_prefix}silver_locations` l ON c.location_id = l.location_id
  LEFT JOIN `${carta_azul.table_prefix}silver_revenue_centers` r ON c.revenue_center_id = r.revenue_center_id
)
SELECT
  features.*,
  -- These are shares of the recorded gross/net sales before tax, not policy rates.
  CASE WHEN monetary_analysis_eligible AND gross_sales_amount > 0
    THEN TRY_DIVIDE(tip_amount, gross_sales_amount) END AS recorded_tip_rate_gross,
  CASE WHEN monetary_analysis_eligible AND net_sales_amount > 0
    THEN TRY_DIVIDE(tip_amount, net_sales_amount) END AS recorded_tip_rate_net,
  CASE WHEN monetary_analysis_eligible AND gross_sales_amount > 0
    THEN TRY_DIVIDE(total_discount_amount, gross_sales_amount) END AS total_discount_rate_gross,
  CASE WHEN monetary_analysis_eligible AND gross_sales_amount > 0
    THEN TRY_DIVIDE(employee_discount_amount, gross_sales_amount) END AS employee_discount_rate_gross,
  COALESCE(
    monetary_analysis_eligible AND source_duplicate_count = 1
    AND location_reference_resolved AND revenue_center_reference_resolved
    AND item_discount_reconciliation_evaluable AND item_discount_reconciliation_pass
    AND discount_amount_missing_count = 0
    AND incomplete_discount_event_count = 0
    AND payment_credit_card_flag_missing_count = 0 AND payment_category_missing_count = 0
    AND gross_sales_amount > 0 AND net_sales_amount > 0
    AND negative_item_count = 0 AND negative_discount_count = 0
    AND negative_payment_count = 0 AND positive_gratuity_count = 0
    AND void_count = 0 AND tip_amount >= 0,
    FALSE
  ) AS ordinary_comparison_eligible,
  '${carta_azul.rule_version}' AS rule_version,
  'sold_minus_ticket_discount; tip=-raw_gratuity; no_quantity_multiplier; v1_empirical' AS monetary_definition_version
FROM features;
