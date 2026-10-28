-- Event amounts remain additive across discount types; population denominators do
-- not. Never sum the repeated daily denominator columns over discount types.
CREATE OR REFRESH MATERIALIZED VIEW `${carta_azul.table_prefix}gold_discount_daily`
COMMENT 'Daily discount event amounts by descriptive label; distinct-ticket frequency divided by all monetarily eligible tickets in the same day/location/channel. Denominators repeat by label and are not additive across labels.'
AS
WITH population AS (
  SELECT
    brand, business_date, location_id, revenue_center_id,
    COUNT(*) AS eligible_ticket_denominator,
    SUM(gross_sales_amount) AS eligible_gross_sales_denominator,
    SUM(net_sales_amount) AS eligible_net_sales_denominator
  FROM `${carta_azul.table_prefix}gold_check_features`
  WHERE monetary_analysis_eligible
  GROUP BY brand, business_date, location_id, revenue_center_id
), discount_totals AS (
  SELECT
    f.brand, f.business_date, f.location_id, f.revenue_center_id,
    d.discount_id, d.discount_name, d.is_employee_discount,
    COUNT(*) AS discount_event_count,
    COUNT(DISTINCT f.ticket_key) AS discounted_ticket_count,
    COUNT(DISTINCT CASE WHEN d.discount_scope = 'ticket' THEN f.ticket_key END) AS ticket_scope_ticket_count,
    COUNT(DISTINCT CASE WHEN d.discount_scope = 'item' THEN f.ticket_key END) AS item_scope_ticket_count,
    COUNT_IF(d.discount_scope = 'ticket') AS ticket_discount_event_count,
    COUNT_IF(d.discount_scope = 'item') AS item_discount_event_count,
    SUM(d.amount) AS discount_amount,
    SUM(CASE WHEN d.discount_scope = 'ticket' THEN d.amount ELSE CAST(0 AS DECIMAL(18,4)) END) AS ticket_discount_amount,
    SUM(CASE WHEN d.discount_scope = 'item' THEN d.amount ELSE CAST(0 AS DECIMAL(18,4)) END) AS item_discount_amount,
    COUNT_IF(NOT d.actor_reference_resolved) AS unresolved_actor_event_count,
    COUNT_IF(NOT d.approver_reference_resolved) AS unresolved_approver_event_count
  FROM `${carta_azul.table_prefix}silver_discounts` d
  INNER JOIN `${carta_azul.table_prefix}gold_check_features` f ON d.ticket_key = f.ticket_key
  WHERE f.monetary_analysis_eligible
  GROUP BY f.brand, f.business_date, f.location_id, f.revenue_center_id,
    d.discount_id, d.discount_name, d.is_employee_discount
)
SELECT
  d.*,
  p.eligible_ticket_denominator, p.eligible_gross_sales_denominator,
  p.eligible_net_sales_denominator,
  TRY_DIVIDE(d.discounted_ticket_count, p.eligible_ticket_denominator) AS discounted_ticket_frequency,
  TRY_DIVIDE(d.discount_amount, p.eligible_gross_sales_denominator) AS discount_amount_share_of_population_gross,
  'all_monetarily_eligible_tickets_in_same_brand_day_location_channel' AS denominator_definition,
  '${carta_azul.rule_version}' AS rule_version
FROM discount_totals d
INNER JOIN population p
  ON d.brand <=> p.brand AND d.business_date <=> p.business_date
  AND d.location_id <=> p.location_id AND d.revenue_center_id <=> p.revenue_center_id;
