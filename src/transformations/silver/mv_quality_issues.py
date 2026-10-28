"""Explained source-quality observations; never a list of alleged fraud cases."""
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from silver._common import RULE_VERSION, spark, table_name


def _issue_rows(frame, entity_type, entity_key, rules):
    """One row per failing rule, with no raw JSON or employee personal fields."""
    columns = set(frame.columns)
    context = [
        (F.col("ticket_key") if "ticket_key" in columns else F.lit(None).cast("string")).alias("ticket_key"),
        (F.col("business_date") if "business_date" in columns else F.lit(None).cast("date")).alias("business_date"),
        (F.col("location_id") if "location_id" in columns else F.lit(None).cast("long")).alias("location_id"),
        F.lit(entity_type).alias("entity_type"),
        F.col(entity_key).cast("string").alias("entity_key"),
        "source_file",
        (F.col("source_duplicate_count") if "source_duplicate_count" in columns
         else F.col("reference_duplicate_count") if "reference_duplicate_count" in columns
         else F.lit(1)).cast("long").alias("affected_source_rows"),
    ]
    observations = F.array(*[
        F.when(F.coalesce(condition, F.lit(False)), F.struct(
            F.lit(code).alias("issue_code"), F.lit(severity).alias("severity"),
            F.lit(detail).alias("issue_detail"),
        ))
        for code, severity, condition, detail in rules
    ])
    return (
        frame.select(*context, F.explode(F.filter(observations, lambda x: x.isNotNull())).alias("issue"))
        .select("ticket_key", "business_date", "location_id", "entity_type", "entity_key",
                "issue.*", "source_file", "affected_source_rows")
        .withColumn("rule_version", F.lit(RULE_VERSION))
    )


@dp.materialized_view(
    name=table_name("silver_quality_issues"),
    comment="One observation per entity and quality rule. INFO signs are preserved context, not rejection rules; affected rows overlap across rules.",
)
@dp.expect_all({
    "quality_code_present": "issue_code IS NOT NULL",
    "quality_severity_known": "severity IN ('INFO','WARNING','ERROR')",
    "quality_source_present": "source_file IS NOT NULL",
})
def silver_quality_issues():
    checks = spark.read.table(table_name("silver_checks"))
    locations = spark.read.table(table_name("silver_locations")).where("key_is_valid AND reference_duplicate_count = 1")
    channels = spark.read.table(table_name("silver_revenue_centers")).where("key_is_valid AND reference_duplicate_count = 1")
    checks = (
        checks.join(F.broadcast(locations.select(F.col("location_id").alias("_location_key"))),
                    F.col("location_id") == F.col("_location_key"), "left")
        .join(F.broadcast(channels.select(F.col("revenue_center_id").alias("_channel_key"))),
              F.col("revenue_center_id") == F.col("_channel_key"), "left")
    )
    frames = [_issue_rows(checks, "check", "ticket_key", [
        ("INVALID_TICKET_KEY", "ERROR", ~F.col("key_is_valid"), "Brand, partition date, location and check Id must identify the ticket."),
        ("DUPLICATE_TICKET_KEY", "ERROR", F.col("source_duplicate_count") > 1, "Repeated source key; deterministic inspection row only, excluded from analytics."),
        ("BUSINESS_DATE_MISMATCH", "ERROR", ~F.col("business_date_matches_partition"), "BusinessDate is invalid or differs from its source partition; original value is retained."),
        ("INCOMPLETE_FINANCIAL_FIELDS", "ERROR", ~F.col("monetary_fields_complete"), "A required array, amount, quantity or service-charge field is absent or null; zero-based display totals are not analytically eligible."),
        ("EMPTY_ITEMS", "WARNING", F.col("item_count") == 0, "No sold-item rows; settlement is not evaluable."),
        ("EMPTY_PAYMENTS", "WARNING", F.col("payment_count") == 0, "No payment rows; settlement is not evaluable."),
        ("SETTLEMENT_DIFFERENCE", "WARNING", F.col("reconciliation_evaluable") & ~F.col("reconciliation_pass"), "Payment plus raw gratuity differs from net sales plus tax plus service charge beyond configured tolerance."),
        ("ITEM_DISCOUNT_DIFFERENCE", "WARNING", F.col("item_discount_reconciliation_evaluable") & ~F.col("item_discount_reconciliation_pass"), "Gross minus sold item totals differs from item-discount events beyond configured tolerance."),
        ("UNRESOLVED_LOCATION", "WARNING", F.col("_location_key").isNull(), "Location Id is missing or cannot resolve uniquely against historical references."),
        ("UNRESOLVED_REVENUE_CENTER", "WARNING", F.col("_channel_key").isNull(), "Revenue-center Id is missing or cannot resolve uniquely against references."),
        ("POSITIVE_RAW_GRATUITY", "WARNING", F.col("positive_gratuity_count") > 0, "Positive raw gratuity differs from the observed negative-tip sign convention; preserve and review."),
        ("NEGATIVE_PAYMENT_CONTEXT", "INFO", F.col("negative_payment_count") > 0, "Negative payment amount retained; category context is required, including GC Sold."),
        ("NEGATIVE_ITEM_CONTEXT", "INFO", F.col("negative_item_count") > 0, "Negative item quantity or price retained for contextual review."),
        ("NEGATIVE_DISCOUNT_CONTEXT", "INFO", F.col("negative_discount_count") > 0, "Negative discount retained for contextual review."),
    ])]

    payments = spark.read.table(table_name("silver_payments"))
    frames.append(_issue_rows(payments, "payment", "payment_event_key", [
        ("INCOMPLETE_PAYMENT", "ERROR", ~F.col("event_is_complete"), "Payment Amount or Gratuity is missing; raw event remains available."),
        ("UNRESOLVED_PAYMENT_ACTOR", "WARNING", ~F.col("actor_reference_resolved"), "Payment employee Id cannot resolve uniquely to employees.Id; it is not a proven beneficiary."),
        ("MISSING_PAYMENT_CATEGORY", "WARNING", F.col("payment_category_name").isNull() | (F.length(F.trim("payment_category_name")) == 0), "Payment category name is missing or blank; sign interpretation may be incomplete."),
        ("MISSING_PAYMENT_CARD_FLAG", "WARNING", F.col("is_credit_card").isNull(), "Payment credit-card indicator is unknown; do not interpret it as a non-card payment."),
    ]))
    discounts = spark.read.table(table_name("silver_discounts"))
    frames.append(_issue_rows(discounts, "discount", "discount_event_key", [
        ("INCOMPLETE_DISCOUNT", "ERROR", ~F.col("event_is_complete"), "Discount amount, Id or label is missing; raw event remains available."),
        ("UNRESOLVED_DISCOUNT_ACTOR", "WARNING", ~F.col("actor_reference_resolved"), "Discount actor Id cannot resolve uniquely to employees.Id; it is not a proven beneficiary."),
        ("APPROVER_ZERO", "INFO", F.col("approver_employee_id") == 0, "ManagerId zero is an unresolved reference, not evidence of missing authorization."),
        ("UNRESOLVED_DISCOUNT_APPROVER", "WARNING", ~F.col("approver_reference_resolved") & (F.col("approver_employee_id").isNull() | (F.col("approver_employee_id") != 0)), "Discount approver Id cannot resolve uniquely to employees.Id."),
    ]))
    for dataset, entity, key in [
        ("silver_locations", "location_reference", "location_id"),
        ("silver_revenue_centers", "revenue_center_reference", "revenue_center_id"),
        ("silver_employees", "employee_reference", "employee_id"),
    ]:
        refs = spark.read.table(table_name(dataset))
        if dataset == "silver_employees":
            refs = refs.withColumn("location_id", F.col("employee_location_id"))
        frames.append(_issue_rows(refs, entity, key, [
            ("MISSING_REFERENCE_KEY", "ERROR", ~F.col("key_is_valid"), "Reference key is null; the reference cannot resolve an event."),
            ("DUPLICATE_REFERENCE_KEY", "ERROR", F.col("reference_duplicate_count") > 1, "Reference key occurs more than once; ambiguous references do not resolve events."),
        ]))
    return reduce(lambda left, right: left.unionByName(right), frames)
