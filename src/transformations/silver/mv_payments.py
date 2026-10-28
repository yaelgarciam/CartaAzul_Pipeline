"""Payment events expanded independently, with original amount and tip signs."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from silver._common import employee_reference, money, spark, table_name


@dp.materialized_view(
    name=table_name("silver_payments"),
    comment="One row per payment position. Negative GC Sold payments are preserved; normalized tip is minus raw Gratuity.",
)
@dp.expect_all({
    "payment_amount_present": "amount IS NOT NULL",
    "gratuity_present": "gratuity_raw_amount IS NOT NULL",
    "payment_actor_resolves": "actor_reference_resolved",
    "observed_gratuity_sign": "NOT has_positive_gratuity",
})
def silver_payments():
    source = spark.read.table(table_name("silver_checks"))
    frame = source.select(
        "ticket_key", "brand", "business_date", "partition_date", "location_id",
        "check_id", "revenue_center_id", "source_file", "source_duplicate_count",
        F.col("analytic_eligible").alias("check_analytic_eligible"),
        F.posexplode("payments").alias("payment_position", "p"),
    ).select(
        "ticket_key", "brand", "business_date", "partition_date", "location_id",
        "check_id", "revenue_center_id", "source_file", "source_duplicate_count",
        "check_analytic_eligible", "payment_position",
        F.col("p.Amount").alias("amount"),
        F.col("p.Gratuity").alias("gratuity_raw_amount"),
        money(-F.col("p.Gratuity")).alias("tip_amount"),
        F.col("p.PaymentCategoryId").alias("payment_category_id"),
        F.col("p.PayCatName").alias("payment_category_name"),
        F.col("p.PayCatNo").alias("payment_category_number"),
        F.col("p.EmployeeId").alias("actor_employee_id"),
        F.col("p.PosSourceId").alias("pos_source_id"),
        F.col("p.IsCashDebit").alias("is_cash_debit"),
        F.col("p.IsCreditCard").alias("is_credit_card"),
        F.col("p.IsGiftCard").alias("is_gift_card"),
        F.col("p.IsGiftCert").alias("is_gift_cert"),
        F.col("p.IsNonPOS").alias("is_non_pos"),
        F.col("p.IsPettyCash").alias("is_petty_cash"),
    )
    frame = (
        frame.withColumn(
            "payment_event_key", F.sha2(F.to_json(F.struct("ticket_key", "payment_position")), 256)
        )
        .withColumn("has_negative_amount", F.coalesce(F.col("amount") < 0, F.lit(False)))
        .withColumn("has_positive_gratuity", F.coalesce(F.col("gratuity_raw_amount") > 0, F.lit(False)))
    )
    frame = frame.join(
        F.broadcast(employee_reference("actor")), F.col("actor_employee_id") == F.col("_actor_id"), "left"
    ).withColumn("actor_reference_resolved", F.col("_actor_id").isNotNull()).drop("_actor_id")
    return frame.withColumn(
        "event_is_complete", F.col("amount").isNotNull() & F.col("gratuity_raw_amount").isNotNull()
    )
