"""Ticket and item discounts are independent events, never cross-joined."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from silver._common import employee_reference, spark, table_name


@dp.materialized_view(
    name=table_name("silver_discounts"),
    comment="One row per discount scope and array position. Employee labels are exploratory; actor and approver are not proven beneficiaries.",
)
@dp.expect_all({
    "discount_amount_present": "amount IS NOT NULL",
    "discount_name_present": "discount_name IS NOT NULL AND length(trim(discount_name)) > 0",
    "discount_id_present": "discount_id IS NOT NULL",
    "discount_actor_resolves": "actor_reference_resolved",
    "discount_approver_resolves": "approver_reference_resolved",
})
def silver_discounts():
    source = spark.read.table(table_name("silver_checks"))
    keys = ["ticket_key", "brand", "business_date", "partition_date", "location_id",
            "check_id", "revenue_center_id", "source_file", "source_duplicate_count"]
    ticket = source.select(
        *keys, F.col("analytic_eligible").alias("check_analytic_eligible"),
        F.posexplode("discounts").alias("discount_position", "d"),
    ).select(
        *keys, "check_analytic_eligible", F.lit("ticket").alias("discount_scope"),
        F.lit(-1).cast("int").alias("item_position"),
        F.lit(None).cast("long").alias("item_sequence"),
        F.lit(None).cast("long").alias("item_id"), "discount_position", "d",
    )
    items = source.select(
        *keys, F.col("analytic_eligible").alias("check_analytic_eligible"),
        F.posexplode("items_sold").alias("item_position", "i"),
    ).select(
        *keys, "check_analytic_eligible", "item_position",
        F.col("i.Sequence").alias("item_sequence"), F.col("i.ItemId").alias("item_id"),
        F.posexplode("i.Discounts").alias("discount_position", "d"),
    ).withColumn("discount_scope", F.lit("item"))
    frame = ticket.unionByName(items).select(
        *keys, "check_analytic_eligible", "discount_scope", "item_position",
        "item_sequence", "item_id", "discount_position",
        F.col("d.DiscountId").alias("discount_id"),
        F.col("d.DiscountName").alias("discount_name"),
        F.col("d.Amount").alias("amount"),
        F.col("d.EmployeeId").alias("actor_employee_id"),
        F.col("d.ManagerId").alias("approver_employee_id"),
    ).withColumn(
        "discount_event_key", F.sha2(F.to_json(F.struct(
            "ticket_key", "discount_scope", "item_position", "discount_position"
        )), 256)
    ).withColumn(
        "is_employee_discount",
        F.coalesce(F.upper(F.trim("discount_name")).rlike(r"^EMP(?:\s|$)"), F.lit(False)),
    )
    for role, field in [("actor", "actor_employee_id"), ("approver", "approver_employee_id")]:
        frame = frame.join(
            F.broadcast(employee_reference(role)), F.col(field) == F.col(f"_{role}_id"), "left"
        ).withColumn(f"{role}_reference_resolved", F.col(f"_{role}_id").isNotNull()).drop(f"_{role}_id")
    return frame.withColumn(
        "event_is_complete", F.col("amount").isNotNull() & F.col("discount_id").isNotNull()
        & F.col("discount_name").isNotNull() & (F.length(F.trim("discount_name")) > 0),
    )
