"""One canonical row per source business key, with array-derived exact money totals.

Repeated keys are retained once for inspection, flagged with their raw row count,
and excluded from analytic eligibility. No conflicting row is silently accepted.
The original files remain the authoritative record of all source rows.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from silver._common import (
    TOLERANCE, array_count, array_money_total, checks_paths, money,
    read_json, table_name, zero_money,
)
from silver._schemas import CHECKS_SCHEMA


def _count_where(array_name, predicate):
    return F.coalesce(F.array_size(F.filter(F.col(array_name), predicate)), F.lit(0))


@dp.materialized_view(
    name=table_name("silver_checks"),
    comment="Historical check base. Amounts are line totals, never multiplied by Quantity; nested arrays remain separate.",
)
@dp.expect_all({
    "check_key_present": "key_is_valid",
    "check_key_unique": "source_duplicate_count = 1",
    "business_date_matches_partition": "business_date_matches_partition",
    "financial_fields_present": "monetary_fields_complete",
    "settlement_reconciles_when_evaluable": "NOT reconciliation_evaluable OR reconciliation_pass",
    "item_discounts_reconcile_when_evaluable": "NOT item_discount_reconciliation_evaluable OR item_discount_reconciliation_pass",
    "observed_gratuity_sign": "positive_gratuity_count = 0",
})
def silver_checks():
    # These daily JSON arrays are unsplittable. Free Edition's Photon workers
    # cannot hold several SimdJsonReader buffers concurrently. A narrow coalesce
    # serializes the source scan; repartition would shuffle only after parsing.
    raw = read_json(checks_paths(), CHECKS_SCHEMA).coalesce(1)
    # No current_timestamp: a historical refresh produces identical row contents.
    source_fields = [f.name for f in CHECKS_SCHEMA.fields if f.name != "_corrupt_record"]
    base = raw.select(
        F.regexp_extract("source_file", r"/brand=([^/]+)/", 1).alias("brand"),
        F.expr("try_cast(regexp_extract(source_file, '/dt=([^/]+)/', 1) as date)").alias("partition_date"),
        F.col("Id").alias("check_id"),
        F.col("LocationId").alias("location_id"),
        F.col("RevenueCenterId").alias("revenue_center_id"),
        F.col("BusinessDate").alias("business_date_raw"),
        F.expr("try_cast(BusinessDate as date)").alias("business_date"),
        F.col("OpenTime").alias("open_time_raw"),
        F.col("CloseTime").alias("close_time_raw"),
        F.col("TrafficCount").alias("traffic_count"),
        F.col("ServiceCharge").alias("service_charge_amount"),
        F.col("ItemsSold").alias("items_sold"),
        F.col("Payments").alias("payments"),
        F.col("Discounts").alias("discounts"),
        F.col("Tax").alias("taxes"),
        F.col("Voids").alias("voids"),
        F.sha2(F.to_json(F.struct(*source_fields)), 256).alias("source_record_hash"),
        "source_file",
    )
    base = base.withColumn(
        "key_is_valid",
        (F.length("brand") > 0) & F.col("partition_date").isNotNull()
        & F.col("location_id").isNotNull() & F.col("check_id").isNotNull(),
    ).withColumn(
        "business_date_matches_partition",
        F.coalesce(F.col("business_date") == F.col("partition_date"), F.lit(False)),
    )
    # JSON struct hashing avoids ambiguous concatenations. Bad keys receive a
    # provenance-based identity, never the same hash as a valid business key.
    base = base.withColumn(
        "ticket_key",
        F.when(F.col("key_is_valid"), F.sha2(F.to_json(F.struct(
            "brand", "partition_date", "location_id", "check_id"
        )), 256)).otherwise(F.sha2(F.to_json(F.struct(
            F.lit("invalid_key").alias("kind"), "source_file", "source_record_hash"
        )), 256)),
    )
    key_window = Window.partitionBy("ticket_key")
    base = (
        base.withColumn("source_duplicate_count", F.count("*").over(key_window))
        .withColumn("_canonical_rank", F.row_number().over(
            key_window.orderBy("source_record_hash", "source_file")
        ))
        .where(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank")
    )
    valid_items = F.forall("items_sold", lambda x:
        x.isNotNull() & x.GrossPrice.isNotNull() & x.SoldPrice.isNotNull()
        & x.Quantity.isNotNull() & x.Discounts.isNotNull()
        & F.forall(x.Discounts, lambda d: d.isNotNull() & d.Amount.isNotNull()))
    valid_payments = F.forall("payments", lambda x:
        x.isNotNull() & x.Amount.isNotNull() & x.Gratuity.isNotNull())
    valid_discounts = F.forall("discounts", lambda x: x.isNotNull() & x.Amount.isNotNull())
    valid_taxes = F.forall("taxes", lambda x: x.isNotNull() & x.Amount.isNotNull())
    base = base.withColumn(
        "monetary_fields_complete",
        F.coalesce(valid_items & valid_payments & valid_discounts & valid_taxes
                   & F.col("service_charge_amount").isNotNull(), F.lit(False)),
    )
    base = base.select(
        "*",
        array_count("items_sold").alias("item_count"),
        array_count("payments").alias("payment_count"),
        array_count("discounts").alias("check_discount_count"),
        array_count("voids").alias("void_count"),
        F.expr("coalesce(aggregate(items_sold, 0, (n,i) -> n + coalesce(array_size(i.Discounts),0)),0)").alias("item_discount_count"),
        array_money_total("items_sold", "GrossPrice").alias("gross_sales_amount"),
        array_money_total("items_sold", "SoldPrice").alias("sold_sales_amount"),
        array_money_total("discounts", "Amount").alias("check_discount_amount"),
        F.expr("""coalesce(aggregate(items_sold, cast(0 as decimal(18,4)),
            (s,i) -> cast(s + coalesce(aggregate(i.Discounts, cast(0 as decimal(18,4)),
            (t,d) -> cast(t + coalesce(d.Amount,cast(0 as decimal(18,4))) as decimal(18,4))),
            cast(0 as decimal(18,4))) as decimal(18,4))), cast(0 as decimal(18,4)))""").alias("item_discount_amount"),
        array_money_total("taxes", "Amount").alias("tax_amount"),
        array_money_total("payments", "Amount").alias("payment_amount"),
        array_money_total("payments", "Gratuity").alias("gratuity_raw_amount"),
        _count_where("payments", lambda p: p.Amount < 0).alias("negative_payment_count"),
        _count_where("payments", lambda p: p.Gratuity > 0).alias("positive_gratuity_count"),
        _count_where("items_sold", lambda i:
            (i.Quantity < 0) | (i.GrossPrice < 0) | (i.SoldPrice < 0)).alias("negative_item_count"),
        (_count_where("discounts", lambda d: d.Amount < 0)
         + F.expr("coalesce(aggregate(items_sold, 0, (n,i) -> n + coalesce(array_size(filter(i.Discounts, d -> d.Amount < 0)),0)),0)")
        ).alias("negative_discount_count"),
    )
    base = (
        base.withColumn("tip_amount", money(-F.col("gratuity_raw_amount")))
        .withColumn("net_sales_amount", money(F.col("sold_sales_amount") - F.col("check_discount_amount")))
        .withColumn("expected_settlement_amount", money(
            F.col("net_sales_amount") + F.col("tax_amount") + F.coalesce("service_charge_amount", zero_money())
        ))
        .withColumn("actual_settlement_amount", money(F.col("payment_amount") + F.col("gratuity_raw_amount")))
        .withColumn("reconciliation_difference", money(F.col("actual_settlement_amount") - F.col("expected_settlement_amount")))
        .withColumn("reconciliation_evaluable", F.col("monetary_fields_complete") & (F.col("item_count") > 0) & (F.col("payment_count") > 0))
        .withColumn("reconciliation_pass", F.when(F.col("reconciliation_evaluable"), F.abs("reconciliation_difference") <= F.lit(TOLERANCE)))
        .withColumn("item_discount_reconciliation_difference", money(
            F.col("gross_sales_amount") - F.col("sold_sales_amount") - F.col("item_discount_amount")
        ))
        .withColumn("item_discount_reconciliation_evaluable", F.col("monetary_fields_complete") & (F.col("item_count") > 0))
        .withColumn("item_discount_reconciliation_pass", F.when(
            F.col("item_discount_reconciliation_evaluable"),
            F.abs("item_discount_reconciliation_difference") <= F.lit(TOLERANCE)
        ))
        .withColumn("analytic_eligible", F.coalesce(
            F.col("key_is_valid") & F.col("business_date_matches_partition")
            & (F.col("source_duplicate_count") == 1) & F.col("monetary_fields_complete")
            & F.col("reconciliation_evaluable") & F.col("reconciliation_pass")
            & F.col("item_discount_reconciliation_pass"), F.lit(False)
        ))
    )
    return base
