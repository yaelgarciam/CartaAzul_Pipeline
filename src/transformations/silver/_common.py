"""Shared, side-effect-free expressions for the declarative Carta Azul pipeline."""
from datetime import date, timedelta
from decimal import Decimal
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("Carta Azul must run inside a Databricks Spark session.")

BASE_PATH = spark.conf.get(
    "carta_azul.source_root", "/Volumes/workspace/applebees/raw_files/data/Dataset"
).rstrip("/")
TABLE_PREFIX = spark.conf.get("carta_azul.table_prefix", "")
if not re.fullmatch(r"[a-z_][a-z0-9_]*|", TABLE_PREFIX):
    raise ValueError("carta_azul.table_prefix must contain lowercase letters, digits and underscores.")

RULE_VERSION = spark.conf.get("carta_azul.rule_version", "exploratory_v1")
TOLERANCE = Decimal(spark.conf.get("carta_azul.reconciliation_tolerance", "0.05"))
if not TOLERANCE.is_finite() or TOLERANCE < 0:
    raise ValueError("reconciliation_tolerance must be finite and nonnegative.")


def table_name(name):
    """Unqualified names use the catalog and schema configured on the pipeline."""
    return TABLE_PREFIX + name


def checks_paths():
    """Read only the configured inclusive dates; no listing or Spark action needed."""
    start = date.fromisoformat(spark.conf.get("carta_azul.start_date", "2026-04-28"))
    end = date.fromisoformat(spark.conf.get("carta_azul.end_date", "2026-08-20"))
    brand = spark.conf.get("carta_azul.brand", "rmh")
    if end < start:
        raise ValueError("carta_azul.end_date must be on or after start_date.")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", brand):
        raise ValueError("carta_azul.brand must be a single partition value.")
    return [
        f"{BASE_PATH}/facts/checks/brand={brand}/dt={start + timedelta(days=i)}/checks.json"
        for i in range((end - start).days + 1)
    ]


def read_json(paths, schema):
    """Explicit schema, complete JSON arrays, strict structural parsing, provenance."""
    return (
        spark.read.schema(schema)
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .option("recursiveFileLookup", "true")
        .json(paths)
        .select("*", F.col("_metadata.file_path").alias("source_file"))
    )


def money(value):
    return value.cast("decimal(18,4)")


def zero_money():
    return F.lit(Decimal("0.0000")).cast("decimal(18,4)")


def array_count(name):
    return F.coalesce(F.array_size(F.col(name)), F.lit(0))


def array_money_total(array_name, field_name):
    # Keep the accumulator type unchanged: decimal addition otherwise widens it.
    return F.expr(
        f"coalesce(aggregate({array_name}, cast(0 as decimal(18,4)), "
        f"(s,x) -> cast(s + coalesce(x.{field_name}, cast(0 as decimal(18,4))) "
        f"as decimal(18,4))), cast(0 as decimal(18,4)))"
    )


def canonical_reference(frame, key_column, value_columns):
    """Keep deterministic context but mark every repeated key as ambiguous."""
    frame = frame.withColumn(
        "reference_record_hash", F.sha2(F.to_json(F.struct(*value_columns)), 256)
    )
    window = Window.partitionBy(key_column)
    ordered = window.orderBy("reference_record_hash", "source_file")
    return (
        frame.withColumn("reference_duplicate_count", F.count("*").over(window))
        .withColumn("_canonical_rank", F.row_number().over(ordered))
        .where(F.col("_canonical_rank") == 1)
        .drop("_canonical_rank", "_corrupt_record")
        .withColumn("key_is_valid", F.col(key_column).isNotNull())
    )


def employee_reference(role):
    """Only unique nonzero Id references resolve; Deleted is historical context."""
    return (
        spark.read.table(table_name("silver_employees"))
        .where("key_is_valid AND reference_duplicate_count = 1 AND employee_id <> 0")
        .select(
            F.col("employee_id").alias(f"_{role}_id"),
            F.col("employee_location_id").alias(f"{role}_reference_location_id"),
            F.col("deleted").alias(f"{role}_reference_deleted"),
        )
    )
