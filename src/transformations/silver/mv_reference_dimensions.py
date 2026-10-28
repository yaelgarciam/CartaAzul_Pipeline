"""Historical reference tables, without employee personal or payroll attributes."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from silver._common import BASE_PATH, canonical_reference, read_json, table_name
from silver._schemas import EMPLOYEES_SCHEMA, LOCATIONS_SCHEMA, REVENUE_CENTERS_SCHEMA


@dp.materialized_view(
    name=table_name("silver_locations"),
    comment="Historical location references; duplicate keys remain flagged and unresolved.",
)
@dp.expect_all({"location_key_present": "key_is_valid", "location_key_unique": "reference_duplicate_count = 1"})
def silver_locations():
    source = read_json(f"{BASE_PATH}/dimensions/locations/*.json", LOCATIONS_SCHEMA)
    frame = source.select(
        F.col("Id").alias("location_id"),
        F.trim("Name").alias("location_name"),
        F.col("TimeZoneName").alias("time_zone_name"),
        F.col("OpenDate").alias("open_date_raw"),
        F.col("ClosedDate").alias("closed_date_raw"),
        "source_file",
    )
    return canonical_reference(frame, "location_id", frame.columns)


@dp.materialized_view(
    name=table_name("silver_revenue_centers"),
    comment="Revenue-center channel labels from all JSON files in the supplied directory.",
)
@dp.expect_all({"revenue_center_key_present": "key_is_valid", "revenue_center_key_unique": "reference_duplicate_count = 1"})
def silver_revenue_centers():
    source = read_json(f"{BASE_PATH}/dimensions/revenue_centers/*.json", REVENUE_CENTERS_SCHEMA)
    frame = source.select(
        F.col("Id").alias("revenue_center_id"),
        F.trim("Name").alias("revenue_center_name"),
        "source_file",
    )
    return canonical_reference(frame, "revenue_center_id", frame.columns)


@dp.materialized_view(
    name=table_name("silver_employees"),
    comment="Minimal historical employee Id references. Deleted records are retained; names, birth dates and payroll IDs are excluded.",
)
@dp.expect_all({"employee_key_present": "key_is_valid", "employee_key_unique": "reference_duplicate_count = 1"})
def silver_employees():
    source = read_json(f"{BASE_PATH}/dimensions/employees/*.json", EMPLOYEES_SCHEMA)
    frame = source.select(
        F.col("Id").alias("employee_id"),
        F.col("LocationId").alias("employee_location_id"),
        F.col("HomeLocationId").alias("home_location_id"),
        F.col("Deleted").alias("deleted"),
        F.col("Borrowed").alias("borrowed"),
        "source_file",
    )
    return canonical_reference(frame, "employee_id", frame.columns)
