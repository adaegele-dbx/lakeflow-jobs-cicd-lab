import dlt
from pyspark.sql.functions import (
    col,
    to_date,
    date_format,
    sum as _sum,
    count,
    avg,
    countDistinct,
)

# =============================================================================
# Lakeflow Declarative Pipeline — Medallion Architecture
#
# This single file defines the entire bronze → silver → gold ETL pipeline
# using Lakeflow Declarative Pipelines (Delta Live Tables).
#
# Key concepts:
#   @dlt.table           — declares a managed Delta table
#   @dlt.expect_or_drop  — enforces data quality rules (bad rows are dropped)
#   dlt.read("table")    — reads from another table in this pipeline
#
# The pipeline's catalog, target schema, and configuration parameters
# (like volume_path) are set at the pipeline level — not in this code.
# =============================================================================


# ── Bronze Layer ─────────────────────────────────────────────────────────────
# Raw CSV ingestion — all columns kept as strings for a full audit trail.

@dlt.table(
    comment="Raw e-commerce orders ingested from CSV — all columns kept as strings"
)
def bronze_orders():
    volume_path = spark.conf.get("pipeline.volume_path")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(volume_path)
    )


# ── Silver Layer ─────────────────────────────────────────────────────────────
# Cleanse, type-cast, deduplicate, and enrich.  Data quality expectations
# automatically drop rows that fail validation.

@dlt.table(
    comment="Cleansed, typed, and deduplicated orders with derived fields"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect_or_drop("positive_price", "unit_price > 0")
@dlt.expect_or_drop("valid_status", "status IN ('completed', 'pending', 'cancelled')")
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .dropDuplicates(["order_id"])
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("total_amount", col("quantity") * col("unit_price"))
        .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))
    )


# ── Gold Layer ───────────────────────────────────────────────────────────────
# Business-level aggregations consumed by dashboards and reports.

@dlt.table(
    comment="Monthly revenue aggregated by region — completed orders only"
)
def gold_sales_by_region():
    return (
        dlt.read("silver_orders")
        .filter(col("status") == "completed")
        .groupBy("region", "year_month")
        .agg(
            _sum("total_amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("region", "year_month")
    )


@dlt.table(
    comment="Product performance summary — completed orders only"
)
def gold_top_products():
    return (
        dlt.read("silver_orders")
        .filter(col("status") == "completed")
        .groupBy("product", "category")
        .agg(
            _sum("total_amount").alias("total_revenue"),
            _sum("quantity").alias("total_units_sold"),
            count("order_id").alias("order_count"),
            avg("unit_price").alias("avg_unit_price"),
        )
        .orderBy(col("total_revenue").desc())
    )
