# Databricks notebook source

# MAGIC %md
# MAGIC # Medallion ETL Notebook — Bronze, Silver & Gold
# MAGIC
# MAGIC This notebook implements the full **medallion architecture** in a single task:
# MAGIC bronze (raw ingestion), silver (cleanse & transform), and gold (business aggregations).
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog` and `schema` as **job parameters** via `dbutils.widgets`
# MAGIC 2. **Bronze** — reads raw CSV from the source volume, writes `bronze_orders`
# MAGIC 3. **Silver** — type-casts, deduplicates, filters, adds derived fields → `silver_orders`
# MAGIC 4. **Gold** — aggregates into `gold_sales_by_region` and `gold_top_products`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace",   "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

volume_path = f"/Volumes/{catalog}/{schema}/raw_data"

print(f"catalog     = {catalog}")
print(f"schema      = {schema}")
print(f"source path = {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingestion
# MAGIC
# MAGIC Ingest all CSV files from the source volume exactly as they arrive — no type
# MAGIC casting, no filtering.  Preserving the raw data gives you a complete audit trail.

# COMMAND ----------

raw_df = (
    spark.read
    .format("csv")
    .option("header",      "true")
    .option("inferSchema", "false")   # keep all columns as strings in bronze
    .load(volume_path)
)

bronze_table = f"{catalog}.{schema}.bronze_orders"

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

bronze_count = spark.table(bronze_table).count()
print(f"Bronze : {bronze_table}  ({bronze_count:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Cleanse & Transform
# MAGIC
# MAGIC Cast types, add derived columns, drop duplicates, and filter out rows
# MAGIC that fail quality rules.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.types import IntegerType, DoubleType

bronze_df = spark.table(bronze_table)

# Cast types and add derived columns
typed_df = (
    bronze_df
    .withColumn("quantity",    col("quantity").cast(IntegerType()))
    .withColumn("unit_price",  col("unit_price").cast(DoubleType()))
    .withColumn("order_date",  to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    .withColumn("year_month",  date_format(col("order_date"), "yyyy-MM"))
)

# Drop duplicates on the natural key
deduped_df = typed_df.dropDuplicates(["order_id"])
dupe_count = bronze_count - deduped_df.count()

# Apply quality filters
valid_df = deduped_df.filter(
    col("order_id").isNotNull()
    & (col("quantity")   > 0)
    & (col("unit_price") > 0)
    & col("status").isin("completed", "pending", "cancelled")
)

dropped_count = deduped_df.count() - valid_df.count()
silver_count  = valid_df.count()

silver_table = f"{catalog}.{schema}.silver_orders"

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver : {silver_table}  ({silver_count:,} rows)")
print(f"  Duplicates removed : {dupe_count:,}")
print(f"  Quality drops      : {dropped_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Business Aggregations
# MAGIC
# MAGIC Produce two analytics-ready tables from completed orders:
# MAGIC `gold_sales_by_region` and `gold_top_products`.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, avg, countDistinct

silver_df = spark.table(silver_table).filter(col("status") == "completed")

# ── gold_sales_by_region ────────────────────────────────────────────────────
region_df = (
    silver_df
    .groupBy("region", "year_month")
    .agg(
        _sum("total_amount")        .alias("total_revenue"),
        count("order_id")           .alias("order_count"),
        avg("total_amount")         .alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("region", "year_month")
)

region_table = f"{catalog}.{schema}.gold_sales_by_region"
region_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(region_table)
print(f"Gold   : {region_table}  ({region_df.count():,} rows)")

# ── gold_top_products ───────────────────────────────────────────────────────
product_df = (
    silver_df
    .groupBy("product", "category")
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _sum("quantity")    .alias("total_units_sold"),
        count("order_id")   .alias("order_count"),
        avg("unit_price")   .alias("avg_unit_price"),
    )
    .orderBy(col("total_revenue").desc())
)

product_table = f"{catalog}.{schema}.gold_top_products"
product_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(product_table)
print(f"Gold   : {product_table}  ({product_df.count():,} rows)")

# COMMAND ----------

print()
print("=" * 55)
print("  MEDALLION ETL COMPLETE")
print("=" * 55)
print(f"  Bronze : {bronze_count:>6,} rows")
print(f"  Silver : {silver_count:>6,} rows")
print(f"  Gold   : {region_df.count():>6,} region rows, {product_df.count()} product rows")
print("=" * 55)
