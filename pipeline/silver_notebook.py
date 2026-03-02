# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Notebook — Cleanse & Transform
# MAGIC
# MAGIC This notebook is the **silver layer** of the medallion architecture.  It reads from
# MAGIC `bronze_orders`, applies type casts and data quality rules, adds derived fields, and
# MAGIC writes the cleaned result to `silver_orders`.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog` and `schema` as job parameters via `dbutils.widgets`
# MAGIC 2. Reads `bronze_orders`
# MAGIC 3. Casts columns to their correct types
# MAGIC 4. Derives `total_amount` and `year_month`
# MAGIC 5. Drops duplicate `order_id` rows
# MAGIC 6. Filters out rows that fail quality rules, logging counts before and after
# MAGIC 7. Writes the cleaned data as `silver_orders`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace",   "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog = {catalog}")
print(f"schema  = {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.types import IntegerType, DoubleType

bronze_df = spark.table(f"{catalog}.{schema}.bronze_orders")
bronze_count = bronze_df.count()
print(f"Bronze rows read: {bronze_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanse & Transform

# COMMAND ----------

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
print(f"Duplicate rows removed : {dupe_count:,}")

# Apply quality filters — log how many rows each rule removes
valid_df = deduped_df.filter(
    col("order_id").isNotNull()
    & (col("quantity")   > 0)
    & (col("unit_price") > 0)
    & col("status").isin("completed", "pending", "cancelled")
)

dropped_count = deduped_df.count() - valid_df.count()
print(f"Rows dropped (quality) : {dropped_count:,}")

silver_count = valid_df.count()
print(f"Silver rows to write   : {silver_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Table

# COMMAND ----------

target_table = f"{catalog}.{schema}.silver_orders"

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Written  : {target_table}")
print(f"Rows     : {silver_count:,}")
print()
print("Silver cleanse complete.")
print()
print("=== Quality summary ===")
print(f"  Bronze in   : {bronze_count:>6,}")
print(f"  Duplicates  : {dupe_count:>6,}")
print(f"  Bad rows    : {dropped_count:>6,}")
print(f"  Silver out  : {silver_count:>6,}")
