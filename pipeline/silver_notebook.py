# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — Cleanse & Transform
# MAGIC
# MAGIC Reads `bronze_orders`, applies type casts, adds derived columns, drops duplicates,
# MAGIC and filters out rows that fail quality rules.  Writes `silver_orders`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace",   "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog = {catalog}")
print(f"schema  = {schema}")

# COMMAND ----------

from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.types import IntegerType, DoubleType

bronze_table = f"{catalog}.{schema}.bronze_orders"
bronze_df    = spark.table(bronze_table)
bronze_count = bronze_df.count()

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

# COMMAND ----------

silver_table = f"{catalog}.{schema}.silver_orders"

(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

silver_count = spark.table(silver_table).count()
print(f"Written : {silver_table}  ({silver_count:,} rows)")
print(f"  Duplicates removed : {dupe_count:,}")
print(f"  Quality drops      : {dropped_count:,}")
