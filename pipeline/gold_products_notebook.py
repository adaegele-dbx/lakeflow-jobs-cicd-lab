# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Top Products
# MAGIC
# MAGIC Reads `silver_orders`, filters to completed orders, and aggregates revenue,
# MAGIC units sold, order counts, and average unit price **by product and category**.
# MAGIC Writes `gold_top_products`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace",   "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog = {catalog}")
print(f"schema  = {schema}")

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, avg

silver_table = f"{catalog}.{schema}.silver_orders"
silver_df    = spark.table(silver_table).filter(col("status") == "completed")

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

# COMMAND ----------

target_table = f"{catalog}.{schema}.gold_top_products"

(
    product_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

row_count = spark.table(target_table).count()
print(f"Written : {target_table}  ({row_count:,} rows)")
