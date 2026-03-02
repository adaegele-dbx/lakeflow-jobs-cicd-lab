# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Notebook — Business Aggregations
# MAGIC
# MAGIC This notebook is the **gold layer** of the medallion architecture.  It reads from
# MAGIC `silver_orders` and produces two analytics-ready aggregation tables that are
# MAGIC consumed by the reporting notebook and downstream dashboards.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog` and `schema` as job parameters via `dbutils.widgets`
# MAGIC 2. Reads `silver_orders` (completed orders only)
# MAGIC 3. Writes `gold_sales_by_region` — monthly revenue rolled up by region
# MAGIC 4. Writes `gold_top_products` — product performance summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "main",        "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog = {catalog}")
print(f"schema  = {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, avg, countDistinct, col

silver_df = (
    spark.table(f"{catalog}.{schema}.silver_orders")
    .filter(col("status") == "completed")
)

silver_count = silver_df.count()
print(f"Completed silver rows: {silver_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write gold_sales_by_region

# COMMAND ----------

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

print(f"Written : {region_table}  ({region_df.count():,} rows)")
display(region_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write gold_top_products

# COMMAND ----------

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

print(f"Written : {product_table}  ({product_df.count():,} rows)")
display(product_df)

# COMMAND ----------

print("Gold aggregations complete.")
