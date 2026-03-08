# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Sales by Region
# MAGIC
# MAGIC Reads `silver_orders`, filters to completed orders, and aggregates revenue,
# MAGIC order counts, average order value, and unique customers **by region and month**.
# MAGIC Writes `gold_sales_by_region`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace",   "Catalog")
dbutils.widgets.text("schema",  "lakeflow_lab", "Schema")

catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")

print(f"catalog = {catalog}")
print(f"schema  = {schema}")

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count, avg, countDistinct

silver_table = f"{catalog}.{schema}.silver_orders"
silver_df    = spark.table(silver_table).filter(col("status") == "completed")

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

# COMMAND ----------

target_table = f"{catalog}.{schema}.gold_sales_by_region"

(
    region_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

row_count = spark.table(target_table).count()
print(f"Written : {target_table}  ({row_count:,} rows)")
