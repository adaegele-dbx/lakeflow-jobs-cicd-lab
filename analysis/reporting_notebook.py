# Databricks notebook source

# MAGIC %md
# MAGIC # Reporting Notebook — Lakeflow Jobs Task
# MAGIC
# MAGIC This notebook is the **third task** in the Lakeflow Job defined in `databricks.yml`.
# MAGIC It runs *after* the Lakeflow Declarative Pipeline has finished processing data through
# MAGIC the medallion architecture.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog`, `schema`, and `run_date` as **job parameters** via `dbutils.widgets`
# MAGIC 2. Reads the gold-layer tables produced by the pipeline
# MAGIC 3. Computes a data quality summary comparing bronze → silver record counts
# MAGIC 4. Surfaces the top business insights (revenue by region, top products)
# MAGIC 5. Writes a concise `reporting_summary` table for downstream consumption
# MAGIC 6. Prints a final run report stamped with the `run_date` parameter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC The job passes `catalog`, `schema`, and `run_date` into this notebook at runtime via
# MAGIC job parameter pushdown.  Default values are used when running interactively or via `%run`.

# COMMAND ----------

dbutils.widgets.text("catalog",  "workspace",   "Catalog")
dbutils.widgets.text("schema",   "lakeflow_lab", "Schema")
dbutils.widgets.text("run_date", "2024-03-31",   "Run Date (YYYY-MM-DD)")

catalog  = dbutils.widgets.get("catalog")
schema   = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")

print(f"catalog  = {catalog}")
print(f"schema   = {schema}")
print(f"run_date = {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Data Quality Summary
# MAGIC
# MAGIC Compare record counts across the medallion layers to understand how many rows
# MAGIC were dropped by the quality expectations in the silver layer.

# COMMAND ----------

bronze_count = spark.table(f"{catalog}.{schema}.bronze_orders").count()
silver_count = spark.table(f"{catalog}.{schema}.silver_orders").count()
dropped      = bronze_count - silver_count
drop_pct     = (dropped / bronze_count * 100) if bronze_count > 0 else 0

print("=" * 50)
print("  DATA QUALITY SUMMARY")
print("=" * 50)
print(f"  Bronze records  : {bronze_count:>8,}")
print(f"  Silver records  : {silver_count:>8,}")
print(f"  Rows dropped    : {dropped:>8,}  ({drop_pct:.1f}%)")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Regional Sales Performance

# COMMAND ----------

print("Regional Sales Performance (completed orders only)")
display(
    spark.table(f"{catalog}.{schema}.gold_sales_by_region")
         .orderBy("total_revenue", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Top 10 Products by Revenue

# COMMAND ----------

print("Top 10 Products by Revenue")
display(
    spark.table(f"{catalog}.{schema}.gold_top_products")
         .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write Reporting Summary Table
# MAGIC
# MAGIC Aggregate the regional data into a single-row summary and persist it as
# MAGIC `reporting_summary` for use by dashboards or downstream notebooks.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, round as _round, lit

summary_df = (
    spark.table(f"{catalog}.{schema}.gold_sales_by_region")
    .agg(
        _round(_sum("total_revenue"),  2).alias("total_revenue"),
        _sum("order_count")             .alias("total_orders"),
        _round(avg("avg_order_value"),  2).alias("avg_order_value"),
        _sum("unique_customers")        .alias("total_unique_customers"),
    )
    .withColumn("run_date", lit(run_date))   # stamp the run_date parameter on every row
)

summary_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.reporting_summary")

print(f"Reporting summary written to {catalog}.{schema}.reporting_summary")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Final Run Report

# COMMAND ----------

row = summary_df.collect()[0]

print()
print("=" * 55)
print("  PIPELINE RUN COMPLETE")
print("=" * 55)
print(f"  Run date                : {run_date}")
print(f"  Total revenue           : ${row['total_revenue']:>12,.2f}")
print(f"  Total completed orders  : {row['total_orders']:>12,}")
print(f"  Avg order value         : ${row['avg_order_value']:>12,.2f}")
print(f"  Unique customers        : {row['total_unique_customers']:>12,}")
print("=" * 55)
print()
print("Tables written this run:")
print(f"  - {catalog}.{schema}.reporting_summary  (overwrite)")
print()
print("Reporting notebook complete.")
