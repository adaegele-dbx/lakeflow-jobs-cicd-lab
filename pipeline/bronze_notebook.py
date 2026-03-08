# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Raw Ingestion
# MAGIC
# MAGIC Ingests raw CSV files from the source volume exactly as they arrive — no type
# MAGIC casting, no filtering.  Preserving the raw data gives a complete audit trail.

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

raw_df = (
    spark.read
    .format("csv")
    .option("header",      "true")
    .option("inferSchema", "false")   # keep all columns as strings in bronze
    .load(volume_path)
)

target_table = f"{catalog}.{schema}.bronze_orders"

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

row_count = spark.table(target_table).count()
print(f"Written : {target_table}  ({row_count:,} rows)")
