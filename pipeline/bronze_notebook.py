# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Notebook — Raw Ingestion
# MAGIC
# MAGIC This notebook is the **bronze layer** of the medallion architecture.  It ingests raw
# MAGIC CSV files from the source volume exactly as they arrive — no type casting, no filtering,
# MAGIC no business logic.  Preserving the raw data at this layer gives you a complete audit
# MAGIC trail and lets you re-derive downstream layers at any time.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog` and `schema` as job parameters via `dbutils.widgets`
# MAGIC 2. Reads all CSV files from the source volume
# MAGIC 3. Writes them as a Delta table (`bronze_orders`) with all columns as strings

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
# MAGIC ## Ingest

# COMMAND ----------

raw_df = (
    spark.read
    .format("csv")
    .option("header",      "true")
    .option("inferSchema", "false")   # keep all columns as strings in bronze
    .load(volume_path)
)

print(f"Columns : {raw_df.columns}")
print(f"Rows    : {raw_df.count():,}")
display(raw_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Table

# COMMAND ----------

target_table = f"{catalog}.{schema}.bronze_orders"

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

final_count = spark.table(target_table).count()

print(f"Written  : {target_table}")
print(f"Rows     : {final_count:,}")
print()
print("Bronze ingestion complete.")
