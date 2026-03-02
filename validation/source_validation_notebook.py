# Databricks notebook source

# MAGIC %md
# MAGIC # Source Validation Notebook — Lakeflow Jobs Task
# MAGIC
# MAGIC This notebook is the **first task** in the Lakeflow Job.  It runs *before* the
# MAGIC pipeline so that bad state (missing source files, empty volume) causes the job to
# MAGIC fail fast at a cheap notebook task rather than waste DLT compute on a doomed run.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Receives `catalog`, `schema`, and `run_date` as **job parameters** via `dbutils.widgets`
# MAGIC 2. Verifies the source volume exists and contains CSV files
# MAGIC 3. Counts the raw source records
# MAGIC 4. Prints a pre-flight summary and raises an exception if validation fails,
# MAGIC    which blocks all downstream tasks from running

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Widgets are the mechanism by which Lakeflow Jobs passes **job parameters** into a
# MAGIC notebook task at runtime.
# MAGIC
# MAGIC - When the notebook runs as a **job task**, the values come from the job's
# MAGIC   `parameters` block (or from a manual "Run now with different parameters" override).
# MAGIC - When the notebook runs **interactively** (or via `%run`), the default values
# MAGIC   declared in `dbutils.widgets.text()` are used automatically.
# MAGIC
# MAGIC This means the same notebook works in both contexts with no code changes.

# COMMAND ----------

# Declare widgets — these define both the parameter names the job will pass in
# and the default values used when running the notebook interactively.
dbutils.widgets.text("catalog",  "main",        "Catalog")
dbutils.widgets.text("schema",   "lakeflow_lab", "Schema")
dbutils.widgets.text("run_date", "2024-03-31",   "Run Date (YYYY-MM-DD)")

# Read the values (job parameters override defaults at runtime)
catalog  = dbutils.widgets.get("catalog")
schema   = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")

print(f"catalog  = {catalog}")
print(f"schema   = {schema}")
print(f"run_date = {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checks

# COMMAND ----------

import os

volume_path = f"/Volumes/{catalog}/{schema}/raw_data"

# ── Check 1: Volume path is accessible ────────────────────────────────────────
try:
    entries = os.listdir(f"/dbfs{volume_path}")
except FileNotFoundError:
    raise Exception(
        f"Volume not found: {volume_path}\n"
        "Run the Setup cell in lab_notebook.py to create the schema, volume, and sample data."
    )

csv_files = [f for f in entries if f.endswith(".csv")]

if not csv_files:
    raise Exception(
        f"No CSV files found in {volume_path}.\n"
        "Run the Setup cell in lab_notebook.py to generate the sample data."
    )

print(f"✅  Check 1 passed — found {len(csv_files)} CSV file(s) in {volume_path}")
for f in csv_files:
    print(f"       {f}")

# ── Check 2: Source data contains rows ────────────────────────────────────────
source_df  = spark.read.format("csv").option("header", "true").load(volume_path)
row_count  = source_df.count()
col_count  = len(source_df.columns)

if row_count == 0:
    raise Exception(f"Source volume is empty — no rows to process in {volume_path}")

print(f"\n✅  Check 2 passed — {row_count:,} rows, {col_count} columns")
print(f"       Columns: {', '.join(source_df.columns)}")

# ── Summary ───────────────────────────────────────────────────────────────────
print()
print("=" * 55)
print("  PRE-FLIGHT VALIDATION PASSED")
print("=" * 55)
print(f"  Run date   : {run_date}")
print(f"  Source     : {volume_path}")
print(f"  CSV files  : {len(csv_files)}")
print(f"  Rows ready : {row_count:,}")
print("=" * 55)
print()
print("Downstream tasks are cleared to run.")
