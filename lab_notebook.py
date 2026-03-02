# Databricks notebook source

# MAGIC %md
# MAGIC # Lakeflow Jobs & CI/CD with Databricks Asset Bundles
# MAGIC ### Hands-On Lab
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What you'll build
# MAGIC
# MAGIC In this lab you will assemble a complete, production-style data pipeline on Databricks:
# MAGIC
# MAGIC | Step | What you'll do |
# MAGIC |------|----------------|
# MAGIC | **Setup** | Create Unity Catalog schemas and volumes for dev and prod, then generate synthetic sales data |
# MAGIC | **Part 1** | Explore three **medallion notebooks** — bronze, silver, and gold layers |
# MAGIC | **Part 2** | Explore two **supporting task notebooks** — source validation and reporting |
# MAGIC | **Part 3** | Learn **Lakeflow Jobs** concepts — parameters and task dependencies |
# MAGIC | **Part 4** | Build the five-task job in the **Databricks Jobs UI** and run a first test |
# MAGIC | **Part 5** | Learn **Databricks Asset Bundles (DABs)**, configure dev and prod targets, fill in `databricks.yml` |
# MAGIC | **Part 6** | Deploy to **dev** with `databricks bundle deploy` from the terminal |
# MAGIC | **Part 7** | Run the bundle-managed job, verify results, and promote to **prod** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - This notebook is running in a Databricks workspace with **Unity Catalog** enabled
# MAGIC   (all free-tier workspaces have UC by default)
# MAGIC - This repository has been cloned as a **Git Folder** in your workspace
# MAGIC   (`Workspace` → `Create` → `Git folder`)
# MAGIC
# MAGIC > **Tip:** Run each cell with `Shift + Enter` and read the markdown cells between
# MAGIC > them — they contain the lab instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup — Create Schemas, Volumes & Sample Data
# MAGIC
# MAGIC The cell below bootstraps everything the pipeline needs for both environments:
# MAGIC
# MAGIC 1. Creates two Unity Catalog schemas — `workspace.lakeflow_lab_dev` (dev) and
# MAGIC    `workspace.lakeflow_lab` (prod)
# MAGIC 2. Creates a Unity Catalog **Volume** named `raw_data` inside each schema
# MAGIC    (a Volume is a managed directory that holds non-tabular files such as CSVs)
# MAGIC 3. Generates **500 synthetic e-commerce orders** spanning three months and writes the
# MAGIC    CSV into each volume so either environment can be used independently
# MAGIC
# MAGIC Run this cell once before proceeding to Part 1.

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG  = "workspace"
SCHEMAS  = ["lakeflow_lab_dev", "lakeflow_lab"]   # dev and prod schemas
VOLUME   = "raw_data"
# ──────────────────────────────────────────────────────────────────────────────

# 1. Generate synthetic orders ─────────────────────────────────────────────────
random.seed(42)

PRODUCTS = [
    ("Laptop Pro 15",       "Electronics",  1299.99),
    ("Wireless Headphones", "Electronics",   199.99),
    ("Standing Desk",       "Furniture",     449.99),
    ("Office Chair",        "Furniture",     329.99),
    ("Mechanical Keyboard", "Accessories",   129.99),
    ("USB-C Hub",           "Accessories",    59.99),
    ("Monitor 27in",        "Electronics",   399.99),
    ("Webcam HD",           "Electronics",    89.99),
    ("Desk Lamp",           "Furniture",      49.99),
    ("Mouse Pad XL",        "Accessories",    29.99),
]

REGIONS  = ["North", "South", "East", "West"]
STATUSES = ["completed"] * 7 + ["pending"] * 2 + ["cancelled"] * 1  # 70/20/10 split

BASE_DATE = datetime(2024, 1, 1)
orders    = []

for i in range(500):
    product, category, base_price = random.choice(PRODUCTS)
    orders.append({
        "order_id":    f"ORD-{i + 1:05d}",
        "customer_id": f"CUST-{random.randint(1, 100):04d}",
        "product":     product,
        "category":    category,
        "quantity":    random.randint(1, 5),
        "unit_price":  round(base_price * random.uniform(0.95, 1.05), 2),
        "order_date":  (BASE_DATE + timedelta(days=random.randint(0, 89))).strftime("%Y-%m-%d"),
        "region":      random.choice(REGIONS),
        "status":      random.choice(STATUSES),
    })

df = pd.DataFrame(orders)
print(f"Generated {len(df):,} synthetic orders\n")

# 2. Create schemas, volumes, and write CSV for each environment ───────────────
for SCHEMA in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME  IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/orders.csv"
    df.to_csv(volume_path, index=False)
    print(f"✅  Schema  : {CATALOG}.{SCHEMA}")
    print(f"✅  Volume  : {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"📁  Written → {volume_path}\n")

print("Sample rows:")
display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — Medallion Architecture Notebooks
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The medallion architecture organises data into three progressively refined layers.
# MAGIC Each layer is implemented here as a standalone notebook — making each step
# MAGIC independently testable, retryable, and observable.
# MAGIC
# MAGIC ```
# MAGIC  /Volumes/main/lakeflow_lab/raw_data/orders.csv
# MAGIC           │
# MAGIC           ▼   pipeline/bronze_notebook.py
# MAGIC  ┌─────────────────┐
# MAGIC  │  bronze_orders  │  Raw CSV ingested as strings — nothing dropped at this layer
# MAGIC  └────────┬────────┘
# MAGIC           │   pipeline/silver_notebook.py
# MAGIC           │   type casts · quality filters · deduplication · derived fields
# MAGIC           ▼
# MAGIC  ┌─────────────────┐
# MAGIC  │  silver_orders  │  Cleaned, typed, enriched data
# MAGIC  └────────┬────────┘
# MAGIC           │   pipeline/gold_notebook.py
# MAGIC       ┌───┴────────────────────────┐
# MAGIC       ▼                            ▼
# MAGIC  ┌──────────────────────┐  ┌──────────────────┐
# MAGIC  │ gold_sales_by_region │  │ gold_top_products │
# MAGIC  └──────────────────────┘  └──────────────────┘
# MAGIC           Analytics-ready aggregations
# MAGIC ```
# MAGIC
# MAGIC ### What to look for
# MAGIC
# MAGIC Open each notebook in a new tab and read through it — **do not run them yet**.
# MAGIC You will run them later as tasks in a Lakeflow Job.
# MAGIC
# MAGIC - `pipeline/bronze_notebook.py`
# MAGIC - `pipeline/silver_notebook.py`
# MAGIC - `pipeline/gold_notebook.py`
# MAGIC
# MAGIC As you read, notice that each notebook:
# MAGIC - Uses `dbutils.widgets` to receive `catalog` and `schema` as **parameters**
# MAGIC - Reads its input from the table written by the previous step
# MAGIC - Overwrites its output table so re-runs are **idempotent**
# MAGIC - Prints a clear summary of what was read and written
# MAGIC
# MAGIC > **Question to consider:** If you ran bronze, silver, and gold as three separate job
# MAGIC > tasks — what `depends_on` relationships would you declare to ensure they run in order?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — The Supporting Task Notebooks
# MAGIC
# MAGIC Two more notebooks complete the five-task job:
# MAGIC
# MAGIC ```
# MAGIC  Task 1             Tasks 2-4           Task 5
# MAGIC  ──────────────     ──────────────────  ─────────────────────
# MAGIC  validate_source →  bronze/silver/gold → run_reporting
# MAGIC  (notebook)         (notebooks)          (notebook)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2a — Source Validation Notebook
# MAGIC
# MAGIC **File:** `validation/source_validation_notebook.py`
# MAGIC
# MAGIC Open this file and read through it — **do not run it yet**.
# MAGIC
# MAGIC This notebook runs as **Task 1** — before any data processing — so that a bad
# MAGIC state (missing volume, empty source file) causes a **fast, cheap failure** instead
# MAGIC of running three notebooks only to find there was nothing to process.
# MAGIC
# MAGIC #### Job parameters and `dbutils.widgets`
# MAGIC
# MAGIC Look at the top section.  You'll see this pattern:
# MAGIC
# MAGIC ```python
# MAGIC dbutils.widgets.text("catalog",  "workspace",   "Catalog")
# MAGIC dbutils.widgets.text("schema",   "lakeflow_lab", "Schema")
# MAGIC dbutils.widgets.text("run_date", "2024-03-31",   "Run Date (YYYY-MM-DD)")
# MAGIC
# MAGIC catalog  = dbutils.widgets.get("catalog")
# MAGIC schema   = dbutils.widgets.get("schema")
# MAGIC run_date = dbutils.widgets.get("run_date")
# MAGIC ```
# MAGIC
# MAGIC **`dbutils.widgets`** is the bridge between a Lakeflow Job and a notebook task:
# MAGIC
# MAGIC | Context | How the value is resolved |
# MAGIC |---------|--------------------------|
# MAGIC | Running as a **job task** | The job parameter value, pushed down automatically to the notebook |
# MAGIC | Running **interactively** | The default declared in `dbutils.widgets.text()` |
# MAGIC
# MAGIC This means **the same notebook works in both contexts with no code changes**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2b — Reporting Notebook
# MAGIC
# MAGIC **File:** `analysis/reporting_notebook.py`
# MAGIC
# MAGIC Open this file and read through it — **do not run it yet**.
# MAGIC
# MAGIC This notebook runs as **Task 5** — after all three medallion layers complete.
# MAGIC Notice the same `dbutils.widgets` pattern, plus this detail in the aggregation step:
# MAGIC
# MAGIC ```python
# MAGIC summary_df = (
# MAGIC     spark.table(f"{catalog}.{schema}.gold_sales_by_region")
# MAGIC     .agg(...)
# MAGIC     .withColumn("run_date", lit(run_date))   # ← stamps the parameter on every row
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC The `run_date` parameter is written into the `reporting_summary` table, making
# MAGIC every row traceable back to the job run that produced it.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — Lakeflow Jobs: Parameters & Task Dependencies
# MAGIC
# MAGIC Before building the job in the UI, let's understand two concepts that shape how
# MAGIC multi-task jobs work.
# MAGIC
# MAGIC ### Job Parameters
# MAGIC
# MAGIC **Job parameters** let you define named values at the job level that every task in
# MAGIC the job can access.
# MAGIC
# MAGIC | | Bundle variables (`${var.*}`) | Job parameters (`parameters:`) |
# MAGIC |---|---|---|
# MAGIC | **Resolved at** | Deploy time | Run time |
# MAGIC | **Used for** | Resource names, catalog/schema, infra config | Business logic inputs — dates, filters, flags |
# MAGIC | **Override via** | `databricks bundle deploy --var foo=bar` | UI "Run with different parameters" or CLI `-p` flag |
# MAGIC | **Consumed by** | YAML expressions (`${var.catalog}`) | Notebooks (`dbutils.widgets.get("run_date")`) |
# MAGIC
# MAGIC > **Connecting the two:** In Part 5 you'll set the `schema` job parameter's default
# MAGIC > value to `${var.schema}`.  That single change makes the schema environment-aware —
# MAGIC > deploying to `dev` automatically targets `lakeflow_lab_dev`, and deploying to `prod`
# MAGIC > automatically targets `lakeflow_lab`.
# MAGIC
# MAGIC In our job we'll declare three parameters:
# MAGIC
# MAGIC | Parameter | Default | Purpose |
# MAGIC |-----------|---------|---------|
# MAGIC | `run_date` | `2024-03-31` | Stamped on reporting output rows |
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog name |
# MAGIC | `schema` | `lakeflow_lab` | Unity Catalog schema name |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Task Dependencies
# MAGIC
# MAGIC A Lakeflow Job models its tasks as a **DAG (directed acyclic graph)**.  Each task
# MAGIC can declare which other tasks must succeed before it is allowed to start:
# MAGIC
# MAGIC ```
# MAGIC  validate_source
# MAGIC        │
# MAGIC        ▼
# MAGIC      bronze
# MAGIC        │
# MAGIC        ▼
# MAGIC      silver
# MAGIC        │
# MAGIC        ▼
# MAGIC       gold
# MAGIC        │
# MAGIC        ▼
# MAGIC   run_reporting
# MAGIC ```
# MAGIC
# MAGIC **Behaviour when a task fails:**
# MAGIC - All tasks that (directly or transitively) depend on the failed task are
# MAGIC   automatically **skipped** — marked `SKIPPED`, not `FAILED`
# MAGIC - You can **retry a single failed task** from the UI without rerunning earlier tasks
# MAGIC
# MAGIC In our five-task job:
# MAGIC - If `validate_source` fails, all four downstream tasks are skipped immediately
# MAGIC - If `silver` fails, `gold` and `run_reporting` are skipped; `validate_source` and
# MAGIC   `bronze` are already complete and won't rerun on a retry

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4 — Build the Job in the Databricks Jobs UI
# MAGIC
# MAGIC Now you'll create the five-task job by hand in the UI.  This gives you a feel for
# MAGIC the job structure before you see it expressed as code.
# MAGIC
# MAGIC ### Job configuration options
# MAGIC
# MAGIC When setting up a job in the UI, two configuration tabs are worth exploring beyond
# MAGIC tasks and parameters:
# MAGIC
# MAGIC | Option | What it does | When to use it |
# MAGIC |--------|-------------|----------------|
# MAGIC | **Schedule** | Runs the job automatically on a cron-based schedule | Daily ETL, regular reporting, SLA-driven pipelines |
# MAGIC | **Notifications** | Sends email or webhook alerts on job success, failure, or skips | Keeping your team informed without manual monitoring |
# MAGIC
# MAGIC You'll configure both in Step 4d below.
# MAGIC
# MAGIC ### Step 4a — Create a new job
# MAGIC
# MAGIC 1. Click **Workflows** in the left sidebar
# MAGIC 2. Click **Create job**
# MAGIC 3. Name the job: **`Lakeflow Lab - Orchestration Job`**
# MAGIC
# MAGIC ### Step 4b — Add the five tasks
# MAGIC
# MAGIC Add each task in order.  For every task, set **Type = Notebook** and
# MAGIC **Source = Workspace**.  Use the paths exactly as shown.
# MAGIC
# MAGIC | Task key | Notebook path | Depends on |
# MAGIC |----------|---------------|------------|
# MAGIC | `validate_source` | `./validation/source_validation_notebook` | *(none — runs first)* |
# MAGIC | `bronze` | `./pipeline/bronze_notebook` | `validate_source` |
# MAGIC | `silver` | `./pipeline/silver_notebook` | `bronze` |
# MAGIC | `gold` | `./pipeline/gold_notebook` | `silver` |
# MAGIC | `run_reporting` | `./analysis/reporting_notebook` | `gold` |
# MAGIC
# MAGIC > **Tip:** After adding each task (except the first), use the **"Depends on"** dropdown
# MAGIC > to wire up the dependency.  The canvas should show a linear chain of five boxes.
# MAGIC
# MAGIC ### Step 4c — Add job parameters
# MAGIC
# MAGIC Click the **Parameters** tab (at the job level, not the task level) and add:
# MAGIC
# MAGIC | Name | Default value |
# MAGIC |------|---------------|
# MAGIC | `run_date` | `2024-03-31` |
# MAGIC | `catalog` | `workspace` |
# MAGIC | `schema` | `lakeflow_lab` |
# MAGIC
# MAGIC ### Step 4d — Add a schedule and notifications
# MAGIC
# MAGIC **Schedule — run daily at 6 AM:**
# MAGIC 1. Click the **Schedules & Triggers** tab on the job page
# MAGIC 2. Click **Add a schedule**
# MAGIC 3. Set the schedule to **Every day at 06:00 AM** (or enter cron: `0 0 6 * * ?`)
# MAGIC 4. Click **Save**
# MAGIC
# MAGIC **Email notification on failure:**
# MAGIC 1. Click the **Notifications** tab on the job page
# MAGIC 2. Under **On failure**, click **Add notification** and enter your email address
# MAGIC 3. Click **Save**
# MAGIC
# MAGIC ### Step 4e — Verify the DAG
# MAGIC
# MAGIC Click the **Tasks** canvas.  You should see five boxes connected left-to-right with
# MAGIC arrows.  If any task is floating (no incoming or outgoing arrow where expected),
# MAGIC check its **Depends on** setting.
# MAGIC
# MAGIC ### Step 4f — Run the job and verify
# MAGIC
# MAGIC Click **Run now** to trigger a test run.  Watch each task progress through the DAG —
# MAGIC tasks turn green one at a time as each finishes before the next begins.
# MAGIC
# MAGIC Once all five tasks show green checkmarks, run the cell below to confirm every
# MAGIC table was created correctly.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "lakeflow_lab"

expected_tables = [
    "bronze_orders",
    "silver_orders",
    "gold_sales_by_region",
    "gold_top_products",
    "reporting_summary",
]

print("=== Part 4 verification ===\n")
all_ok = True
for tbl in expected_tables:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
        print(f"  ✅  {tbl:<30} {n:>6,} rows")
    except Exception:
        print(f"  ❌  {tbl:<30} not found")
        all_ok = False

print()
if all_ok:
    print("All tables present — job is working correctly. Proceed to Part 5.")
else:
    print("Some tables are missing — has the job finished running?")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5 — Databricks Asset Bundles & Exporting to `databricks.yml`
# MAGIC
# MAGIC ### What is a Databricks Asset Bundle?
# MAGIC
# MAGIC A **Databricks Asset Bundle (DAB)** is a YAML-based project format that lets you define,
# MAGIC version-control, and deploy Databricks resources — jobs, pipelines, notebooks, and more —
# MAGIC as **code**.
# MAGIC
# MAGIC The core file is `databricks.yml` at the root of this repository.
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my-bundle
# MAGIC
# MAGIC variables:
# MAGIC   schema:
# MAGIC     default: lakeflow_lab
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab_dev   # dev writes to lakeflow_lab_dev
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab       # prod writes to lakeflow_lab
# MAGIC
# MAGIC resources:        # ← Declare jobs, pipelines, etc. here
# MAGIC   jobs: ...
# MAGIC ```
# MAGIC
# MAGIC ### Key bundle concepts
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Bundle** | A project containing code + YAML resource definitions |
# MAGIC | **Target** | A named deployment environment (`dev`, `staging`, `prod`) |
# MAGIC | **`mode: development`** | Prefixes resource names with `[dev username]` so devs don't collide |
# MAGIC | **Variables** | Bundle-level parameters — resolved at deploy time; can be overridden per target |
# MAGIC | **`bundle deploy`** | Syncs files and creates/updates workspace resources |
# MAGIC | **`bundle run`** | Triggers a job defined in the bundle |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5a — Export the job configuration from the UI
# MAGIC
# MAGIC The Databricks UI can generate the DABs YAML for a job you've already built:
# MAGIC
# MAGIC 1. Open the job you created in Part 4
# MAGIC 2. Click the **kebab menu** (three dots `⋮`) in the top-right of the job page
# MAGIC 3. Select **Export** → **Download DABs configuration**
# MAGIC 4. Open the downloaded `.yml` file — it contains the full job definition in DABs format
# MAGIC
# MAGIC > **What you'll see:** The exported YAML has the same structure as
# MAGIC > `resources/job_definition_template.yml` — tasks, `depends_on`, parameters, schedules,
# MAGIC > and notification settings are all represented as YAML keys.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5b — Inspect the current `databricks.yml`
# MAGIC
# MAGIC Open `databricks.yml` from the file tree on the left (click the file to view it).
# MAGIC
# MAGIC Notice it already has a `variables` section (`catalog`, `schema`) and a `targets` section
# MAGIC with both `dev` and `prod` entries.  The `dev` target overrides `schema` to
# MAGIC `lakeflow_lab_dev`; the `prod` target inherits the default `lakeflow_lab`.
# MAGIC Your task in Step 5c is to fill in the empty `resources:` section.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5c — Fill in `databricks.yml`
# MAGIC
# MAGIC Right now `databricks.yml` has an empty `resources:` section.  Your task is to add
# MAGIC the five-task job definition you exported in Step 5a.
# MAGIC
# MAGIC You can either:
# MAGIC - **Paste the exported YAML** directly into the `resources:` section, or
# MAGIC - **Use the annotated template** in `resources/job_definition_template.yml` as a reference
# MAGIC
# MAGIC One important change to make after pasting: update the `catalog` and `schema` parameter
# MAGIC defaults to use bundle variables so the job automatically targets the right schema for
# MAGIC each deployment environment.
# MAGIC
# MAGIC When complete, the full `databricks.yml` should look like:
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: lakeflow-jobs-ci-cd-lab
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: workspace
# MAGIC   schema:
# MAGIC     default: lakeflow_lab
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab_dev
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab
# MAGIC
# MAGIC resources:
# MAGIC
# MAGIC   jobs:
# MAGIC     lakeflow_lab_job:
# MAGIC       name: "Lakeflow Lab - Orchestration Job [${bundle.target}]"
# MAGIC
# MAGIC       parameters:
# MAGIC         - name: run_date
# MAGIC           default: "2024-03-31"
# MAGIC         - name: catalog
# MAGIC           default: ${var.catalog}      # resolved at deploy time from variables
# MAGIC         - name: schema
# MAGIC           default: ${var.schema}       # lakeflow_lab_dev (dev) or lakeflow_lab (prod)
# MAGIC
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 6 * * ?"
# MAGIC         timezone_id: "UTC"
# MAGIC
# MAGIC       email_notifications:
# MAGIC         on_failure:
# MAGIC           - your-email@example.com
# MAGIC
# MAGIC       tasks:
# MAGIC
# MAGIC         - task_key: validate_source        # Task 1 — no dependency
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./validation/source_validation_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: bronze                 # Task 2 — depends on Task 1
# MAGIC           depends_on:
# MAGIC             - task_key: validate_source
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/bronze_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: silver                 # Task 3 — depends on Task 2
# MAGIC           depends_on:
# MAGIC             - task_key: bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/silver_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: gold                   # Task 4 — depends on Task 3
# MAGIC           depends_on:
# MAGIC             - task_key: silver
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/gold_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_reporting          # Task 5 — depends on Task 4
# MAGIC           depends_on:
# MAGIC             - task_key: gold
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./analysis/reporting_notebook
# MAGIC             source: WORKSPACE
# MAGIC ```
# MAGIC
# MAGIC **Things to notice:**
# MAGIC - `${var.schema}` in the job parameter default is resolved at **deploy time** — deploying
# MAGIC   to `dev` bakes in `lakeflow_lab_dev`; deploying to `prod` bakes in `lakeflow_lab`
# MAGIC - Job parameters are then pushed down automatically to all notebook tasks at **run time**
# MAGIC - `schedule` and `email_notifications` are top-level job keys — replace
# MAGIC   `your-email@example.com` with your address
# MAGIC
# MAGIC ### Step 5d — Validate the bundle
# MAGIC
# MAGIC DABs CLI commands must be run from a terminal, not from inside a serverless notebook.
# MAGIC Open a web terminal in your Databricks workspace:
# MAGIC **Workspace** → navigate to your Git Folder → right-click → **Open in terminal**
# MAGIC (or use the terminal icon at the bottom of the screen if your IDE supports it).
# MAGIC
# MAGIC From the terminal, `cd` to your Git Folder root if needed, then run:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate --target dev
# MAGIC ```
# MAGIC
# MAGIC A valid bundle prints a JSON summary of the resolved resources with no errors.
# MAGIC Fix any YAML errors flagged before proceeding.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6 — Deploy to Dev with `databricks bundle deploy`
# MAGIC
# MAGIC ### What does `bundle deploy` do?
# MAGIC
# MAGIC 1. **Syncs files** — uploads your notebooks and Python files to the workspace under
# MAGIC    `${workspace.root_path}/files/`
# MAGIC 2. **Creates or updates resources** — creates the job in your workspace
# MAGIC    (or updates it if it already exists)
# MAGIC 3. **Prefixes names** — in `dev` mode, resource names are prefixed with
# MAGIC    `[dev your@email.com]` so your dev resources don't collide with colleagues'
# MAGIC
# MAGIC In the same terminal you opened in Step 5d, run:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy --target dev
# MAGIC ```
# MAGIC
# MAGIC You should see output confirming that files were uploaded and the job was created or
# MAGIC updated.  Once it completes, proceed to Part 7.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7 — Run the Job with Custom Parameters & Verify
# MAGIC
# MAGIC ### Explore the deployed job in the UI
# MAGIC
# MAGIC 1. Click **Workflows** in the left sidebar
# MAGIC 2. Find **`[dev your@email.com] Lakeflow Lab - Orchestration Job`** and open it
# MAGIC 3. Click the **Tasks** tab — you should see all five tasks connected by arrows:
# MAGIC    `validate_source` → `bronze` → `silver` → `gold` → `run_reporting`
# MAGIC 4. Click the **Parameters** tab — confirm `schema` defaults to `lakeflow_lab_dev`
# MAGIC    (the dev target variable was baked in at deploy time)
# MAGIC
# MAGIC ### Run with the default parameters
# MAGIC
# MAGIC Click **Run now** and watch the tasks execute one at a time.  This run writes to
# MAGIC `workspace.lakeflow_lab_dev` — your isolated dev schema.
# MAGIC
# MAGIC ### Run with an overridden run_date
# MAGIC
# MAGIC 1. Click **Run now** → **"Run now with different parameters"**
# MAGIC 2. Change `run_date` to `2024-02-15` and click **Run**
# MAGIC 3. After the job finishes, query `reporting_summary` below — you should see
# MAGIC    `run_date = 2024-02-15` stamped on the row
# MAGIC
# MAGIC ### Or trigger from the terminal
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run lakeflow_lab_job --target dev \
# MAGIC   -p run_date=2024-02-15
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the dev run output

# COMMAND ----------

# Dev job writes to lakeflow_lab_dev
CATALOG = "workspace"
SCHEMA  = "lakeflow_lab_dev"

expected_tables = [
    "bronze_orders",
    "silver_orders",
    "gold_sales_by_region",
    "gold_top_products",
    "reporting_summary",
]

print("=== Part 7 verification (dev) ===\n")
all_ok = True
for tbl in expected_tables:
    try:
        n = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}").count()
        print(f"  ✅  {tbl:<30} {n:>6,} rows")
    except Exception:
        print(f"  ❌  {tbl:<30} not found")
        all_ok = False

print()
if all_ok:
    print(f"All tables present in {CATALOG}.{SCHEMA} — dev run successful!")
else:
    print("Some tables are missing — has the job finished running?")

# COMMAND ----------

# Inspect reporting_summary — confirm run_date was stamped by the job parameter
print("reporting_summary (should include the run_date parameter value):")
display(spark.table(f"{CATALOG}.{SCHEMA}.reporting_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Promote to prod
# MAGIC
# MAGIC Once you're satisfied the dev run is correct, deploy and run the job for the `prod`
# MAGIC target.  The prod job writes to `workspace.lakeflow_lab` (no `[dev ...]` name prefix).
# MAGIC
# MAGIC In the terminal:
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy the job to the prod target
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Run it with the default parameters
# MAGIC databricks bundle run lakeflow_lab_job --target prod
# MAGIC ```
# MAGIC
# MAGIC > **Note:** The prod target has no `mode: development`, so resource names are not
# MAGIC > prefixed and the job will be visible to everyone with access to the workspace.
# MAGIC > Make sure the pipeline is working correctly before promoting.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lab Complete
# MAGIC
# MAGIC Congratulations! You've built a complete CI/CD-ready data pipeline on Databricks:
# MAGIC
# MAGIC ```
# MAGIC  Git Repository
# MAGIC  ├── databricks.yml
# MAGIC  ├── pipeline/
# MAGIC  │   ├── bronze_notebook.py
# MAGIC  │   ├── silver_notebook.py
# MAGIC  │   └── gold_notebook.py
# MAGIC  ├── validation/
# MAGIC  │   └── source_validation_notebook.py
# MAGIC  └── analysis/
# MAGIC      └── reporting_notebook.py
# MAGIC
# MAGIC  databricks bundle deploy --target dev   → schema = lakeflow_lab_dev
# MAGIC  databricks bundle deploy --target prod  → schema = lakeflow_lab
# MAGIC  └── Creates: Lakeflow Job
# MAGIC        parameters: run_date, catalog, schema (default from ${var.schema})
# MAGIC        │
# MAGIC        ├── Task 1: validate_source  (no dependency)
# MAGIC        ├── Task 2: bronze           (depends_on: validate_source)
# MAGIC        ├── Task 3: silver           (depends_on: bronze)
# MAGIC        ├── Task 4: gold             (depends_on: silver)
# MAGIC        └── Task 5: run_reporting    (depends_on: gold)
# MAGIC ```
# MAGIC
# MAGIC ### What to explore next
# MAGIC
# MAGIC - **CI/CD pipeline** — trigger `bundle deploy --target prod` from GitHub Actions or
# MAGIC   Azure DevOps on every merge to `main` for fully automated promotion
# MAGIC - **Staging target** — add a third `staging` target between dev and prod with its own
# MAGIC   schema, giving you a three-tier promotion pipeline
# MAGIC - **Fan-out dependencies** — add a second reporting task that also `depends_on: gold`
# MAGIC   to run two downstream notebooks in parallel after the gold layer completes
# MAGIC - **Webhook notifications** — add a system or Slack webhook alongside email for richer alerting

# COMMAND ----------
