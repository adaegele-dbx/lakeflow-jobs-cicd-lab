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
# MAGIC | **Setup** | Create Unity Catalog schemas, volumes, and sample data; create a pipeline and publish a dashboard |
# MAGIC | **Part 1** | Explore the **Lakeflow Declarative Pipeline** (medallion architecture in a single file) |
# MAGIC | **Part 2** | Explore the supporting artifacts — a **validation notebook** and a **sales dashboard** |
# MAGIC | **Part 3** | Learn **Lakeflow Jobs** concepts — task types, parameters, and task dependencies |
# MAGIC | **Part 4** | Build the three-task job in the **Databricks Jobs UI** and run a first test |
# MAGIC | **Part 5** | Learn **Databricks Asset Bundles (DABs)**, configure dev and prod targets, fill in `databricks.yml` |
# MAGIC | **Part 6** | Deploy to **dev** with `databricks bundle deploy` from the terminal |
# MAGIC | **Part 7** | Run the bundle-managed job and promote to **prod** |
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
# MAGIC Run this cell once before proceeding.

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
    print(f"  Schema  : {CATALOG}.{SCHEMA}")
    print(f"  Volume  : {CATALOG}.{SCHEMA}.{VOLUME}")
    print(f"  Written : {volume_path}\n")

print("Sample rows:")
display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — Create the Declarative Pipeline
# MAGIC
# MAGIC Create the pipeline that the job will trigger as its ETL task.
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar
# MAGIC 2. Click the **Pipelines** tab
# MAGIC 3. Click **Create pipeline**
# MAGIC 4. Configure:
# MAGIC
# MAGIC    | Setting | Value |
# MAGIC    |---------|-------|
# MAGIC    | **Pipeline name** | `Lakeflow Lab - Medallion Pipeline` |
# MAGIC    | **Source code** | Browse to `pipeline/medallion_pipeline.py` in your Git folder |
# MAGIC    | **Destination** → Catalog | `workspace` |
# MAGIC    | **Destination** → Target schema | `lakeflow_lab` |
# MAGIC
# MAGIC 5. Under **Advanced** → **Configuration**, add one key-value pair:
# MAGIC
# MAGIC    | Key | Value |
# MAGIC    |-----|-------|
# MAGIC    | `volume_path` | `/Volumes/workspace/lakeflow_lab/raw_data` |
# MAGIC
# MAGIC 6. Click **Create**
# MAGIC 7. Click **Start** to run a **dry run** — this validates the pipeline graph and
# MAGIC    confirms all four tables (bronze, silver, two golds) are recognized
# MAGIC 8. Once the dry run succeeds, you're done — the pipeline is ready to be
# MAGIC    referenced as a job task
# MAGIC
# MAGIC ### Setup — Publish the Sales Dashboard
# MAGIC
# MAGIC Create the dashboard that the job will refresh as its final task.
# MAGIC
# MAGIC 1. In the workspace file browser, navigate to your Git folder
# MAGIC 2. Open the `dashboards/` folder
# MAGIC 3. Click on **`sales_dashboard.lvdash.json`** — Databricks opens it as a dashboard
# MAGIC 4. Click **Publish** in the top-right corner to make it available as a job task
# MAGIC
# MAGIC > **Note:** The dashboard queries reference `workspace.lakeflow_lab` (the prod schema).
# MAGIC > The tables won't exist until the pipeline runs for the first time, so the dashboard
# MAGIC > will show empty or error states until then — that's expected.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The Lakeflow Declarative Pipeline
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The medallion architecture organises data into three progressively refined layers.
# MAGIC In this lab, all three layers are defined in a **single Lakeflow Declarative Pipeline**
# MAGIC (also known as DLT / Delta Live Tables):
# MAGIC
# MAGIC ```
# MAGIC  /Volumes/workspace/lakeflow_lab/raw_data/orders.csv
# MAGIC           |
# MAGIC           v   @dlt.table
# MAGIC  +------------------+
# MAGIC  |  bronze_orders   |  Raw CSV ingested as strings
# MAGIC  +--------+---------+
# MAGIC           |   @dlt.table + @dlt.expect_or_drop
# MAGIC           |   type casts, quality filters, deduplication, derived fields
# MAGIC           v
# MAGIC  +------------------+
# MAGIC  |  silver_orders   |  Cleaned, typed, enriched data
# MAGIC  +--------+---------+
# MAGIC           |   @dlt.table
# MAGIC       +---+-------------------+
# MAGIC       v                       v
# MAGIC  +--------------------+  +-------------------+
# MAGIC  |gold_sales_by_region|  |gold_top_products  |
# MAGIC  +--------------------+  +-------------------+
# MAGIC           Analytics-ready aggregations
# MAGIC ```
# MAGIC
# MAGIC ### What to look for
# MAGIC
# MAGIC Open `pipeline/medallion_pipeline.py` in a new tab and read through it.
# MAGIC
# MAGIC As you read, notice:
# MAGIC
# MAGIC - **`@dlt.table`** — each function decorated with `@dlt.table` becomes a managed
# MAGIC   Delta table.  The function name becomes the table name.
# MAGIC - **`@dlt.expect_or_drop`** — data quality rules on the silver layer.  Rows that
# MAGIC   fail an expectation are automatically dropped (and counted in the pipeline's
# MAGIC   data quality metrics).
# MAGIC - **`dlt.read("bronze_orders")`** — reads from another table *in the same pipeline*.
# MAGIC   DLT resolves dependencies automatically — no need to specify execution order.
# MAGIC - **`spark.conf.get("volume_path")`** — the source data path comes from
# MAGIC   pipeline configuration, not hardcoded values.  This is set when the pipeline
# MAGIC   resource is defined (you'll see this in Part 5).
# MAGIC - **No `dbutils.widgets`** — unlike notebook tasks, a declarative pipeline gets its
# MAGIC   catalog and target schema from the pipeline definition, not from parameters.
# MAGIC
# MAGIC > **Key takeaway:** A declarative pipeline lets you define the *what* (table
# MAGIC > definitions, quality rules, transformations) and the framework handles the *how*
# MAGIC > (execution order, retries, incremental processing).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — The Supporting Task Artifacts
# MAGIC
# MAGIC Two more artifacts complete the three-task job:
# MAGIC
# MAGIC ```
# MAGIC  Task 1              Task 2                Task 3
# MAGIC  ─────────────────   ────────────────────  ──────────────────────
# MAGIC  validate_source  →  run_pipeline        → refresh_dashboard
# MAGIC  (notebook task)     (pipeline task)        (dashboard task)
# MAGIC ```
# MAGIC
# MAGIC Notice that each task uses a **different task type** — this is one of the strengths
# MAGIC of Lakeflow Jobs: you can orchestrate notebooks, pipelines, dashboards, SQL queries,
# MAGIC dbt projects, and more in a single job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2a — Source Validation Notebook
# MAGIC
# MAGIC **File:** `validation/source_validation_notebook.py`
# MAGIC
# MAGIC Open this file and read through it — **do not run it yet**.
# MAGIC
# MAGIC This notebook runs as **Task 1** — before the pipeline — so that a bad
# MAGIC state (missing volume, empty source file) causes a **fast, cheap failure** instead
# MAGIC of spinning up a pipeline only to find there was nothing to process.
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
# MAGIC ### Part 2b — Sales Dashboard
# MAGIC
# MAGIC **File:** `dashboards/sales_dashboard.lvdash.json`
# MAGIC
# MAGIC You published this dashboard in the Setup step above.  Open it from the workspace
# MAGIC sidebar or click the `.lvdash.json` file again.
# MAGIC
# MAGIC The dashboard queries the **gold-layer tables** produced by the pipeline:
# MAGIC
# MAGIC | Widget | Source table | What it shows |
# MAGIC |--------|-------------|---------------|
# MAGIC | Revenue counter | `gold_sales_by_region` | Total revenue across all regions |
# MAGIC | Orders counter | `gold_sales_by_region` | Total completed order count |
# MAGIC | Avg Order Value counter | `gold_sales_by_region` | Average order value |
# MAGIC | Revenue by Region chart | `gold_sales_by_region` | Bar chart of revenue per region |
# MAGIC | Revenue Trend chart | `gold_sales_by_region` | Monthly revenue trend line |
# MAGIC | Top Products table | `gold_top_products` | Product performance ranked by revenue |
# MAGIC
# MAGIC > **Note:** The dashboard shows empty/error states right now because the gold tables
# MAGIC > don't exist yet.  They'll be populated when the pipeline runs for the first time
# MAGIC > in Part 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — Lakeflow Jobs: Task Types, Parameters & Dependencies
# MAGIC
# MAGIC Before building the job in the UI, let's understand the concepts that shape how
# MAGIC multi-task jobs work.
# MAGIC
# MAGIC ### Task Types
# MAGIC
# MAGIC A Lakeflow Job can orchestrate many different task types.  Our job uses three:
# MAGIC
# MAGIC | Task type | What it does | Our task |
# MAGIC |-----------|-------------|----------|
# MAGIC | **Notebook** | Runs a notebook with parameters via `dbutils.widgets` | `validate_source` |
# MAGIC | **Pipeline** | Triggers a Lakeflow Declarative Pipeline (DLT) | `run_pipeline` |
# MAGIC | **Dashboard** | Refreshes an AI/BI Lakeview dashboard | `refresh_dashboard` |
# MAGIC
# MAGIC Other task types you might use in production: SQL queries, dbt projects, JAR tasks,
# MAGIC Python scripts, and even other jobs (via `run_job` tasks).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Job Parameters
# MAGIC
# MAGIC **Job parameters** let you define named values at the job level that **notebook tasks**
# MAGIC can access via `dbutils.widgets`.
# MAGIC
# MAGIC | | Bundle variables (`${var.*}`) | Job parameters (`parameters:`) |
# MAGIC |---|---|---|
# MAGIC | **Resolved at** | Deploy time | Run time |
# MAGIC | **Used for** | Resource names, catalog/schema, infra config | Business logic inputs — dates, filters, flags |
# MAGIC | **Override via** | `databricks bundle deploy --var foo=bar` | UI "Run with different parameters" or CLI `-p` flag |
# MAGIC | **Consumed by** | YAML expressions (`${var.catalog}`) | Notebooks (`dbutils.widgets.get("run_date")`) |
# MAGIC
# MAGIC > **Important:** Job parameters are pushed down to **notebook tasks** automatically.
# MAGIC > Pipeline tasks get their configuration (catalog, schema, volume path) from the
# MAGIC > **pipeline definition**, not from job parameters.
# MAGIC
# MAGIC In our job we'll declare three parameters:
# MAGIC
# MAGIC | Parameter | Default | Purpose |
# MAGIC |-----------|---------|---------|
# MAGIC | `run_date` | `2024-03-31` | Available to notebook tasks for date-based logic |
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
# MAGIC  validate_source  (notebook)
# MAGIC        |
# MAGIC        v
# MAGIC  run_pipeline     (pipeline)
# MAGIC        |
# MAGIC        v
# MAGIC  refresh_dashboard (dashboard)
# MAGIC ```
# MAGIC
# MAGIC **Behaviour when a task fails:**
# MAGIC - All tasks that (directly or transitively) depend on the failed task are
# MAGIC   automatically **skipped** — marked `SKIPPED`, not `FAILED`
# MAGIC - You can **retry a single failed task** from the UI without rerunning earlier tasks
# MAGIC
# MAGIC In our three-task job:
# MAGIC - If `validate_source` fails, `run_pipeline` and `refresh_dashboard` are skipped
# MAGIC - If `run_pipeline` fails, `refresh_dashboard` is skipped; `validate_source` is
# MAGIC   already complete and won't rerun on a retry

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4 — Build the Job in the Databricks Jobs UI
# MAGIC
# MAGIC Now you'll create the three-task job by hand in the UI.  This gives you a feel for
# MAGIC the job structure before you see it expressed as code in Part 5.
# MAGIC
# MAGIC The pipeline and dashboard were already created during Setup — you'll reference
# MAGIC them by name when adding tasks.
# MAGIC
# MAGIC ### Step 4a — Create a new job
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar, then the **Jobs** tab
# MAGIC 2. Click **Create job**
# MAGIC 3. Name the job: **`Lakeflow Lab - Orchestration Job`**
# MAGIC
# MAGIC ### Step 4b — Add the three tasks
# MAGIC
# MAGIC Add each task in order:
# MAGIC
# MAGIC | Task key | Type | Configuration | Depends on |
# MAGIC |----------|------|---------------|------------|
# MAGIC | `validate_source` | **Notebook** | Path: `./validation/source_validation_notebook`, Source: Workspace | *(none — runs first)* |
# MAGIC | `run_pipeline` | **Pipeline** | Select: `Lakeflow Lab - Medallion Pipeline` | `validate_source` |
# MAGIC | `refresh_dashboard` | **Dashboard** | Select: `Lakeflow Lab - Sales Dashboard` | `run_pipeline` |
# MAGIC
# MAGIC > **Tip:** After adding each task (except the first), use the **"Depends on"** dropdown
# MAGIC > to wire up the dependency.  The canvas should show a linear chain of three boxes.
# MAGIC
# MAGIC ### Step 4c — Add job parameters, schedule, and notifications
# MAGIC
# MAGIC **Parameters** — Click the **Parameters** tab (at the job level) and add:
# MAGIC
# MAGIC | Name | Default value |
# MAGIC |------|---------------|
# MAGIC | `run_date` | `2024-03-31` |
# MAGIC | `catalog` | `workspace` |
# MAGIC | `schema` | `lakeflow_lab` |
# MAGIC
# MAGIC **Schedule — run daily at 6 AM:**
# MAGIC 1. Click the **Schedules & Triggers** tab
# MAGIC 2. Click **Add a schedule**
# MAGIC 3. Set: **Every day at 06:00 AM** (or cron: `0 0 6 * * ?`)
# MAGIC 4. Click **Save**
# MAGIC
# MAGIC **Email notification on failure:**
# MAGIC 1. Click the **Notifications** tab
# MAGIC 2. Under **On failure**, add your email address
# MAGIC 3. Click **Save**
# MAGIC
# MAGIC ### Step 4d — Verify the DAG
# MAGIC
# MAGIC Click the **Tasks** canvas.  You should see three boxes connected left-to-right:
# MAGIC `validate_source` → `run_pipeline` → `refresh_dashboard`.
# MAGIC
# MAGIC ### Step 4e — Run the job
# MAGIC
# MAGIC Click **Run now** to trigger a test run.  Watch the progression:
# MAGIC
# MAGIC 1. `validate_source` runs first — a quick notebook checking that source data exists
# MAGIC 2. `run_pipeline` runs next — the Lakeflow Declarative Pipeline processes all three
# MAGIC    medallion layers (you can click into it to watch the DLT pipeline graph)
# MAGIC 3. `refresh_dashboard` runs last — refreshing the dashboard with the new data
# MAGIC
# MAGIC Once all three tasks show green checkmarks, open the Sales Dashboard to see
# MAGIC the visualizations populated with data.  Proceed to Part 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5 — Databricks Asset Bundles & Exporting to `databricks.yml`
# MAGIC
# MAGIC ### What is a Databricks Asset Bundle?
# MAGIC
# MAGIC A **Databricks Asset Bundle (DAB)** is a YAML-based project format that lets you define,
# MAGIC version-control, and deploy Databricks resources — jobs, pipelines, dashboards, and more —
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
# MAGIC resources:        # Declare pipelines, dashboards, jobs, etc. here
# MAGIC   pipelines: ...
# MAGIC   dashboards: ...
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
# MAGIC 2. Click the **kebab menu** (three dots) in the top-right of the job page
# MAGIC 3. Select **Export** → **Download DABs configuration**
# MAGIC 4. Open the downloaded `.yml` file — it contains the full job definition in DABs format
# MAGIC
# MAGIC > **What you'll see:** The exported YAML has a similar structure to
# MAGIC > `resources/job_definition_template.yml` — tasks, `depends_on`, parameters, schedules,
# MAGIC > and notification settings are all represented as YAML keys.  The export includes the
# MAGIC > job definition but not the pipeline or dashboard resources — you'll add those from
# MAGIC > the template.
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
# MAGIC three resource definitions: a pipeline, a dashboard, and a job.
# MAGIC
# MAGIC You can either:
# MAGIC - **Use the annotated template** in `resources/job_definition_template.yml` — copy
# MAGIC   everything from `resources:` onward and paste it into `databricks.yml`
# MAGIC - **Build it yourself** using the exported YAML from Step 5a as a starting point,
# MAGIC   adding the pipeline and dashboard resources manually
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
# MAGIC     resources:                              # per-target resource overrides
# MAGIC       dashboards:
# MAGIC         sales_dashboard:
# MAGIC           file_path: ./dashboards/sales_dashboard_dev.lvdash.json
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab
# MAGIC
# MAGIC resources:
# MAGIC
# MAGIC   pipelines:
# MAGIC     medallion_pipeline:
# MAGIC       name: "Lakeflow Lab - Medallion Pipeline [${bundle.target}]"
# MAGIC       catalog: ${var.catalog}
# MAGIC       target: ${var.schema}
# MAGIC       configuration:
# MAGIC         volume_path: /Volumes/${var.catalog}/${var.schema}/raw_data
# MAGIC       libraries:
# MAGIC         - file:
# MAGIC             path: ./pipeline/medallion_pipeline.py
# MAGIC
# MAGIC   dashboards:
# MAGIC     sales_dashboard:
# MAGIC       display_name: "Lakeflow Lab - Sales Dashboard [${bundle.target}]"
# MAGIC       file_path: ./dashboards/sales_dashboard.lvdash.json
# MAGIC
# MAGIC   jobs:
# MAGIC     lakeflow_lab_job:
# MAGIC       name: "Lakeflow Lab - Orchestration Job [${bundle.target}]"
# MAGIC
# MAGIC       parameters:
# MAGIC         - name: run_date
# MAGIC           default: "2024-03-31"
# MAGIC         - name: catalog
# MAGIC           default: ${var.catalog}
# MAGIC         - name: schema
# MAGIC           default: ${var.schema}
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
# MAGIC         - task_key: validate_source
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./validation/source_validation_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_pipeline
# MAGIC           depends_on:
# MAGIC             - task_key: validate_source
# MAGIC           pipeline_task:
# MAGIC             pipeline_id: ${resources.pipelines.medallion_pipeline.id}
# MAGIC
# MAGIC         - task_key: refresh_dashboard
# MAGIC           depends_on:
# MAGIC             - task_key: run_pipeline
# MAGIC           dashboard_task:
# MAGIC             dashboard_id: ${resources.dashboards.sales_dashboard.id}
# MAGIC ```
# MAGIC
# MAGIC **Things to notice:**
# MAGIC - The **pipeline resource** sets `catalog` and `target` (schema) using bundle variables —
# MAGIC   deploying to `dev` writes tables to `lakeflow_lab_dev`; deploying to `prod` writes
# MAGIC   to `lakeflow_lab`
# MAGIC - The **dashboard resource** defaults to the prod `.lvdash.json` file, but the `dev`
# MAGIC   target overrides `file_path` to use `sales_dashboard_dev.lvdash.json` — which
# MAGIC   queries `lakeflow_lab_dev` tables.  This means the dev job refreshes a dev
# MAGIC   dashboard, and the prod job refreshes a prod dashboard.
# MAGIC - The **job's pipeline task** references the pipeline by ID using
# MAGIC   `${resources.pipelines.medallion_pipeline.id}` — DABs resolves this automatically
# MAGIC - The **job's dashboard task** similarly references
# MAGIC   `${resources.dashboards.sales_dashboard.id}`
# MAGIC - Replace `your-email@example.com` with your actual email address
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
# MAGIC 1. **Syncs files** — uploads your pipeline code, dashboard definition, and notebooks
# MAGIC    to the workspace under `${workspace.root_path}/files/`
# MAGIC 2. **Creates or updates resources** — creates the pipeline, dashboard, and job in your
# MAGIC    workspace (or updates them if they already exist)
# MAGIC 3. **Prefixes names** — in `dev` mode, resource names are prefixed with
# MAGIC    `[dev your@email.com]` so your dev resources don't collide with colleagues'
# MAGIC
# MAGIC In the same terminal you opened in Step 5d, run:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy --target dev
# MAGIC ```
# MAGIC
# MAGIC You should see output confirming that files were uploaded and three resources were
# MAGIC created or updated (pipeline, dashboard, job).  Once it completes, proceed to Part 7.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7 — Run the Bundle Job & Promote to Prod
# MAGIC
# MAGIC ### Explore the deployed job in the UI
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar
# MAGIC 2. Find **`[dev your@email.com] Lakeflow Lab - Orchestration Job [dev]`** and open it
# MAGIC 3. Click the **Tasks** tab — you should see three tasks connected by arrows:
# MAGIC    `validate_source` → `run_pipeline` → `refresh_dashboard`
# MAGIC 4. Click the **Parameters** tab — confirm `schema` defaults to `lakeflow_lab_dev`
# MAGIC    (the dev target variable was baked in at deploy time)
# MAGIC
# MAGIC ### Run with the default parameters
# MAGIC
# MAGIC Click **Run now** and watch the tasks execute one at a time.  This run:
# MAGIC - Validates the source data in `workspace.lakeflow_lab_dev`
# MAGIC - Runs the medallion pipeline writing to `workspace.lakeflow_lab_dev`
# MAGIC - Refreshes the sales dashboard

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
# MAGIC # Deploy all resources to the prod target
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Run the job with the default parameters
# MAGIC databricks bundle run lakeflow_lab_job --target prod
# MAGIC ```
# MAGIC
# MAGIC > **Note:** The prod target has no `mode: development`, so resource names are not
# MAGIC > prefixed and the resources will be visible to everyone with access to the workspace.
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
# MAGIC  ├── databricks.yml              (bundle config — pipelines, dashboards, jobs)
# MAGIC  ├── pipeline/
# MAGIC  │   └── medallion_pipeline.py   (DLT: bronze → silver → gold)
# MAGIC  ├── validation/
# MAGIC  │   └── source_validation_notebook.py
# MAGIC  └── dashboards/
# MAGIC      └── sales_dashboard.lvdash.json
# MAGIC
# MAGIC  databricks bundle deploy --target dev   → schema = lakeflow_lab_dev
# MAGIC  databricks bundle deploy --target prod  → schema = lakeflow_lab
# MAGIC  └── Creates:
# MAGIC        Pipeline  : Lakeflow Lab - Medallion Pipeline
# MAGIC        Dashboard : Lakeflow Lab - Sales Dashboard
# MAGIC        Job       : Lakeflow Lab - Orchestration Job
# MAGIC          │
# MAGIC          ├── Task 1: validate_source   (notebook — pre-flight gate)
# MAGIC          ├── Task 2: run_pipeline       (pipeline — medallion ETL)
# MAGIC          └── Task 3: refresh_dashboard  (dashboard — update visuals)
# MAGIC ```
# MAGIC
# MAGIC ### What to explore next
# MAGIC
# MAGIC - **CI/CD pipeline** — trigger `bundle deploy --target prod` from GitHub Actions or
# MAGIC   Azure DevOps on every merge to `main` for fully automated promotion
# MAGIC - **Staging target** — add a third `staging` target between dev and prod with its own
# MAGIC   schema, giving you a three-tier promotion pipeline
# MAGIC - **Fan-out dependencies** — add a second downstream task that also `depends_on: run_pipeline`
# MAGIC   to run two tasks in parallel after the pipeline completes
# MAGIC - **More task types** — add a SQL query task or a dbt task to the job to see how
# MAGIC   Lakeflow Jobs can orchestrate diverse workloads in a single DAG

# COMMAND ----------
