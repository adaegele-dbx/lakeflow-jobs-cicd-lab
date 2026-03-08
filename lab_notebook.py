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
# MAGIC | **Setup** | Create Unity Catalog schemas, volumes, and sample data; publish a dashboard; create a SQL alert |
# MAGIC | **Part 1** | Explore the **four medallion ETL notebooks** (bronze, silver, gold sales, gold products) |
# MAGIC | **Part 2** | Explore the **sales dashboard** |
# MAGIC | **Part 3** | Learn **Lakeflow Jobs** concepts — task types, parameters, dependencies, and fan-out/fan-in DAGs |
# MAGIC | **Part 4** | Build the six-task job in the **Databricks Jobs UI** and run a first test |
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
# MAGIC ### Setup — Publish the Sales Dashboard
# MAGIC
# MAGIC Create the dashboard that the job will refresh as one of its tasks.
# MAGIC
# MAGIC 1. In the workspace file browser, navigate to your Git folder
# MAGIC 2. Open the `dashboards/` folder
# MAGIC 3. Click on **`sales_dashboard.lvdash.json`** — Databricks opens it as a dashboard
# MAGIC 4. Click **Publish** in the top-right corner to make it available as a job task
# MAGIC
# MAGIC > **Note:** The dashboard queries reference `workspace.lakeflow_lab` (the prod schema).
# MAGIC > The tables won't exist until the ETL notebooks run for the first time, so the dashboard
# MAGIC > will show empty or error states until then — that's expected.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — Create the Pipeline Health Alert
# MAGIC
# MAGIC Create a SQL alert that the job will evaluate as its final task.  If the pipeline
# MAGIC data is unhealthy, the alert fires and notifies you.
# MAGIC
# MAGIC **1. Create a new SQL query:**
# MAGIC
# MAGIC 1. Click **SQL Editor** in the left sidebar
# MAGIC 2. Click **+** → **Create new query**
# MAGIC 3. Name the query: **`Pipeline Health Check`**
# MAGIC 4. Paste the following SQL and run it (select any SQL warehouse):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   current_timestamp()                              AS checked_at,
# MAGIC   b.cnt                                            AS bronze_rows,
# MAGIC   s.cnt                                            AS silver_rows,
# MAGIC   ROUND(s.cnt / NULLIF(b.cnt, 0) * 100, 1)        AS quality_pass_pct,
# MAGIC   gr.cnt                                           AS gold_region_rows,
# MAGIC   gp.cnt                                           AS gold_product_rows,
# MAGIC   ROUND(gr.revenue, 2)                             AS total_revenue,
# MAGIC   CASE
# MAGIC     WHEN b.cnt = 0           THEN 'EMPTY SOURCE'
# MAGIC     WHEN s.cnt / b.cnt < 0.5 THEN 'HIGH DROP RATE'
# MAGIC     ELSE 'HEALTHY'
# MAGIC   END                                              AS pipeline_status
# MAGIC FROM
# MAGIC   (SELECT COUNT(*) AS cnt FROM workspace.lakeflow_lab.bronze_orders) b,
# MAGIC   (SELECT COUNT(*) AS cnt FROM workspace.lakeflow_lab.silver_orders) s,
# MAGIC   (SELECT COUNT(*) AS cnt, SUM(total_revenue) AS revenue
# MAGIC    FROM workspace.lakeflow_lab.gold_sales_by_region) gr,
# MAGIC   (SELECT COUNT(*) AS cnt FROM workspace.lakeflow_lab.gold_top_products) gp
# MAGIC ```
# MAGIC
# MAGIC > **Note:** The query will fail right now because the tables don't exist yet — that's
# MAGIC > expected.  Save it anyway; the alert will work once the ETL runs in Part 4.
# MAGIC
# MAGIC **2. Create the alert:**
# MAGIC
# MAGIC 1. Click **Alerts** in the left sidebar
# MAGIC 2. Click **+ Create alert**
# MAGIC 3. Select the **`Pipeline Health Check`** query you just saved
# MAGIC 4. Configure the alert condition:
# MAGIC    - **Column:** `pipeline_status`
# MAGIC    - **Condition:** **is not equal to**
# MAGIC    - **Value:** `HEALTHY`
# MAGIC 5. Name the alert: **`Pipeline Health Alert`**
# MAGIC 6. Under **Destinations**, add your email address as a notification destination
# MAGIC 7. Click **Create alert**
# MAGIC
# MAGIC > **How it works:** When the job evaluates this alert task, it re-runs the query and
# MAGIC > checks the condition.  If `pipeline_status` is anything other than `HEALTHY`, the alert
# MAGIC > triggers and sends you a notification.  If the pipeline is healthy, it completes silently.
# MAGIC
# MAGIC **3. Copy the alert ID** — you'll need this in Parts 4 and 5:
# MAGIC
# MAGIC Open the alert you just created.  The alert ID is in the URL:
# MAGIC `https://<workspace>/sql/alerts/<alert-id>`.  Copy it and save it for later.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The Medallion ETL Notebooks
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC The medallion architecture organises data into three progressively refined layers.
# MAGIC In this lab, each layer is its own notebook — giving you **four separate tasks** that
# MAGIC the job will orchestrate:
# MAGIC
# MAGIC ```
# MAGIC  /Volumes/workspace/lakeflow_lab/raw_data/orders.csv
# MAGIC           |
# MAGIC           v   Bronze
# MAGIC  +------------------+
# MAGIC  |  bronze_orders   |  Raw CSV ingested as strings
# MAGIC  +--------+---------+
# MAGIC           |   Silver
# MAGIC           |   type casts, quality filters, deduplication, derived fields
# MAGIC           v
# MAGIC  +------------------+
# MAGIC  |  silver_orders   |  Cleaned, typed, enriched data
# MAGIC  +--------+---------+
# MAGIC           |   Gold
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
# MAGIC Open each notebook from the `pipeline/` folder in a new tab and read through them:
# MAGIC
# MAGIC | Notebook | File | What it does |
# MAGIC |----------|------|-------------|
# MAGIC | **Bronze** | `pipeline/bronze_notebook.py` | Reads raw CSV with `inferSchema=false` (all strings), writes `bronze_orders` |
# MAGIC | **Silver** | `pipeline/silver_notebook.py` | Casts types, adds `total_amount` and `year_month`, drops duplicates, filters bad rows → `silver_orders` |
# MAGIC | **Gold Sales** | `pipeline/gold_sales_notebook.py` | Aggregates completed orders by region and month → `gold_sales_by_region` |
# MAGIC | **Gold Products** | `pipeline/gold_products_notebook.py` | Aggregates completed orders by product and category → `gold_top_products` |
# MAGIC
# MAGIC As you read, notice:
# MAGIC
# MAGIC - **`dbutils.widgets`** at the top of every notebook — each receives `catalog` and `schema`
# MAGIC   as job parameters, so the same notebook works interactively or as a job task
# MAGIC - Each notebook reads from the **output of the previous layer** (bronze reads CSV, silver
# MAGIC   reads bronze table, both golds read silver table)
# MAGIC - Each notebook **overwrites** its output table, making re-runs idempotent
# MAGIC - The two gold notebooks are **independent of each other** — they both read from silver
# MAGIC   but don't depend on one another, which means they can run **in parallel** as job tasks
# MAGIC
# MAGIC > **Key takeaway:** Splitting each layer into its own notebook lets the job run the two
# MAGIC > gold tasks in parallel (fan-out), which is faster and demonstrates a more realistic DAG
# MAGIC > pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — The Sales Dashboard
# MAGIC
# MAGIC **File:** `dashboards/sales_dashboard.lvdash.json`
# MAGIC
# MAGIC You published this dashboard in the Setup step above.  Open it from the workspace
# MAGIC sidebar or click the `.lvdash.json` file again.
# MAGIC
# MAGIC The dashboard queries the **gold-layer tables** produced by the ETL notebooks:
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
# MAGIC > don't exist yet.  They'll be populated when the ETL notebooks run for the first time
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
# MAGIC | Task type | What it does | Our tasks |
# MAGIC |-----------|-------------|----------|
# MAGIC | **Notebook** | Runs a notebook with parameters via `dbutils.widgets` | `run_bronze`, `run_silver`, `run_gold_sales`, `run_gold_products` |
# MAGIC | **Dashboard** | Refreshes an AI/BI Lakeview dashboard | `refresh_dashboard` |
# MAGIC | **SQL alert** | Evaluates a SQL alert and notifies if the condition is met | `check_pipeline_health` |
# MAGIC
# MAGIC Other task types you might use in production: pipelines (DLT), dbt projects, JAR
# MAGIC tasks, Python scripts, and even other jobs (via `run_job` tasks).
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
# MAGIC | **Consumed by** | YAML expressions (`${var.catalog}`) | Notebooks (`dbutils.widgets.get("catalog")`) |
# MAGIC
# MAGIC > **Connecting the two:** In Part 5 you'll set the `schema` job parameter's default
# MAGIC > value to `${var.schema}`.  That single change makes the schema environment-aware —
# MAGIC > deploying to `dev` automatically targets `lakeflow_lab_dev`, and deploying to `prod`
# MAGIC > automatically targets `lakeflow_lab`.
# MAGIC
# MAGIC In our job we'll declare two parameters:
# MAGIC
# MAGIC | Parameter | Default | Purpose |
# MAGIC |-----------|---------|---------|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog name |
# MAGIC | `schema` | `lakeflow_lab` | Unity Catalog schema name |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Task Dependencies & Fan-Out / Fan-In
# MAGIC
# MAGIC A Lakeflow Job models its tasks as a **DAG (directed acyclic graph)**.  Each task
# MAGIC can declare which other tasks must succeed before it is allowed to start.
# MAGIC
# MAGIC Our job demonstrates a **fan-out / fan-in** pattern:
# MAGIC
# MAGIC ```
# MAGIC  run_bronze          (notebook)
# MAGIC       |
# MAGIC       v
# MAGIC  run_silver          (notebook)
# MAGIC     /    \                        <-- fan-out: two tasks start in parallel
# MAGIC    v      v
# MAGIC  run_gold_sales   run_gold_products
# MAGIC  (notebook)       (notebook)
# MAGIC    \      /                       <-- fan-in: both must finish before next
# MAGIC     v    v
# MAGIC  refresh_dashboard  (dashboard)
# MAGIC       |
# MAGIC       v
# MAGIC  check_pipeline_health  (SQL alert)
# MAGIC ```
# MAGIC
# MAGIC **Key behaviors:**
# MAGIC - **Fan-out:** Both gold tasks depend on `run_silver` — once silver completes, both
# MAGIC   gold tasks start **simultaneously**, reducing total job duration
# MAGIC - **Fan-in:** `refresh_dashboard` depends on **both** gold tasks — it waits for both
# MAGIC   to finish before starting
# MAGIC - **Failure propagation:** If any task fails, all downstream tasks are automatically
# MAGIC   **skipped** — you can retry the failed task without rerunning earlier ones

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4 — Build the Job in the Databricks Jobs UI
# MAGIC
# MAGIC Now you'll create the six-task job by hand in the UI.  This gives you a feel for
# MAGIC the job structure before you see it expressed as code in Part 5.
# MAGIC
# MAGIC The dashboard was already published during Setup — you'll reference it by name
# MAGIC when adding the dashboard task.
# MAGIC
# MAGIC ### Step 4a — Create a new job
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar, then the **Jobs** tab
# MAGIC 2. Click **Create job**
# MAGIC 3. Name the job: **`Lakeflow Lab - Orchestration Job`**
# MAGIC
# MAGIC ### Step 4b — Add the six tasks
# MAGIC
# MAGIC Add each task in order.  For notebook tasks, set **Source: Workspace** and browse
# MAGIC to the notebook path.  For the dashboard task, select the published dashboard.
# MAGIC For the SQL task, paste the query and select a SQL warehouse.
# MAGIC
# MAGIC | Task key | Type | Configuration | Depends on |
# MAGIC |----------|------|---------------|------------|
# MAGIC | `run_bronze` | **Notebook** | Path: `./pipeline/bronze_notebook` | *(none — runs first)* |
# MAGIC | `run_silver` | **Notebook** | Path: `./pipeline/silver_notebook` | `run_bronze` |
# MAGIC | `run_gold_sales` | **Notebook** | Path: `./pipeline/gold_sales_notebook` | `run_silver` |
# MAGIC | `run_gold_products` | **Notebook** | Path: `./pipeline/gold_products_notebook` | `run_silver` |
# MAGIC | `refresh_dashboard` | **Dashboard** | Select: `Lakeflow Lab - Sales Dashboard` | `run_gold_sales`, `run_gold_products` |
# MAGIC | `check_pipeline_health` | **SQL alert** | Select: `Pipeline Health Alert`; select a SQL warehouse | `refresh_dashboard` |
# MAGIC
# MAGIC > **Tip:** When wiring dependencies, notice that both gold tasks depend on `run_silver`
# MAGIC > (fan-out) and `refresh_dashboard` depends on **both** gold tasks (fan-in).  The canvas
# MAGIC > should show a diamond shape in the middle.
# MAGIC
# MAGIC > **SQL alert task:** When adding this task, select the **SQL** task type, then choose
# MAGIC > **Alert** (not Query).  Select the **`Pipeline Health Alert`** you created during Setup
# MAGIC > and choose a SQL warehouse to run it on.
# MAGIC
# MAGIC ### Step 4c — Add job parameters, schedule, and notifications
# MAGIC
# MAGIC **Parameters** — Click the **Parameters** tab (at the job level) and add:
# MAGIC
# MAGIC | Name | Default value |
# MAGIC |------|---------------|
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
# MAGIC Click the **Tasks** canvas.  You should see six boxes with a diamond-shaped fan-out/
# MAGIC fan-in pattern: `run_bronze` → `run_silver` → two gold tasks in parallel →
# MAGIC `refresh_dashboard` → `check_pipeline_health`.
# MAGIC
# MAGIC ### Step 4e — Run the job
# MAGIC
# MAGIC Click **Run now** to trigger a test run.  Watch the progression:
# MAGIC
# MAGIC 1. `run_bronze` runs first — ingests CSV into `bronze_orders`
# MAGIC 2. `run_silver` runs next — cleanses and transforms into `silver_orders`
# MAGIC 3. `run_gold_sales` and `run_gold_products` run **in parallel** — fan-out!
# MAGIC 4. `refresh_dashboard` waits for both golds, then refreshes the dashboard — fan-in!
# MAGIC 5. `check_pipeline_health` evaluates the SQL alert — if healthy, it passes silently;
# MAGIC    if unhealthy, it triggers a notification to your email
# MAGIC
# MAGIC Once all six tasks show green checkmarks, open the Sales Dashboard to see
# MAGIC the visualizations populated with data.  The alert task should show `HEALTHY` since
# MAGIC the data is clean.  Proceed to Part 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5 — Databricks Asset Bundles & Exporting to `databricks.yml`
# MAGIC
# MAGIC ### What is a Databricks Asset Bundle?
# MAGIC
# MAGIC A **Databricks Asset Bundle (DAB)** is a YAML-based project format that lets you define,
# MAGIC version-control, and deploy Databricks resources — jobs, dashboards, and more —
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
# MAGIC   warehouse_id:
# MAGIC     description: SQL warehouse ID for the alert task
# MAGIC   alert_id:
# MAGIC     description: SQL alert ID for the pipeline health check
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
# MAGIC resources:        # Declare dashboards, jobs, etc. here
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
# MAGIC > job definition but not the dashboard resource — you'll add that from the template.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5b — Inspect the current `databricks.yml`
# MAGIC
# MAGIC Open `databricks.yml` from the file tree on the left (click the file to view it).
# MAGIC
# MAGIC Notice it already has a `variables` section (`catalog`, `schema`, `warehouse_id`, `alert_id`) and a
# MAGIC `targets` section with both `dev` and `prod` entries.  The `dev` target overrides `schema`
# MAGIC to `lakeflow_lab_dev`; the `prod` target inherits the default `lakeflow_lab`.
# MAGIC Your task in Step 5c is to fill in the empty `resources:` section.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5c — Fill in `databricks.yml`
# MAGIC
# MAGIC Right now `databricks.yml` has an empty `resources:` section.  Your task is to add
# MAGIC two resource definitions: a dashboard and a job.
# MAGIC
# MAGIC You can either:
# MAGIC - **Use the annotated template** in `resources/job_definition_template.yml` — copy
# MAGIC   everything from `resources:` onward and paste it into `databricks.yml`
# MAGIC - **Build it yourself** using the exported YAML from Step 5a as a starting point,
# MAGIC   adding the dashboard resource and alert task manually
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
# MAGIC   warehouse_id:
# MAGIC     description: SQL warehouse ID for the alert task
# MAGIC   alert_id:
# MAGIC     description: SQL alert ID for the pipeline health check
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
# MAGIC         - task_key: run_bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/bronze_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_silver
# MAGIC           depends_on:
# MAGIC             - task_key: run_bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/silver_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_gold_sales
# MAGIC           depends_on:
# MAGIC             - task_key: run_silver
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/gold_sales_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_gold_products
# MAGIC           depends_on:
# MAGIC             - task_key: run_silver
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/gold_products_notebook
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: refresh_dashboard
# MAGIC           depends_on:
# MAGIC             - task_key: run_gold_sales
# MAGIC             - task_key: run_gold_products
# MAGIC           dashboard_task:
# MAGIC             dashboard_id: ${resources.dashboards.sales_dashboard.id}
# MAGIC
# MAGIC         - task_key: check_pipeline_health
# MAGIC           depends_on:
# MAGIC             - task_key: refresh_dashboard
# MAGIC           sql_task:
# MAGIC             warehouse_id: ${var.warehouse_id}
# MAGIC             alert:
# MAGIC               alert_id: ${var.alert_id}
# MAGIC ```
# MAGIC
# MAGIC **Things to notice:**
# MAGIC - `${var.schema}` in the job parameter default is resolved at **deploy time** — deploying
# MAGIC   to `dev` bakes in `lakeflow_lab_dev`; deploying to `prod` bakes in `lakeflow_lab`
# MAGIC - Job parameters are pushed down automatically to all notebook tasks at **run time**
# MAGIC - The **dashboard resource** defaults to the prod `.lvdash.json` file, but the `dev`
# MAGIC   target overrides `file_path` to use `sales_dashboard_dev.lvdash.json` — which
# MAGIC   queries `lakeflow_lab_dev` tables
# MAGIC - The **job's dashboard task** references the dashboard by ID using
# MAGIC   `${resources.dashboards.sales_dashboard.id}` — DABs resolves this automatically
# MAGIC - The **alert task** references the SQL alert by ID using `${var.alert_id}` — this is
# MAGIC   the alert you created during Setup, which checks pipeline health and notifies you
# MAGIC   if the data is unhealthy
# MAGIC - The alert task needs a `warehouse_id` and `alert_id` — you'll provide these when
# MAGIC   validating the bundle
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
# MAGIC databricks bundle validate --target dev \
# MAGIC   --var warehouse_id=<your-warehouse-id> \
# MAGIC   --var alert_id=<your-alert-id>
# MAGIC ```
# MAGIC
# MAGIC > **Finding your warehouse ID:** Go to **SQL Warehouses** in the sidebar, click on a
# MAGIC > warehouse, and copy the ID from the URL or the **Connection details** tab.
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
# MAGIC 1. **Syncs files** — uploads your notebooks and dashboard definition
# MAGIC    to the workspace under `${workspace.root_path}/files/`
# MAGIC 2. **Creates or updates resources** — creates the dashboard and job in your
# MAGIC    workspace (or updates them if they already exist)
# MAGIC 3. **Prefixes names** — in `dev` mode, resource names are prefixed with
# MAGIC    `[dev your@email.com]` so your dev resources don't collide with colleagues'
# MAGIC
# MAGIC In the same terminal you opened in Step 5d, run:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy --target dev \
# MAGIC   --var warehouse_id=<your-warehouse-id> \
# MAGIC   --var alert_id=<your-alert-id>
# MAGIC ```
# MAGIC
# MAGIC You should see output confirming that files were uploaded and two resources were
# MAGIC created or updated (dashboard, job).  Once it completes, proceed to Part 7.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7 — Run the Bundle Job & Promote to Prod
# MAGIC
# MAGIC ### Explore the deployed job in the UI
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar
# MAGIC 2. Find **`[dev your@email.com] Lakeflow Lab - Orchestration Job [dev]`** and open it
# MAGIC 3. Click the **Tasks** tab — you should see six tasks with the fan-out/fan-in diamond
# MAGIC    pattern: `run_bronze` → `run_silver` → two gold tasks → `refresh_dashboard` →
# MAGIC    `check_pipeline_health`
# MAGIC 4. Click the **Parameters** tab — confirm `schema` defaults to `lakeflow_lab_dev`
# MAGIC    (the dev target variable was baked in at deploy time)
# MAGIC
# MAGIC ### Run with the default parameters
# MAGIC
# MAGIC Click **Run now** and watch the tasks execute.  This run:
# MAGIC - Ingests CSV into `workspace.lakeflow_lab_dev.bronze_orders`
# MAGIC - Cleanses into `workspace.lakeflow_lab_dev.silver_orders`
# MAGIC - Fans out to build both gold tables in parallel
# MAGIC - Refreshes the dev sales dashboard
# MAGIC - Evaluates the pipeline health alert — passes silently if healthy

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
# MAGIC databricks bundle deploy --target prod \
# MAGIC   --var warehouse_id=<your-warehouse-id> \
# MAGIC   --var alert_id=<your-alert-id>
# MAGIC
# MAGIC # Run the job with the default parameters
# MAGIC databricks bundle run lakeflow_lab_job --target prod
# MAGIC ```
# MAGIC
# MAGIC > **Note:** The prod target has no `mode: development`, so resource names are not
# MAGIC > prefixed and the resources will be visible to everyone with access to the workspace.
# MAGIC > Make sure everything is working correctly before promoting.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lab Complete
# MAGIC
# MAGIC Congratulations! You've built a complete CI/CD-ready data pipeline on Databricks:
# MAGIC
# MAGIC ```
# MAGIC  Git Repository
# MAGIC  ├── databricks.yml              (bundle config — dashboards, jobs)
# MAGIC  ├── pipeline/
# MAGIC  │   ├── bronze_notebook.py       (raw CSV ingestion)
# MAGIC  │   ├── silver_notebook.py       (cleanse & transform)
# MAGIC  │   ├── gold_sales_notebook.py   (regional aggregations)
# MAGIC  │   └── gold_products_notebook.py(product aggregations)
# MAGIC  └── dashboards/
# MAGIC      ├── sales_dashboard.lvdash.json       (prod)
# MAGIC      └── sales_dashboard_dev.lvdash.json   (dev)
# MAGIC
# MAGIC  databricks bundle deploy --target dev   → schema = lakeflow_lab_dev
# MAGIC  databricks bundle deploy --target prod  → schema = lakeflow_lab
# MAGIC  └── Creates:
# MAGIC        Dashboard : Lakeflow Lab - Sales Dashboard
# MAGIC        Job       : Lakeflow Lab - Orchestration Job
# MAGIC          │
# MAGIC          ├── Task 1: run_bronze          (notebook — raw ingestion)
# MAGIC          ├── Task 2: run_silver          (notebook — cleanse & transform)
# MAGIC          ├── Task 3: run_gold_sales      (notebook — regional aggs)  ← parallel
# MAGIC          ├── Task 4: run_gold_products   (notebook — product aggs)   ← parallel
# MAGIC          ├── Task 5: refresh_dashboard         (dashboard — update visuals)
# MAGIC          └── Task 6: check_pipeline_health   (SQL alert — data quality gate)
# MAGIC ```
# MAGIC
# MAGIC ### What to explore next
# MAGIC
# MAGIC - **CI/CD pipeline** — trigger `bundle deploy --target prod` from GitHub Actions or
# MAGIC   Azure DevOps on every merge to `main` for fully automated promotion
# MAGIC - **Staging target** — add a third `staging` target between dev and prod with its own
# MAGIC   schema, giving you a three-tier promotion pipeline
# MAGIC - **More task types** — add a DLT pipeline task, dbt task, or Python script task to the
# MAGIC   job to see how Lakeflow Jobs can orchestrate diverse workloads in a single DAG
# MAGIC - **Conditional tasks** — use `if/else` conditions on tasks to create branching logic
# MAGIC   based on the results of upstream tasks

# COMMAND ----------

