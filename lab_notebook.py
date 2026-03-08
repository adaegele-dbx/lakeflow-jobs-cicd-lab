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
# MAGIC | **Setup** | Create Unity Catalog schemas, volumes, and sample data; publish a dashboard |
# MAGIC | **Part 1** | Explore the **four medallion ETL notebooks** (bronze, silver, gold sales, gold products) |
# MAGIC | **Part 2** | Explore the **sales dashboard** |
# MAGIC | **Part 3** | Learn **Lakeflow Jobs** concepts — task types, parameters, dependencies, and fan-out/fan-in DAGs |
# MAGIC | **Part 4** | Build the five-task job in the **Databricks Jobs UI** targeting **dev** and run a first test |
# MAGIC | **Part 5** | Learn **Databricks Asset Bundles (DABs)**, configure dev and prod targets, fill in `databricks.yml` |
# MAGIC | **Part 6** | Deploy to **prod** with `databricks bundle deploy` from the terminal |
# MAGIC | **Part 7** | Run the bundle-managed **prod** job and compare both environments |
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
    print(f"  Written : {volume_path}")

    # Pre-create empty tables so the dashboard can be published with working visualizations.
    # The ETL notebooks will overwrite these on first run.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze_orders (
            order_id STRING, customer_id STRING, product STRING, category STRING,
            quantity STRING, unit_price STRING, order_date STRING, region STRING, status STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.silver_orders (
            order_id STRING, customer_id STRING, product STRING, category STRING,
            quantity INT, unit_price DOUBLE, order_date DATE, region STRING, status STRING,
            total_amount DOUBLE, year_month STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_sales_by_region (
            region STRING, year_month STRING, total_revenue DOUBLE,
            order_count LONG, avg_order_value DOUBLE, unique_customers LONG
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_top_products (
            product STRING, category STRING, total_revenue DOUBLE,
            total_units_sold LONG, order_count LONG, avg_unit_price DOUBLE
        ) USING DELTA
    """)
    print(f"  Tables  : bronze_orders, silver_orders, gold_sales_by_region, gold_top_products (empty placeholders)\n")

print("Sample rows:")
display(df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — Publish the Dev Sales Dashboard
# MAGIC
# MAGIC Create the **dev** dashboard that the manual job will refresh.  (The prod dashboard
# MAGIC will be deployed automatically by Databricks Asset Bundles in Part 6.)
# MAGIC
# MAGIC 1. In the workspace file browser, navigate to your Git folder
# MAGIC 2. Open the `dashboards/` folder
# MAGIC 3. Click on **`sales_dashboard_dev.lvdash.json`** — Databricks opens it as a dashboard
# MAGIC 4. Click **Publish** in the top-right corner to make it available as a job task
# MAGIC
# MAGIC > **Note:** This dashboard queries `workspace.lakeflow_lab_dev` (the dev schema).
# MAGIC > The tables are empty until the ETL notebooks run, so the dashboard will show zeros
# MAGIC > initially — that's expected. It will populate after the first job run.

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
# MAGIC  /Volumes/workspace/lakeflow_lab_dev/raw_data/orders.csv
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
# MAGIC **File:** `dashboards/sales_dashboard_dev.lvdash.json`
# MAGIC
# MAGIC You published this dev dashboard in the Setup step above.  Open it from the workspace
# MAGIC sidebar or click the `.lvdash.json` file again.
# MAGIC
# MAGIC The dashboard queries the **gold-layer tables** in the dev schema (`lakeflow_lab_dev`):
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
# MAGIC > **Note:** The dashboard shows empty/error states right now because the dev gold tables
# MAGIC > don't exist yet.  They'll be populated when the ETL notebooks run for the first time
# MAGIC > in Part 4.
# MAGIC
# MAGIC There's also a `sales_dashboard.lvdash.json` for prod (queries `lakeflow_lab`) — you'll
# MAGIC deploy that automatically via Databricks Asset Bundles in Part 6.

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
# MAGIC A Lakeflow Job can orchestrate many different task types.  Our job uses two:
# MAGIC
# MAGIC | Task type | What it does | Our tasks |
# MAGIC |-----------|-------------|----------|
# MAGIC | **Notebook** | Runs a notebook with parameters via `dbutils.widgets` | `run_bronze`, `run_silver`, `run_gold_sales`, `run_gold_products` |
# MAGIC | **Dashboard** | Refreshes an AI/BI Lakeview dashboard | `refresh_dashboard` |
# MAGIC
# MAGIC Other task types you might use in production: pipelines (DLT), SQL queries, SQL alerts,
# MAGIC dbt projects, JAR tasks, Python scripts, and even other jobs (via `run_job` tasks).
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
# MAGIC In our manual dev job we'll declare two parameters:
# MAGIC
# MAGIC | Parameter | Default | Purpose |
# MAGIC |-----------|---------|---------|
# MAGIC | `catalog` | `workspace` | Unity Catalog catalog name |
# MAGIC | `schema` | `lakeflow_lab_dev` | Unity Catalog schema name (dev environment) |
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
# MAGIC ## Part 4 — Build the Dev Job in the Databricks Jobs UI
# MAGIC
# MAGIC Now you'll create the five-task job by hand in the UI, targeting the **dev**
# MAGIC environment.  This gives you a feel for the job structure before you codify it
# MAGIC with Databricks Asset Bundles and promote to prod in Parts 5-7.
# MAGIC
# MAGIC The dev dashboard was already published during Setup — you'll reference it by name
# MAGIC when adding the dashboard task.
# MAGIC
# MAGIC ### Step 4a — Create a new job
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar, then the **Jobs** tab
# MAGIC 2. Click **Create job**
# MAGIC 3. Name the job: **`Lakeflow Lab - Orchestration Job [dev]`**
# MAGIC
# MAGIC ### Step 4b — Add the five tasks
# MAGIC
# MAGIC Add each task in order.  For notebook tasks, set **Source: Workspace** and browse
# MAGIC to the notebook path.  For the dashboard task, select the published dashboard.
# MAGIC
# MAGIC | Task key | Type | Configuration | Depends on |
# MAGIC |----------|------|---------------|------------|
# MAGIC | `run_bronze` | **Notebook** | Path: `./pipeline/bronze_notebook` | *(none — runs first)* |
# MAGIC | `run_silver` | **Notebook** | Path: `./pipeline/silver_notebook` | `run_bronze` |
# MAGIC | `run_gold_sales` | **Notebook** | Path: `./pipeline/gold_sales_notebook` | `run_silver` |
# MAGIC | `run_gold_products` | **Notebook** | Path: `./pipeline/gold_products_notebook` | `run_silver` |
# MAGIC | `refresh_dashboard` | **Dashboard** | Select the dev dashboard you published in Setup | `run_gold_sales`, `run_gold_products` |
# MAGIC
# MAGIC > **Tip:** When wiring dependencies, notice that both gold tasks depend on `run_silver`
# MAGIC > (fan-out) and `refresh_dashboard` depends on **both** gold tasks (fan-in).  The canvas
# MAGIC > should show a diamond shape in the middle.
# MAGIC
# MAGIC ### Step 4c — Add job parameters, schedule, and notifications
# MAGIC
# MAGIC **Parameters** — Click the **Parameters** tab (at the job level) and add:
# MAGIC
# MAGIC | Name | Default value |
# MAGIC |------|---------------|
# MAGIC | `catalog` | `workspace` |
# MAGIC | `schema` | `lakeflow_lab_dev` |
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
# MAGIC Click the **Tasks** canvas.  You should see five boxes with a diamond-shaped fan-out/
# MAGIC fan-in pattern: `run_bronze` → `run_silver` → two gold tasks in parallel →
# MAGIC `refresh_dashboard`.
# MAGIC
# MAGIC ### Step 4e — Run the job
# MAGIC
# MAGIC Click **Run now** to trigger a test run.  Watch the progression:
# MAGIC
# MAGIC 1. `run_bronze` runs first — ingests CSV into `lakeflow_lab_dev.bronze_orders`
# MAGIC 2. `run_silver` runs next — cleanses and transforms into `lakeflow_lab_dev.silver_orders`
# MAGIC 3. `run_gold_sales` and `run_gold_products` run **in parallel** — fan-out!
# MAGIC 4. `refresh_dashboard` waits for both golds, then refreshes the dev dashboard — fan-in!
# MAGIC
# MAGIC Once all five tasks show green checkmarks, open the dev Sales Dashboard to see
# MAGIC the visualizations populated with data.
# MAGIC
# MAGIC > **What you've done so far:** You manually built a complete dev pipeline.  In Parts 5-7
# MAGIC > you'll codify this as a Databricks Asset Bundle and promote it to **prod** — deploying
# MAGIC > the same job definition against `workspace.lakeflow_lab` with a single CLI command.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5 — Databricks Asset Bundles: From Dev to Prod as Code
# MAGIC
# MAGIC > **Note:** The instructions below use the **Databricks CLI** to validate, deploy, and
# MAGIC > run bundles from the terminal.  If you don't have the repo cloned locally or the CLI
# MAGIC > set up on your machine, don't worry — we'll also walk through how to perform DAB
# MAGIC > deployments directly in the **Databricks workspace UI** during the live session.
# MAGIC
# MAGIC ### What is a Databricks Asset Bundle?
# MAGIC
# MAGIC You just built a working dev job by hand in the UI.  Now imagine doing that again
# MAGIC for prod — and again every time you change the pipeline.  **Databricks Asset Bundles
# MAGIC (DABs)** solve this by letting you define jobs, dashboards, and more as YAML **code**
# MAGIC that you can deploy to any environment with a single CLI command.
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
# MAGIC     default: <TO DO: find the warehouse ID in the UI>
# MAGIC     description: SQL warehouse ID for the dashboard task
# MAGIC   dashboard_file_path:
# MAGIC     default: ./dashboards/sales_dashboard.lvdash.json
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab_dev   # dev writes to lakeflow_lab_dev
# MAGIC       dashboard_file_path: ./dashboards/sales_dashboard_dev.lvdash.json
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
# MAGIC > and notification settings are all represented as YAML keys.  The export captures your
# MAGIC > dev job definition — you'll generalize it with bundle variables so the same YAML works
# MAGIC > for both dev and prod.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5b — Inspect the current `databricks.yml`
# MAGIC
# MAGIC Open `databricks.yml` from the file tree on the left (click the file to view it).
# MAGIC
# MAGIC Notice it already has a `variables` section (`catalog`, `schema`, `warehouse_id`,
# MAGIC `dashboard_file_path`) and a `targets` section with both `dev` and `prod` entries.
# MAGIC The `dev` target overrides `schema` to `lakeflow_lab_dev` and `dashboard_file_path` to
# MAGIC the dev dashboard; the `prod` target inherits the defaults.
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
# MAGIC   adding the dashboard resource manually
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
# MAGIC     description: The Unity Catalog catalog to deploy resources into
# MAGIC   schema:
# MAGIC     default: lakeflow_lab
# MAGIC     description: The schema (database) to use for lab tables
# MAGIC   warehouse_id:
# MAGIC     default: <TO DO: find the warehouse ID in the UI>
# MAGIC     description: The SQL warehouse ID for the dashboard task (find under SQL Warehouses in the UI)
# MAGIC   dashboard_file_path:
# MAGIC     default: ./dashboards/sales_dashboard.lvdash.json
# MAGIC     description: The path to the dashboard definition file
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab_dev   # dev writes to its own isolated schema
# MAGIC       dashboard_file_path: ./dashboards/sales_dashboard_dev.lvdash.json
# MAGIC
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       schema: lakeflow_lab       # prod writes to the canonical schema
# MAGIC
# MAGIC resources:
# MAGIC
# MAGIC   dashboards:
# MAGIC     sales_dashboard:
# MAGIC       display_name: "Lakeflow Lab - Sales Dashboard [${bundle.target}]"
# MAGIC       file_path: ${var.dashboard_file_path}
# MAGIC       warehouse_id: ${var.warehouse_id}
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
# MAGIC             notebook_path: ./pipeline/bronze_notebook.py
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_silver
# MAGIC           depends_on:
# MAGIC             - task_key: run_bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/silver_notebook.py
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_gold_sales
# MAGIC           depends_on:
# MAGIC             - task_key: run_silver
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/gold_sales_notebook.py
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: run_gold_products
# MAGIC           depends_on:
# MAGIC             - task_key: run_silver
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./pipeline/gold_products_notebook.py
# MAGIC             source: WORKSPACE
# MAGIC
# MAGIC         - task_key: refresh_dashboard
# MAGIC           depends_on:
# MAGIC             - task_key: run_gold_sales
# MAGIC             - task_key: run_gold_products
# MAGIC           dashboard_task:
# MAGIC             dashboard_id: ${resources.dashboards.sales_dashboard.id}
# MAGIC ```
# MAGIC
# MAGIC **Things to notice:**
# MAGIC - `${var.schema}` in the job parameter default is resolved at **deploy time** — deploying
# MAGIC   to `dev` bakes in `lakeflow_lab_dev`; deploying to `prod` bakes in `lakeflow_lab`
# MAGIC - Job parameters are pushed down automatically to all notebook tasks at **run time**
# MAGIC - The **dashboard resource** uses `${var.dashboard_file_path}` — the `dev` target
# MAGIC   overrides this variable to `sales_dashboard_dev.lvdash.json` (which queries
# MAGIC   `lakeflow_lab_dev` tables), while prod uses the default `sales_dashboard.lvdash.json`
# MAGIC - The dashboard also uses `warehouse_id: ${var.warehouse_id}` — replace the TODO
# MAGIC   placeholder in `databricks.yml` with your actual warehouse ID
# MAGIC - The **job's dashboard task** references the dashboard by ID using
# MAGIC   `${resources.dashboards.sales_dashboard.id}` — DABs resolves this automatically
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
# MAGIC First, update the `warehouse_id` variable in `databricks.yml` — replace the
# MAGIC `<TO DO: find the warehouse ID in the UI>` placeholder with your actual warehouse ID.
# MAGIC
# MAGIC > **Finding your warehouse ID:** Go to **SQL Warehouses** in the sidebar, click on a
# MAGIC > warehouse, and copy the ID from the URL or the **Connection details** tab.
# MAGIC
# MAGIC Then validate:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate --target prod
# MAGIC ```
# MAGIC
# MAGIC > **Why `--target prod`?** You already have a working dev job from Part 4.  Now you're
# MAGIC > using DABs to deploy the same pipeline to prod — targeting `workspace.lakeflow_lab`.
# MAGIC
# MAGIC A valid bundle prints a JSON summary of the resolved resources with no errors.
# MAGIC Fix any YAML errors flagged before proceeding.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6 — Deploy to Prod with `databricks bundle deploy`
# MAGIC
# MAGIC ### What does `bundle deploy` do?
# MAGIC
# MAGIC 1. **Syncs files** — uploads your notebooks and dashboard definition
# MAGIC    to the workspace under `${workspace.root_path}/files/`
# MAGIC 2. **Creates or updates resources** — creates the dashboard and job in your
# MAGIC    workspace (or updates them if they already exist)
# MAGIC 3. **Resolves variables** — the `prod` target resolves `${var.schema}` to `lakeflow_lab`,
# MAGIC    so the prod job writes to the canonical production schema
# MAGIC
# MAGIC In the same terminal you opened in Step 5d, run:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC You should see output confirming that files were uploaded and two resources were
# MAGIC created or updated (dashboard, job).  Once it completes, proceed to Part 7.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7 — Run the Prod Job & Compare Environments
# MAGIC
# MAGIC ### Explore the deployed prod job in the UI
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar
# MAGIC 2. Find **`Lakeflow Lab - Orchestration Job [prod]`** and open it
# MAGIC 3. Click the **Tasks** tab — you should see five tasks with the fan-out/fan-in diamond
# MAGIC    pattern: `run_bronze` → `run_silver` → two gold tasks → `refresh_dashboard`
# MAGIC 4. Click the **Parameters** tab — confirm `schema` defaults to `lakeflow_lab`
# MAGIC    (the prod target variable was baked in at deploy time)
# MAGIC
# MAGIC ### Run the prod job
# MAGIC
# MAGIC From the terminal:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle run lakeflow_lab_job --target prod
# MAGIC ```
# MAGIC
# MAGIC Or click **Run now** in the UI.  This run:
# MAGIC - Ingests CSV into `workspace.lakeflow_lab.bronze_orders`
# MAGIC - Cleanses into `workspace.lakeflow_lab.silver_orders`
# MAGIC - Fans out to build both gold tables in parallel
# MAGIC - Refreshes the **prod** sales dashboard (deployed by DABs from `sales_dashboard.lvdash.json`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare dev and prod
# MAGIC
# MAGIC You now have **two independent environments** running the same pipeline:
# MAGIC
# MAGIC | | Dev (manual, Part 4) | Prod (DABs, Part 6) |
# MAGIC |---|---|---|
# MAGIC | **Job name** | `Lakeflow Lab - Orchestration Job [dev]` | `Lakeflow Lab - Orchestration Job [prod]` |
# MAGIC | **Schema** | `workspace.lakeflow_lab_dev` | `workspace.lakeflow_lab` |
# MAGIC | **Dashboard** | Dev dashboard (published manually) | Prod dashboard (deployed by DABs) |
# MAGIC | **Created by** | UI clicks | `databricks bundle deploy --target prod` |
# MAGIC
# MAGIC > **Key insight:** The dev job was built manually for quick iteration.  The prod job
# MAGIC > was deployed from code — version-controlled, repeatable, and promotable through a CI/CD
# MAGIC > pipeline.  In a real workflow, you'd iterate in dev, then run
# MAGIC > `databricks bundle deploy --target prod` (or trigger it from CI) to promote.

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
# MAGIC  Manual (Part 4)  → dev job → schema = lakeflow_lab_dev
# MAGIC  databricks bundle deploy --target prod  → schema = lakeflow_lab
# MAGIC  └── Creates:
# MAGIC        Dashboard : Lakeflow Lab - Sales Dashboard
# MAGIC        Job       : Lakeflow Lab - Orchestration Job
# MAGIC          │
# MAGIC          ├── Task 1: run_bronze          (notebook — raw ingestion)
# MAGIC          ├── Task 2: run_silver          (notebook — cleanse & transform)
# MAGIC          ├── Task 3: run_gold_sales      (notebook — regional aggs)   ← parallel
# MAGIC          ├── Task 4: run_gold_products   (notebook — product aggs)   ← parallel
# MAGIC          └── Task 5: refresh_dashboard   (dashboard — update visuals)
# MAGIC ```
# MAGIC
# MAGIC ### What to explore next
# MAGIC
# MAGIC - **DABs for dev too** — run `databricks bundle deploy --target dev` to manage the dev
# MAGIC   environment with DABs as well, replacing the manual job from Part 4
# MAGIC - **CI/CD pipeline** — trigger `bundle deploy --target prod` from GitHub Actions or
# MAGIC   Azure DevOps on every merge to `main` for fully automated promotion
# MAGIC - **Staging target** — add a third `staging` target between dev and prod with its own
# MAGIC   schema, giving you a three-tier promotion pipeline
# MAGIC - **More task types** — add a DLT pipeline task, dbt task, or Python script task to the
# MAGIC   job to see how Lakeflow Jobs can orchestrate diverse workloads in a single DAG
# MAGIC - **Conditional tasks** — use `if/else` conditions on tasks to create branching logic
# MAGIC   based on the results of upstream tasks

# COMMAND ----------

