# Lakeflow Jobs & CI/CD with Databricks Asset Bundles

A hands-on lab for learning how to build, orchestrate, and deploy data pipelines on Databricks using **Lakeflow Jobs** and **Databricks Asset Bundles (DABs)** for CI/CD.

## What You'll Build

By the end of this lab you will have:
- Three **medallion notebooks** (bronze / silver / gold) that process raw data through a Delta Lake architecture
- A **source validation notebook** that gates the pipeline with a pre-flight check
- A **reporting notebook** that analyses the processed data and stamps a `run_date` parameter on its output
- A **five-task Lakeflow Job** with explicit `depends_on` task dependencies and job-level parameters
- Everything packaged and deployed via **Databricks Asset Bundles** for CI/CD

## Prerequisites

- A Databricks workspace (free tier works!)
- Unity Catalog enabled (enabled by default on all Databricks workspaces)

## Getting Started

### 1. Clone this repo as a Git Folder in your Databricks workspace

1. In your Databricks workspace, go to **Workspace** in the left sidebar
2. Click **Create** → **Git folder**
3. Paste this repository's URL
4. Click **Create Git folder**

### 2. Open the lab notebook

Navigate to `lab_notebook.py` in the cloned folder and open it. All lab instructions are inside.

---

## Repository Structure

```
lakeflow-jobs-and-ci-cd/
├── README.md
├── databricks.yml                   # DAB bundle config — fill in resources during the lab
├── lab_notebook.py                  # Central lab notebook — START HERE
│
├── pipeline/
│   ├── bronze_notebook.py           # Task 2: raw CSV → bronze_orders (Delta table)
│   ├── silver_notebook.py           # Task 3: cleanse & transform → silver_orders
│   └── gold_notebook.py             # Task 4: aggregate → gold_sales_by_region, gold_top_products
│
├── validation/
│   └── source_validation_notebook.py  # Task 1: pre-flight gate; demonstrates job parameters
│
├── analysis/
│   └── reporting_notebook.py        # Task 5: insights + run_date stamping; demonstrates job parameters
│
└── resources/
    └── job_definition_template.yml  # Annotated 5-task YAML to paste into databricks.yml
```

## Lab Outline

| Part | Topic |
|------|-------|
| **Setup** | Create schema, volume, and generate sample data |
| **Part 1** | Three medallion notebooks — run bronze, silver, gold interactively |
| **Part 2a** | Source validation notebook — job parameters via `dbutils.widgets` |
| **Part 2b** | Reporting notebook — `run_date` parameter stamped on output |
| **Part 3** | Databricks Asset Bundles, **job parameters**, and **task dependencies** |
| **Part 4** | Define the 5-task job in `databricks.yml` |
| **Part 5** | Deploy with `databricks bundle deploy` |
| **Part 6** | Run with custom parameters and verify |

## Job Task DAG

```
validate_source
     │  depends_on: —
     ▼
bronze
     │  depends_on: validate_source
     ▼
silver
     │  depends_on: bronze
     ▼
gold
     │  depends_on: silver
     ▼
run_reporting
     depends_on: gold
```
