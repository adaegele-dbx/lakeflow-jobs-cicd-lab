# Lakeflow Jobs & CI/CD with Databricks Asset Bundles

A hands-on lab for learning how to build, orchestrate, and deploy data pipelines on Databricks using **Lakeflow Jobs** and **Databricks Asset Bundles (DABs)** for CI/CD.

## What You'll Build

By the end of this lab you will have:
- A **four-notebook medallion ETL** implementing the full bronze / silver / gold architecture
- An **AI/BI Lakeview dashboard** that visualizes the gold-layer data
- A **five-task Lakeflow Job** using notebook and dashboard task types with a fan-out/fan-in DAG
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
├── databricks.yml                     # DAB bundle config — fill in resources during the lab
├── lab_notebook.py                    # Central lab notebook — START HERE
│
├── pipeline/
│   ├── bronze_notebook.py             # Task 1: raw CSV ingestion → bronze_orders
│   ├── silver_notebook.py             # Task 2: cleanse & transform → silver_orders
│   ├── gold_sales_notebook.py         # Task 3: regional aggregations → gold_sales_by_region
│   └── gold_products_notebook.py      # Task 4: product aggregations → gold_top_products
│
├── dashboards/
│   ├── sales_dashboard.lvdash.json      # AI/BI dashboard — queries prod (lakeflow_lab)
│   └── sales_dashboard_dev.lvdash.json  # AI/BI dashboard — queries dev (lakeflow_lab_dev)
│
└── resources/
    └── job_definition_template.yml    # Annotated YAML to paste into databricks.yml
```

## Lab Outline

| Part | Topic |
|------|-------|
| **Setup** | Create schemas, volumes, generate sample data, and publish dev dashboard |
| **Part 1** | Explore the four medallion ETL notebooks (bronze, silver, gold x2) |
| **Part 2** | Explore the sales dashboard |
| **Part 3** | Lakeflow Jobs concepts — task types, parameters, dependencies, and fan-out/fan-in |
| **Part 4** | Build the five-task job in the Databricks Jobs UI targeting **dev** |
| **Part 5** | Databricks Asset Bundles — define resources in `databricks.yml` |
| **Part 6** | Deploy to **prod** with `databricks bundle deploy` |
| **Part 7** | Run the prod job and compare both environments |

## Job Task DAG

```
run_bronze          (notebook task)
     │
     v
run_silver          (notebook task)
   /    \
  v      v
run_gold_sales    run_gold_products
(notebook task)   (notebook task)
  \      /
   v    v
refresh_dashboard   (dashboard task)
```
