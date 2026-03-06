# Lakeflow Jobs & CI/CD with Databricks Asset Bundles

A hands-on lab for learning how to build, orchestrate, and deploy data pipelines on Databricks using **Lakeflow Jobs** and **Databricks Asset Bundles (DABs)** for CI/CD.

## What You'll Build

By the end of this lab you will have:
- A **medallion ETL notebook** implementing the full bronze / silver / gold architecture
- A **source validation notebook** that gates the ETL with a pre-flight check
- An **AI/BI Lakeview dashboard** that visualizes the gold-layer data
- A **three-task Lakeflow Job** using notebook and dashboard task types
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
│   └── medallion_notebook.py          # ETL notebook (bronze → silver → gold)
│
├── validation/
│   └── source_validation_notebook.py  # Task 1: pre-flight gate; demonstrates job parameters
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
| **Setup** | Create schemas, volumes, generate sample data, and publish dashboard |
| **Part 1** | Explore the medallion ETL notebook (bronze, silver, gold) |
| **Part 2** | Explore the validation notebook and sales dashboard |
| **Part 3** | Lakeflow Jobs concepts — task types, parameters, and dependencies |
| **Part 4** | Build the three-task job in the Databricks Jobs UI |
| **Part 5** | Databricks Asset Bundles — define resources in `databricks.yml` |
| **Part 6** | Deploy with `databricks bundle deploy` |
| **Part 7** | Run the bundle job and promote to prod |

## Job Task DAG

```
validate_source    (notebook task)
     │  depends_on: —
     ▼
run_etl            (notebook task)
     │  depends_on: validate_source
     ▼
refresh_dashboard  (dashboard task)
     depends_on: run_etl
```
