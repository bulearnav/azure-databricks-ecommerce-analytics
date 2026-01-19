# 🛒 E-Commerce Analytics Platform

> Enterprise-grade data engineering solution built on Azure Databricks with Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)

## 📋 Overview

This platform processes e-commerce customer behavior data (~110M events) through a **Medallion Architecture** (Bronze → Silver → Gold) using:

- **Unity Catalog** for data governance
- **Delta Live Tables** for declarative pipelines
- **Databricks Asset Bundles** for CI/CD deployment
- **Structured Streaming** with Auto Loader for incremental ingestion

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Unity Catalog                            │
├─────────────────────────────────────────────────────────────────┤
│  ecommerce_analytics_{env}/                                     │
│  ├── bronze_layer/                                              │
│  │   ├── events_raw (Delta Table)                              │
│  │   └── raw_data (Volume - CSVs)                              │
│  ├── silver_layer/                                              │
│  │   ├── events_cleaned                                         │
│  │   ├── users_dim                                              │
│  │   └── products_dim                                           │
│  └── gold_layer/                                                │
│      ├── customer_metrics                                       │
│      ├── product_performance                                    │
│      ├── daily_sales_summary                                    │
│      └── conversion_funnel                                      │
└─────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
azure-databricks-ecommerce-analytics/
├── databricks.yml              # DABs main configuration
├── README.md
├── environments/               # Environment-specific configs
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
├── resources/                  # DABs resource definitions
│   ├── jobs.yml
│   ├── pipelines.yml
│   └── clusters.yml
├── src/
│   ├── bronze/                 # Bronze layer notebooks
│   │   ├── ingest_events_csv.py
│   │   └── schema_events.py
│   ├── silver/                 # Silver layer notebooks
│   │   ├── transform_events_cleaned.py
│   │   └── data_quality_rules.py
│   ├── gold/                   # Gold layer notebooks
│   │   ├── agg_customer_metrics.py
│   │   ├── agg_product_performance.py
│   │   ├── agg_daily_sales.py
│   │   └── agg_conversion_funnel.py
│   ├── dlt/                    # Delta Live Tables
│   │   └── dlt_bronze_to_gold.py
│   └── streaming/              # Streaming pipelines
│       └── stream_events_autoloader.py
├── setup/                      # Unity Catalog setup scripts
│   ├── unity_catalog_setup.sql
│   ├── external_locations.sql
│   ├── security_policies.sql
│   └── compute_policies.sql
├── tests/                      # Unit tests
│   ├── test_bronze_ingestion.py
│   ├── test_silver_transformations.py
│   └── test_gold_aggregations.py
├── docs/                       # Documentation
│   ├── architecture.md
│   ├── medallion_design.md
│   ├── security_model.md
│   ├── performance_optimizations.md
│   └── demo_walkthrough.md
└── .github/
    └── workflows/
        └── deploy.yml          # CI/CD pipeline
```

## 🚀 Quick Start (Free Trial / Serverless)

> **✅ Serverless Compatible**: This project is configured to run on Databricks Free Trial with serverless compute.

### Prerequisites

- Azure Databricks **Free Trial** workspace
- Unity Catalog enabled (automatic on new workspaces)
- Databricks CLI v0.200+

### 1. Download Dataset

First, download the dataset from Kaggle and upload to your workspace:

1. Go to [Kaggle Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
2. Download CSV files (~14GB)
3. Create Unity Catalog volume and upload (or use included notebook)

### 2. Create Serverless SQL Warehouse

1. In Databricks UI, go to **SQL Warehouses**
2. Click **Create SQL Warehouse**
3. Select **Serverless** type
4. Name: `ecommerce-analytics-warehouse`
5. Copy the **Warehouse ID** for configuration

### 3. Configure CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure with your workspace
databricks configure --token
# Enter your workspace URL and Personal Access Token
```

### 4. Update Configuration

Edit `databricks.yml` and set your workspace URL:

```yaml
variables:
  databricks_host:
    default: "https://adb-xxxxx.azuredatabricks.net"
  warehouse_id:
    default: "your-warehouse-id-here"
```

### 5. Validate & Deploy

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev
```

### 6. Run Pipelines

```bash
# Option 1: Run batch ingestion job
databricks bundle run -t dev bronze_ingestion_job

# Option 2: Run full pipeline (Bronze → Silver → Gold)
databricks bundle run -t dev full_pipeline_job

# Option 3: Run DLT pipeline (serverless)
databricks bundle run -t dev ecommerce_dlt_pipeline
```

### 7. Interactive Notebooks (Alternative)

For Free Trial, you can also run notebooks directly:

1. Navigate to Workspace in Databricks UI
2. Open notebooks from deployed bundle
3. Attach to **Serverless** compute
4. Run cells interactively

## 📊 Dataset

**Source**: [Kaggle - eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

| Metric       | Value                                  |
| ------------ | -------------------------------------- |
| Total Events | ~110 million                           |
| Time Period  | October - November 2019                |
| File Size    | ~14 GB (CSV)                           |
| Event Types  | view, cart, remove_from_cart, purchase |

### Schema

| Column        | Type      | Description                         |
| ------------- | --------- | ----------------------------------- |
| event_time    | timestamp | Event timestamp (UTC)               |
| event_type    | string    | view/cart/remove_from_cart/purchase |
| product_id    | long      | Product identifier                  |
| category_id   | long      | Category identifier                 |
| category_code | string    | Category taxonomy (nullable)        |
| brand         | string    | Brand name (nullable)               |
| price         | double    | Product price                       |
| user_id       | long      | User identifier                     |
| user_session  | string    | Session identifier                  |

## 🔐 Security Features

- **Row Level Security (RLS)**: Data isolation by user segment
- **Column Level Security (CLS)**: PII protection for sensitive columns
- **Dynamic Data Masking**: Real-time masking based on user roles
- **Audit Logging**: Complete access trail via Unity Catalog

## 📈 Key Metrics (Gold Layer)

### Customer Metrics

- Customer Lifetime Value (CLV)
- Session counts and engagement
- Purchase frequency
- Churn indicators

### Product Performance

- Conversion funnel (View → Cart → Purchase)
- Revenue by product/category/brand
- Top performing products

### Daily Sales Summary

- Revenue trends
- Order counts
- Average order value
- Peak shopping hours

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_silver_transformations.py -v
```

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [Medallion Design](docs/medallion_design.md)
- [Security Model](docs/security_model.md)
- [Performance Optimizations](docs/performance_optimizations.md)
- [Demo Walkthrough](docs/demo_walkthrough.md)

## 🤝 Contributing

1. Create a feature branch from `main`
2. Make changes and test locally
3. Submit a pull request
4. CI/CD will validate and deploy to dev

## 📄 License

This project is for educational/assessment purposes.

---

**Author**: Built for Azure Databricks Associate Practical Assessment  
**Dataset Credit**: [Michael Kechinov](https://www.kaggle.com/mkechinov)
