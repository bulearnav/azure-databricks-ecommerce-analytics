# E-Commerce Analytics Platform - Architecture

## Overview

This document describes the architecture of the E-Commerce Analytics Platform built on Azure Databricks with Unity Catalog governance.

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Sources
        CSV[📄 CSV Files<br/>Kaggle Dataset]
    end

    subgraph Ingestion
        AL[🔄 Auto Loader<br/>Structured Streaming]
        UC_VOL[(📁 Unity Catalog<br/>Volume)]
    end

    subgraph Bronze["🥉 Bronze Layer"]
        B_RAW[events_raw<br/>Delta Table]
    end

    subgraph Silver["🥈 Silver Layer"]
        S_CLEAN[events_cleaned<br/>Delta Table]
        S_QUAR[events_quarantine<br/>Dead Letter Queue]
    end

    subgraph Gold["🥇 Gold Layer"]
        G_CUST[customer_metrics]
        G_PROD[product_performance]
        G_DAILY[daily_sales_summary]
        G_FUNNEL[conversion_funnel]
    end

    subgraph Governance
        UC[Unity Catalog]
        RLS[Row Level Security]
        CLS[Column Level Security]
        AUDIT[Audit Logging]
    end

    subgraph Consumers
        BI[📊 BI Dashboards]
        ML[🤖 ML Models]
        API[🔌 APIs]
    end

    CSV --> UC_VOL
    UC_VOL --> AL
    AL --> B_RAW
    B_RAW --> S_CLEAN
    B_RAW --> S_QUAR
    S_CLEAN --> G_CUST
    S_CLEAN --> G_PROD
    S_CLEAN --> G_DAILY
    S_CLEAN --> G_FUNNEL

    G_CUST --> BI
    G_PROD --> BI
    G_DAILY --> BI
    G_FUNNEL --> ML

    UC --> Bronze
    UC --> Silver
    UC --> Gold
    RLS --> Gold
    CLS --> Silver
```

## Component Architecture

### Data Ingestion Layer

```mermaid
flowchart LR
    subgraph External
        KAGGLE[Kaggle API]
        ADLS[ADLS Gen2]
    end

    subgraph Databricks
        VOL[Unity Catalog<br/>Volume]
        AL[Auto Loader<br/>cloudFiles]
        STREAM[Structured<br/>Streaming]
    end

    subgraph Processing
        BATCH[Batch Jobs]
        DLT[Delta Live<br/>Tables]
    end

    KAGGLE --> VOL
    ADLS --> VOL
    VOL --> AL
    AL --> STREAM
    STREAM --> DLT
    VOL --> BATCH
    BATCH --> DLT
```

### Unity Catalog Structure

```
ecommerce_analytics_${environment}/
│
├── bronze_layer/                    # Raw data schema
│   ├── events_raw                   # Raw events table (partitioned by event_type)
│   ├── raw_data/                    # Volume - CSV files
│   └── _checkpoints/                # Volume - Streaming checkpoints
│
├── silver_layer/                    # Cleaned data schema
│   ├── events_cleaned               # Cleaned events (partitioned by event_date)
│   └── events_quarantine            # Failed quality check records
│
├── gold_layer/                      # Business metrics schema
│   ├── customer_metrics             # Customer-level aggregations
│   ├── product_performance          # Product-level metrics
│   ├── daily_sales_summary          # Daily time-series
│   └── conversion_funnel            # Funnel analysis
│
└── dlt_silver_layer/                # DLT-managed tables
    └── (DLT creates tables here)
```

## Data Flow Architecture

### Batch Processing Flow

```mermaid
sequenceDiagram
    participant CSV as CSV Files
    participant Bronze as Bronze Layer
    participant Silver as Silver Layer
    participant Gold as Gold Layer
    participant BI as BI/Analytics

    CSV->>Bronze: Ingest with audit columns
    Note over Bronze: Add ingestion_timestamp,<br/>source_file, created_at

    Bronze->>Silver: Transform & Validate
    Note over Silver: Dedup, standardize,<br/>add derived columns

    Silver->>Gold: Aggregate
    Note over Gold: Customer, Product,<br/>Daily, Funnel metrics

    Gold->>BI: Consume
    Note over BI: Dashboards,<br/>Reports, ML
```

### Streaming Processing Flow

```mermaid
sequenceDiagram
    participant Source as New CSV File
    participant AL as Auto Loader
    participant Checkpoint as Checkpoint
    participant Bronze as Bronze Table
    participant DLT as DLT Pipeline

    Source->>AL: Detect new file
    AL->>Checkpoint: Check processed files
    Checkpoint-->>AL: Return last position
    AL->>Bronze: Stream insert new rows
    AL->>Checkpoint: Update checkpoint
    Bronze->>DLT: Trigger DLT refresh
    DLT->>DLT: Process Silver → Gold
```

## Security Architecture

```mermaid
flowchart TB
    subgraph Users
        ENG[Data Engineers]
        ANA[Data Analysts]
        DS[Data Scientists]
        SVC[Service Principal]
    end

    subgraph "Access Control"
        UCG[Unity Catalog<br/>Grants]
        RLS[Row Level<br/>Security]
        CLS[Column Level<br/>Security]
        DDM[Dynamic Data<br/>Masking]
    end

    subgraph "Data Layers"
        BRONZE[Bronze<br/>Full Access]
        SILVER[Silver<br/>Masked PII]
        GOLD[Gold<br/>Filtered Views]
    end

    ENG --> UCG
    ANA --> UCG
    DS --> UCG
    SVC --> UCG

    UCG --> RLS
    UCG --> CLS
    UCG --> DDM

    RLS --> GOLD
    CLS --> SILVER
    DDM --> SILVER
    DDM --> GOLD
```

## Deployment Architecture

### CI/CD Pipeline

```mermaid
flowchart LR
    subgraph Development
        DEV[Developer]
        LOCAL[Local Testing]
    end

    subgraph "Version Control"
        GIT[GitHub/ADO]
        PR[Pull Request]
    end

    subgraph "CI Pipeline"
        VAL[Validate DABs]
        TEST[Run Tests]
        LINT[Linting]
    end

    subgraph "CD Pipeline"
        DEP_DEV[Deploy Dev]
        DEP_STG[Deploy Staging]
        DEP_PROD[Deploy Prod]
    end

    subgraph Environments
        ENV_DEV[Dev Workspace]
        ENV_STG[Staging Workspace]
        ENV_PROD[Prod Workspace]
    end

    DEV --> LOCAL
    LOCAL --> GIT
    GIT --> PR
    PR --> VAL
    VAL --> TEST
    TEST --> LINT

    LINT --> DEP_DEV
    DEP_DEV --> DEP_STG
    DEP_STG --> DEP_PROD

    DEP_DEV --> ENV_DEV
    DEP_STG --> ENV_STG
    DEP_PROD --> ENV_PROD
```

### Databricks Asset Bundles Structure

```
ecommerce-analytics-platform/
├── databricks.yml           # Main bundle configuration
├── environments/
│   ├── dev.yml              # Dev environment variables
│   ├── staging.yml          # Staging environment variables
│   └── prod.yml             # Prod environment variables
├── resources/
│   ├── jobs.yml             # Workflow job definitions
│   ├── pipelines.yml        # DLT pipeline definitions
│   └── clusters.yml         # Cluster configurations
└── src/                     # Notebooks and code
```

## Compute Architecture

```mermaid
flowchart TB
    subgraph "Job Clusters"
        JC1[Bronze Ingestion<br/>DS3_v2 x 1]
        JC2[Silver Transform<br/>DS4_v2 x 2]
        JC3[Gold Aggregation<br/>DS4_v2 x 2]
    end

    subgraph "DLT Clusters"
        DLT1[DLT Pipeline<br/>Autoscale 1-4]
        DLT2[Streaming Pipeline<br/>DS4_v2 x 2]
    end

    subgraph "Interactive Clusters"
        IC1[Dev Cluster<br/>Single Node]
        IC2[Analytics Cluster<br/>Autoscale 1-4]
    end

    subgraph "Policies"
        POL1[Dev Policy<br/>Max 2 workers]
        POL2[Prod Policy<br/>Photon, 2-8 workers]
        POL3[Cost Optimized<br/>Spot instances]
    end

    POL1 --> IC1
    POL2 --> JC1
    POL2 --> JC2
    POL2 --> JC3
    POL2 --> DLT1
    POL2 --> DLT2
    POL3 --> IC2
```

## Monitoring Architecture

```mermaid
flowchart LR
    subgraph "Data Sources"
        JOBS[Job Runs]
        DLT[DLT Metrics]
        TABLES[Table Stats]
        AUDIT[Audit Logs]
    end

    subgraph "System Tables"
        ST1[system.compute.clusters]
        ST2[system.workflow.jobs]
        ST3[system.access.audit]
        ST4[system.billing.usage]
    end

    subgraph "Monitoring"
        DASH[Monitoring Dashboard]
        ALERT[Alert Rules]
        NOTIFY[Notifications]
    end

    JOBS --> ST2
    DLT --> ST2
    TABLES --> ST3
    AUDIT --> ST3

    ST1 --> DASH
    ST2 --> DASH
    ST3 --> DASH
    ST4 --> DASH

    DASH --> ALERT
    ALERT --> NOTIFY
```

## Performance Considerations

### Data Partitioning Strategy

| Layer  | Table               | Partition Column | Rationale                        |
| ------ | ------------------- | ---------------- | -------------------------------- |
| Bronze | events_raw          | event_type       | Balance cardinality (4 values)   |
| Silver | events_cleaned      | event_date       | Time-based queries, data pruning |
| Gold   | daily_sales_summary | N/A              | Small table, no partition needed |

### Optimization Techniques

1. **Z-Ordering**: Applied on frequently filtered columns
2. **Auto Optimize**: Enabled for all Delta tables
3. **Data Skipping**: Leveraging min/max statistics
4. **Caching**: Enabled for frequently accessed tables
5. **Photon**: Enabled for production workloads

## Disaster Recovery

```mermaid
flowchart TB
    subgraph Primary["Primary Region"]
        P_UC[Unity Catalog]
        P_STOR[ADLS Gen2]
        P_WS[Workspace]
    end

    subgraph Secondary["DR Region"]
        S_UC[Unity Catalog<br/>Replica]
        S_STOR[ADLS Gen2<br/>GRS Replication]
        S_WS[Workspace<br/>Standby]
    end

    P_UC --> S_UC
    P_STOR --> S_STOR
    P_WS -.-> S_WS

    style S_WS stroke-dasharray: 5 5
```

### Recovery Objectives

| Metric                         | Target                  |
| ------------------------------ | ----------------------- |
| RPO (Recovery Point Objective) | 1 hour                  |
| RTO (Recovery Time Objective)  | 4 hours                 |
| Data Retention                 | 30 days (time travel)   |
| Backup Frequency               | Continuous (Delta logs) |
