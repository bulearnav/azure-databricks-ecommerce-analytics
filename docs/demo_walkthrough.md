# Demo Walkthrough

## Overview

This document provides a step-by-step guide for demonstrating the E-Commerce Analytics Platform during the assessment presentation.

## Demo Duration: 20 minutes

## Demonstration Outline

| Section              | Time  | Topics                        |
| -------------------- | ----- | ----------------------------- |
| 1. Platform Overview | 2 min | Architecture, data flow       |
| 2. Unity Catalog     | 3 min | Catalog structure, governance |
| 3. DABs & CI/CD      | 3 min | Git integration, deployment   |
| 4. Data Pipelines    | 5 min | Bronze/Silver/Gold, DLT       |
| 5. Security          | 3 min | RLS, CLS, masking             |
| 6. Business Insights | 3 min | Dashboards, metrics           |
| 7. Q&A               | 1 min | Questions                     |

---

## Section 1: Platform Overview (2 min)

### Talking Points

1. **Introduction**
   - "This is an enterprise-grade e-commerce analytics platform"
   - "Built on Azure Databricks with Unity Catalog governance"
   - "Processes 110+ million customer behavior events"

2. **Architecture Diagram**
   - Show architecture.md diagram
   - Explain Medallion pattern: Bronze → Silver → Gold

3. **Dataset Overview**
   - Source: Kaggle e-commerce behavior data
   - Events: view, cart, remove_from_cart, purchase
   - Time period: October-November 2019

### Demo Actions

```sql
-- Show data volume
SELECT COUNT(*) as total_events FROM silver_layer.events_cleaned;
-- Expected: ~109 million
```

---

## Section 2: Unity Catalog (3 min)

### Talking Points

1. **3-Tier Namespace**
   - Catalog: `ecommerce_analytics_dev`
   - Schemas: `bronze_layer`, `silver_layer`, `gold_layer`
   - Tables and Volumes

2. **Governance Features**
   - Centralized metadata management
   - Data lineage tracking
   - Access control

### Demo Actions

```sql
-- Show catalog structure
SHOW CATALOGS LIKE 'ecommerce*';

-- Switch to dev catalog
USE CATALOG ecommerce_analytics_dev;

-- Show schemas
SHOW SCHEMAS;

-- Show bronze tables
USE SCHEMA bronze_layer;
SHOW TABLES;

-- Show volumes
SHOW VOLUMES;
LIST '/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data';
```

### Lineage Demo

1. Navigate to Unity Catalog in Databricks UI
2. Select `gold_layer.customer_metrics`
3. Click "Lineage" tab
4. Show upstream dependencies (Silver → Bronze → CSV)

---

## Section 3: DABs & CI/CD (3 min)

### Talking Points

1. **Databricks Asset Bundles**
   - Infrastructure as Code
   - Multi-environment support (dev/staging/prod)
   - Version-controlled resources

2. **Git Integration**
   - Repository structure
   - Commit history
   - Branch strategy

3. **CI/CD Pipeline**
   - Automated validation
   - Testing on PR
   - Deploy on merge

### Demo Actions

```bash
# Show project structure in terminal
tree azure-databricks-ecommerce-analytics/

# Validate DABs bundle
databricks bundle validate -t dev

# Show Git history
git log --oneline -10
```

### Key Files to Highlight

- `databricks.yml` - Main configuration
- `resources/jobs.yml` - Job definitions
- `.github/workflows/deploy.yml` - CI/CD pipeline

---

## Section 4: Data Pipelines (5 min)

### 4.1 Bronze Layer (1 min)

```sql
-- Show bronze table structure
DESCRIBE TABLE bronze_layer.events_raw;

-- Show audit columns
SELECT
    event_time,
    event_type,
    ingestion_timestamp,
    source_file
FROM bronze_layer.events_raw
LIMIT 5;
```

### 4.2 Silver Layer (2 min)

```sql
-- Show transformation: derived columns
SELECT
    event_id,
    event_time,
    event_date,
    event_hour,
    category_code,
    category_l1,
    category_l2
FROM silver_layer.events_cleaned
LIMIT 5;

-- Show data quality improvement
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN brand = 'unknown' THEN 1 ELSE 0 END) as null_brands_handled
FROM silver_layer.events_cleaned;
```

### 4.3 DLT Pipeline (1 min)

1. Navigate to Workflows → Delta Live Tables
2. Show pipeline graph (Bronze → Silver → Gold)
3. Point out data quality expectations

### 4.4 Gold Layer (1 min)

```sql
-- Show customer metrics
SELECT
    user_id,
    customer_lifetime_value,
    total_purchases,
    customer_segment,
    rfm_score
FROM gold_layer.customer_metrics
ORDER BY customer_lifetime_value DESC
LIMIT 10;

-- Show product performance
SELECT
    product_id,
    brand,
    total_revenue,
    overall_conversion_rate,
    performance_tier
FROM gold_layer.product_performance
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Section 5: Security (3 min)

### Talking Points

1. **Row Level Security**
   - Analysts see only their assigned segments
   - Based on user-to-segment mapping

2. **Column Level Security**
   - PII columns hidden from analysts
   - user_id, user_session protected

3. **Dynamic Data Masking**
   - Real-time masking based on role
   - Different views for different users

### Demo Actions

```sql
-- Show security mapping table
SELECT * FROM gold_layer.user_segment_access;

-- Show masked view
SELECT * FROM gold_layer.customer_metrics_masked LIMIT 5;

-- Compare engineer vs analyst view
-- (Switch user context if possible)
SELECT
    current_user() as current_user,
    is_member('data-engineers') as is_engineer;
```

### Audit Log Demo

```sql
-- Show recent data access
SELECT
    event_time,
    user_identity.email,
    action_name,
    request_params.full_name_arg as table_accessed
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%ecommerce%'
ORDER BY event_time DESC
LIMIT 10;
```

---

## Section 6: Business Insights (3 min)

### 6.1 Customer Segmentation

```sql
-- Segment distribution
SELECT
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
    ROUND(AVG(total_purchases), 1) as avg_purchases
FROM gold_layer.customer_metrics
GROUP BY customer_segment
ORDER BY customer_count DESC;
```

### 6.2 Conversion Funnel

```sql
-- Overall funnel
SELECT
    SUM(sessions_with_view) as views,
    SUM(sessions_with_cart) as carts,
    SUM(sessions_with_purchase) as purchases,
    ROUND(SUM(sessions_with_cart) / SUM(sessions_with_view) * 100, 2) as view_to_cart_pct,
    ROUND(SUM(sessions_with_purchase) / SUM(sessions_with_cart) * 100, 2) as cart_to_purchase_pct
FROM gold_layer.conversion_funnel;
```

### 6.3 Daily Trends

```sql
-- Revenue trend
SELECT
    event_date,
    total_revenue,
    revenue_7d_avg,
    unique_users
FROM gold_layer.daily_sales_summary
ORDER BY event_date DESC
LIMIT 30;
```

### Visualization

1. Create a simple bar chart showing:
   - Customer segment distribution
   - Top 10 products by revenue
   - Daily revenue trend

---

## Section 7: Summary & Q&A (1 min)

### Key Points to Emphasize

1. **Complete Implementation**
   - All 5 assessment domains covered
   - Production-ready architecture

2. **Enterprise Features**
   - Unity Catalog governance
   - Security at every layer
   - CI/CD automation

3. **Business Value**
   - 110M+ events processed
   - Customer insights generated
   - Real-time analytics ready

### Anticipated Questions

**Q: How do you handle schema changes?**

> Auto Loader with schema evolution, managed schema locations

**Q: What's the data freshness?**

> DLT maintains < 5 min freshness in streaming mode

**Q: How is data quality enforced?**

> DLT expectations, quarantine table for failed records

**Q: Can this scale?**

> Yes, autoscaling clusters, Delta Lake handles petabytes

---

## Pre-Demo Checklist

- [ ] Databricks workspace accessible
- [ ] All tables populated with data
- [ ] DLT pipeline ran successfully
- [ ] Security views created
- [ ] Git repository up to date
- [ ] Presentation slides ready
- [ ] Network/VPN connected

## Troubleshooting

| Issue               | Solution                        |
| ------------------- | ------------------------------- |
| Table not found     | Run ingestion job first         |
| Permission denied   | Check Unity Catalog grants      |
| DLT pipeline failed | Check error logs, fix and retry |
| Slow queries        | Ensure tables are optimized     |
