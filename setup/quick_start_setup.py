# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Quick Start Setup
# MAGIC 
# MAGIC This notebook creates all Unity Catalog structures for the e-commerce analytics platform.
# MAGIC 
# MAGIC **Single Workspace Deployment**: All environments use the same workspace with different catalogs:
# MAGIC - `ecommerce_analytics_dev` - Development
# MAGIC - `ecommerce_analytics_staging` - Staging  
# MAGIC - `ecommerce_analytics_prod` - Production
# MAGIC 
# MAGIC **Run this notebook ONCE before deploying DABs.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Development Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_analytics_dev
# MAGIC COMMENT 'Development catalog for e-commerce analytics platform';
# MAGIC 
# MAGIC -- Create Staging Catalog  
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_analytics_staging
# MAGIC COMMENT 'Staging catalog for e-commerce analytics platform';
# MAGIC 
# MAGIC -- Create Production Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce_analytics_prod
# MAGIC COMMENT 'Production catalog for e-commerce analytics platform';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify catalogs created
# MAGIC SHOW CATALOGS LIKE 'ecommerce_analytics_*'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas for DEV

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce_analytics_dev;
# MAGIC 
# MAGIC -- Bronze Layer Schema
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_layer
# MAGIC COMMENT 'Bronze layer: Raw data ingested from source systems';
# MAGIC 
# MAGIC -- Silver Layer Schema
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_layer
# MAGIC COMMENT 'Silver layer: Cleaned and validated data';
# MAGIC 
# MAGIC -- Gold Layer Schema
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_layer
# MAGIC COMMENT 'Gold layer: Business-level aggregations and metrics';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume for Raw Data (DEV)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce_analytics_dev;
# MAGIC USE SCHEMA bronze_layer;
# MAGIC 
# MAGIC -- Create volume for raw CSV files
# MAGIC CREATE VOLUME IF NOT EXISTS raw_data
# MAGIC COMMENT 'Volume for raw CSV files from Kaggle dataset';
# MAGIC 
# MAGIC -- Create volume for checkpoints
# MAGIC CREATE VOLUME IF NOT EXISTS _checkpoints
# MAGIC COMMENT 'Volume for streaming checkpoints';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Schemas for STAGING

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce_analytics_staging;
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_layer
# MAGIC COMMENT 'Bronze layer: Raw data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_layer
# MAGIC COMMENT 'Silver layer: Cleaned data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_layer
# MAGIC COMMENT 'Gold layer: Business metrics';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze_layer;
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS raw_data
# MAGIC COMMENT 'Volume for raw CSV files';
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS _checkpoints
# MAGIC COMMENT 'Volume for streaming checkpoints';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Schemas for PROD

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ecommerce_analytics_prod;
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_layer
# MAGIC COMMENT 'Bronze layer: Raw data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_layer
# MAGIC COMMENT 'Silver layer: Cleaned data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_layer
# MAGIC COMMENT 'Gold layer: Business metrics';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA bronze_layer;
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS raw_data
# MAGIC COMMENT 'Volume for raw CSV files';
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS _checkpoints
# MAGIC COMMENT 'Volume for streaming checkpoints';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all catalogs
# MAGIC SHOW CATALOGS LIKE 'ecommerce_analytics_*'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show schemas in dev
# MAGIC USE CATALOG ecommerce_analytics_dev;
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show volumes
# MAGIC SHOW VOLUMES IN bronze_layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Upload Data to DEV Volume
# MAGIC 
# MAGIC Now upload the Kaggle CSV files to the volume:
# MAGIC 
# MAGIC **Option A: Via UI**
# MAGIC 1. Go to Catalog → `ecommerce_analytics_dev` → `bronze_layer` → `raw_data`
# MAGIC 2. Click "Upload" and select your CSV files
# MAGIC 
# MAGIC **Option B: Via Python (if files are already downloaded)**

# COMMAND ----------

# Uncomment and modify if you have files locally
# import shutil
# import os
# 
# VOLUME_PATH = "/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data"
# LOCAL_PATH = "/path/to/your/csv/files"
# 
# for f in os.listdir(LOCAL_PATH):
#     if f.endswith('.csv'):
#         shutil.copy(os.path.join(LOCAL_PATH, f), VOLUME_PATH)
#         print(f"Copied: {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC 
# MAGIC You now have:
# MAGIC - 3 catalogs: `ecommerce_analytics_dev`, `ecommerce_analytics_staging`, `ecommerce_analytics_prod`
# MAGIC - Each with schemas: `bronze_layer`, `silver_layer`, `gold_layer`
# MAGIC - Volumes for raw data in each bronze_layer
# MAGIC 
# MAGIC **Next Steps:**
# MAGIC 1. Upload CSV files to dev volume (if not done)
# MAGIC 2. Deploy DABs: `databricks bundle deploy -t dev`
# MAGIC 3. Run bronze ingestion job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("UNITY CATALOG SETUP SUMMARY")
print("=" * 60)
print()
print("Catalogs Created:")
print("  ✅ ecommerce_analytics_dev")
print("  ✅ ecommerce_analytics_staging")
print("  ✅ ecommerce_analytics_prod")
print()
print("Schemas per Catalog:")
print("  ✅ bronze_layer")
print("  ✅ silver_layer")
print("  ✅ gold_layer")
print()
print("Volumes (in bronze_layer):")
print("  ✅ raw_data - for CSV files")
print("  ✅ _checkpoints - for streaming")
print()
print("=" * 60)
print("Ready for DABs deployment!")
print("=" * 60)
