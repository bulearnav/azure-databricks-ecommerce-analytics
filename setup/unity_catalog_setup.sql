-- ============================================
-- Unity Catalog Setup Script
-- E-Commerce Analytics Platform
-- ============================================
-- This script creates the Unity Catalog structure for the 
-- e-commerce analytics platform including catalogs, schemas, 
-- volumes, and initial permissions.
--
-- Run this script with appropriate admin privileges.
-- ============================================

-- ============================================
-- SECTION 1: CREATE CATALOGS
-- ============================================

-- Development Catalog
CREATE CATALOG IF NOT EXISTS ecommerce_analytics_dev
COMMENT 'Development catalog for e-commerce analytics platform';

-- Staging Catalog
CREATE CATALOG IF NOT EXISTS ecommerce_analytics_staging
COMMENT 'Staging catalog for e-commerce analytics platform';

-- Production Catalog
CREATE CATALOG IF NOT EXISTS ecommerce_analytics_prod
COMMENT 'Production catalog for e-commerce analytics platform';


-- ============================================
-- SECTION 2: CREATE SCHEMAS (DEV)
-- ============================================

USE CATALOG ecommerce_analytics_dev;

-- Bronze Layer Schema
CREATE SCHEMA IF NOT EXISTS bronze_layer
COMMENT 'Bronze layer: Raw data ingested from source systems';

-- Silver Layer Schema
CREATE SCHEMA IF NOT EXISTS silver_layer
COMMENT 'Silver layer: Cleaned and validated data';

-- Gold Layer Schema
CREATE SCHEMA IF NOT EXISTS gold_layer
COMMENT 'Gold layer: Business-level aggregations and metrics';

-- DLT Schema (for Delta Live Tables output)
CREATE SCHEMA IF NOT EXISTS dlt_silver_layer
COMMENT 'DLT output schema for silver layer tables';


-- ============================================
-- SECTION 3: CREATE VOLUMES (DEV)
-- ============================================

USE CATALOG ecommerce_analytics_dev;
USE SCHEMA bronze_layer;

-- Volume for raw CSV data
CREATE VOLUME IF NOT EXISTS raw_data
COMMENT 'Volume for raw CSV files from Kaggle dataset';

-- Volume for checkpoints
CREATE VOLUME IF NOT EXISTS _checkpoints
COMMENT 'Volume for streaming checkpoints and schema locations';


-- ============================================
-- SECTION 4: CREATE SCHEMAS (STAGING)
-- ============================================

USE CATALOG ecommerce_analytics_staging;

CREATE SCHEMA IF NOT EXISTS bronze_layer
COMMENT 'Bronze layer: Raw data ingested from source systems';

CREATE SCHEMA IF NOT EXISTS silver_layer
COMMENT 'Silver layer: Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS gold_layer
COMMENT 'Gold layer: Business-level aggregations and metrics';

CREATE SCHEMA IF NOT EXISTS dlt_silver_layer
COMMENT 'DLT output schema for silver layer tables';

-- Volumes for staging
USE SCHEMA bronze_layer;

CREATE VOLUME IF NOT EXISTS raw_data
COMMENT 'Volume for raw CSV files';

CREATE VOLUME IF NOT EXISTS _checkpoints
COMMENT 'Volume for streaming checkpoints';


-- ============================================
-- SECTION 5: CREATE SCHEMAS (PROD)
-- ============================================

USE CATALOG ecommerce_analytics_prod;

CREATE SCHEMA IF NOT EXISTS bronze_layer
COMMENT 'Bronze layer: Raw data ingested from source systems';

CREATE SCHEMA IF NOT EXISTS silver_layer
COMMENT 'Silver layer: Cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS gold_layer
COMMENT 'Gold layer: Business-level aggregations and metrics';

CREATE SCHEMA IF NOT EXISTS dlt_silver_layer
COMMENT 'DLT output schema for silver layer tables';

-- Volumes for production
USE SCHEMA bronze_layer;

CREATE VOLUME IF NOT EXISTS raw_data
COMMENT 'Volume for raw CSV files';

CREATE VOLUME IF NOT EXISTS _checkpoints
COMMENT 'Volume for streaming checkpoints';


-- ============================================
-- SECTION 6: GRANT PERMISSIONS
-- ============================================

-- Development Permissions (more permissive)
USE CATALOG ecommerce_analytics_dev;

GRANT USE CATALOG ON CATALOG ecommerce_analytics_dev TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA bronze_layer TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA silver_layer TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA gold_layer TO `data-engineers`;

GRANT CREATE TABLE ON SCHEMA bronze_layer TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA silver_layer TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA gold_layer TO `data-engineers`;

GRANT READ VOLUME ON VOLUME bronze_layer.raw_data TO `data-engineers`;
GRANT WRITE VOLUME ON VOLUME bronze_layer.raw_data TO `data-engineers`;

-- Allow data analysts to read from silver and gold
GRANT USE CATALOG ON CATALOG ecommerce_analytics_dev TO `data-analysts`;
GRANT USE SCHEMA ON SCHEMA silver_layer TO `data-analysts`;
GRANT USE SCHEMA ON SCHEMA gold_layer TO `data-analysts`;
GRANT SELECT ON SCHEMA silver_layer TO `data-analysts`;
GRANT SELECT ON SCHEMA gold_layer TO `data-analysts`;


-- Production Permissions (more restrictive)
USE CATALOG ecommerce_analytics_prod;

-- Engineers can manage tables
GRANT USE CATALOG ON CATALOG ecommerce_analytics_prod TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA bronze_layer TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA silver_layer TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA gold_layer TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze_layer TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver_layer TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold_layer TO `data-engineers`;

-- Analysts can only read from gold
GRANT USE CATALOG ON CATALOG ecommerce_analytics_prod TO `data-analysts`;
GRANT USE SCHEMA ON SCHEMA gold_layer TO `data-analysts`;
GRANT SELECT ON SCHEMA gold_layer TO `data-analysts`;

-- Service principal for jobs
-- GRANT USE CATALOG ON CATALOG ecommerce_analytics_prod TO `ecommerce-analytics-sp`;
-- GRANT USE SCHEMA ON SCHEMA bronze_layer TO `ecommerce-analytics-sp`;
-- GRANT ALL PRIVILEGES ON SCHEMA bronze_layer TO `ecommerce-analytics-sp`;
-- GRANT ALL PRIVILEGES ON SCHEMA silver_layer TO `ecommerce-analytics-sp`;
-- GRANT ALL PRIVILEGES ON SCHEMA gold_layer TO `ecommerce-analytics-sp`;


-- ============================================
-- SECTION 7: VERIFICATION QUERIES
-- ============================================

-- List all catalogs
SHOW CATALOGS LIKE 'ecommerce_analytics_*';

-- List schemas in dev catalog
USE CATALOG ecommerce_analytics_dev;
SHOW SCHEMAS;

-- List volumes
SHOW VOLUMES IN bronze_layer;

-- Verify grants
SHOW GRANTS ON CATALOG ecommerce_analytics_dev;


-- ============================================
-- SECTION 8: TABLE PROPERTIES TEMPLATE
-- ============================================

-- Template for creating tables with proper properties
-- (These will be created by the pipeline notebooks)

/*
CREATE TABLE IF NOT EXISTS ecommerce_analytics_dev.bronze_layer.events_raw (
    event_time TIMESTAMP,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DOUBLE,
    user_id BIGINT,
    user_session STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    created_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_type)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 30 days'
)
COMMENT 'Raw e-commerce events from CSV files';
*/


-- ============================================
-- SECTION 9: CLEANUP (USE WITH CAUTION)
-- ============================================

-- Uncomment to drop and recreate (DESTRUCTIVE!)
/*
DROP CATALOG IF EXISTS ecommerce_analytics_dev CASCADE;
DROP CATALOG IF EXISTS ecommerce_analytics_staging CASCADE;
DROP CATALOG IF EXISTS ecommerce_analytics_prod CASCADE;
*/
