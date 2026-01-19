-- ============================================
-- External Locations Setup Script
-- E-Commerce Analytics Platform
-- ============================================
-- This script configures external locations and storage credentials
-- for accessing Azure Data Lake Storage Gen2 from Unity Catalog.
--
-- Prerequisites:
-- 1. Azure Service Principal with Storage Blob Data Contributor role
-- 2. Azure Storage Account with hierarchical namespace enabled
-- 3. Container created for Databricks data
-- ============================================


-- ============================================
-- SECTION 1: CREATE STORAGE CREDENTIAL
-- ============================================

-- Create storage credential using Azure Service Principal
-- Replace placeholders with actual values

/*
CREATE STORAGE CREDENTIAL IF NOT EXISTS ecommerce_analytics_credential
COMMENT 'Storage credential for e-commerce analytics ADLS Gen2 access'
WITH (
    AZURE_SERVICE_PRINCIPAL (
        DIRECTORY_ID = '<your-tenant-id>',
        APPLICATION_ID = '<your-app-id>',
        CLIENT_SECRET = '<your-client-secret>'
    )
);
*/

-- Alternative: Using Azure Managed Identity (recommended for production)
/*
CREATE STORAGE CREDENTIAL IF NOT EXISTS ecommerce_analytics_managed_identity
COMMENT 'Storage credential using managed identity'
WITH (
    AZURE_MANAGED_IDENTITY (
        ACCESS_CONNECTOR_ID = '/subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<connector-name>'
    )
);
*/


-- ============================================
-- SECTION 2: CREATE EXTERNAL LOCATIONS
-- ============================================

-- External location for raw data (Bronze)
/*
CREATE EXTERNAL LOCATION IF NOT EXISTS ecommerce_raw_data
URL 'abfss://raw-data@<storage-account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL ecommerce_analytics_credential)
COMMENT 'External location for raw e-commerce data files';
*/

-- External location for processed data (Silver/Gold)
/*
CREATE EXTERNAL LOCATION IF NOT EXISTS ecommerce_processed_data
URL 'abfss://processed-data@<storage-account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL ecommerce_analytics_credential)
COMMENT 'External location for processed e-commerce data';
*/

-- External location for checkpoints
/*
CREATE EXTERNAL LOCATION IF NOT EXISTS ecommerce_checkpoints
URL 'abfss://checkpoints@<storage-account>.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL ecommerce_analytics_credential)
COMMENT 'External location for streaming checkpoints';
*/


-- ============================================
-- SECTION 3: GRANT PERMISSIONS ON LOCATIONS
-- ============================================

-- Grant access to data engineers
/*
GRANT READ FILES ON EXTERNAL LOCATION ecommerce_raw_data TO `data-engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION ecommerce_raw_data TO `data-engineers`;
GRANT READ FILES ON EXTERNAL LOCATION ecommerce_processed_data TO `data-engineers`;
GRANT WRITE FILES ON EXTERNAL LOCATION ecommerce_processed_data TO `data-engineers`;
*/

-- Grant read access to analysts (processed data only)
/*
GRANT READ FILES ON EXTERNAL LOCATION ecommerce_processed_data TO `data-analysts`;
*/

-- Grant access to service principal for jobs
/*
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ecommerce_raw_data TO `ecommerce-analytics-sp`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ecommerce_processed_data TO `ecommerce-analytics-sp`;
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION ecommerce_checkpoints TO `ecommerce-analytics-sp`;
*/


-- ============================================
-- SECTION 4: CREATE EXTERNAL TABLES (Optional)
-- ============================================

-- Example: Create external table pointing to ADLS
/*
CREATE TABLE IF NOT EXISTS ecommerce_analytics_prod.bronze_layer.events_raw_external
USING DELTA
LOCATION 'abfss://raw-data@<storage-account>.dfs.core.windows.net/events_raw/'
COMMENT 'External Delta table for raw events'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
*/


-- ============================================
-- SECTION 5: VERIFY EXTERNAL LOCATIONS
-- ============================================

-- List all storage credentials
-- SHOW STORAGE CREDENTIALS;

-- List all external locations
-- SHOW EXTERNAL LOCATIONS;

-- Describe specific location
-- DESCRIBE EXTERNAL LOCATION ecommerce_raw_data;

-- Test access to external location
-- LIST 'abfss://raw-data@<storage-account>.dfs.core.windows.net/';


-- ============================================
-- SECTION 6: AZURE CLI COMMANDS (Reference)
-- ============================================

/*
# Create Azure Service Principal
az ad sp create-for-rbac --name "ecommerce-analytics-sp" --role "Storage Blob Data Contributor" \
    --scopes /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>

# Create Storage Account with hierarchical namespace
az storage account create \
    --name <storage-account> \
    --resource-group <rg> \
    --location <region> \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hns true

# Create containers
az storage container create --name raw-data --account-name <storage-account>
az storage container create --name processed-data --account-name <storage-account>
az storage container create --name checkpoints --account-name <storage-account>

# Assign role to Databricks Access Connector (for managed identity)
az role assignment create \
    --assignee <access-connector-principal-id> \
    --role "Storage Blob Data Contributor" \
    --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>
*/


-- ============================================
-- SECTION 7: CLEANUP (USE WITH CAUTION)
-- ============================================

/*
DROP EXTERNAL LOCATION IF EXISTS ecommerce_raw_data;
DROP EXTERNAL LOCATION IF EXISTS ecommerce_processed_data;
DROP EXTERNAL LOCATION IF EXISTS ecommerce_checkpoints;
DROP STORAGE CREDENTIAL IF EXISTS ecommerce_analytics_credential;
*/
