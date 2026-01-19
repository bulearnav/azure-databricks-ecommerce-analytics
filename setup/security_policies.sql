-- ============================================
-- Security Policies Setup Script
-- E-Commerce Analytics Platform
-- ============================================
-- This script implements security controls including:
-- - Row Level Security (RLS)
-- - Column Level Security (CLS)
-- - Dynamic Data Masking
-- - Audit logging configuration
-- ============================================


-- ============================================
-- SECTION 1: ROW LEVEL SECURITY (RLS)
-- ============================================

-- RLS allows filtering rows based on user attributes.
-- Use case: Analysts can only see data for their assigned regions/segments.

USE CATALOG ecommerce_analytics_dev;
USE SCHEMA gold_layer;

-- Create a user-to-segment mapping table
CREATE TABLE IF NOT EXISTS user_segment_access (
    user_name STRING COMMENT 'Databricks user email',
    customer_segment STRING COMMENT 'Allowed customer segment',
    category_l1 STRING COMMENT 'Allowed product category (L1)',
    is_admin BOOLEAN DEFAULT FALSE COMMENT 'Admin can see all data'
)
USING DELTA
COMMENT 'Maps users to their data access permissions for RLS';

-- Insert sample access rules
INSERT INTO user_segment_access VALUES
    ('analyst1@company.com', 'Champion', 'electronics', FALSE),
    ('analyst1@company.com', 'Loyal', 'electronics', FALSE),
    ('analyst2@company.com', 'Champion', 'appliances', FALSE),
    ('admin@company.com', NULL, NULL, TRUE);

-- Create a function to check user access
CREATE OR REPLACE FUNCTION check_segment_access(segment STRING)
RETURNS BOOLEAN
RETURN (
    SELECT COALESCE(
        MAX(is_admin) = TRUE OR MAX(customer_segment = segment) = TRUE,
        FALSE
    )
    FROM user_segment_access
    WHERE user_name = current_user()
);

-- Create RLS-enabled view for customer_metrics
CREATE OR REPLACE VIEW customer_metrics_secured AS
SELECT *
FROM customer_metrics
WHERE check_segment_access(customer_segment) = TRUE;

-- Grant access to the secured view
GRANT SELECT ON VIEW customer_metrics_secured TO `data-analysts`;


-- Alternative: Using SQL functions for RLS
CREATE OR REPLACE VIEW product_performance_secured AS
SELECT *
FROM product_performance
WHERE (
    EXISTS (
        SELECT 1 FROM user_segment_access
        WHERE user_name = current_user()
        AND (is_admin = TRUE OR category_l1 = product_performance.category_l1)
    )
);

GRANT SELECT ON VIEW product_performance_secured TO `data-analysts`;


-- ============================================
-- SECTION 2: COLUMN LEVEL SECURITY (CLS)
-- ============================================

-- CLS restricts access to specific columns based on user roles.
-- Use case: Hide PII (user_id) from non-privileged users.

-- Method 1: Using views with column projection
CREATE OR REPLACE VIEW customer_metrics_no_pii AS
SELECT 
    -- Exclude PII columns
    -- user_id is hidden
    total_sessions,
    total_events,
    total_views,
    total_cart_adds,
    total_purchases,
    customer_lifetime_value,
    customer_segment,
    rfm_score,
    churn_risk,
    processed_timestamp
FROM customer_metrics;

GRANT SELECT ON VIEW customer_metrics_no_pii TO `data-analysts`;


-- Method 2: Column masking with dynamic views
CREATE OR REPLACE VIEW customer_metrics_masked AS
SELECT 
    CASE 
        WHEN is_member('data-engineers') THEN user_id
        ELSE NULL 
    END AS user_id,
    total_sessions,
    total_events,
    total_views,
    total_cart_adds,
    total_purchases,
    customer_lifetime_value,
    customer_segment,
    rfm_score,
    churn_risk,
    processed_timestamp
FROM customer_metrics;

GRANT SELECT ON VIEW customer_metrics_masked TO `data-analysts`;


-- ============================================
-- SECTION 3: DYNAMIC DATA MASKING
-- ============================================

-- Dynamic masking functions for different data types

-- Mask user_id to show only last 4 digits
CREATE OR REPLACE FUNCTION mask_user_id(user_id BIGINT)
RETURNS STRING
RETURN CONCAT('****', RIGHT(CAST(user_id AS STRING), 4));

-- Mask email addresses
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(
    LEFT(email, 2),
    '****',
    SUBSTRING(email, INSTR(email, '@'))
);

-- Mask price (show range instead of exact value)
CREATE OR REPLACE FUNCTION mask_price(price DOUBLE)
RETURNS STRING
RETURN CASE
    WHEN price < 10 THEN '$0-$10'
    WHEN price < 50 THEN '$10-$50'
    WHEN price < 100 THEN '$50-$100'
    WHEN price < 500 THEN '$100-$500'
    ELSE '$500+'
END;

-- Mask session ID
CREATE OR REPLACE FUNCTION mask_session(session_id STRING)
RETURNS STRING
RETURN CONCAT(LEFT(session_id, 8), '****');


-- Create dynamically masked view
CREATE OR REPLACE VIEW events_cleaned_masked AS
SELECT 
    event_id,
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    CASE 
        WHEN is_member('data-engineers') THEN price
        ELSE CAST(mask_price(price) AS DOUBLE)
    END AS price,
    CASE 
        WHEN is_member('data-engineers') THEN user_id
        ELSE CAST(mask_user_id(user_id) AS BIGINT)
    END AS user_id,
    CASE 
        WHEN is_member('data-engineers') THEN user_session
        ELSE mask_session(user_session)
    END AS user_session,
    event_date,
    event_hour,
    category_l1,
    category_l2,
    processed_timestamp
FROM silver_layer.events_cleaned;


-- ============================================
-- SECTION 4: AUDIT LOGGING
-- ============================================

-- Enable system tables for audit logging
-- (Requires workspace admin)

-- Query audit logs for data access
/*
SELECT 
    event_time,
    user_identity.email as user_email,
    action_name,
    request_params.full_name_arg as table_name,
    request_params.commandText as query_text,
    response.status_code
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit', 'queryEnd')
    AND request_params.full_name_arg LIKE '%ecommerce_analytics%'
ORDER BY event_time DESC
LIMIT 100;
*/

-- Create a view for easier audit analysis
/*
CREATE OR REPLACE VIEW gold_layer.data_access_audit AS
SELECT 
    DATE(event_time) as access_date,
    user_identity.email as user_email,
    action_name,
    request_params.full_name_arg as resource_name,
    response.status_code,
    event_time
FROM system.access.audit
WHERE request_params.full_name_arg LIKE '%ecommerce_analytics%'
    AND event_time >= CURRENT_DATE - INTERVAL 30 DAYS;
*/


-- ============================================
-- SECTION 5: DATA CLASSIFICATION TAGS
-- ============================================

-- Apply sensitivity tags to tables and columns
-- (Governance feature for data discovery)

-- Tag silver table with sensitivity level
ALTER TABLE silver_layer.events_cleaned
SET TAGS ('sensitivity' = 'confidential', 'contains_pii' = 'true');

-- Tag specific columns
/*
ALTER TABLE silver_layer.events_cleaned
ALTER COLUMN user_id SET TAGS ('pii' = 'true', 'mask_type' = 'partial');

ALTER TABLE silver_layer.events_cleaned
ALTER COLUMN user_session SET TAGS ('pii' = 'true', 'mask_type' = 'hash');

ALTER TABLE silver_layer.events_cleaned
ALTER COLUMN price SET TAGS ('sensitivity' = 'internal');
*/


-- ============================================
-- SECTION 6: ACCESS CONTROL SUMMARY
-- ============================================

-- Create a summary view of access controls
CREATE OR REPLACE VIEW gold_layer.security_summary AS
SELECT
    'customer_metrics' as table_name,
    'RLS' as security_type,
    'Filter by customer_segment based on user mapping' as description
UNION ALL
SELECT
    'customer_metrics_no_pii',
    'CLS',
    'View without user_id column'
UNION ALL
SELECT
    'customer_metrics_masked',
    'Dynamic Masking',
    'user_id masked for non-engineers'
UNION ALL
SELECT
    'events_cleaned_masked',
    'Dynamic Masking',
    'user_id, user_session, price masked for analysts'
UNION ALL
SELECT
    'product_performance_secured',
    'RLS',
    'Filter by category_l1 based on user mapping';


-- ============================================
-- SECTION 7: VERIFICATION QUERIES
-- ============================================

-- Check current user's access
SELECT current_user() as current_user;

-- Check if user is member of a group
SELECT is_member('data-engineers') as is_engineer;

-- Test RLS function
SELECT check_segment_access('Champion') as can_access_champion;

-- View security summary
SELECT * FROM gold_layer.security_summary;

-- Check grants on a table
-- SHOW GRANTS ON TABLE gold_layer.customer_metrics;


-- ============================================
-- SECTION 8: CLEANUP (USE WITH CAUTION)
-- ============================================

/*
DROP VIEW IF EXISTS gold_layer.customer_metrics_secured;
DROP VIEW IF EXISTS gold_layer.customer_metrics_no_pii;
DROP VIEW IF EXISTS gold_layer.customer_metrics_masked;
DROP VIEW IF EXISTS gold_layer.events_cleaned_masked;
DROP VIEW IF EXISTS gold_layer.product_performance_secured;
DROP VIEW IF EXISTS gold_layer.security_summary;
DROP TABLE IF EXISTS gold_layer.user_segment_access;
DROP FUNCTION IF EXISTS check_segment_access;
DROP FUNCTION IF EXISTS mask_user_id;
DROP FUNCTION IF EXISTS mask_email;
DROP FUNCTION IF EXISTS mask_price;
DROP FUNCTION IF EXISTS mask_session;
*/
