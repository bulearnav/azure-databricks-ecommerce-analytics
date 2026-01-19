-- ============================================
-- Compute Policies Setup Script
-- E-Commerce Analytics Platform
-- ============================================
-- This script defines cluster policies for governance and cost control.
-- Policies restrict cluster configurations to approved settings.
--
-- Note: Cluster policies are created via API or Terraform.
-- This file documents the policy definitions for reference.
-- ============================================


-- ============================================
-- SECTION 1: DEVELOPMENT POLICY
-- ============================================

/*
Policy Name: ecommerce-dev-policy
Description: Restrictive policy for development clusters

JSON Definition:
{
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-scala2\\.12",
    "defaultValue": "14.3.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2"
    ],
    "defaultValue": "Standard_DS3_v2"
  },
  "num_workers": {
    "type": "range",
    "minValue": 0,
    "maxValue": 2,
    "defaultValue": 0
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 60,
    "defaultValue": 30
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "USER_ISOLATION"
  },
  "custom_tags.environment": {
    "type": "fixed",
    "value": "development"
  },
  "custom_tags.cost_center": {
    "type": "fixed",
    "value": "data-engineering"
  }
}
*/


-- ============================================
-- SECTION 2: PRODUCTION POLICY
-- ============================================

/*
Policy Name: ecommerce-prod-policy
Description: Policy for production job clusters

JSON Definition:
{
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-(photon-)?scala2\\.12",
    "defaultValue": "14.3.x-photon-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS4_v2",
      "Standard_DS5_v2",
      "Standard_E4ds_v5",
      "Standard_E8ds_v5"
    ],
    "defaultValue": "Standard_DS4_v2"
  },
  "autoscale.min_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 4,
    "defaultValue": 2
  },
  "autoscale.max_workers": {
    "type": "range",
    "minValue": 2,
    "maxValue": 8,
    "defaultValue": 4
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 30,
    "maxValue": 180,
    "defaultValue": 120
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "USER_ISOLATION"
  },
  "spark_conf.spark.databricks.delta.preview.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "spark_conf.spark.databricks.io.cache.enabled": {
    "type": "fixed",
    "value": "true"
  },
  "custom_tags.environment": {
    "type": "fixed",
    "value": "production"
  },
  "custom_tags.sla": {
    "type": "fixed",
    "value": "high"
  }
}
*/


-- ============================================
-- SECTION 3: COST OPTIMIZED POLICY
-- ============================================

/*
Policy Name: ecommerce-cost-optimized-policy
Description: Policy using spot instances for cost savings

JSON Definition:
{
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-scala2\\.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_D4s_v3",
      "Standard_E4s_v3"
    ]
  },
  "azure_attributes.availability": {
    "type": "fixed",
    "value": "SPOT_WITH_FALLBACK_AZURE"
  },
  "azure_attributes.spot_bid_max_price": {
    "type": "range",
    "minValue": -1,
    "maxValue": 0.5
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 4
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 30,
    "defaultValue": 15
  },
  "custom_tags.cost_optimization": {
    "type": "fixed",
    "value": "spot-instances"
  }
}
*/


-- ============================================
-- SECTION 4: DLT PIPELINE POLICY
-- ============================================

/*
Policy Name: ecommerce-dlt-policy
Description: Policy for Delta Live Tables pipeline clusters

JSON Definition:
{
  "cluster_type": {
    "type": "fixed",
    "value": "dlt"
  },
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-(photon-)?scala2\\.12",
    "defaultValue": "14.3.x-photon-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2",
      "Standard_DS5_v2"
    ]
  },
  "autoscale.min_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 2
  },
  "autoscale.max_workers": {
    "type": "range",
    "minValue": 2,
    "maxValue": 4
  },
  "custom_tags.pipeline_type": {
    "type": "fixed",
    "value": "dlt"
  }
}
*/


-- ============================================
-- SECTION 5: ANALYTICS POLICY
-- ============================================

/*
Policy Name: ecommerce-analytics-policy
Description: Policy for data analyst interactive clusters

JSON Definition:
{
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-scala2\\.12",
    "defaultValue": "14.3.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2"
    ],
    "defaultValue": "Standard_DS3_v2"
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 2,
    "defaultValue": 1
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 30,
    "maxValue": 120,
    "defaultValue": 60
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "USER_ISOLATION"
  },
  "single_user_name": {
    "type": "regex",
    "pattern": ".*@company\\.com",
    "hidden": true
  },
  "custom_tags.purpose": {
    "type": "fixed",
    "value": "analytics"
  }
}
*/


-- ============================================
-- SECTION 6: API COMMANDS TO CREATE POLICIES
-- ============================================

/*
# Create policy via Databricks CLI

# Dev Policy
databricks cluster-policies create --json '{
  "name": "ecommerce-dev-policy",
  "definition": "{\"spark_version\":{\"type\":\"regex\",\"pattern\":\"14\\\\.[0-9]+\\\\.x-scala2\\\\.12\"},\"num_workers\":{\"type\":\"range\",\"minValue\":0,\"maxValue\":2},\"autotermination_minutes\":{\"type\":\"range\",\"minValue\":10,\"maxValue\":60,\"defaultValue\":30}}"
}'

# Get policy ID
databricks cluster-policies list --output JSON | jq '.[] | select(.name=="ecommerce-dev-policy") | .policy_id'

# Update policy
databricks cluster-policies edit --policy-id <policy-id> --json '{
  "name": "ecommerce-dev-policy",
  "definition": "<updated-json>"
}'

# Delete policy
databricks cluster-policies delete --policy-id <policy-id>
*/


-- ============================================
-- SECTION 7: GRANT POLICY PERMISSIONS
-- ============================================

/*
-- Grant permission to use a policy
-- (Done via API or Terraform)

# Grant to group
databricks permissions update clusters/policies/<policy-id> \
  --json '{"access_control_list":[{"group_name":"data-engineers","permission_level":"CAN_USE"}]}'

# Grant to user
databricks permissions update clusters/policies/<policy-id> \
  --json '{"access_control_list":[{"user_name":"user@company.com","permission_level":"CAN_USE"}]}'
*/


-- ============================================
-- SECTION 8: POLICY ASSIGNMENTS
-- ============================================

/*
Policy Assignment Summary:

| Policy                       | Groups              | Use Case                    |
|------------------------------|---------------------|----------------------------|
| ecommerce-dev-policy         | data-engineers      | Development & testing      |
| ecommerce-prod-policy        | data-engineers      | Production job clusters    |
| ecommerce-cost-optimized     | data-engineers      | Non-critical batch jobs    |
| ecommerce-dlt-policy         | data-engineers      | DLT pipelines              |
| ecommerce-analytics-policy   | data-analysts       | Ad-hoc analytics           |
*/


-- ============================================
-- SECTION 9: MONITORING & COMPLIANCE
-- ============================================

-- Query to check cluster compliance with policies
/*
SELECT 
    cluster_id,
    cluster_name,
    policy_id,
    state,
    node_type_id,
    num_workers,
    autotermination_minutes,
    create_time
FROM system.compute.clusters
WHERE policy_id IS NOT NULL
    AND cluster_name LIKE '%ecommerce%'
ORDER BY create_time DESC;
*/

-- Query to identify clusters without policies (potential compliance issue)
/*
SELECT 
    cluster_id,
    cluster_name,
    creator_user_name,
    node_type_id,
    num_workers,
    create_time
FROM system.compute.clusters
WHERE policy_id IS NULL
    AND cluster_name LIKE '%ecommerce%'
ORDER BY create_time DESC;
*/
