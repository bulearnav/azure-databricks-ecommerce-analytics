# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Events Schema Definition
# MAGIC 
# MAGIC This module defines the schema for the e-commerce events data, 
# MAGIC including both the raw CSV schema and the enriched bronze table schema.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw CSV Schema
# MAGIC Schema for reading CSV files from the Kaggle dataset.

# COMMAND ----------

# Schema for raw CSV files
RAW_CSV_SCHEMA = StructType([
    StructField("event_time", StringType(), True),      # Will be parsed to timestamp
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),   # Nullable - often null for accessories
    StructField("brand", StringType(), True),           # Nullable - some products have no brand
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table Schema
# MAGIC Extended schema with audit columns for the bronze Delta table.

# COMMAND ----------

# Schema for bronze Delta table (includes audit columns)
BRONZE_TABLE_SCHEMA = StructType([
    # Original columns
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
    
    # Audit columns
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("source_file", StringType(), False),
    StructField("created_at", TimestampType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Definitions

# COMMAND ----------

# Column names for reference
COLUMNS = {
    "event_time": "Timestamp when the event occurred (UTC)",
    "event_type": "Type of user action: view, cart, remove_from_cart, purchase",
    "product_id": "Unique product identifier",
    "category_id": "Product's category ID",
    "category_code": "Category taxonomy/code name (may be null)",
    "brand": "Downcased brand name (may be null)",
    "price": "Product price in store currency",
    "user_id": "Permanent user identifier",
    "user_session": "Temporary session ID (changes on user return)"
}

# Valid event types
VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_raw_schema():
    """Returns the schema for reading raw CSV files."""
    return RAW_CSV_SCHEMA

def get_bronze_schema():
    """Returns the schema for the bronze Delta table."""
    return BRONZE_TABLE_SCHEMA

def get_valid_event_types():
    """Returns list of valid event types."""
    return VALID_EVENT_TYPES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Validation

# COMMAND ----------

def validate_dataframe_schema(df, expected_columns):
    """
    Validates that a DataFrame contains all expected columns.
    
    Args:
        df: Spark DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        Tuple (is_valid: bool, missing_columns: list, extra_columns: list)
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set
    
    is_valid = len(missing) == 0
    
    return is_valid, list(missing), list(extra)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Type Mapping

# COMMAND ----------

# Mapping from CSV string types to Spark types
CSV_TO_SPARK_TYPE_MAP = {
    "event_time": "timestamp",
    "event_type": "string",
    "product_id": "long",
    "category_id": "long",
    "category_code": "string",
    "brand": "string",
    "price": "double",
    "user_id": "long",
    "user_session": "string"
}

def get_type_for_column(column_name):
    """Returns the expected Spark data type for a column."""
    return CSV_TO_SPARK_TYPE_MAP.get(column_name, "string")
