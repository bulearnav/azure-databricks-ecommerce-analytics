# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Quality Rules
# MAGIC 
# MAGIC This module defines data quality expectations and validation functions 
# MAGIC for the e-commerce events data.
# MAGIC 
# MAGIC **Quality Dimensions:**
# MAGIC - Completeness: Required fields are not null
# MAGIC - Validity: Values are within expected ranges/formats
# MAGIC - Consistency: Data types and formats are standardized
# MAGIC - Uniqueness: No duplicate events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum,
    isnan, isnull, length, regexp_extract
)
from typing import Dict, List, Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Rule Definitions

# COMMAND ----------

# Valid values for categorical columns
VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]

# Required columns that should never be null
REQUIRED_COLUMNS = ["event_time", "event_type", "product_id", "user_id", "price"]

# Columns that can be null (but tracked)
NULLABLE_COLUMNS = ["category_code", "brand", "user_session"]

# Business rules for numeric ranges
PRICE_MIN = 0.0
PRICE_MAX = 100000.0  # Reasonable max price

# Session ID pattern (UUID-like)
SESSION_PATTERN = r"^[a-f0-9\-]{30,40}$"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations (for DLT)

# COMMAND ----------

def get_dlt_expectations() -> Dict[str, str]:
    """
    Returns dictionary of DLT expectations.
    Key: expectation name
    Value: SQL expression that should evaluate to TRUE for valid records
    """
    return {
        # Completeness checks
        "valid_event_time": "event_time IS NOT NULL",
        "valid_event_type": "event_type IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_user_id": "user_id IS NOT NULL",
        "valid_price": "price IS NOT NULL",
        
        # Validity checks
        "event_type_in_allowed_values": f"event_type IN ('view', 'cart', 'remove_from_cart', 'purchase')",
        "price_is_positive": "price >= 0",
        "price_within_range": f"price <= {PRICE_MAX}",
        "product_id_is_positive": "product_id > 0",
        "user_id_is_positive": "user_id > 0",
        
        # Format checks
        "category_id_is_positive": "category_id IS NULL OR category_id > 0",
    }

def get_dlt_quarantine_expectations() -> Dict[str, str]:
    """
    Returns expectations for quarantine - these fail the row.
    Records failing these go to dead letter queue.
    """
    return {
        "critical_event_time": "event_time IS NOT NULL",
        "critical_user_id": "user_id IS NOT NULL",
        "critical_product_id": "product_id IS NOT NULL",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Functions

# COMMAND ----------

def validate_completeness(df: DataFrame) -> DataFrame:
    """
    Adds a column indicating completeness issues.
    Returns DataFrame with 'completeness_issues' column.
    """
    issues = []
    
    for col_name in REQUIRED_COLUMNS:
        issues.append(
            when(col(col_name).isNull(), lit(col_name))
        )
    
    # Combine all issues
    from pyspark.sql.functions import concat_ws, array, array_remove
    
    return df.withColumn(
        "completeness_issues",
        array_remove(array(*issues), None)
    ).withColumn(
        "is_complete",
        col("completeness_issues").isNull() | (length(col("completeness_issues")) == 0)
    )

# COMMAND ----------

def validate_event_type(df: DataFrame) -> DataFrame:
    """
    Validates event_type values and flags invalid ones.
    """
    return df.withColumn(
        "is_valid_event_type",
        col("event_type").isin(VALID_EVENT_TYPES)
    )

# COMMAND ----------

def validate_price(df: DataFrame) -> DataFrame:
    """
    Validates price is within expected range.
    """
    return df.withColumn(
        "is_valid_price",
        (col("price") >= PRICE_MIN) & (col("price") <= PRICE_MAX)
    )

# COMMAND ----------

def validate_ids(df: DataFrame) -> DataFrame:
    """
    Validates ID columns are positive.
    """
    return df.withColumn(
        "is_valid_product_id",
        col("product_id") > 0
    ).withColumn(
        "is_valid_user_id",
        col("user_id") > 0
    ).withColumn(
        "is_valid_category_id",
        col("category_id").isNull() | (col("category_id") > 0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Report Functions

# COMMAND ----------

def generate_quality_report(df: DataFrame) -> Dict[str, any]:
    """
    Generates a comprehensive data quality report.
    
    Returns dict with:
    - total_records: Total row count
    - null_counts: Dict of null counts per column
    - invalid_event_types: Count of invalid event types
    - out_of_range_prices: Count of prices outside valid range
    - quality_score: Overall percentage of valid records
    """
    total = df.count()
    
    # Null counts
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count
    
    # Invalid event types
    invalid_events = df.filter(~col("event_type").isin(VALID_EVENT_TYPES)).count()
    
    # Price issues
    price_issues = df.filter(
        (col("price") < PRICE_MIN) | (col("price") > PRICE_MAX) | col("price").isNull()
    ).count()
    
    # Calculate quality score
    # A record is "valid" if it passes all critical checks
    valid_records = df.filter(
        col("event_time").isNotNull() &
        col("event_type").isin(VALID_EVENT_TYPES) &
        col("product_id").isNotNull() &
        col("user_id").isNotNull() &
        (col("price") >= PRICE_MIN) &
        (col("price") <= PRICE_MAX)
    ).count()
    
    quality_score = (valid_records / total * 100) if total > 0 else 0
    
    return {
        "total_records": total,
        "null_counts": null_counts,
        "invalid_event_types": invalid_events,
        "out_of_range_prices": price_issues,
        "valid_records": valid_records,
        "quality_score": round(quality_score, 2)
    }

# COMMAND ----------

def print_quality_report(report: Dict[str, any]) -> None:
    """Pretty prints the quality report."""
    print("=" * 60)
    print("DATA QUALITY REPORT")
    print("=" * 60)
    print(f"Total Records: {report['total_records']:,}")
    print(f"Valid Records: {report['valid_records']:,}")
    print(f"Quality Score: {report['quality_score']}%")
    print("-" * 60)
    print("Null Counts:")
    for col_name, count in report['null_counts'].items():
        pct = (count / report['total_records'] * 100) if report['total_records'] > 0 else 0
        print(f"  {col_name}: {count:,} ({pct:.2f}%)")
    print("-" * 60)
    print(f"Invalid Event Types: {report['invalid_event_types']:,}")
    print(f"Out of Range Prices: {report['out_of_range_prices']:,}")
    print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate Detection

# COMMAND ----------

def count_duplicates(df: DataFrame, key_columns: List[str]) -> int:
    """
    Counts duplicate records based on key columns.
    
    Args:
        df: Input DataFrame
        key_columns: List of columns that define uniqueness
        
    Returns:
        Count of duplicate records (total - distinct)
    """
    total = df.count()
    distinct = df.dropDuplicates(key_columns).count()
    return total - distinct

def get_duplicate_records(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Returns DataFrame containing duplicate records.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.partitionBy(key_columns).orderBy(col("event_time"))
    
    return (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") > 1)
        .drop("row_num")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Filters

# COMMAND ----------

def filter_valid_records(df: DataFrame) -> DataFrame:
    """
    Filters DataFrame to only include valid records.
    This is the main function used in silver transformation.
    """
    return df.filter(
        # Required fields are not null
        col("event_time").isNotNull() &
        col("event_type").isNotNull() &
        col("product_id").isNotNull() &
        col("user_id").isNotNull() &
        col("price").isNotNull() &
        
        # Event type is valid
        col("event_type").isin(VALID_EVENT_TYPES) &
        
        # Price is in valid range
        (col("price") >= PRICE_MIN) &
        (col("price") <= PRICE_MAX) &
        
        # IDs are positive
        (col("product_id") > 0) &
        (col("user_id") > 0)
    )

def get_quarantined_records(df: DataFrame) -> DataFrame:
    """
    Returns records that failed quality checks (dead letter queue).
    """
    return df.filter(
        col("event_time").isNull() |
        col("event_type").isNull() |
        col("product_id").isNull() |
        col("user_id").isNull() |
        col("price").isNull() |
        ~col("event_type").isin(VALID_EVENT_TYPES) |
        (col("price") < PRICE_MIN) |
        (col("price") > PRICE_MAX) |
        (col("product_id") <= 0) |
        (col("user_id") <= 0)
    )
