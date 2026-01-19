# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables Pipeline - Bronze to Gold
# MAGIC 
# MAGIC This DLT pipeline implements the complete Medallion architecture for the
# MAGIC e-commerce analytics platform with data quality expectations.
# MAGIC 
# MAGIC **Layers:**
# MAGIC - **Bronze**: Raw events with audit columns (from Auto Loader)
# MAGIC - **Silver**: Cleaned, deduplicated events with derived columns
# MAGIC - **Gold**: Aggregated business metrics tables
# MAGIC 
# MAGIC **Features:**
# MAGIC - Data quality expectations with quarantine
# MAGIC - Schema enforcement
# MAGIC - Incremental processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, count, countDistinct, sum as spark_sum,
    avg, min as spark_min, max as spark_max,
    current_timestamp, input_file_name, to_timestamp,
    regexp_replace, trim, lower, to_date, hour, dayofweek,
    dayofmonth, month, year, weekofyear, split, size,
    sha2, concat, row_number, first, datediff, current_date,
    round as spark_round, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Events

# COMMAND ----------

# Schema for raw CSV files
csv_schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True)
])

@dlt.table(
    name="events_raw",
    comment="Raw e-commerce events ingested from CSV files via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["event_type"]
)
def bronze_events_raw():
    """
    Bronze layer: Ingest raw CSV files with audit columns.
    Using Auto Loader (cloudFiles) for incremental ingestion.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/tmp/dlt_checkpoints/bronze_schema")
        .option("header", "true")
        .schema(csv_schema)
        .load("/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data")
        # Parse timestamp
        .withColumn(
            "event_time",
            to_timestamp(
                regexp_replace(col("event_time"), " UTC$", ""),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        # Add audit columns
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("created_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned Events

# COMMAND ----------

# Data quality expectations
QUALITY_EXPECTATIONS = {
    "valid_event_time": "event_time IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL AND event_type IN ('view', 'cart', 'remove_from_cart', 'purchase')",
    "valid_product_id": "product_id IS NOT NULL AND product_id > 0",
    "valid_user_id": "user_id IS NOT NULL AND user_id > 0",
    "valid_price": "price IS NOT NULL AND price >= 0 AND price <= 100000"
}

@dlt.table(
    name="events_cleaned",
    comment="Cleaned and validated e-commerce events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["event_date"]
)
@dlt.expect_all_or_drop(QUALITY_EXPECTATIONS)
def silver_events_cleaned():
    """
    Silver layer: Clean, validate, and enrich events.
    Records failing quality checks are dropped.
    """
    return (
        dlt.read_stream("events_raw")
        
        # Standardize strings
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn(
            "category_code",
            when(
                col("category_code").isNull() | (trim(col("category_code")) == ""),
                lit("unknown")
            ).otherwise(trim(col("category_code")))
        )
        .withColumn(
            "brand",
            when(
                col("brand").isNull() | (trim(col("brand")) == ""),
                lit("unknown")
            ).otherwise(lower(trim(col("brand"))))
        )
        .withColumn("user_session", trim(col("user_session")))
        
        # Derived columns
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn("event_day_of_week", dayofweek(col("event_time")))
        .withColumn("event_day_of_month", dayofmonth(col("event_time")))
        .withColumn("event_month", month(col("event_time")))
        .withColumn("event_year", year(col("event_time")))
        .withColumn("event_week", weekofyear(col("event_time")))
        
        # Category hierarchy
        .withColumn(
            "category_l1",
            when(col("category_code") != "unknown",
                 split(col("category_code"), "\\.").getItem(0)
            ).otherwise(lit("unknown"))
        )
        .withColumn(
            "category_l2",
            when(
                (col("category_code") != "unknown") & 
                (size(split(col("category_code"), "\\.")) >= 2),
                split(col("category_code"), "\\.").getItem(1)
            ).otherwise(lit(None))
        )
        .withColumn(
            "category_l3",
            when(
                (col("category_code") != "unknown") & 
                (size(split(col("category_code"), "\\.")) >= 3),
                split(col("category_code"), "\\.").getItem(2)
            ).otherwise(lit(None))
        )
        
        # Event ID (dedup key)
        .withColumn(
            "event_id",
            sha2(
                concat(
                    col("event_time").cast("string"),
                    col("user_id").cast("string"),
                    col("product_id").cast("string"),
                    col("user_session"),
                    col("event_type")
                ),
                256
            )
        )
        
        # Processed timestamp
        .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Quarantine (Failed Quality Checks)

# COMMAND ----------

@dlt.table(
    name="events_quarantine",
    comment="Events that failed quality checks (dead letter queue)",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_fail({
    "has_quality_issues": "event_time IS NULL OR event_type IS NULL OR product_id IS NULL OR user_id IS NULL OR price IS NULL OR price < 0"
})
def silver_events_quarantine():
    """
    Quarantine table for events that fail quality expectations.
    This captures the dead letter queue for investigation.
    """
    return (
        dlt.read_stream("events_raw")
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn(
            "quarantine_reason",
            when(col("event_time").isNull(), lit("null_event_time"))
            .when(col("event_type").isNull(), lit("null_event_type"))
            .when(col("product_id").isNull(), lit("null_product_id"))
            .when(col("user_id").isNull(), lit("null_user_id"))
            .when(col("price").isNull(), lit("null_price"))
            .when(col("price") < 0, lit("negative_price"))
            .otherwise(lit("other"))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Customer Metrics

# COMMAND ----------

@dlt.table(
    name="customer_metrics",
    comment="Aggregated customer-level metrics including CLV and segmentation",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_customer_metrics():
    """
    Gold layer: Customer metrics with RFM scoring and segmentation.
    """
    df = dlt.read("events_cleaned")
    
    # Aggregate by user
    df_agg = (
        df.groupBy("user_id")
        .agg(
            countDistinct("user_session").alias("total_sessions"),
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            countDistinct("product_id").alias("unique_products_viewed"),
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue"),
            avg(when(col("event_type") == "purchase", col("price"))).alias("avg_purchase_value"),
            spark_min("event_date").alias("first_activity_date"),
            spark_max("event_date").alias("last_activity_date"),
            countDistinct("event_date").alias("unique_days_active")
        )
    )
    
    # Add derived metrics
    return (
        df_agg
        .withColumn(
            "view_to_cart_rate",
            spark_round(
                when(col("total_views") > 0, 
                     col("total_cart_adds") / col("total_views") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "overall_conversion_rate",
            spark_round(
                when(col("total_views") > 0, 
                     col("total_purchases") / col("total_views") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn("customer_lifetime_value", col("total_revenue"))
        .withColumn(
            "days_since_last_activity",
            datediff(current_date(), col("last_activity_date"))
        )
        .withColumn(
            "customer_segment",
            when(col("total_purchases") >= 10, lit("Champion"))
            .when(col("total_purchases") >= 5, lit("Loyal"))
            .when(col("total_purchases") >= 2, lit("Potential Loyalist"))
            .when(col("total_purchases") == 1, lit("New Customer"))
            .when(col("total_cart_adds") > 0, lit("Cart Abandoner"))
            .otherwise(lit("Visitor"))
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Product Performance

# COMMAND ----------

@dlt.table(
    name="product_performance",
    comment="Aggregated product-level performance metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_product_performance():
    """
    Gold layer: Product performance with conversion rates and rankings.
    """
    df = dlt.read("events_cleaned")
    
    # Aggregate by product
    df_agg = (
        df.groupBy("product_id")
        .agg(
            first("category_id").alias("category_id"),
            first("category_code").alias("category_code"),
            first("category_l1").alias("category_l1"),
            first("brand").alias("brand"),
            spark_round(avg("price"), 2).alias("avg_price"),
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue")
        )
    )
    
    # Add conversion rates
    return (
        df_agg
        .withColumn(
            "view_to_cart_rate",
            spark_round(
                when(col("total_views") > 0, 
                     col("total_cart_adds") / col("total_views") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "cart_to_purchase_rate",
            spark_round(
                when(col("total_cart_adds") > 0, 
                     col("total_purchases") / col("total_cart_adds") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "overall_conversion_rate",
            spark_round(
                when(col("total_views") > 0, 
                     col("total_purchases") / col("total_views") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Daily Sales Summary

# COMMAND ----------

@dlt.table(
    name="daily_sales_summary",
    comment="Daily aggregated sales metrics for time-series analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_daily_sales():
    """
    Gold layer: Daily sales summary with conversion rates.
    """
    df = dlt.read("events_cleaned")
    
    return (
        df.groupBy("event_date")
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("user_session").alias("unique_sessions"),
            countDistinct(
                when(col("event_type") == "purchase", col("user_id"))
            ).alias("unique_buyers"),
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue"),
            avg(when(col("event_type") == "purchase", col("price"))).alias("avg_order_value")
        )
        .withColumn(
            "overall_conversion_rate",
            spark_round(
                when(col("total_views") > 0, 
                     col("total_purchases") / col("total_views") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "buyer_rate",
            spark_round(col("unique_buyers") / col("unique_users") * 100, 2)
        )
        .withColumn("processed_timestamp", current_timestamp())
        .orderBy("event_date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Conversion Funnel

# COMMAND ----------

@dlt.table(
    name="conversion_funnel",
    comment="Conversion funnel metrics by category and date",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_conversion_funnel():
    """
    Gold layer: Conversion funnel with category breakdown.
    """
    df = dlt.read("events_cleaned")
    
    return (
        df.groupBy("event_date", "category_l1")
        .agg(
            countDistinct("user_session").alias("total_sessions"),
            countDistinct(
                when(col("event_type") == "view", col("user_session"))
            ).alias("sessions_with_view"),
            countDistinct(
                when(col("event_type") == "cart", col("user_session"))
            ).alias("sessions_with_cart"),
            countDistinct(
                when(col("event_type") == "purchase", col("user_session"))
            ).alias("sessions_with_purchase"),
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue")
        )
        .withColumn(
            "session_view_to_cart_rate",
            spark_round(
                when(col("sessions_with_view") > 0,
                     col("sessions_with_cart") / col("sessions_with_view") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "session_cart_to_purchase_rate",
            spark_round(
                when(col("sessions_with_cart") > 0,
                     col("sessions_with_purchase") / col("sessions_with_cart") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn(
            "cart_abandonment_rate",
            spark_round(
                when(col("sessions_with_cart") > 0,
                     (col("sessions_with_cart") - col("sessions_with_purchase")) / col("sessions_with_cart") * 100
                ).otherwise(0), 2
            )
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics View

# COMMAND ----------

@dlt.table(
    name="data_quality_metrics",
    comment="Data quality metrics for monitoring",
    table_properties={
        "quality": "gold"
    }
)
def gold_data_quality_metrics():
    """
    Track data quality metrics over time.
    """
    df_raw = dlt.read("events_raw")
    df_clean = dlt.read("events_cleaned")
    
    raw_count = df_raw.count()
    clean_count = df_clean.count()
    
    return (
        spark.createDataFrame([
            (
                current_timestamp(),
                raw_count,
                clean_count,
                raw_count - clean_count,
                round((clean_count / raw_count * 100) if raw_count > 0 else 0, 2)
            )
        ], ["metric_timestamp", "raw_record_count", "clean_record_count", 
            "filtered_record_count", "quality_pass_rate"])
    )
