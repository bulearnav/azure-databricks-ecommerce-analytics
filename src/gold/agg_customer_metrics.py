# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Customer Metrics
# MAGIC 
# MAGIC This notebook creates aggregated customer-level metrics for business analytics.
# MAGIC 
# MAGIC **Metrics:**
# MAGIC - Session and activity counts
# MAGIC - Customer Lifetime Value (CLV)
# MAGIC - Purchase frequency and recency
# MAGIC - Engagement scores
# MAGIC - Customer segmentation attributes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("catalog", "ecommerce_analytics_dev", "Catalog Name")
dbutils.widgets.text("schema_silver", "silver_layer", "Silver Schema")
dbutils.widgets.text("schema_gold", "gold_layer", "Gold Schema")

# Get parameters
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")

# Table names
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.events_cleaned"
GOLD_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.customer_metrics"

print(f"Configuration:")
print(f"  Source: {SILVER_TABLE}")
print(f"  Target: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, count, countDistinct, sum as spark_sum,
    avg, min as spark_min, max as spark_max,
    datediff, current_date, current_timestamp,
    first, last, collect_set, size, array_distinct,
    round as spark_round, expr
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Gold Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")
print(f"Schema {CATALOG}.{SCHEMA_GOLD} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Silver Data

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)
print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Customer Metrics

# COMMAND ----------

df_customer_metrics = (
    df_silver
    .groupBy("user_id")
    .agg(
        # ---- Activity Metrics ----
        countDistinct("user_session").alias("total_sessions"),
        count("*").alias("total_events"),
        
        # Event type counts
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
        spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
        spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("total_cart_removes"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        
        # ---- Product Engagement ----
        countDistinct("product_id").alias("unique_products_viewed"),
        countDistinct(
            when(col("event_type") == "cart", col("product_id"))
        ).alias("unique_products_carted"),
        countDistinct(
            when(col("event_type") == "purchase", col("product_id"))
        ).alias("unique_products_purchased"),
        
        # ---- Category Engagement ----
        countDistinct("category_l1").alias("unique_categories_engaged"),
        countDistinct("brand").alias("unique_brands_engaged"),
        
        # ---- Revenue Metrics ----
        spark_sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0)
        ).alias("total_revenue"),
        
        avg(
            when(col("event_type") == "purchase", col("price"))
        ).alias("avg_purchase_value"),
        
        spark_max(
            when(col("event_type") == "purchase", col("price"))
        ).alias("max_purchase_value"),
        
        # ---- Temporal Metrics ----
        spark_min("event_time").alias("first_activity_time"),
        spark_max("event_time").alias("last_activity_time"),
        spark_min("event_date").alias("first_activity_date"),
        spark_max("event_date").alias("last_activity_date"),
        
        # First and last purchase dates
        spark_min(
            when(col("event_type") == "purchase", col("event_date"))
        ).alias("first_purchase_date"),
        spark_max(
            when(col("event_type") == "purchase", col("event_date"))
        ).alias("last_purchase_date"),
        
        # ---- Behavior Patterns ----
        # Most active hour
        expr("""
            mode(event_hour)
        """).alias("most_active_hour"),
        
        # Top category
        expr("""
            mode(category_l1)
        """).alias("top_category"),
        
        # Top brand
        expr("""
            mode(CASE WHEN brand != 'unknown' THEN brand END)
        """).alias("top_brand"),
        
        # Unique days active
        countDistinct("event_date").alias("unique_days_active")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Derived Metrics

# COMMAND ----------

df_enriched = (
    df_customer_metrics
    
    # ---- Conversion Rates ----
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
    
    # ---- Customer Lifetime Value ----
    .withColumn("customer_lifetime_value", col("total_revenue"))
    
    # ---- Recency ----
    .withColumn(
        "days_since_last_activity",
        datediff(current_date(), col("last_activity_date"))
    )
    .withColumn(
        "days_since_last_purchase",
        when(col("last_purchase_date").isNotNull(),
             datediff(current_date(), col("last_purchase_date"))
        ).otherwise(lit(None))
    )
    
    # ---- Customer Tenure ----
    .withColumn(
        "customer_tenure_days",
        datediff(col("last_activity_date"), col("first_activity_date"))
    )
    
    # ---- Frequency Metrics ----
    .withColumn(
        "avg_events_per_session",
        spark_round(col("total_events") / col("total_sessions"), 2)
    )
    .withColumn(
        "avg_events_per_day",
        spark_round(
            when(col("unique_days_active") > 0,
                 col("total_events") / col("unique_days_active")
            ).otherwise(0), 2
        )
    )
    
    # ---- Customer Segmentation ----
    .withColumn(
        "customer_segment",
        when(col("total_purchases") >= 10, lit("Champion"))
        .when(col("total_purchases") >= 5, lit("Loyal"))
        .when(col("total_purchases") >= 2, lit("Potential Loyalist"))
        .when(col("total_purchases") == 1, lit("New Customer"))
        .when(col("total_cart_adds") > 0, lit("Cart Abandoner"))
        .when(col("total_views") > 10, lit("Engaged Browser"))
        .otherwise(lit("Visitor"))
    )
    
    # ---- RFM Scoring ----
    # Recency Score (1-5, 5 is best - most recent)
    .withColumn(
        "recency_score",
        when(col("days_since_last_activity") <= 7, 5)
        .when(col("days_since_last_activity") <= 14, 4)
        .when(col("days_since_last_activity") <= 30, 3)
        .when(col("days_since_last_activity") <= 60, 2)
        .otherwise(1)
    )
    
    # Frequency Score (1-5, 5 is best - most sessions)
    .withColumn(
        "frequency_score",
        when(col("total_sessions") >= 20, 5)
        .when(col("total_sessions") >= 10, 4)
        .when(col("total_sessions") >= 5, 3)
        .when(col("total_sessions") >= 2, 2)
        .otherwise(1)
    )
    
    # Monetary Score (1-5, 5 is best - highest CLV)
    .withColumn(
        "monetary_score",
        when(col("total_revenue") >= 1000, 5)
        .when(col("total_revenue") >= 500, 4)
        .when(col("total_revenue") >= 100, 3)
        .when(col("total_revenue") > 0, 2)
        .otherwise(1)
    )
    
    # Combined RFM Score
    .withColumn(
        "rfm_score",
        col("recency_score") + col("frequency_score") + col("monetary_score")
    )
    
    # ---- Churn Risk ----
    .withColumn(
        "churn_risk",
        when(
            (col("days_since_last_activity") > 30) & (col("total_purchases") > 0),
            lit("High")
        )
        .when(
            (col("days_since_last_activity") > 14) & (col("total_purchases") > 0),
            lit("Medium")
        )
        .when(col("total_purchases") > 0, lit("Low"))
        .otherwise(lit("N/A"))
    )
    
    # ---- Audit Column ----
    .withColumn("processed_timestamp", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Gold Table

# COMMAND ----------

# Write to gold table
(
    df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

print(f"Gold table created: {GOLD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Results

# COMMAND ----------

# Count records
gold_count = spark.table(GOLD_TABLE).count()
print(f"Total customers in {GOLD_TABLE}: {gold_count:,}")

# COMMAND ----------

# Sample data
display(spark.table(GOLD_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Customer Segment Distribution

# COMMAND ----------

print("Customer Segment Distribution:")
display(
    spark.table(GOLD_TABLE)
    .groupBy("customer_segment")
    .agg(
        count("*").alias("customer_count"),
        spark_round(avg("total_revenue"), 2).alias("avg_revenue"),
        spark_round(avg("total_purchases"), 2).alias("avg_purchases")
    )
    .orderBy("customer_count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: High-Value Customers

# COMMAND ----------

print("Top 10 Customers by CLV:")
display(
    spark.table(GOLD_TABLE)
    .select(
        "user_id", "customer_lifetime_value", "total_purchases",
        "customer_segment", "rfm_score", "churn_risk"
    )
    .orderBy("customer_lifetime_value", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
summary = spark.table(GOLD_TABLE).agg(
    count("*").alias("total_customers"),
    spark_sum("total_revenue").alias("total_revenue"),
    avg("total_revenue").alias("avg_clv"),
    avg("total_purchases").alias("avg_purchases"),
    spark_sum(when(col("churn_risk") == "High", 1).otherwise(0)).alias("high_churn_risk")
).collect()[0]

print("=" * 60)
print("CUSTOMER METRICS COMPLETE")
print("=" * 60)
print(f"Target Table: {GOLD_TABLE}")
print(f"Total Customers: {summary['total_customers']:,}")
print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
print(f"Average CLV: ${summary['avg_clv']:.2f}")
print(f"Average Purchases: {summary['avg_purchases']:.2f}")
print(f"High Churn Risk Customers: {summary['high_churn_risk']:,}")
print("=" * 60)
