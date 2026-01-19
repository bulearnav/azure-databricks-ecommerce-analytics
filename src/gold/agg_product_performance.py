# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Product Performance
# MAGIC 
# MAGIC This notebook creates aggregated product-level metrics for business analytics.
# MAGIC 
# MAGIC **Metrics:**
# MAGIC - Views, cart adds, purchases per product
# MAGIC - Conversion rates (view→cart, cart→purchase)
# MAGIC - Revenue by product, category, brand
# MAGIC - Product ranking and popularity scores

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
GOLD_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.product_performance"

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
    first, last, current_timestamp,
    round as spark_round, percent_rank, dense_rank
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Silver Data

# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)
print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate Product Metrics

# COMMAND ----------

df_product_metrics = (
    df_silver
    .groupBy("product_id")
    .agg(
        # ---- Product Attributes (first occurrence) ----
        first("category_id").alias("category_id"),
        first("category_code").alias("category_code"),
        first("category_l1").alias("category_l1"),
        first("category_l2").alias("category_l2"),
        first("category_l3").alias("category_l3"),
        first("brand").alias("brand"),
        
        # ---- Price Stats ----
        spark_round(avg("price"), 2).alias("avg_price"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        first("price").alias("current_price"),  # Assuming latest is first
        
        # ---- Event Counts ----
        count("*").alias("total_events"),
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
        spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
        spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("total_cart_removes"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        
        # ---- Unique User Engagement ----
        countDistinct("user_id").alias("unique_users_total"),
        countDistinct(
            when(col("event_type") == "view", col("user_id"))
        ).alias("unique_users_viewed"),
        countDistinct(
            when(col("event_type") == "cart", col("user_id"))
        ).alias("unique_users_carted"),
        countDistinct(
            when(col("event_type") == "purchase", col("user_id"))
        ).alias("unique_users_purchased"),
        
        # ---- Session Engagement ----
        countDistinct("user_session").alias("unique_sessions"),
        
        # ---- Revenue ----
        spark_sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0)
        ).alias("total_revenue"),
        
        # ---- Temporal Metrics ----
        spark_min("event_date").alias("first_seen_date"),
        spark_max("event_date").alias("last_seen_date"),
        countDistinct("event_date").alias("days_with_activity")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Derived Metrics

# COMMAND ----------

df_enriched = (
    df_product_metrics
    
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
    
    # ---- Cart Abandonment Rate ----
    .withColumn(
        "cart_abandonment_rate",
        spark_round(
            when(col("total_cart_adds") > 0,
                 (col("total_cart_adds") - col("total_purchases")) / col("total_cart_adds") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Avg Revenue per View (RPV) ----
    .withColumn(
        "revenue_per_view",
        spark_round(
            when(col("total_views") > 0,
                 col("total_revenue") / col("total_views")
            ).otherwise(0), 4
        )
    )
    
    # ---- Avg Views per Session ----
    .withColumn(
        "views_per_session",
        spark_round(
            when(col("unique_sessions") > 0,
                 col("total_views") / col("unique_sessions")
            ).otherwise(0), 2
        )
    )
    
    # ---- User Engagement Ratio ----
    .withColumn(
        "purchase_user_ratio",
        spark_round(
            when(col("unique_users_viewed") > 0,
                 col("unique_users_purchased") / col("unique_users_viewed") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Repeat Purchase Indicator ----
    .withColumn(
        "avg_purchases_per_buyer",
        spark_round(
            when(col("unique_users_purchased") > 0,
                 col("total_purchases") / col("unique_users_purchased")
            ).otherwise(0), 2
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Add Rankings

# COMMAND ----------

# Define window for ranking
window_all = Window.orderBy(col("total_revenue").desc())
window_category = Window.partitionBy("category_l1").orderBy(col("total_revenue").desc())
window_brand = Window.partitionBy("brand").orderBy(col("total_revenue").desc())

df_ranked = (
    df_enriched
    
    # Global rankings
    .withColumn("revenue_rank_global", dense_rank().over(window_all))
    .withColumn(
        "revenue_percentile", 
        spark_round(percent_rank().over(window_all) * 100, 2)
    )
    
    # Category rankings
    .withColumn("revenue_rank_in_category", dense_rank().over(window_category))
    
    # Brand rankings
    .withColumn("revenue_rank_in_brand", dense_rank().over(window_brand))
    
    # ---- Product Performance Tier ----
    .withColumn(
        "performance_tier",
        when(col("revenue_percentile") >= 90, lit("Top 10%"))
        .when(col("revenue_percentile") >= 75, lit("Top 25%"))
        .when(col("revenue_percentile") >= 50, lit("Top 50%"))
        .when(col("revenue_percentile") >= 25, lit("Bottom 50%"))
        .otherwise(lit("Bottom 25%"))
    )
    
    # ---- Popularity Score (composite) ----
    # Weighted score: views (0.2) + cart_adds (0.3) + purchases (0.5)
    .withColumn(
        "popularity_score",
        spark_round(
            (col("total_views") * 0.2) + 
            (col("total_cart_adds") * 0.3) + 
            (col("total_purchases") * 0.5),
            2
        )
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
    df_ranked
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
print(f"Total products in {GOLD_TABLE}: {gold_count:,}")

# COMMAND ----------

# Sample top products
print("Top 10 Products by Revenue:")
display(
    spark.table(GOLD_TABLE)
    .select(
        "product_id", "brand", "category_l1", "total_revenue",
        "total_purchases", "overall_conversion_rate", "performance_tier"
    )
    .orderBy("total_revenue", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Category Analysis

# COMMAND ----------

print("Revenue by Category:")
display(
    spark.table(GOLD_TABLE)
    .groupBy("category_l1")
    .agg(
        count("*").alias("product_count"),
        spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
        spark_round(avg("overall_conversion_rate"), 2).alias("avg_conversion_rate"),
        spark_sum("total_views").alias("total_views")
    )
    .orderBy("total_revenue", ascending=False)
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Brand Analysis

# COMMAND ----------

print("Top 10 Brands by Revenue:")
display(
    spark.table(GOLD_TABLE)
    .filter(col("brand") != "unknown")
    .groupBy("brand")
    .agg(
        count("*").alias("product_count"),
        spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
        spark_round(avg("overall_conversion_rate"), 2).alias("avg_conversion_rate")
    )
    .orderBy("total_revenue", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
summary = spark.table(GOLD_TABLE).agg(
    count("*").alias("total_products"),
    spark_sum("total_revenue").alias("total_revenue"),
    spark_round(avg("overall_conversion_rate"), 2).alias("avg_conversion_rate"),
    spark_sum("total_purchases").alias("total_purchases")
).collect()[0]

print("=" * 60)
print("PRODUCT PERFORMANCE COMPLETE")
print("=" * 60)
print(f"Target Table: {GOLD_TABLE}")
print(f"Total Products: {summary['total_products']:,}")
print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
print(f"Total Purchases: {summary['total_purchases']:,}")
print(f"Avg Conversion Rate: {summary['avg_conversion_rate']:.2f}%")
print("=" * 60)
