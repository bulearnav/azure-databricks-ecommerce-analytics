# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Daily Sales Summary
# MAGIC 
# MAGIC This notebook creates daily aggregated sales metrics for time-series analysis.
# MAGIC 
# MAGIC **Metrics:**
# MAGIC - Daily revenue, orders, average order value
# MAGIC - Hourly patterns
# MAGIC - Day-of-week trends
# MAGIC - Rolling averages and growth rates

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
GOLD_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.daily_sales_summary"

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
    dayofweek, date_format, current_timestamp,
    round as spark_round, lag
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
# MAGIC ## Step 2: Daily Aggregations

# COMMAND ----------

df_daily = (
    df_silver
    .groupBy("event_date")
    .agg(
        # ---- Date Attributes ----
        first(dayofweek("event_date")).alias("day_of_week"),
        first(date_format("event_date", "EEEE")).alias("day_name"),
        first(col("event_month")).alias("month"),
        first(col("event_year")).alias("year"),
        first(col("event_week")).alias("week_of_year"),
        
        # ---- Event Counts ----
        count("*").alias("total_events"),
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
        spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
        spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("total_cart_removes"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        
        # ---- User Metrics ----
        countDistinct("user_id").alias("unique_users"),
        countDistinct("user_session").alias("unique_sessions"),
        countDistinct(
            when(col("event_type") == "purchase", col("user_id"))
        ).alias("unique_buyers"),
        
        # ---- Product Metrics ----
        countDistinct("product_id").alias("unique_products_interacted"),
        countDistinct(
            when(col("event_type") == "purchase", col("product_id"))
        ).alias("unique_products_sold"),
        
        # ---- Revenue ----
        spark_sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0)
        ).alias("total_revenue"),
        
        avg(
            when(col("event_type") == "purchase", col("price"))
        ).alias("avg_order_value"),
        
        spark_max(
            when(col("event_type") == "purchase", col("price"))
        ).alias("max_order_value"),
        
        # ---- Category/Brand Diversity ----
        countDistinct(
            when(col("event_type") == "purchase", col("category_l1"))
        ).alias("categories_with_sales"),
        countDistinct(
            when(col("event_type") == "purchase", col("brand"))
        ).alias("brands_with_sales")
    )
    .orderBy("event_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Derived Metrics

# COMMAND ----------

# Window for calculating lag/rolling metrics
window_date = Window.orderBy("event_date")
window_rolling_7 = Window.orderBy("event_date").rowsBetween(-6, 0)
window_rolling_30 = Window.orderBy("event_date").rowsBetween(-29, 0)

df_enriched = (
    df_daily
    
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
    
    # ---- Events per User/Session ----
    .withColumn(
        "events_per_user",
        spark_round(col("total_events") / col("unique_users"), 2)
    )
    .withColumn(
        "events_per_session",
        spark_round(col("total_events") / col("unique_sessions"), 2)
    )
    .withColumn(
        "revenue_per_user",
        spark_round(col("total_revenue") / col("unique_users"), 2)
    )
    
    # ---- Buyer Conversion ----
    .withColumn(
        "buyer_rate",
        spark_round(col("unique_buyers") / col("unique_users") * 100, 2)
    )
    
    # ---- Day-over-Day Changes ----
    .withColumn("prev_day_revenue", lag("total_revenue", 1).over(window_date))
    .withColumn(
        "revenue_dod_change",
        spark_round(
            when(col("prev_day_revenue") > 0,
                 (col("total_revenue") - col("prev_day_revenue")) / col("prev_day_revenue") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn("prev_day_users", lag("unique_users", 1).over(window_date))
    .withColumn(
        "users_dod_change",
        spark_round(
            when(col("prev_day_users") > 0,
                 (col("unique_users") - col("prev_day_users")) / col("prev_day_users") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- 7-Day Rolling Averages ----
    .withColumn(
        "revenue_7d_avg",
        spark_round(avg("total_revenue").over(window_rolling_7), 2)
    )
    .withColumn(
        "users_7d_avg",
        spark_round(avg("unique_users").over(window_rolling_7), 2)
    )
    .withColumn(
        "conversion_7d_avg",
        spark_round(avg("overall_conversion_rate").over(window_rolling_7), 2)
    )
    
    # ---- 30-Day Rolling Totals ----
    .withColumn(
        "revenue_30d_total",
        spark_round(spark_sum("total_revenue").over(window_rolling_30), 2)
    )
    
    # ---- Weekend Flag ----
    .withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    # ---- Audit Column ----
    .withColumn("processed_timestamp", current_timestamp())
    
    # Drop intermediate columns
    .drop("prev_day_revenue", "prev_day_users")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to Gold Table

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
# MAGIC ## Step 5: Verify Results

# COMMAND ----------

# Count records
gold_count = spark.table(GOLD_TABLE).count()
print(f"Total days in {GOLD_TABLE}: {gold_count:,}")

# COMMAND ----------

# Sample data
display(
    spark.table(GOLD_TABLE)
    .orderBy("event_date", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Daily Revenue Trend

# COMMAND ----------

print("Daily Revenue Summary (Last 30 Days):")
display(
    spark.table(GOLD_TABLE)
    .select(
        "event_date", "day_name", "total_revenue", "revenue_dod_change",
        "revenue_7d_avg", "unique_users", "total_purchases"
    )
    .orderBy("event_date", ascending=False)
    .limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Day-of-Week Analysis

# COMMAND ----------

print("Performance by Day of Week:")
display(
    spark.table(GOLD_TABLE)
    .groupBy("day_name", "day_of_week")
    .agg(
        count("*").alias("num_days"),
        spark_round(avg("total_revenue"), 2).alias("avg_revenue"),
        spark_round(avg("unique_users"), 2).alias("avg_users"),
        spark_round(avg("overall_conversion_rate"), 2).alias("avg_conversion_rate")
    )
    .orderBy("day_of_week")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
summary = spark.table(GOLD_TABLE).agg(
    count("*").alias("total_days"),
    spark_sum("total_revenue").alias("total_revenue"),
    spark_round(avg("total_revenue"), 2).alias("avg_daily_revenue"),
    spark_round(avg("unique_users"), 2).alias("avg_daily_users"),
    spark_round(avg("overall_conversion_rate"), 2).alias("avg_conversion_rate")
).collect()[0]

print("=" * 60)
print("DAILY SALES SUMMARY COMPLETE")
print("=" * 60)
print(f"Target Table: {GOLD_TABLE}")
print(f"Total Days: {summary['total_days']:,}")
print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
print(f"Avg Daily Revenue: ${summary['avg_daily_revenue']:,.2f}")
print(f"Avg Daily Users: {summary['avg_daily_users']:,.0f}")
print(f"Avg Conversion Rate: {summary['avg_conversion_rate']:.2f}%")
print("=" * 60)
