# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Conversion Funnel Analysis
# MAGIC 
# MAGIC This notebook creates conversion funnel metrics for customer journey analysis.
# MAGIC 
# MAGIC **Metrics:**
# MAGIC - View → Cart → Purchase rates
# MAGIC - Drop-off analysis by category/brand
# MAGIC - Session-based conversion tracking
# MAGIC - Funnel visualization data

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
GOLD_TABLE = f"{CATALOG}.{SCHEMA_GOLD}.conversion_funnel"

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
    first, current_timestamp, expr,
    round as spark_round, array, struct
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
# MAGIC ## Step 2: Overall Funnel Metrics

# COMMAND ----------

# Overall funnel by category and date
df_funnel = (
    df_silver
    .groupBy("event_date", "category_l1")
    .agg(
        # ---- Session Counts ----
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
        
        # ---- User Counts ----
        countDistinct("user_id").alias("total_users"),
        
        countDistinct(
            when(col("event_type") == "view", col("user_id"))
        ).alias("users_who_viewed"),
        
        countDistinct(
            when(col("event_type") == "cart", col("user_id"))
        ).alias("users_who_carted"),
        
        countDistinct(
            when(col("event_type") == "purchase", col("user_id"))
        ).alias("users_who_purchased"),
        
        # ---- Event Counts ----
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
        spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
        spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("total_cart_removes"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        
        # ---- Revenue ----
        spark_sum(
            when(col("event_type") == "purchase", col("price")).otherwise(0)
        ).alias("total_revenue"),
        
        # ---- Product Counts ----
        countDistinct("product_id").alias("unique_products_viewed"),
        countDistinct(
            when(col("event_type") == "cart", col("product_id"))
        ).alias("unique_products_carted"),
        countDistinct(
            when(col("event_type") == "purchase", col("product_id"))
        ).alias("unique_products_purchased")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Conversion Rates

# COMMAND ----------

df_enriched = (
    df_funnel
    
    # ---- Session-based Conversion Rates ----
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
        "session_overall_conversion_rate",
        spark_round(
            when(col("sessions_with_view") > 0,
                 col("sessions_with_purchase") / col("sessions_with_view") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- User-based Conversion Rates ----
    .withColumn(
        "user_view_to_cart_rate",
        spark_round(
            when(col("users_who_viewed") > 0,
                 col("users_who_carted") / col("users_who_viewed") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "user_cart_to_purchase_rate",
        spark_round(
            when(col("users_who_carted") > 0,
                 col("users_who_purchased") / col("users_who_carted") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "user_overall_conversion_rate",
        spark_round(
            when(col("users_who_viewed") > 0,
                 col("users_who_purchased") / col("users_who_viewed") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Event-based Rates ----
    .withColumn(
        "event_view_to_cart_rate",
        spark_round(
            when(col("total_views") > 0,
                 col("total_cart_adds") / col("total_views") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "event_cart_to_purchase_rate",
        spark_round(
            when(col("total_cart_adds") > 0,
                 col("total_purchases") / col("total_cart_adds") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Drop-off Counts ----
    .withColumn(
        "view_dropoff",
        col("sessions_with_view") - col("sessions_with_cart")
    )
    .withColumn(
        "cart_dropoff",
        col("sessions_with_cart") - col("sessions_with_purchase")
    )
    
    # ---- Drop-off Rates ----
    .withColumn(
        "view_dropoff_rate",
        spark_round(
            when(col("sessions_with_view") > 0,
                 col("view_dropoff") / col("sessions_with_view") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "cart_abandonment_rate",
        spark_round(
            when(col("sessions_with_cart") > 0,
                 col("cart_dropoff") / col("sessions_with_cart") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Net Cart Change ----
    .withColumn(
        "net_cart_adds",
        col("total_cart_adds") - col("total_cart_removes")
    )
    
    # ---- Revenue Metrics ----
    .withColumn(
        "revenue_per_purchasing_session",
        spark_round(
            when(col("sessions_with_purchase") > 0,
                 col("total_revenue") / col("sessions_with_purchase")
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "revenue_per_total_session",
        spark_round(
            when(col("total_sessions") > 0,
                 col("total_revenue") / col("total_sessions")
            ).otherwise(0), 2
        )
    )
    
    # ---- Product Conversion ----
    .withColumn(
        "product_view_to_cart_rate",
        spark_round(
            when(col("unique_products_viewed") > 0,
                 col("unique_products_carted") / col("unique_products_viewed") * 100
            ).otherwise(0), 2
        )
    )
    .withColumn(
        "product_cart_to_purchase_rate",
        spark_round(
            when(col("unique_products_carted") > 0,
                 col("unique_products_purchased") / col("unique_products_carted") * 100
            ).otherwise(0), 2
        )
    )
    
    # ---- Funnel Stage Percentages (for visualization) ----
    .withColumn(
        "view_stage_pct",
        lit(100.0)
    )
    .withColumn(
        "cart_stage_pct",
        col("session_view_to_cart_rate")
    )
    .withColumn(
        "purchase_stage_pct",
        col("session_overall_conversion_rate")
    )
    
    # ---- Audit Column ----
    .withColumn("processed_timestamp", current_timestamp())
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
print(f"Total records in {GOLD_TABLE}: {gold_count:,}")

# COMMAND ----------

# Sample data
display(
    spark.table(GOLD_TABLE)
    .select(
        "event_date", "category_l1", 
        "sessions_with_view", "sessions_with_cart", "sessions_with_purchase",
        "session_view_to_cart_rate", "session_cart_to_purchase_rate",
        "cart_abandonment_rate"
    )
    .orderBy("event_date", ascending=False, "total_sessions", ascending=False)
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Overall Funnel Summary

# COMMAND ----------

print("Overall Funnel Summary:")
display(
    spark.table(GOLD_TABLE)
    .groupBy()
    .agg(
        spark_sum("sessions_with_view").alias("total_view_sessions"),
        spark_sum("sessions_with_cart").alias("total_cart_sessions"),
        spark_sum("sessions_with_purchase").alias("total_purchase_sessions"),
        spark_round(
            spark_sum("sessions_with_cart") / spark_sum("sessions_with_view") * 100, 2
        ).alias("overall_view_to_cart_rate"),
        spark_round(
            spark_sum("sessions_with_purchase") / spark_sum("sessions_with_cart") * 100, 2
        ).alias("overall_cart_to_purchase_rate"),
        spark_round(
            spark_sum("sessions_with_purchase") / spark_sum("sessions_with_view") * 100, 2
        ).alias("overall_conversion_rate"),
        spark_sum("total_revenue").alias("total_revenue")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Funnel by Category

# COMMAND ----------

print("Conversion Funnel by Category:")
display(
    spark.table(GOLD_TABLE)
    .groupBy("category_l1")
    .agg(
        spark_sum("sessions_with_view").alias("view_sessions"),
        spark_sum("sessions_with_cart").alias("cart_sessions"),
        spark_sum("sessions_with_purchase").alias("purchase_sessions"),
        spark_round(avg("session_view_to_cart_rate"), 2).alias("avg_view_to_cart"),
        spark_round(avg("session_cart_to_purchase_rate"), 2).alias("avg_cart_to_purchase"),
        spark_round(avg("cart_abandonment_rate"), 2).alias("avg_cart_abandonment"),
        spark_sum("total_revenue").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Cart Abandonment Analysis

# COMMAND ----------

print("Categories with Highest Cart Abandonment:")
display(
    spark.table(GOLD_TABLE)
    .groupBy("category_l1")
    .agg(
        spark_sum("sessions_with_cart").alias("total_cart_sessions"),
        spark_sum("cart_dropoff").alias("total_cart_abandonments"),
        spark_round(avg("cart_abandonment_rate"), 2).alias("avg_abandonment_rate")
    )
    .filter(col("total_cart_sessions") > 100)  # Filter for significance
    .orderBy("avg_abandonment_rate", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Summary statistics
summary = spark.table(GOLD_TABLE).agg(
    spark_sum("sessions_with_view").alias("total_view_sessions"),
    spark_sum("sessions_with_purchase").alias("total_purchase_sessions"),
    spark_round(
        spark_sum("sessions_with_purchase") / spark_sum("sessions_with_view") * 100, 2
    ).alias("overall_conversion"),
    spark_round(avg("cart_abandonment_rate"), 2).alias("avg_cart_abandonment"),
    spark_sum("total_revenue").alias("total_revenue")
).collect()[0]

print("=" * 60)
print("CONVERSION FUNNEL COMPLETE")
print("=" * 60)
print(f"Target Table: {GOLD_TABLE}")
print(f"Total View Sessions: {summary['total_view_sessions']:,}")
print(f"Total Purchase Sessions: {summary['total_purchase_sessions']:,}")
print(f"Overall Conversion Rate: {summary['overall_conversion']:.2f}%")
print(f"Avg Cart Abandonment Rate: {summary['avg_cart_abandonment']:.2f}%")
print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
print("=" * 60)
