# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Events Transformation
# MAGIC 
# MAGIC This notebook transforms bronze events data into a cleaned, deduplicated
# MAGIC silver table with proper data types and derived columns.
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Remove duplicates (by event_time, user_id, product_id, user_session, event_type)
# MAGIC - Handle null values (category_code, brand with defaults)
# MAGIC - Standardize data types and formats
# MAGIC - Add derived columns (event_date, event_hour, day_of_week)
# MAGIC - Apply data quality filters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("catalog", "ecommerce_analytics_dev", "Catalog Name")
dbutils.widgets.text("schema_bronze", "bronze_layer", "Bronze Schema")
dbutils.widgets.text("schema_silver", "silver_layer", "Silver Schema")

# Get parameters
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")

# Table names
BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.events_raw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.events_cleaned"

print(f"Configuration:")
print(f"  Source: {BRONZE_TABLE}")
print(f"  Target: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, coalesce, trim, lower, upper,
    to_date, hour, dayofweek, dayofmonth, month, year, weekofyear,
    current_timestamp, concat, sha2, row_number,
    regexp_replace, split, size
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Silver Schema (if not exists)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
print(f"Schema {CATALOG}.{SCHEMA_SILVER} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Bronze Data

# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)
print(f"Bronze records: {df_bronze.count():,}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Data Quality Checks

# COMMAND ----------

# Import quality rules
# %run ./data_quality_rules  # Uncomment when running in Databricks

# Inline validation for standalone execution
VALID_EVENT_TYPES = ["view", "cart", "remove_from_cart", "purchase"]
PRICE_MIN = 0.0
PRICE_MAX = 100000.0

# COMMAND ----------

# Check for nulls in required columns
print("Null counts in bronze data:")
required_cols = ["event_time", "event_type", "product_id", "user_id", "price"]
for c in required_cols:
    null_count = df_bronze.filter(col(c).isNull()).count()
    print(f"  {c}: {null_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Transformations

# COMMAND ----------

df_transformed = (
    df_bronze
    
    # ---- Data Quality Filters ----
    # Remove records with null required fields
    .filter(col("event_time").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("price").isNotNull())
    
    # Filter valid event types
    .filter(col("event_type").isin(VALID_EVENT_TYPES))
    
    # Filter valid price range
    .filter((col("price") >= PRICE_MIN) & (col("price") <= PRICE_MAX))
    
    # ---- Standardization ----
    # Standardize event_type to lowercase
    .withColumn("event_type", lower(trim(col("event_type"))))
    
    # Handle null category_code
    .withColumn(
        "category_code",
        when(
            col("category_code").isNull() | (trim(col("category_code")) == ""),
            lit("unknown")
        ).otherwise(trim(col("category_code")))
    )
    
    # Handle null brand
    .withColumn(
        "brand",
        when(
            col("brand").isNull() | (trim(col("brand")) == ""),
            lit("unknown")
        ).otherwise(lower(trim(col("brand"))))
    )
    
    # Trim user_session
    .withColumn("user_session", trim(col("user_session")))
    
    # ---- Derived Columns ----
    # Date components
    .withColumn("event_date", to_date(col("event_time")))
    .withColumn("event_hour", hour(col("event_time")))
    .withColumn("event_day_of_week", dayofweek(col("event_time")))
    .withColumn("event_day_of_month", dayofmonth(col("event_time")))
    .withColumn("event_month", month(col("event_time")))
    .withColumn("event_year", year(col("event_time")))
    .withColumn("event_week", weekofyear(col("event_time")))
    
    # Category hierarchy (split category_code)
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
    
    # Create unique event ID (hash of composite key)
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
    
    # ---- Audit Columns ----
    .withColumn("processed_timestamp", current_timestamp())
)

print(f"Transformed records: {df_transformed.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Remove Duplicates

# COMMAND ----------

# Define deduplication key
dedup_columns = ["event_time", "user_id", "product_id", "user_session", "event_type"]

# Count duplicates before
total_before = df_transformed.count()
distinct_before = df_transformed.dropDuplicates(dedup_columns).count()
duplicates = total_before - distinct_before
print(f"Duplicate records to remove: {duplicates:,}")

# Remove duplicates - keep first occurrence
window = Window.partitionBy(dedup_columns).orderBy(col("ingestion_timestamp"))

df_deduped = (
    df_transformed
    .withColumn("row_num", row_number().over(window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

print(f"Records after deduplication: {df_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Select Final Columns

# COMMAND ----------

# Define final column order
final_columns = [
    # Unique identifier
    "event_id",
    
    # Original columns
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    
    # Derived columns
    "event_date",
    "event_hour",
    "event_day_of_week",
    "event_day_of_month",
    "event_month",
    "event_year",
    "event_week",
    "category_l1",
    "category_l2",
    "category_l3",
    
    # Audit columns
    "source_file",
    "ingestion_timestamp",
    "processed_timestamp"
]

df_silver = df_deduped.select(final_columns)
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver Table

# COMMAND ----------

# Check if table exists
table_exists = spark.catalog.tableExists(SILVER_TABLE)
print(f"Table {SILVER_TABLE} exists: {table_exists}")

# COMMAND ----------

if not table_exists:
    # Create new table with partitioning
    print(f"Creating new table: {SILVER_TABLE}")
    
    (
        df_silver
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")  # Partition by date for query performance
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )
    
    print("Table created successfully!")
    
else:
    # Merge for incremental updates
    print(f"Merging into existing table: {SILVER_TABLE}")
    
    delta_table = DeltaTable.forName(spark, SILVER_TABLE)
    
    (
        delta_table.alias("target")
        .merge(
            df_silver.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print("Merge completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify Results

# COMMAND ----------

# Count records
silver_count = spark.table(SILVER_TABLE).count()
print(f"Total records in {SILVER_TABLE}: {silver_count:,}")

# COMMAND ----------

# Sample data
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate Statistics

# COMMAND ----------

# Event type distribution
print("Event Type Distribution:")
display(
    spark.table(SILVER_TABLE)
    .groupBy("event_type")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# Category distribution (L1)
print("Top 10 Categories:")
display(
    spark.table(SILVER_TABLE)
    .groupBy("category_l1")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
)

# COMMAND ----------

# Brand distribution
print("Top 10 Brands:")
display(
    spark.table(SILVER_TABLE)
    .filter(col("brand") != "unknown")
    .groupBy("brand")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Calculate transformation metrics
bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
records_filtered = bronze_count - silver_count

print("=" * 60)
print("SILVER TRANSFORMATION COMPLETE")
print("=" * 60)
print(f"Source Table: {BRONZE_TABLE}")
print(f"Target Table: {SILVER_TABLE}")
print(f"Bronze Records: {bronze_count:,}")
print(f"Silver Records: {silver_count:,}")
print(f"Records Filtered/Deduped: {records_filtered:,}")
print(f"Retention Rate: {(silver_count/bronze_count*100):.2f}%")
print("=" * 60)
