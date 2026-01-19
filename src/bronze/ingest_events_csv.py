# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - CSV Events Ingestion
# MAGIC 
# MAGIC This notebook ingests raw CSV files from the Unity Catalog volume and creates
# MAGIC the bronze `events_raw` Delta table with audit columns.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Schema inference and validation
# MAGIC - Audit columns: `ingestion_timestamp`, `source_file`, `created_at`
# MAGIC - Idempotent execution (MERGE for upserts)
# MAGIC - Supports incremental loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters (can be overridden by jobs)
dbutils.widgets.text("catalog", "ecommerce_analytics_dev", "Catalog Name")
dbutils.widgets.text("schema_bronze", "bronze_layer", "Bronze Schema")
dbutils.widgets.text("volume_path", "/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data", "Raw Data Volume")

# Get parameters
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
VOLUME_PATH = dbutils.widgets.get("volume_path")

# Table names
BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.events_raw"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA_BRONZE}")
print(f"  Volume: {VOLUME_PATH}")
print(f"  Target Table: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    to_timestamp, regexp_replace, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

# Schema for CSV files - using string for timestamp to handle parsing
CSV_SCHEMA = StructType([
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: List CSV Files in Volume

# COMMAND ----------

# List all CSV files in the volume
csv_files = [f.path for f in dbutils.fs.ls(VOLUME_PATH) if f.name.endswith(".csv")]
print(f"Found {len(csv_files)} CSV files:")
for f in csv_files:
    print(f"  - {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read CSV Files with Schema

# COMMAND ----------

# Read all CSV files with defined schema
df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")  # Use explicit schema
    .schema(CSV_SCHEMA)
    .csv(VOLUME_PATH)
)

print(f"Raw records read: {df_raw.count():,}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add Audit Columns

# COMMAND ----------

# Transform and add audit columns
df_bronze = (
    df_raw
    # Parse event_time string to timestamp
    # Format: "2019-10-01 00:00:00 UTC"
    .withColumn(
        "event_time",
        to_timestamp(
            regexp_replace(col("event_time"), " UTC$", ""),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    # Trim string columns
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("category_code", trim(col("category_code")))
    .withColumn("brand", trim(col("brand")))
    .withColumn("user_session", trim(col("user_session")))
    # Add audit columns
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("created_at", current_timestamp())
)

print("Schema after transformation:")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create or Merge into Bronze Table

# COMMAND ----------

# Check if table exists
table_exists = spark.catalog.tableExists(BRONZE_TABLE)
print(f"Table {BRONZE_TABLE} exists: {table_exists}")

# COMMAND ----------

if not table_exists:
    # Create new table with partitioning
    print(f"Creating new table: {BRONZE_TABLE}")
    
    (
        df_bronze
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_type")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
    )
    
    print(f"Table created successfully!")
    
else:
    # Merge for idempotent upserts
    print(f"Merging into existing table: {BRONZE_TABLE}")
    
    delta_table = DeltaTable.forName(spark, BRONZE_TABLE)
    
    # Merge using composite key
    (
        delta_table.alias("target")
        .merge(
            df_bronze.alias("source"),
            """
            target.event_time = source.event_time 
            AND target.user_id = source.user_id 
            AND target.product_id = source.product_id 
            AND target.user_session = source.user_session
            AND target.event_type = source.event_type
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    
    print("Merge completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Results

# COMMAND ----------

# Count records in bronze table
bronze_count = spark.table(BRONZE_TABLE).count()
print(f"Total records in {BRONZE_TABLE}: {bronze_count:,}")

# COMMAND ----------

# Show sample data
display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Table Statistics

# COMMAND ----------

# Event type distribution
print("Event Type Distribution:")
display(
    spark.table(BRONZE_TABLE)
    .groupBy("event_type")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# Date range
print("Date Range:")
display(
    spark.sql(f"""
        SELECT 
            MIN(event_time) as min_date,
            MAX(event_time) as max_date,
            DATEDIFF(MAX(event_time), MIN(event_time)) as days_span
        FROM {BRONZE_TABLE}
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Optimize Table (Optional)

# COMMAND ----------

# Optimize table for better query performance
# Uncomment to run optimization
# spark.sql(f"OPTIMIZE {BRONZE_TABLE}")
# print("Table optimized!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Target Table: {BRONZE_TABLE}")
print(f"Total Records: {bronze_count:,}")
print(f"Source Files: {len(csv_files)}")
print("=" * 60)
