# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming Pipeline - Auto Loader for Events
# MAGIC 
# MAGIC This notebook implements structured streaming with Auto Loader for
# MAGIC incremental CSV ingestion into the Bronze layer.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Auto Loader (cloudFiles) for incremental processing
# MAGIC - Checkpointing for fault tolerance
# MAGIC - Schema evolution support
# MAGIC - Configurable trigger intervals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("catalog", "ecommerce_analytics_dev", "Catalog Name")
dbutils.widgets.text("schema_bronze", "bronze_layer", "Bronze Schema")
dbutils.widgets.text("volume_path", "/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data", "Raw Data Volume")
dbutils.widgets.text("checkpoint_path", "/Volumes/ecommerce_analytics_dev/bronze_layer/_checkpoints/streaming", "Checkpoint Path")
dbutils.widgets.dropdown("trigger_mode", "availableNow", ["availableNow", "processingTime"], "Trigger Mode")
dbutils.widgets.text("trigger_interval", "1 minute", "Trigger Interval (if processingTime)")

# Get parameters
CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
VOLUME_PATH = dbutils.widgets.get("volume_path")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")
TRIGGER_MODE = dbutils.widgets.get("trigger_mode")
TRIGGER_INTERVAL = dbutils.widgets.get("trigger_interval")

# Table names
BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.events_raw_streaming"

print(f"Configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Source: {VOLUME_PATH}")
print(f"  Target: {BRONZE_TABLE}")
print(f"  Checkpoint: {CHECKPOINT_PATH}")
print(f"  Trigger: {TRIGGER_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    to_timestamp, regexp_replace, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

# Schema for CSV files
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
# MAGIC ## Step 1: Create Bronze Schema (if not exists)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
print(f"Schema {CATALOG}.{SCHEMA_BRONZE} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configure Auto Loader Stream

# COMMAND ----------

# Create streaming DataFrame with Auto Loader
df_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
    .option("header", "true")
    .schema(CSV_SCHEMA)
    .load(VOLUME_PATH)
)

print("Auto Loader stream configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Transform Stream

# COMMAND ----------

df_transformed = (
    df_stream
    # Parse event_time string to timestamp
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

print("Stream transformations applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Define Write Stream

# COMMAND ----------

# Configure trigger based on mode
if TRIGGER_MODE == "availableNow":
    trigger_config = {"availableNow": True}
else:
    trigger_config = {"processingTime": TRIGGER_INTERVAL}

# Write stream to Delta table
query = (
    df_transformed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
    .option("mergeSchema", "true")  # Allow schema evolution
    .trigger(**trigger_config)
    .partitionBy("event_type")
    .toTable(BRONZE_TABLE)
)

print(f"Streaming query started: {query.name}")
print(f"Query ID: {query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Monitor Stream

# COMMAND ----------

# Wait for stream to complete (for availableNow mode)
if TRIGGER_MODE == "availableNow":
    query.awaitTermination()
    print("Stream processing complete (availableNow mode)")
else:
    print("Stream running in continuous mode")
    print("Use query.stop() to stop the stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Results

# COMMAND ----------

# Check if table exists and count records
if spark.catalog.tableExists(BRONZE_TABLE):
    count = spark.table(BRONZE_TABLE).count()
    print(f"Records in {BRONZE_TABLE}: {count:,}")
    
    # Show sample
    display(spark.table(BRONZE_TABLE).limit(5))
else:
    print(f"Table {BRONZE_TABLE} does not exist yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Stream Statistics

# COMMAND ----------

# Get stream progress (if running)
try:
    if query.isActive:
        progress = query.lastProgress
        if progress:
            print(f"Input rows: {progress.get('numInputRows', 0)}")
            print(f"Processing rate: {progress.get('processedRowsPerSecond', 0):.2f} rows/sec")
            print(f"Batch ID: {progress.get('batchId', 0)}")
except:
    print("Stream has completed or is not active")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def stop_stream():
    """Stop the active stream."""
    if query.isActive:
        query.stop()
        print("Stream stopped")
    else:
        print("Stream is not active")

def get_stream_status():
    """Get current stream status."""
    return {
        "is_active": query.isActive,
        "query_id": str(query.id),
        "name": query.name,
        "status": query.status
    }

def get_processed_files():
    """Get list of processed files from checkpoint."""
    try:
        files = dbutils.fs.ls(f"{CHECKPOINT_PATH}/bronze/sources/0")
        return [f.name for f in files]
    except:
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("STREAMING PIPELINE SUMMARY")
print("=" * 60)
print(f"Source: {VOLUME_PATH}")
print(f"Target Table: {BRONZE_TABLE}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print(f"Trigger Mode: {TRIGGER_MODE}")
print(f"Stream Active: {query.isActive if 'query' in dir() else 'N/A'}")
print("=" * 60)
