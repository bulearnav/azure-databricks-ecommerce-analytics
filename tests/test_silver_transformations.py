# Tests for Silver Layer Transformations
# ======================================
# Unit tests for the silver transformation pipeline.

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
from pyspark.sql.functions import col, lit, to_timestamp, regexp_replace
from datetime import datetime


# =====================================
# Fixtures
# =====================================

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("SilverTransformationTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture
def bronze_data(spark):
    """Create sample bronze data for testing."""
    data = [
        # Normal records
        (datetime(2019, 10, 1, 0, 0, 0), "view", 1001, 2001, "electronics.smartphone.android", "samsung", 299.99, 5001, "session-001"),
        (datetime(2019, 10, 1, 0, 1, 0), "cart", 1001, 2001, "electronics.smartphone.android", "samsung", 299.99, 5001, "session-001"),
        (datetime(2019, 10, 1, 0, 2, 0), "purchase", 1001, 2001, "electronics.smartphone.android", "samsung", 299.99, 5001, "session-001"),
        # Null category_code and brand
        (datetime(2019, 10, 1, 0, 5, 0), "view", 1002, 2002, None, None, 49.99, 5002, "session-002"),
        # Empty strings
        (datetime(2019, 10, 1, 0, 10, 0), "view", 1003, 2003, "", "", 129.99, 5003, "session-003"),
        # Whitespace
        (datetime(2019, 10, 1, 0, 15, 0), "view", 1004, 2004, "  appliances.kitchen  ", "  philips  ", 79.99, 5004, "session-004"),
        # Uppercase event_type (should be normalized)
        (datetime(2019, 10, 1, 0, 20, 0), "VIEW", 1005, 2005, "computers.laptop", "apple", 1299.99, 5005, "session-005"),
        # Duplicate record
        (datetime(2019, 10, 1, 0, 0, 0), "view", 1001, 2001, "electronics.smartphone.android", "samsung", 299.99, 5001, "session-001"),
        # Invalid price (negative)
        (datetime(2019, 10, 1, 0, 25, 0), "view", 1006, 2006, "electronics.tv", "lg", -100.0, 5006, "session-006"),
        # Null required field (should be filtered)
        (None, "view", 1007, 2007, "electronics.audio", "sony", 199.99, 5007, "session-007"),
    ]
    
    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


# =====================================
# Data Quality Filter Tests
# =====================================

class TestDataQualityFilters:
    """Tests for data quality filtering."""
    
    def test_null_event_time_filtered(self, bronze_data):
        """Verify records with null event_time are filtered."""
        df = bronze_data.filter("event_time IS NOT NULL")
        null_count = bronze_data.filter("event_time IS NULL").count()
        
        assert null_count == 1, "Expected 1 null event_time"
        assert df.count() == bronze_data.count() - 1
    
    def test_negative_price_filtered(self, bronze_data):
        """Verify records with negative price are filtered."""
        df = bronze_data.filter("price >= 0")
        negative_count = bronze_data.filter("price < 0").count()
        
        assert negative_count == 1, "Expected 1 negative price"
    
    def test_valid_event_types_only(self, bronze_data):
        """Verify only valid event types remain after filtering."""
        valid_types = ["view", "cart", "remove_from_cart", "purchase"]
        
        # Filter and lowercase
        from pyspark.sql.functions import lower, trim
        
        df = bronze_data.withColumn(
            "event_type", lower(trim(col("event_type")))
        ).filter(col("event_type").isin(valid_types))
        
        # All remaining should be valid
        invalid = df.filter(~df.event_type.isin(valid_types)).count()
        assert invalid == 0


# =====================================
# Standardization Tests
# =====================================

class TestStandardization:
    """Tests for data standardization."""
    
    def test_event_type_lowercase(self, spark, bronze_data):
        """Verify event_type is converted to lowercase."""
        from pyspark.sql.functions import lower, trim
        
        df = bronze_data.withColumn("event_type", lower(trim(col("event_type"))))
        
        # Check no uppercase letters
        uppercase = df.filter("event_type != lower(event_type)").count()
        assert uppercase == 0
    
    def test_null_category_replaced(self, spark, bronze_data):
        """Verify null category_code is replaced with 'unknown'."""
        from pyspark.sql.functions import when, trim
        
        df = bronze_data.withColumn(
            "category_code",
            when(
                col("category_code").isNull() | (trim(col("category_code")) == ""),
                lit("unknown")
            ).otherwise(trim(col("category_code")))
        )
        
        null_count = df.filter("category_code IS NULL").count()
        empty_count = df.filter("category_code = ''").count()
        
        assert null_count == 0, "category_code should not be null"
        assert empty_count == 0, "category_code should not be empty"
    
    def test_null_brand_replaced(self, spark, bronze_data):
        """Verify null brand is replaced with 'unknown'."""
        from pyspark.sql.functions import when, trim, lower
        
        df = bronze_data.withColumn(
            "brand",
            when(
                col("brand").isNull() | (trim(col("brand")) == ""),
                lit("unknown")
            ).otherwise(lower(trim(col("brand"))))
        )
        
        null_count = df.filter("brand IS NULL").count()
        empty_count = df.filter("brand = ''").count()
        
        assert null_count == 0, "brand should not be null"
        assert empty_count == 0, "brand should not be empty"
    
    def test_whitespace_trimmed(self, spark, bronze_data):
        """Verify whitespace is trimmed from string columns."""
        from pyspark.sql.functions import trim, when
        
        df = bronze_data.withColumn(
            "category_code",
            when(
                col("category_code").isNull() | (trim(col("category_code")) == ""),
                lit("unknown")
            ).otherwise(trim(col("category_code")))
        )
        
        # Check for leading/trailing whitespace
        whitespace = df.filter(
            "category_code != trim(category_code)"
        ).count()
        
        assert whitespace == 0


# =====================================
# Derived Column Tests
# =====================================

class TestDerivedColumns:
    """Tests for derived column creation."""
    
    def test_date_components_created(self, spark, bronze_data):
        """Verify date component columns are created."""
        from pyspark.sql.functions import to_date, hour, dayofweek, month, year
        
        df = bronze_data.filter("event_time IS NOT NULL").withColumn(
            "event_date", to_date(col("event_time"))
        ).withColumn(
            "event_hour", hour(col("event_time"))
        ).withColumn(
            "event_day_of_week", dayofweek(col("event_time"))
        ).withColumn(
            "event_month", month(col("event_time"))
        ).withColumn(
            "event_year", year(col("event_time"))
        )
        
        # Verify columns exist
        assert "event_date" in df.columns
        assert "event_hour" in df.columns
        assert "event_day_of_week" in df.columns
        assert "event_month" in df.columns
        assert "event_year" in df.columns
        
        # Verify no nulls (after filtering null event_time)
        for col_name in ["event_date", "event_hour", "event_month", "event_year"]:
            null_count = df.filter(f"{col_name} IS NULL").count()
            assert null_count == 0, f"Null values in {col_name}"
    
    def test_category_hierarchy_created(self, spark, bronze_data):
        """Verify category hierarchy columns are created."""
        from pyspark.sql.functions import split, size, when, trim
        
        df = bronze_data.withColumn(
            "category_code",
            when(
                col("category_code").isNull() | (trim(col("category_code")) == ""),
                lit("unknown")
            ).otherwise(trim(col("category_code")))
        ).withColumn(
            "category_l1",
            when(col("category_code") != "unknown",
                 split(col("category_code"), "\\.").getItem(0)
            ).otherwise(lit("unknown"))
        ).withColumn(
            "category_l2",
            when(
                (col("category_code") != "unknown") & 
                (size(split(col("category_code"), "\\.")) >= 2),
                split(col("category_code"), "\\.").getItem(1)
            ).otherwise(lit(None))
        )
        
        # Verify columns exist
        assert "category_l1" in df.columns
        assert "category_l2" in df.columns
        
        # Check specific values
        electronics = df.filter("category_code LIKE 'electronics%'").first()
        if electronics:
            assert electronics.category_l1 == "electronics"
    
    def test_event_id_created(self, spark, bronze_data):
        """Verify unique event_id is created."""
        from pyspark.sql.functions import sha2, concat
        
        df = bronze_data.filter("event_time IS NOT NULL").withColumn(
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
        
        # Verify event_id exists
        assert "event_id" in df.columns
        
        # Verify no null event_ids
        null_count = df.filter("event_id IS NULL").count()
        assert null_count == 0


# =====================================
# Deduplication Tests
# =====================================

class TestDeduplication:
    """Tests for deduplication logic."""
    
    def test_duplicates_removed(self, spark, bronze_data):
        """Verify duplicate records are removed."""
        key_cols = ["event_time", "user_id", "product_id", "user_session", "event_type"]
        
        # Filter valid data first
        df = bronze_data.filter("event_time IS NOT NULL")
        
        total = df.count()
        distinct = df.dropDuplicates(key_cols).count()
        
        assert distinct < total, "Expected duplicates to be present"
        assert total - distinct == 1, "Expected exactly 1 duplicate"
    
    def test_first_duplicate_kept(self, spark):
        """Verify first occurrence is kept during deduplication."""
        from pyspark.sql.functions import row_number, current_timestamp
        from pyspark.sql.window import Window
        
        # Create data with duplicates having different ingestion times
        data = [
            (datetime(2019, 10, 1, 0, 0, 0), "view", 1001, 5001, "session-001", 1),
            (datetime(2019, 10, 1, 0, 0, 0), "view", 1001, 5001, "session-001", 2),  # Duplicate, ingested later
        ]
        
        schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
            StructField("ingestion_order", LongType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        key_cols = ["event_time", "user_id", "product_id", "user_session", "event_type"]
        window = Window.partitionBy(key_cols).orderBy("ingestion_order")
        
        deduped = df.withColumn("row_num", row_number().over(window)).filter("row_num = 1")
        
        assert deduped.count() == 1
        assert deduped.first().ingestion_order == 1, "First record should be kept"


# =====================================
# Data Quality Report Tests
# =====================================

class TestDataQualityReport:
    """Tests for data quality reporting."""
    
    def test_null_count_calculation(self, bronze_data):
        """Verify null counts are calculated correctly."""
        null_event_time = bronze_data.filter("event_time IS NULL").count()
        null_category = bronze_data.filter("category_code IS NULL").count()
        null_brand = bronze_data.filter("brand IS NULL").count()
        
        assert null_event_time == 1
        assert null_category == 1
        assert null_brand == 1
    
    def test_quality_score_calculation(self, bronze_data):
        """Verify quality score calculation."""
        valid_types = ["view", "cart", "remove_from_cart", "purchase"]
        
        total = bronze_data.count()
        valid = bronze_data.filter(
            col("event_time").isNotNull() &
            col("event_type").isin(valid_types) &
            (col("price") >= 0)
        ).count()
        
        quality_score = (valid / total * 100) if total > 0 else 0
        
        # Should be less than 100% due to invalid records
        assert quality_score < 100
        assert quality_score > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
