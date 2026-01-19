# Tests for Bronze Layer Ingestion
# ================================
# Unit tests for the bronze ingestion pipeline.

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, TimestampType
)
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
        .appName("BronzeIngestionTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture
def sample_csv_schema():
    """Schema for raw CSV files."""
    return StructType([
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


@pytest.fixture
def sample_raw_data(spark):
    """Create sample raw data for testing."""
    data = [
        ("2019-10-01 00:00:00 UTC", "view", 1001, 2001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001"),
        ("2019-10-01 00:01:00 UTC", "cart", 1001, 2001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001"),
        ("2019-10-01 00:02:00 UTC", "purchase", 1001, 2001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001"),
        ("2019-10-01 00:05:00 UTC", "view", 1002, 2002, None, None, 49.99, 5002, "session-002"),
        ("2019-10-01 00:10:00 UTC", "view", 1003, 2003, "appliances.kitchen", "philips", 129.99, 5003, "session-003"),
    ]
    
    schema = StructType([
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
    
    return spark.createDataFrame(data, schema)


# =====================================
# Schema Tests
# =====================================

class TestBronzeSchema:
    """Tests for schema validation."""
    
    def test_csv_schema_has_required_columns(self, sample_csv_schema):
        """Verify CSV schema has all required columns."""
        expected_columns = [
            "event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session"
        ]
        
        actual_columns = [field.name for field in sample_csv_schema.fields]
        
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"
    
    def test_csv_schema_data_types(self, sample_csv_schema):
        """Verify correct data types in schema."""
        type_map = {
            "event_time": StringType,
            "event_type": StringType,
            "product_id": LongType,
            "category_id": LongType,
            "category_code": StringType,
            "brand": StringType,
            "price": DoubleType,
            "user_id": LongType,
            "user_session": StringType
        }
        
        for field in sample_csv_schema.fields:
            expected_type = type_map.get(field.name)
            if expected_type:
                assert isinstance(field.dataType, expected_type), \
                    f"Column {field.name} has wrong type: {field.dataType}"


# =====================================
# Data Ingestion Tests
# =====================================

class TestBronzeIngestion:
    """Tests for bronze ingestion logic."""
    
    def test_raw_data_row_count(self, sample_raw_data):
        """Verify raw data has expected row count."""
        assert sample_raw_data.count() == 5
    
    def test_event_types_present(self, sample_raw_data):
        """Verify all event types are present."""
        event_types = [row.event_type for row in sample_raw_data.select("event_type").distinct().collect()]
        
        assert "view" in event_types
        assert "cart" in event_types
        assert "purchase" in event_types
    
    def test_null_handling_in_category_code(self, sample_raw_data):
        """Verify null values exist in category_code."""
        null_count = sample_raw_data.filter("category_code IS NULL").count()
        
        assert null_count == 1, "Expected 1 null category_code"
    
    def test_null_handling_in_brand(self, sample_raw_data):
        """Verify null values exist in brand."""
        null_count = sample_raw_data.filter("brand IS NULL").count()
        
        assert null_count == 1, "Expected 1 null brand"


# =====================================
# Timestamp Parsing Tests
# =====================================

class TestTimestampParsing:
    """Tests for timestamp parsing logic."""
    
    def test_timestamp_parsing(self, spark, sample_raw_data):
        """Verify timestamp parsing from CSV format."""
        from pyspark.sql.functions import to_timestamp, regexp_replace, col
        
        df = sample_raw_data.withColumn(
            "parsed_time",
            to_timestamp(
                regexp_replace(col("event_time"), " UTC$", ""),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        
        # Check no null timestamps after parsing
        null_count = df.filter("parsed_time IS NULL").count()
        assert null_count == 0, "Timestamp parsing failed"
    
    def test_timestamp_type_after_parsing(self, spark, sample_raw_data):
        """Verify timestamp is correct type after parsing."""
        from pyspark.sql.functions import to_timestamp, regexp_replace, col
        
        df = sample_raw_data.withColumn(
            "parsed_time",
            to_timestamp(
                regexp_replace(col("event_time"), " UTC$", ""),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        
        # Check data type
        assert str(df.schema["parsed_time"].dataType) == "TimestampType()"


# =====================================
# Audit Column Tests
# =====================================

class TestAuditColumns:
    """Tests for audit column creation."""
    
    def test_audit_columns_added(self, spark, sample_raw_data):
        """Verify audit columns are added correctly."""
        from pyspark.sql.functions import current_timestamp, lit
        
        df = sample_raw_data.withColumn(
            "ingestion_timestamp", current_timestamp()
        ).withColumn(
            "source_file", lit("test_file.csv")
        ).withColumn(
            "created_at", current_timestamp()
        )
        
        assert "ingestion_timestamp" in df.columns
        assert "source_file" in df.columns
        assert "created_at" in df.columns
    
    def test_audit_columns_not_null(self, spark, sample_raw_data):
        """Verify audit columns are not null."""
        from pyspark.sql.functions import current_timestamp, lit
        
        df = sample_raw_data.withColumn(
            "ingestion_timestamp", current_timestamp()
        ).withColumn(
            "source_file", lit("test_file.csv")
        ).withColumn(
            "created_at", current_timestamp()
        )
        
        # Check no null values in audit columns
        for col_name in ["ingestion_timestamp", "source_file", "created_at"]:
            null_count = df.filter(f"{col_name} IS NULL").count()
            assert null_count == 0, f"Null values in {col_name}"


# =====================================
# Data Validation Tests
# =====================================

class TestDataValidation:
    """Tests for data validation rules."""
    
    def test_price_is_positive(self, sample_raw_data):
        """Verify all prices are positive."""
        negative_count = sample_raw_data.filter("price < 0").count()
        assert negative_count == 0, "Found negative prices"
    
    def test_product_id_is_positive(self, sample_raw_data):
        """Verify all product IDs are positive."""
        invalid_count = sample_raw_data.filter("product_id <= 0").count()
        assert invalid_count == 0, "Found invalid product IDs"
    
    def test_user_id_is_positive(self, sample_raw_data):
        """Verify all user IDs are positive."""
        invalid_count = sample_raw_data.filter("user_id <= 0").count()
        assert invalid_count == 0, "Found invalid user IDs"
    
    def test_valid_event_types(self, sample_raw_data):
        """Verify only valid event types exist."""
        valid_types = ["view", "cart", "remove_from_cart", "purchase"]
        
        invalid_count = sample_raw_data.filter(
            ~sample_raw_data.event_type.isin(valid_types)
        ).count()
        
        assert invalid_count == 0, "Found invalid event types"


# =====================================
# Deduplication Tests
# =====================================

class TestDeduplication:
    """Tests for deduplication logic."""
    
    def test_duplicate_detection(self, spark):
        """Test that duplicates are detected."""
        # Create data with duplicates
        data = [
            ("2019-10-01 00:00:00 UTC", "view", 1001, 5001, "session-001"),
            ("2019-10-01 00:00:00 UTC", "view", 1001, 5001, "session-001"),  # Duplicate
            ("2019-10-01 00:01:00 UTC", "cart", 1001, 5001, "session-001"),
        ]
        
        schema = StructType([
            StructField("event_time", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        key_cols = ["event_time", "user_id", "product_id", "user_session", "event_type"]
        
        total = df.count()
        distinct = df.dropDuplicates(key_cols).count()
        
        assert total == 3
        assert distinct == 2
        assert total - distinct == 1, "Expected 1 duplicate"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
