# Tests for Gold Layer Aggregations
# ==================================
# Unit tests for the gold aggregation pipelines.

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, 
    TimestampType, DateType, IntegerType
)
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime, date


# =====================================
# Fixtures
# =====================================

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("GoldAggregationTests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture
def silver_data(spark):
    """Create sample silver data for testing."""
    data = [
        # User 5001 - Champion customer (many purchases)
        (datetime(2019, 10, 1, 10, 0), "view", 1001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001", date(2019, 10, 1), 10, "electronics"),
        (datetime(2019, 10, 1, 10, 5), "cart", 1001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001", date(2019, 10, 1), 10, "electronics"),
        (datetime(2019, 10, 1, 10, 10), "purchase", 1001, "electronics.smartphone", "samsung", 299.99, 5001, "session-001", date(2019, 10, 1), 10, "electronics"),
        (datetime(2019, 10, 2, 11, 0), "view", 1002, "electronics.laptop", "apple", 1299.99, 5001, "session-002", date(2019, 10, 2), 11, "electronics"),
        (datetime(2019, 10, 2, 11, 5), "purchase", 1002, "electronics.laptop", "apple", 1299.99, 5001, "session-002", date(2019, 10, 2), 11, "electronics"),
        
        # User 5002 - Cart abandoner (views and carts, no purchase)
        (datetime(2019, 10, 1, 12, 0), "view", 1003, "appliances.kitchen", "philips", 79.99, 5002, "session-003", date(2019, 10, 1), 12, "appliances"),
        (datetime(2019, 10, 1, 12, 5), "cart", 1003, "appliances.kitchen", "philips", 79.99, 5002, "session-003", date(2019, 10, 1), 12, "appliances"),
        (datetime(2019, 10, 1, 12, 10), "remove_from_cart", 1003, "appliances.kitchen", "philips", 79.99, 5002, "session-003", date(2019, 10, 1), 12, "appliances"),
        
        # User 5003 - Visitor (views only)
        (datetime(2019, 10, 1, 14, 0), "view", 1001, "electronics.smartphone", "samsung", 299.99, 5003, "session-004", date(2019, 10, 1), 14, "electronics"),
        (datetime(2019, 10, 1, 14, 5), "view", 1002, "electronics.laptop", "apple", 1299.99, 5003, "session-004", date(2019, 10, 1), 14, "electronics"),
    ]
    
    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("event_hour", IntegerType(), True),
        StructField("category_l1", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


# =====================================
# Customer Metrics Tests
# =====================================

class TestCustomerMetrics:
    """Tests for customer metrics aggregation."""
    
    def test_customer_count(self, silver_data):
        """Verify correct number of unique customers."""
        customer_count = silver_data.select("user_id").distinct().count()
        assert customer_count == 3
    
    def test_event_counts_per_customer(self, spark, silver_data):
        """Verify event counts are calculated correctly."""
        from pyspark.sql.functions import count, when, sum as spark_sum
        
        df = silver_data.groupBy("user_id").agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases")
        ).orderBy("user_id")
        
        results = df.collect()
        
        # User 5001: 5 events, 2 views, 2 purchases
        user_5001 = [r for r in results if r.user_id == 5001][0]
        assert user_5001.total_events == 5
        assert user_5001.total_views == 2
        assert user_5001.total_purchases == 2
        
        # User 5002: 3 events, 1 view, 0 purchases
        user_5002 = [r for r in results if r.user_id == 5002][0]
        assert user_5002.total_events == 3
        assert user_5002.total_views == 1
        assert user_5002.total_purchases == 0
    
    def test_customer_lifetime_value(self, spark, silver_data):
        """Verify CLV is calculated correctly."""
        from pyspark.sql.functions import sum as spark_sum, when
        
        df = silver_data.groupBy("user_id").agg(
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue")
        ).orderBy("user_id")
        
        results = df.collect()
        
        # User 5001: 299.99 + 1299.99 = 1599.98
        user_5001 = [r for r in results if r.user_id == 5001][0]
        assert abs(user_5001.total_revenue - 1599.98) < 0.01
        
        # User 5002: 0 (no purchases)
        user_5002 = [r for r in results if r.user_id == 5002][0]
        assert user_5002.total_revenue == 0
    
    def test_customer_segmentation(self, spark, silver_data):
        """Verify customer segmentation logic."""
        from pyspark.sql.functions import sum as spark_sum, when
        
        df = silver_data.groupBy("user_id").agg(
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_cart_adds"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views")
        ).withColumn(
            "customer_segment",
            when(col("total_purchases") >= 10, lit("Champion"))
            .when(col("total_purchases") >= 5, lit("Loyal"))
            .when(col("total_purchases") >= 2, lit("Potential Loyalist"))
            .when(col("total_purchases") == 1, lit("New Customer"))
            .when(col("total_cart_adds") > 0, lit("Cart Abandoner"))
            .otherwise(lit("Visitor"))
        ).orderBy("user_id")
        
        results = df.collect()
        
        # User 5001: 2 purchases -> Potential Loyalist
        user_5001 = [r for r in results if r.user_id == 5001][0]
        assert user_5001.customer_segment == "Potential Loyalist"
        
        # User 5002: 0 purchases, 1 cart -> Cart Abandoner
        user_5002 = [r for r in results if r.user_id == 5002][0]
        assert user_5002.customer_segment == "Cart Abandoner"
        
        # User 5003: 0 purchases, 0 carts -> Visitor
        user_5003 = [r for r in results if r.user_id == 5003][0]
        assert user_5003.customer_segment == "Visitor"


# =====================================
# Product Performance Tests
# =====================================

class TestProductPerformance:
    """Tests for product performance aggregation."""
    
    def test_product_count(self, silver_data):
        """Verify correct number of unique products."""
        product_count = silver_data.select("product_id").distinct().count()
        assert product_count == 3
    
    def test_product_event_counts(self, spark, silver_data):
        """Verify event counts per product."""
        from pyspark.sql.functions import count, when, sum as spark_sum
        
        df = silver_data.groupBy("product_id").agg(
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases")
        ).orderBy("product_id")
        
        results = df.collect()
        
        # Product 1001: 2 views, 1 purchase
        product_1001 = [r for r in results if r.product_id == 1001][0]
        assert product_1001.total_views == 2
        assert product_1001.total_purchases == 1
    
    def test_conversion_rate_calculation(self, spark, silver_data):
        """Verify conversion rate is calculated correctly."""
        from pyspark.sql.functions import sum as spark_sum, when, round as spark_round
        
        df = silver_data.groupBy("product_id").agg(
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases")
        ).withColumn(
            "conversion_rate",
            spark_round(
                when(col("total_views") > 0,
                     col("total_purchases") / col("total_views") * 100
                ).otherwise(0), 2
            )
        ).orderBy("product_id")
        
        results = df.collect()
        
        # Product 1001: 1/2 = 50% conversion
        product_1001 = [r for r in results if r.product_id == 1001][0]
        assert product_1001.conversion_rate == 50.0
        
        # Product 1003: 0/1 = 0% conversion
        product_1003 = [r for r in results if r.product_id == 1003][0]
        assert product_1003.conversion_rate == 0.0
    
    def test_product_revenue(self, spark, silver_data):
        """Verify product revenue is calculated correctly."""
        from pyspark.sql.functions import sum as spark_sum, when
        
        df = silver_data.groupBy("product_id").agg(
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue")
        ).orderBy("product_id")
        
        results = df.collect()
        
        # Product 1001: 299.99
        product_1001 = [r for r in results if r.product_id == 1001][0]
        assert abs(product_1001.total_revenue - 299.99) < 0.01
        
        # Product 1002: 1299.99
        product_1002 = [r for r in results if r.product_id == 1002][0]
        assert abs(product_1002.total_revenue - 1299.99) < 0.01


# =====================================
# Daily Sales Tests
# =====================================

class TestDailySales:
    """Tests for daily sales aggregation."""
    
    def test_daily_record_count(self, spark, silver_data):
        """Verify correct number of daily records."""
        daily_count = silver_data.select("event_date").distinct().count()
        assert daily_count == 2  # Oct 1 and Oct 2
    
    def test_daily_revenue(self, spark, silver_data):
        """Verify daily revenue is calculated correctly."""
        from pyspark.sql.functions import sum as spark_sum, when
        
        df = silver_data.groupBy("event_date").agg(
            spark_sum(
                when(col("event_type") == "purchase", col("price")).otherwise(0)
            ).alias("total_revenue")
        ).orderBy("event_date")
        
        results = df.collect()
        
        # Oct 1: 299.99 (user 5001)
        oct_1 = [r for r in results if r.event_date == date(2019, 10, 1)][0]
        assert abs(oct_1.total_revenue - 299.99) < 0.01
        
        # Oct 2: 1299.99 (user 5001)
        oct_2 = [r for r in results if r.event_date == date(2019, 10, 2)][0]
        assert abs(oct_2.total_revenue - 1299.99) < 0.01
    
    def test_daily_user_count(self, spark, silver_data):
        """Verify daily unique user count."""
        from pyspark.sql.functions import countDistinct
        
        df = silver_data.groupBy("event_date").agg(
            countDistinct("user_id").alias("unique_users")
        ).orderBy("event_date")
        
        results = df.collect()
        
        # Oct 1: 3 users
        oct_1 = [r for r in results if r.event_date == date(2019, 10, 1)][0]
        assert oct_1.unique_users == 3
        
        # Oct 2: 1 user
        oct_2 = [r for r in results if r.event_date == date(2019, 10, 2)][0]
        assert oct_2.unique_users == 1


# =====================================
# Conversion Funnel Tests
# =====================================

class TestConversionFunnel:
    """Tests for conversion funnel aggregation."""
    
    def test_funnel_session_counts(self, spark, silver_data):
        """Verify session counts at each funnel stage."""
        from pyspark.sql.functions import countDistinct, when
        
        df = silver_data.groupBy("event_date", "category_l1").agg(
            countDistinct("user_session").alias("total_sessions"),
            countDistinct(
                when(col("event_type") == "view", col("user_session"))
            ).alias("sessions_with_view"),
            countDistinct(
                when(col("event_type") == "cart", col("user_session"))
            ).alias("sessions_with_cart"),
            countDistinct(
                when(col("event_type") == "purchase", col("user_session"))
            ).alias("sessions_with_purchase")
        ).orderBy("event_date", "category_l1")
        
        results = df.collect()
        
        # Electronics on Oct 1
        elec_oct1 = [r for r in results 
                     if r.event_date == date(2019, 10, 1) and r.category_l1 == "electronics"][0]
        assert elec_oct1.sessions_with_view == 2  # session-001 and session-004
        assert elec_oct1.sessions_with_cart == 1   # session-001
        assert elec_oct1.sessions_with_purchase == 1  # session-001
    
    def test_cart_abandonment_rate(self, spark, silver_data):
        """Verify cart abandonment rate calculation."""
        from pyspark.sql.functions import countDistinct, when, round as spark_round
        
        df = silver_data.groupBy("category_l1").agg(
            countDistinct(
                when(col("event_type") == "cart", col("user_session"))
            ).alias("sessions_with_cart"),
            countDistinct(
                when(col("event_type") == "purchase", col("user_session"))
            ).alias("sessions_with_purchase")
        ).withColumn(
            "cart_abandonment_rate",
            spark_round(
                when(col("sessions_with_cart") > 0,
                     (col("sessions_with_cart") - col("sessions_with_purchase")) / 
                     col("sessions_with_cart") * 100
                ).otherwise(0), 2
            )
        )
        
        results = df.collect()
        
        # Electronics: 1 cart, 1 purchase -> 0% abandonment
        elec = [r for r in results if r.category_l1 == "electronics"][0]
        assert elec.cart_abandonment_rate == 0.0
        
        # Appliances: 1 cart, 0 purchase -> 100% abandonment
        appl = [r for r in results if r.category_l1 == "appliances"][0]
        assert appl.cart_abandonment_rate == 100.0


# =====================================
# Edge Case Tests
# =====================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""
    
    def test_division_by_zero_handled(self, spark):
        """Verify division by zero is handled gracefully."""
        from pyspark.sql.functions import when
        
        # Create data with 0 views
        data = [(0, 1)]
        df = spark.createDataFrame(data, ["views", "purchases"])
        
        result = df.withColumn(
            "rate",
            when(col("views") > 0, col("purchases") / col("views") * 100).otherwise(0)
        ).collect()[0]
        
        assert result.rate == 0
    
    def test_null_handling_in_aggregations(self, spark):
        """Verify nulls are handled in aggregations."""
        from pyspark.sql.functions import sum as spark_sum, coalesce
        
        data = [(1, None), (2, 100.0), (3, None)]
        df = spark.createDataFrame(data, ["id", "price"])
        
        result = df.agg(
            spark_sum(coalesce(col("price"), lit(0))).alias("total")
        ).collect()[0]
        
        assert result.total == 100.0
    
    def test_empty_dataframe_handling(self, spark):
        """Verify empty dataframes are handled correctly."""
        from pyspark.sql.functions import count
        
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        empty_df = spark.createDataFrame([], schema)
        result = empty_df.agg(count("*").alias("count")).collect()[0]
        
        assert result["count"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
