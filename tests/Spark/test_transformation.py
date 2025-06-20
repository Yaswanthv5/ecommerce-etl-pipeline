import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType
from pyspark.sql.functions import col, to_date, when
from datetime import date

# Assuming your Spark transformation logic is in a separate module/function for reusability
# For example, if 01_extract_and_clean.py had a function like `clean_orders_data(spark_session, input_df)`

@pytest.fixture(scope="module")
def spark():
    """Provides a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("PySpark Unit Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_clean_orders_data(spark):
    """Test the cleaning of orders data."""
    # Arrange: Sample raw data
    raw_data = [
        (1, 101, "2023-01-01", 100.00, "COMPLETED"),
        (2, 102, "2023-01-02", 50.50, "PENDING"),
        (3, None, "2023-01-03", 75.25, "CANCELLED"), # Missing customer_id
        (4, 104, "invalid_date", 200.00, "COMPLETED"), # Invalid date
        (5, 105, "2023-01-05", None, "COMPLETED") # Missing total_amount
    ]
    raw_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])
    df_raw = spark.createDataFrame(raw_data, raw_schema)

    # Act: Apply cleaning logic (simulated from 01_extract_and_clean.py)
    # In a real scenario, you'd import and call the actual cleaning function
    df_cleaned = df_raw.select(
        col("order_id").cast(IntegerType()).alias("order_id"),
        col("customer_id").cast(IntegerType()).alias("customer_id"),
        when(
            col("order_date").rlike(r"\d{4}-\d{2}-\d{2}"),
            to_date(col("order_date"), "yyyy-MM-dd")
        ).otherwise(None).alias("order_date"),
        col("total_amount").cast(DoubleType()).alias("total_amount"),
        col("status").cast(StringType()).alias("order_status")
    ).na.drop(subset=["order_id", "customer_id", "order_date", "total_amount"])

    # Assert: Expected cleaned data
    expected_data = [
        (1, 101, date(2023,1,1), 100.00, "COMPLETED"),
        (2, 102, date(2023,1,2), 50.50, "PENDING")
    ]
    expected_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", DateType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_status", StringType(), True)
    ])
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert df_cleaned.count() == df_expected.count()
    assert df_cleaned.subtract(df_expected).count() == 0
    assert df_expected.subtract(df_cleaned).count() == 0

    # Test for schema
    assert set(df_cleaned.columns) == set(df_expected.columns)
    # You can also test specific data types if needed

def test_silver_layer_sales_join(spark):
    """Test the join logic for silver layer sales."""
    # Arrange: Mock cleaned dataframes
    orders_data = [(1, 101, "2023-01-01", 100.00, "COMPLETED"), (2, 102, "2023-01-02", 50.00, "PENDING")]
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_status", StringType(), True)
    ])
    df_orders = spark.createDataFrame(orders_data, orders_schema)

    order_items_data = [(1, 1, 10, 2, 5.0), (2, 1, 11, 1, 90.0), (3, 2, 12, 1, 50.0)]
    order_items_schema = StructType([
        StructField("order_item_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True)
    ])
    df_order_items = spark.createDataFrame(order_items_data, order_items_schema)

    products_data = [(10, "ProdA", "Cat1", 5.0), (11, "ProdB", "Cat1", 90.0), (12, "ProdC", "Cat2", 50.0)]
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    df_products = spark.createDataFrame(products_data, products_schema)

    customers_data = [(101, "John", "Doe", "john@example.com", "2022-01-01", "USA"), (102, "Jane", "Smith", "jane@example.com", "2022-02-01", "CAN")]
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("country", StringType(), True)
    ])
    df_customers = spark.createDataFrame(customers_data, customers_schema)

    # Act: Simulate the join logic from 02_transform_to_silver.py
    from pyspark.sql.functions import sum, countDistinct, round # Re-import for clarity in test
    import pyspark.sql.functions as F

    df_sales_details = df_order_items.join(df_orders, "order_id", "inner") \
        .join(df_products, "product_id", "inner") \
        .withColumn("line_item_total", col("quantity") * col("unit_price"))

    df_orders_enriched = df_sales_details.groupBy("order_id", "customer_id", "order_date", "order_status", "total_amount") \
        .agg(
            sum("line_item_total").alias("calculated_order_total"),
            countDistinct("product_id").alias("distinct_products_in_order"),
            sum("quantity").alias("total_quantity_in_order")
        )

    df_orders_enriched = df_orders_enriched.withColumn(
        "total_amount_mismatch_flag",
        F.when(F.abs(col("total_amount") - col("calculated_order_total")) > 0.01, True).otherwise(False)
    )

    df_silver_sales = df_orders_enriched.join(df_customers, "customer_id", "inner") \
        .select(
            col("order_id"),
            col("customer_id"),
            col("first_name").alias("customer_first_name"),
            col("last_name").alias("customer_last_name"),
            col("email").alias("customer_email"),
            col("country").alias("customer_country"),
            to_date(col("registration_date"), "yyyy-MM-dd").alias("customer_registration_date"), # Cast to DateType
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"), # Cast to DateType
            col("order_status"),
            col("total_amount"),
            col("calculated_order_total"),
            col("distinct_products_in_order"),
            col("total_quantity_in_order"),
            col("total_amount_mismatch_flag")
        )

    # Assert: Expected output
    expected_data = [
        (1, 101, "John", "Doe", "john@example.com", "USA", date(2022,1,1), date(2023,1,1), "COMPLETED", 100.00, 100.00, 2, 3, False),
        (2, 102, "Jane", "Smith", "jane@example.com", "CAN", date(2022,2,1), date(2023,1,2), "PENDING", 50.00, 50.00, 1, 1, False)
    ]
    expected_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("customer_first_name", StringType(), True),
        StructField("customer_last_name", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("customer_registration_date", DateType(), True),
        StructField("order_date", DateType(), True),
        StructField("order_status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("calculated_order_total", DoubleType(), True),
        StructField("distinct_products_in_order", IntegerType(), True),
        StructField("total_quantity_in_order", IntegerType(), True),
        StructField("total_amount_mismatch_flag", BooleanType(), True)
    ])
    df_expected = spark.createDataFrame(expected_data, expected_schema)

    # Order by common columns to ensure comparison consistency
    df_silver_sales_sorted = df_silver_sales.sort("order_id", "customer_id", "order_date").drop("processed_timestamp") # Drop timestamp as it's dynamic
    df_expected_sorted = df_expected.sort("order_id", "customer_id", "order_date")

    assert df_silver_sales_sorted.count() == df_expected_sorted.count()
    assert df_silver_sales_sorted.subtract(df_expected_sorted).count() == 0
    assert df_expected_sorted.subtract(df_silver_sales_sorted).count() == 0