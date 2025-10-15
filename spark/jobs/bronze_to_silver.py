import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when
from pyspark.sql.types import DoubleType


def create_spark_session():
    """
    Initialize Spark session with configuration for local mode
    """
    return (
        SparkSession.builder.appName("FRED Bronze to Silver")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def process_fred_data(spark, bronze_path, silver_path):
    """
    Process FRED data from Bronze to Silver layer

    Transformations:
    - Parse and validate date fields
    - Convert value column to proper numeric type
    - Handle missing values (.)
    - Standardize column names
    - Add data quality flags
    """
    print(f"Reading data from: {bronze_path}")

    # Read all parquet files from bronze layer
    df = spark.read.parquet(f"{bronze_path}/*.parquet")

    print(f"Initial record count: {df.count()}")
    print("Schema:")
    df.printSchema()

    # Data transformations
    df_silver = (
        df.withColumn("observation_date", to_date(col("date")))
        .withColumn("value_raw", col("value"))
        .withColumn(
            "value_numeric",
            when(col("value") == ".", None).otherwise(col("value").cast(DoubleType())),
        )
        .withColumn("is_missing", when(col("value") == ".", True).otherwise(False))
        .withColumn("is_valid", when(col("value_numeric").isNotNull(), True).otherwise(False))
        .select(
            col("series_id"),
            col("series_name"),
            col("observation_date"),
            col("value_numeric").alias("value"),
            col("value_raw"),
            col("is_missing"),
            col("is_valid"),
            col("realtime_start"),
            col("realtime_end"),
            col("ingestion_timestamp"),
        )
    )

    # Data quality summary
    total_records = df_silver.count()
    valid_records = df_silver.filter(col("is_valid")).count()
    missing_records = df_silver.filter(col("is_missing")).count()

    print("\nData Quality Summary:")
    print(f"  Total records: {total_records}")
    print(f"  Valid records: {valid_records} ({100*valid_records/total_records:.2f}%)")
    print(f"  Missing records: {missing_records} ({100*missing_records/total_records:.2f}%)")

    # Show sample data
    print("\nSample processed data:")
    df_silver.show(5, truncate=False)

    # Write to Silver layer partitioned by series_id
    print(f"\nWriting to Silver layer: {silver_path}")
    df_silver.write.mode("overwrite").partitionBy("series_id").parquet(silver_path)

    print("Silver layer processing complete")


def main():
    """
    Main execution function
    """
    # Paths
    bronze_path = "/opt/airflow/data/bronze/fred"
    silver_path = "/opt/airflow/data/silver/fred"

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Process data
        process_fred_data(spark, bronze_path, silver_path)

        print("\nProcessing completed successfully")
        return 0

    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
