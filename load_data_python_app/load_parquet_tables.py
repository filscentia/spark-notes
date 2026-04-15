from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

WAREHOUSE_PATH = '/opt/spark-warehouse'
METASTORE_PATH = '/opt/spark-metastore'


def display_table_info(spark, df, table_name):
    """Display comprehensive information about a table"""
    print("\n" + "=" * 60)
    print(f"TABLE: {table_name}")
    print("=" * 60)
    
    # Display schema
    print("\nSchema:")
    df.printSchema()
    
    # Display row count
    row_count = df.count()
    print(f"\nTotal Records: {row_count:,}")
    
    # Display sample data
    print(f"\nSample Data (first 10 rows):")
    df.show(10, truncate=False)
    

def main():
    # Initialize Spark Session
    print("\n" + "=" * 60)
    print("LOADING PARQUET FILES INTO SPARK TABLES")
    print("=" * 60)
    
    Path(WAREHOUSE_PATH).mkdir(parents=True, exist_ok=True)
    Path(METASTORE_PATH).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("ParquetTablesLoader")
        .config("spark.sql.warehouse.dir", WAREHOUSE_PATH)
        .config(
            'javax.jdo.option.ConnectionURL',
            f'jdbc:derby:;databaseName={METASTORE_PATH}/metastore_db;create=true',
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Silence Spark framework logs
    spark.sparkContext.setLogLevel("ERROR")
    
    # File paths (mounted in Docker container)
    tests_path = "/opt/spark-data/tests_subset_v3_2023.parquet"
    vehicles_path = "/opt/spark-data/vehicles_subset_v3_2023.parquet"
    
    try:
        # Drop existing tables if present
        print("\nChecking for existing tables...")
        spark.sql("DROP TABLE IF EXISTS tests")
        spark.sql("DROP TABLE IF EXISTS vehicles")
        print("✓ Existing tables removed (if any)")
        
        # Load tests parquet file
        print(f"\nLoading: tests_subset_v3_2023.parquet")
        tests_df = spark.read.parquet(tests_path)
        tests_df.write.mode("overwrite").saveAsTable("tests")
        print("✓ Successfully loaded and registered as permanent 'tests' table")
        
        # Load vehicles parquet file
        print(f"\nLoading: vehicles_subset_v3_2023.parquet")
        vehicles_df = spark.read.parquet(vehicles_path)
        vehicles_df.write.mode("overwrite").saveAsTable("vehicles")
        print("✓ Successfully loaded and registered as permanent 'vehicles' table")
        
        # Display information for both tables
        tests_count = display_table_info(spark, tests_df, "tests")
        vehicles_count = display_table_info(spark, vehicles_df, "vehicles")
        
    
    except FileNotFoundError as e:
        print(f"\n✗ Error: Could not find parquet file")
        print(f"  {e}")
    except Exception as e:
        print(f"\n✗ Error: Failed to load parquet files")
        print(f"  {e}")
    finally:
        # Clean up
        spark.stop()
        print("Spark session stopped.\n")

if __name__ == "__main__":
    main()
