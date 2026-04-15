from pathlib import Path

from pyspark.sql import SparkSession

WAREHOUSE_PATH = '/opt/spark-warehouse'
METASTORE_PATH = '/opt/spark-metastore'


def display_table_info(spark, table_name):
    """Display comprehensive information about a table"""
    print("\n" + "=" * 60)
    print(f"TABLE: {table_name}")
    print("=" * 60)
    
    try:
        # Read the table
        df = spark.table(table_name)
        
        # Display schema
        print("\nSchema:")
        df.printSchema()
        
        # Display row count
        row_count = df.count()
        print(f"\nTotal Records: {row_count:,}")
        
        # Display sample data
        print(f"\nSample Data (first 10 rows):")
        df.show(10, truncate=False)
        
    except Exception as e:
        print(f"\n✗ Error reading table '{table_name}': {e}")


def main():
    # Initialize Spark Session
    print("\n" + "=" * 60)
    print("DISPLAYING SPARK TABLE INFORMATION")
    print("=" * 60)

    spark = (
        SparkSession.builder.appName("TableInfoViewer")
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
    
    try:
        # List available tables
        print("\nAvailable tables:")
        tables = spark.sql("SHOW TABLES").collect()
        if tables:
            for table in tables:
                print(f"  - {table.tableName}")
        else:
            print("  No tables found")
        
        # Display information for tests table
        display_table_info(spark, "tests")
        
        # Display information for vehicles table
        display_table_info(spark, "vehicles")
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        # Clean up
        spark.stop()
        print("\nSpark session stopped.\n")


if __name__ == "__main__":
    main()
