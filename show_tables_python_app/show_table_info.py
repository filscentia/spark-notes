from pyspark.sql import SparkSession


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
        SparkSession.builder
        .appName("TableInfoViewer")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
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
            
            # Display information for all tables
            for table in tables:
                display_table_info(spark, table.tableName)
        else:
            print("  No tables found")
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        # Clean up
        spark.stop()
        print("\nSpark session stopped.\n")


if __name__ == "__main__":
    main()
