from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan


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
    # Initialize Spark Session with Hive Metastore support
    print("\n" + "=" * 60)
    print("LOADING PARQUET FILES INTO SPARK TABLES")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("ParquetTablesLoader")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Silence Spark framework logs
    spark.sparkContext.setLogLevel("ERROR")
    
    # All tables to load (original + TPC-H)
    all_tables = {
        # Original tables
        'tests': '/opt/spark-data/tests_subset_v3_2023.parquet',
        'vehicles': '/opt/spark-data/vehicles_subset_v3_2023.parquet',
        # TPC-H tables
        'customer': '/opt/spark-data/customer.parquet',
        'lineitem': '/opt/spark-data/lineitem.parquet',
        'nation': '/opt/spark-data/nation.parquet',
        'orders': '/opt/spark-data/orders.parquet',
        'part': '/opt/spark-data/part.parquet',
        'partsupp': '/opt/spark-data/partsupp.parquet',
        'region': '/opt/spark-data/region.parquet',
        'supplier': '/opt/spark-data/supplier.parquet',
    }
    
    try:
        # Drop existing tables if present
        print("\nChecking for existing tables...")
        for table_name in all_tables.keys():
            spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
        print("✓ Existing tables removed (if any)")
        
        # Load each table
        for table_name, parquet_path in all_tables.items():
            print(f"\nLoading: {table_name}")
            df = spark.read.parquet(parquet_path)
            df.write.format("parquet").mode("overwrite").saveAsTable(table_name)
            print(f"✓ Successfully loaded and registered as permanent '{table_name}' table")
            
            # Display information for the table - read from the actual table, not the DataFrame
            table_df = spark.table(table_name)
            display_table_info(spark, table_df, table_name)
        
        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Successfully loaded {len(all_tables)} tables:")
        print("\nOriginal tables:")
        print("  - tests")
        print("  - vehicles")
        print("\nTPC-H tables:")
        for table_name in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier','ORDERS']:
            print(f"  - {table_name}")
        print("=" * 60)
    
    except FileNotFoundError as e:
        print(f"\n✗ Error: Could not find parquet file")
        print(f"  {e}")
    except Exception as e:
        print(f"\n✗ Error: Failed to load parquet files")
        print(f"  {e}")
    finally:
        # Clean up
        spark.stop()
        print("\nSpark session stopped.\n")

if __name__ == "__main__":
    main()
