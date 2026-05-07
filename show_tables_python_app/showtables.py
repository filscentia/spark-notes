from pathlib import Path

from pyspark.sql import SparkSession

WAREHOUSE_PATH = '/opt/spark-warehouse'
METASTORE_PATH = '/opt/spark-metastore'


def display_table_info(spark, table_name):
    """Display comprehensive information about a table"""
    print("\n" + "=" * 60)
    print(f"TABLE: {table_name}")

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
        print("\nSample Data (first 10 rows):")
        df.show(10, truncate=False)

    except Exception as e:
        print(f"\n✗ Error reading table '{table_name}': {e}")
    finally:
        print("=" * 60)


def main():
    # Initialize Spark Session
    print("\n" + "=" * 60)
    print("DISPLAYING SPARK TABLE INFORMATION")
    print("=" * 60)

    spark = (
        SparkSession.builder.appName("TableInfoViewer")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/opt/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Get all configs as a dictionary
    configurations = dict(spark.sparkContext.getConf().getAll())

    for key in sorted(configurations):
        print(f"{key}: {configurations[key]}")

    defaultCatalog = configurations['spark.sql.defaultCatalog']
    defaultDatabase = next((configurations[key] for key in configurations if key.endswith('defaultDatabase')), 'default')

    print("Default Catalog set to " + defaultCatalog)
    print("Default Database (aka schema aka namespace) set to " + defaultDatabase)

    print("Config set current databases "+spark.catalog.currentDatabase())
    print("Config set current catalog "+spark.catalog.currentCatalog())

    spark.catalog.setCurrentDatabase(defaultDatabase)

    try:
        try:
            print("[SQL] calling show catalogs  {show catalogs}")
            spark.sql("show catalogs").show()
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            print("[SQL] calling show databases {show databases}")
            spark.sql("show databases").show()
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            print("[SQL] calling show tables (no db or namespace) {show table}")
            spark.sql("show tables").show()
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            sql = "SHOW TABLES in " + defaultCatalog + "." + defaultDatabase
            print("[SQL] calling show tables (explicity with supplied defaults)  {"+sql+"}")
            spark.sql(sql).show()
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            # List available tables
            print("\n[SQL] Available table info (explicity with supplied defaults) :")
            tables = spark.sql("SHOW TABLES in " + defaultCatalog + "." + defaultDatabase).collect()
            if tables:
                for table in tables:
                    print(f"  - {table.tableName}")
                    display_table_info(spark, defaultCatalog + "." + defaultDatabase + "." + table.tableName)
            else:
                print("  No tables found")
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            # List available tables
            print("\n[SQL] Available table info (with no db or namespace) :")
            tables = spark.sql("SHOW TABLES").collect()
            if tables:
                for table in tables:
                    print(f"  - {table.tableName}")
                    display_table_info(spark, defaultCatalog + "." + defaultDatabase + "." + table.tableName)
            else:
                print("  No tables found")
        except Exception as e:
            print(f"\n✗ Error: {e}")



        try:
            # List available tables
            print("\n[SQL] Available table info (searched with supplied defailts,but queried without ) :")
            tables = spark.sql("SHOW TABLES").collect()
            if tables:
                for table in tables:
                    print(f"  - {table.tableName}")
                    display_table_info(spark, table.tableName)
            else:
                print("  No tables found")
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            print("\n[API] calling listCatalogs (no values specified)")
            print(spark.catalog.listCatalogs())
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            print("\n[API] calling listDatabases")
            print(spark.catalog.listDatabases())
        except Exception as e:
            print(f"\n✗ Error: {e}")

        try:
            print("\n[API] calling listTables")
            print(spark.catalog.listTables());
        except Exception as e:
            print(f"\n✗ Error: {e}")

    except Exception as e:
        print(f"\n✗ Error: {e}")
    finally:
        # Clean up
        spark.stop()
        print("\nSpark session stopped.\n")


if __name__ == "__main__":
    main()