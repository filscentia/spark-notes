from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckParquet").config("spark.sql.catalogImplementation", "hive").config("hive.metastore.uris", "thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n=== Checking tests parquet file ===")
df = spark.read.parquet("/opt/spark-data/tests_subset_v3_2023.parquet")
print(f"Tests parquet row count: {df.count()}")
df.show(5)

print("\n=== Checking vehicles parquet file ===")
df2 = spark.read.parquet("/opt/spark-data/vehicles_subset_v3_2023.parquet")
print(f"Vehicles parquet row count: {df2.count()}")
df2.show(5)

spark.stop()
