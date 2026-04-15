from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("LoadDataApp") \
    .getOrCreate()

# Silences the Spark framework logs
spark.sparkContext.setLogLevel("ERROR")
# Load the CSV file
# 'header=True' treats the first row as column names
# 'inferSchema=True' automatically detects numbers vs strings
file_path = "/opt/spark-data/data.csv"

try:
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    print("\n" + "="*40)
    print("Full Dataset:")
    df.show()

    print("Filtering for Engineering Department:")
    df.filter(df.department == "Engineering").show()
    
    print(f"Total records loaded: {df.count()}")
    print("="*40 + "\n")

except Exception as e:
    print(f"Error: Could not find or read the file at {file_path}")
    print(e)

spark.stop()