from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Parquet Sample Generator") \
    .getOrCreate()

# Create sample data
data = [
    ("Ken", 50, "Dallas"),
    ("Laura", 40, "Boston"),
    ("Mallory", 18, "San Jose"),
    ("Nina", 60, "Orlando"),
    ("Oscar", 22, "Las Vegas")
]

# Define DataFrame schema
columns = ["name", "age", "city"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Write to Parquet format
df.write.mode("overwrite").parquet("sample_data.parquet")
