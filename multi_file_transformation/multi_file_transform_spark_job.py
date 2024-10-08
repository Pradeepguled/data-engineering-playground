import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, upper, lower, avg, countDistinct, when, rank, broadcast
from pyspark.sql.window import Window
import os

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Multiple File Formats with Advanced Spark Features") \
    .config("spark.sql.shuffle.partitions", "50")  # Reducing the number of shuffle partitions to optimize performance for smaller data
    .getOrCreate()

# Set directory paths
input_directory = "/path/to/files"
output_directory = "/path/to/output"

# Define regex patterns for each file type
csv_pattern = re.compile(r'.*\.csv$')
json_pattern = re.compile(r'.*\.json$')
parquet_pattern = re.compile(r'.*\.parquet$')

# List of files in the directory
file_list = [f for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]

# Initialize an empty list to store DataFrames
dataframes = []

# Iterate through the files and apply transformations
for file in file_list:
    file_path = os.path.join(input_directory, file)
    
    # Match CSV files
    if csv_pattern.match(file):
        df = spark.read.format("csv").option("header", "true").load(file_path)
        print(f"Reading CSV file: {file}")

        # Transformation - Adding Columns:
        # Why: To add metadata and new information that can be used later for partitioning or processing.
        df = df.withColumn("source_format", lit("csv")) \
               .withColumn("processed_at", lit("2024-10-05")) \
               .withColumnRenamed("name", "csv_name") \
               .withColumn("csv_name_upper", upper(col("csv_name")))  # Convert names to uppercase for uniformity

        # SQL Queries:
        # Why: SQL syntax is familiar and makes complex transformations easier. We can register a DataFrame as a SQL view and query it using SQL.
        df.createOrReplaceTempView("csv_table")
        
        # SQL Query: Perform transformations using SQL.
        df = spark.sql("""
            SELECT 
                csv_name_upper AS name_upper,
                age,
                CASE WHEN age >= 18 THEN 'Yes' ELSE 'No' END AS is_adult,  -- Determine if the person is an adult
                processed_at
            FROM csv_table
            WHERE age IS NOT NULL  -- Exclude rows with NULL age
        """)

        # SQL Aggregation:
        # Why: To calculate summary statistics, like the average age and distinct names, which is useful for analyzing the data.
        df.createOrReplaceTempView("csv_table_transformed")
        df = spark.sql("""
            SELECT 
                is_adult,
                AVG(age) AS average_age,  -- Calculate average age
                COUNT(DISTINCT name_upper) AS unique_names  -- Count distinct names
            FROM csv_table_transformed
            GROUP BY is_adult  -- Group by the "is_adult" column
        """)

        # Caching:
        # Why: Caching stores the DataFrame in memory for faster future computations. Useful when the DataFrame is reused multiple times.
        df.cache()

        # Action: Count and Show
        # Why: Actions trigger the actual execution of the transformations. Here, `count()` forces the computation and `show()` displays the result.
        print(f"Count of rows in CSV file: {df.count()}")
        df.show()

        # Partitioning:
        # Why: Partitioning the data allows Spark to process chunks of the data in parallel, improving performance during writing and future reads.
        output_csv_path = os.path.join(output_directory, "csv_output", file)
        df.write.mode("overwrite").partitionBy("is_adult").format("parquet").save(output_csv_path)  # Partition output by "is_adult" for faster querying

        dataframes.append(df)

    # Match JSON files
    elif json_pattern.match(file):
        df = spark.read.format("json").load(file_path)
        print(f"Reading JSON file: {file}")
        
        # Transformation - Adding Columns:
        df = df.withColumn("source_format", lit("json")) \
               .withColumn("processed_at", lit("2024-10-05")) \
               .withColumnRenamed("name", "json_name") \
               .withColumn("json_name_lower", lower(col("json_name")))  # Convert names to lowercase for uniformity

        # Register DataFrame as a temporary SQL view for querying
        df.createOrReplaceTempView("json_table")
        
        # SQL Query: Transform the data and categorize ages.
        df = spark.sql("""
            SELECT 
                json_name_lower AS name_lower,
                age,
                CASE 
                    WHEN age >= 60 THEN 'Senior'
                    WHEN age >= 18 THEN 'Adult'
                    ELSE 'Minor' 
                END AS age_category,  -- Categorize people by age group
                processed_at
            FROM json_table
            WHERE age > 0
        """)

        # Aggregation: Count unique names and calculate average age grouped by age category.
        df.createOrReplaceTempView("json_table_transformed")
        df = spark.sql("""
            SELECT 
                age_category,
                COUNT(DISTINCT name_lower) AS unique_json_names,  -- Count distinct names
                AVG(age) AS avg_age  -- Calculate average age
            FROM json_table_transformed
            GROUP BY age_category
        """)

        # Caching:
        df.cache()

        # Action: Count and Show
        print(f"Count of rows in JSON file: {df.count()}")
        df.show()

        # Partitioning the JSON data by "age_category"
        output_json_path = os.path.join(output_directory, "json_output", file)
        df.write.mode("overwrite").partitionBy("age_category").format("parquet").save(output_json_path)

        dataframes.append(df)

    # Match Parquet files
    elif parquet_pattern.match(file):
        df = spark.read.format("parquet").load(file_path)
        print(f"Reading Parquet file: {file}")
        
        # Transformation - Adding Columns:
        df = df.withColumn("source_format", lit("parquet")) \
               .withColumn("processed_at", lit("2024-10-05")) \
               .withColumnRenamed("name", "parquet_name") \
               .withColumn("parquet_name_reversed", col("parquet_name").substr(-1, 1))  # Example of string manipulation

        # Register DataFrame as a temporary SQL view for querying
        df.createOrReplaceTempView("parquet_table")
        
        # SQL Query: Select and filter the parquet data.
        df = spark.sql("""
            SELECT 
                parquet_name_reversed AS name_reversed,
                age,
                CASE 
                    WHEN age >= 60 THEN 'Yes'
                    ELSE 'No'
                END AS is_senior,  -- Mark as senior based on age
                processed_at
            FROM parquet_table
            WHERE age > 0
        """)

        # Aggregation:
        df.createOrReplaceTempView("parquet_table_transformed")
        df = spark.sql("""
            SELECT 
                is_senior,
                AVG(age) AS average_age,  -- Calculate average age
                COUNT(DISTINCT name_reversed) AS unique_parquet_names  -- Count distinct reversed names
            FROM parquet_table_transformed
            GROUP BY is_senior
        """)

        # Window Functions:
        # Why: Window functions allow us to apply functions (like rank, row_number) across a partition of data. Here, we rank people by age within each "is_senior" group.
        window_spec = Window.partitionBy("is_senior").orderBy(col("age").desc())  # Define the window partition and order by age descending
        df = df.withColumn("rank", rank().over(window_spec))  # Rank each person by age within their group

        # Caching:
        df.cache()

        # Action: Count and Show
        print(f"Count of rows in Parquet file: {df.count()}")
        df.show()

        # Write Parquet data with partitioning based on "is_senior"
        output_parquet_path = os.path.join(output_directory, "parquet_output", file)
        df.write.mode("overwrite").partitionBy("is_senior").format("parquet").save(output_parquet_path)

        dataframes.append(df)

# Union:
# Why: `unionByName` allows us to combine multiple DataFrames with the same schema. If the schema doesn't exactly match, `allowMissingColumns=True` can be used.
if dataframes:
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)  # Combine DataFrames

    # Broadcast Join (optional):
    # Why: Broadcast joins are used when one of the DataFrames is small enough to be sent to all worker nodes. This avoids a costly shuffle, making the join faster.
    # lookup_df = spark.read.parquet("/path/to/lookup")
    # combined_df = combined_df.join(broadcast(lookup_df), combined_df.some_column == lookup_df.some_column)

    # Save the combined DataFrame to a separate folder
    combined_output_path = os.path.join(output_directory, "combined_output")
    combined_df.write.mode("overwrite").format("parquet").save(combined_output_path)

    # Action: Trigger computation for the combined DataFrame
    print(f"Total count of combined rows: {combined_df.count()}")
    combined_df.show()
