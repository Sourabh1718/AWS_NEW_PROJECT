from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession with Hive support enabled
spark = SparkSession.builder \
    .appName("S3ToHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the path to the CSV file on S3
s3_path = "s3://food-data-gds/"

# Define your Hive table name
hive_table_name = "food_report.inspection_data"

# Read the CSV file from S3, inferring schema and using the first row as header
df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)

# Prepare new column names by replacing spaces with underscores
new_columns = [c.replace(" ", "_") for c in df.columns]

# Rename the columns in the DataFrame
df = df.toDF(*new_columns)

df.printSchema()
df.show()

# Create Hive database
spark.sql("CREATE DATABASE IF NOT EXISTS food_report")
print(f"Database Created Successfully !!")

# Create Hive internal table with the specified schema
spark.sql("""
CREATE TABLE IF NOT EXISTS food_report.inspection_data (
    Name STRING,
    Program_Identifier STRING,
    Inspection_Date STRING,
    Description STRING,
    Address STRING,
    City STRING,
    Zip_Code STRING,
    Phone STRING,
    Longitude DOUBLE,
    Latitude DOUBLE,
    Inspection_Business_Name STRING,
    Inspection_Type STRING,
    Inspection_Score INT,
    Inspection_Result STRING,
    Inspection_Closed_Business BOOLEAN,
    Violation_Type STRING,
    Violation_Description STRING,
    Violation_Points INT,
    Business_ID STRING,
    Inspection_Serial_Num STRING,
    Violation_Record_ID STRING,
    Grade INT
) STORED AS PARQUET
""")
          
print(f"Table Created Successfully !!")

# Ingest DataFrame into the Hive table
df.write.mode("overwrite").insertInto("food_report.inspection_data")

print(f"Data ingested successfully into {hive_table_name}")

# Stop the Spark session
spark.stop()
