from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count
import time
import sys

start_time = time.time()

# Initialize Spark Session
spark = SparkSession.builder.appName("SyntheticCustomerDataProcessing").getOrCreate()

# Load dataset into a DataFrame
try:
    df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
except Exception as e:
    print(f"Error loading csv file {e}")
    sys.exit(1)

# Filter unnecessary data
excluded_countries = ["IT", "RU"]
df = df.filter(~col("Country").isin(excluded_countries))

# Data Standardization
df = df.withColumn("Country", 
                    when(col("Country") == "US", "United States")
                    .when(col("Country") == "UK", "United Kingdom")
                    .otherwise(col("Country")))

# Aggregate salary data
df_avg_salary = df.groupBy("Country").agg(
    avg("Salary").alias("Avg_Salary"),
    count("Salary").alias("Count")
).orderBy("Country")

df_avg_salary.show()

print("Execution Time: %.2f seconds" % (time.time() - start_time))