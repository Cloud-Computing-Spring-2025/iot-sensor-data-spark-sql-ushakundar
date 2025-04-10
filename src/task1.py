from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

# Load the CSV with inferred schema
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create a temporary view
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

# Count total number of records
print("Total Records:", df.count())

# Distinct locations
df.select("location").distinct().show()

# Save output
df.write.csv("task1_output.csv", header=True, mode="overwrite")
