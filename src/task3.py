from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, to_timestamp

spark = SparkSession.builder.appName("Task3_Time_Analysis").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))

df.createOrReplaceTempView("sensor_readings")

# Hourly average temp
hourly_avg = df.withColumn("hour_of_day", hour("timestamp")) \
    .groupBy("hour_of_day") \
    .agg({"temperature": "avg"}) \
    .withColumnRenamed("avg(temperature)", "avg_temp") \
    .orderBy("hour_of_day")

hourly_avg.show()

hourly_avg.write.csv("outputs/task3_output.csv", header=True, mode="overwrite")
