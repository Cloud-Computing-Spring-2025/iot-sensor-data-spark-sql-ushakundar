from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, hour, to_timestamp, round

spark = SparkSession.builder.appName("Task5_Pivot").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("hour_of_day", hour("timestamp"))

pivot_df = df.groupBy("location").pivot("hour_of_day", list(range(24))) \
    .agg(round(avg("temperature"), 1))

pivot_df.show()

pivot_df.write.csv("outputs/task5_output.csv", header=True, mode="overwrite")
