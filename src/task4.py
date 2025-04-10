from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Task4_Window_Rank").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

avg_temp_df = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))

windowSpec = Window.orderBy(avg_temp_df["avg_temp"].desc())
ranked_df = avg_temp_df.withColumn("rank_temp", dense_rank().over(windowSpec))

ranked_df.show(5)

ranked_df.limit(5).write.csv("outputs/task4_output.csv", header=True, mode="overwrite")
