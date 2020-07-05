from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import argparse

spark= SparkSession.builder.appName("Aggregation of Counties").config("spark.jars", "C:\spark\jars\mysql-connector-java-5.1.49-bin.jar").\
    getOrCreate()
ap = argparse.ArgumentParser()


filename = ap.args(0)
df_read = spark.read.format("csv").option("header", "true").load(filename)
df_read = df_read.withColumn("File Name", input_file_name())
df_read.createOrReplaceTempView("COVID")


df_read.show()