from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys

spark= SparkSession.builder.appName("ji").getOrCreate()

read_df = spark.read.format("csv")
