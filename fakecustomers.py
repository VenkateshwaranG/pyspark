from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Fake Customers").getOrCreate()

read_df= spark.read.format("csv").option("header","false").option("infer).load("K:/BigData/DataSource/fake_customers.csv")
read_df.show()