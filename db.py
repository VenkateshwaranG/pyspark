from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("DB").getOrCreate()
param = sys.argv[1]
print(param)
sdf=spark.read.format('jdbc'). \
     options(
         url='jdbc:mysql://localhost:3306/', # database url (local, remote)
         dbtable=sys.argv[1],
         user='root',
         password='root',
         driver='com.mysql.jdbc.Driver'). \
     load()

sdf.show()