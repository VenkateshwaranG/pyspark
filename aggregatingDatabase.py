from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("Aggregation Using Database values")\
    .config("spark.jars", "C:\spark\jars\mysql-connector-java-5.1.49-bin.jar").getOrCreate()


#Need to get it from properties
my_sql_driver = "com.mysql.jdbc.Driver"
my_sql_host = "localhost"
my_sql_port = "3306"
my_sql_user = "root"
my_sql_pass = "root"
my_sql_db = "practice"
my_sql_URL = "jdbc:mysql://" + my_sql_host + ":" + my_sql_port + "/" + my_sql_db


schema_df = spark.read.format("jdbc").option("url", my_sql_URL)\
    .option("host", my_sql_host)\
    .option("driver", my_sql_driver)\
    .option("dbtable", "information_schema.columns")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .load()

schema_df.createOrReplaceTempView("TABLE_SCHEMA")

table_df = spark.read.format("jdbc").option("url", my_sql_URL)\
    .option("host", my_sql_host)\
    .option("driver", my_sql_driver)\
    .option("dbtable", "US_COUNTIES")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .load()
table_df.createOrReplaceTempView("US_COUNTIES")

### STRING MANIPULATION
string_df = spark.sql("select  table_name,column_name, string(data_type)  as datatype from TABLE_SCHEMA where table_name = 'US_COUNTIES' and string(data_type) IN( 'varchar','char','boolean')")

for i in string_df.rdd.collect():

    string_write_df = spark.sql("select  monotonically_increasing_id() as id, 'US_COUNTIES' as table_name, '"+i.column_name+"' as column_name, '"+i.datatype+"' as data_type, "+i.column_name+" as unique_values, COUNT("+i.column_name+") as count_of_unique_values, current_timestamp() as DateOfCapture  from US_COUNTIES GROUP BY "+i.column_name+" ")
    string_write_df.write.format("jdbc") \
    .option("url", my_sql_URL) \
    .option("driver", my_sql_driver) \
    .option("dbtable", "PROFILE_STR") \
    .option("user", my_sql_user) \
    .option("password", my_sql_pass) \
    .mode('overwrite').save()

### INTEGER MANIPULATION
int_df = spark.sql("select  table_name, column_name, string(data_type) as datatype from TABLE_SCHEMA where table_name = 'US_COUNTIES'  and string(data_type) IN('int','double','float', 'date')")

for i in int_df.collect():

    if i.datatype == 'date':
        int_write_df = spark.sql(
            "SELECT monotonically_increasing_id() as id, 'US_COUNTIES' as table_name, '"+ i.column_name +"' as column_name, '" + i.datatype + "' as data_type, 0 as Min_value, 0 as Max_value, 0 as Avg_value, 0 as stddev_value, min(" + i.column_name + ") as Min_date_value, max(" + i.column_name + ") as Max_date_value, current_timestamp() as DateOfCapture  from US_COUNTIES GROUP BY ' " + i.column_name + " ' ")

    else:

        int_write_df = spark.sql(
            "SELECT monotonically_increasing_id() as id, 'US_COUNTIES' as table_name, '" + i.column_name + "' as column_name, '" + i.datatype + "' as data_type, min(" + i.column_name + ") as Min_value, max(" + i.column_name + ") as Max_value, avg(" + i.column_name + ") as Avg_value, string(stddev(" + i.column_name + ")) as stddev_value, 'null' as Min_date_value, 'null' as Max_date_value, current_timestamp() as DateOfCapture  from US_COUNTIES GROUP BY ' " + i.column_name + "' ")

    int_write_df.show()
    int_write_df.write.format("jdbc")\
    .option("url", my_sql_URL)\
    .option("driver",my_sql_driver)\
    .option("dbtable", "PROFILE_NUMBER_AND_DATE")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .mode('append').save()

### DATE MANIPULATION
date_df = spark.sql("select table_name, column_name, string(data_type) as datatype from TABLE_SCHEMA where table_name = 'US_COUNTIES'  and string(data_type) IN('date','timestamp','datetime')")

for i in date_df.collect():
    print(i.column_name)
    date_write_df = spark.sql(
        "SELECT monotonically_increasing_id() as id, 'US_COUNTIES' as table_name, '" + i.column_name + "' as column_name, '" + i.datatype + "' as data_type, "+i.column_name+" as Unique_values, count("+i.column_name+") as count_of_unique_values, current_timestamp() as DateOfCapture from US_COUNTIES group by  "+i.column_name+" ")
    date_write_df.write.format("jdbc") \
        .option("url", my_sql_URL) \
        .option("driver", my_sql_driver) \
        .option("dbtable", "PROFILE_DATETIME") \
        .option("user", my_sql_user) \
        .option("password", my_sql_pass) \
        .mode('overwrite').save()