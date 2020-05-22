from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("Data Split").config("spark.jars", "C:\spark\jars\mysql-connector-java-5.1.49-bin.jar") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

my_sql_driver = "com.mysql.jdbc.Driver"
my_sql_host = "localhost"
my_sql_port = "3306"
my_sql_user = "root"
my_sql_pass = "root"
my_sql_db = "practice"
my_sql_URL = "jdbc:mysql://" + my_sql_host + ":" + my_sql_port + "/" + my_sql_db

read_data = spark.read.format("csv").option("header", "true").load("K:/BigData/DataSource/us-counties.csv") #Dataframe
read_data.createOrReplaceTempView("COVIDCASES")

df_int = spark.sql("select  county, MIN(cases) as minimum_cases, MAX(cases) as max_cases, AVG(cases) as Average_cases from covidcases group by county")
df_str = spark.sql("select state as name_of_the_state, date, count(state) as no_of_occurances from covidcases group by state,date")

#To add key column with running numbers
rdd_int_df = df_int.rdd.zipWithIndex()
df_final = rdd_int_df.toDF()

rdd_str_df = df_str.rdd.zipWithIndex()
df_str_final = rdd_str_df.toDF()

#We need to split the content from dataframe again since, zpwithindex would have clubbed the data into object
df_final = df_final.withColumn('county', df_final['_1'].getItem("county"))
df_final = df_final.withColumn('minimum_cases', df_final['_1'].getItem("minimum_cases"))
df_final = df_final.withColumn('max_cases', df_final['_1'].getItem("max_cases"))
df_final = df_final.withColumn('Average_cases', df_final['_1'].getItem("Average_cases"))
df_final = df_final.withColumn('current_time', F.current_timestamp())

df_str_final = df_str_final.withColumn('name_of_the_state', df_str_final['_1'].getItem("name_of_the_state"))
df_str_final = df_str_final.withColumn('no_of_occurances', df_str_final['_1'].getItem("no_of_occurances"))
df_str_final = df_str_final.withColumn('current_time', F.current_timestamp())


df_final.createOrReplaceTempView("COVID_INT_AGG")
df_str_final.createOrReplaceTempView("COVID_STR_AGG")

df_write_int_table = spark.sql("select _2 as id, county, minimum_cases, max_cases, average_cases, current_time from COVID_INT_AGG")
df_write_str_table = spark.sql("select _2 as id, name_of_the_state,no_of_occurances, current_time from COVID_STR_AGG")

df_write_int_table.write.format("jdbc")\
    .option("url", my_sql_URL)\
    .option("driver", my_sql_driver)\
    .option("dbtable", "COVID_INT_AGG")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .mode('overwrite').save()

df_write_str_table.write.format("jdbc")\
    .option("url", my_sql_URL)\
    .option("driver", my_sql_driver)\
    .option("dbtable", "COVID_STR_AGG")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .mode('overwrite').save()
