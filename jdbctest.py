from pyspark.sql import SparkSession
#from pyspark.conf import SparkConf


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


print(my_sql_URL)

read_df = spark.read.format("jdbc")\
    .option("url", my_sql_URL)\
    .option("driver", my_sql_driver)\
    .option("dbtable", "US_COUNTIES")\
    .option("user", my_sql_user)\
    .option("password", my_sql_pass)\
    .load()

read_df.createOrReplaceTempView("strcounty")
spark.sql("SELECT 1,2 FROM strcounty ").show()
