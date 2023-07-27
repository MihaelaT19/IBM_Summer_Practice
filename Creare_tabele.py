#from pyspark.sql import SparkSession
#spark = SparkSession.builder \
 #   .appName("PySpark Oracle Connection") \
  #  .getOrCreate()
#oracle_url = "jdbc:oracle:thin:@hostname:port/service_name"
#oracle_properties = {
 #   "user": "root",
  #  "password": "root",
   # "driver": "oracle.jdbc.driver.OracleDriver"
#}

#df = spark.read \
 #   .format("jdbc") \
  #  .option("url", oracle_url) \
   # .option("dbtable", "your_table_name") \
    #.options(**oracle_properties) \
    #.load()
#spark.stop()
