from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
spark
df=spark.read.csv("C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv")
df.show(100)
