from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
spark
df=spark.read.csv("C:\\Users\\mihae\\OneDrive\\Desktop\\Erasmus.csv",header=True, inferSchema=True)
df.show(100)
###Var1-Filter
df.filter((df['Receiving Country Code']=='LV')|
          (df['Receiving Country Code']=='MK')|
          (df['Receiving Country Code']=='MT')).select('Receiving Country Code','Sending Country Code').show()

###Var 2-groupBy
df.groupby(df['Receiving Country Code']& df['Sending Country Code' ]).count().show(1004 )