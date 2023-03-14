from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create context
sc = SparkContext('local')
# create spark session
spark = SparkSession.builder.master("local[1]") \
                    .appName('Spark_weather_data_analysis') \
                    .getOrCreate()

# create rdd by loading weather data
rdd=sc.textFile("/home/laya/Codes/weather_fine.txt")
rdd1=rdd.map(lambda y:y.split(" "))

# create dataframe from rdd
df=spark.createDataFrame(rdd1)
df.show()

# cast the temperature values to floats
df=df.withColumn("temp_1",col("_6").cast("float"))
df=df.withColumn("temp_2",col("_7").cast("float"))

#Display Max Temperature
df.select(max("temp_1")).show()
#Display min Temperature
df.select(min("temp_2")).show()
#Display MonthWise max Temperature
df.groupBy(month("_2")).max("temp_1").show()
#Display Monthwise Minimum Temperature
df.groupBy(month("_2")).min("temp_2").show()