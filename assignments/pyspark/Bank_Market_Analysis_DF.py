from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create context
sc = SparkContext('local')
# create spark session
spark = SparkSession.builder.master("local[1]") \
                    .appName('Spark_bank_market_data_analysis') \
                    .getOrCreate()

# create rdd by loading bank market data
#rdd=sc.textFile("C:\\Users\\miles\\Desktop\\Big Data\\pyspark\\bank_data_modified.csv")
#df = spark.createDataFrame(rdd)

# 1
df = spark.read.options(delimiter=';',header=True).csv("/home/laya/Codes/bank_market_modified.csv")
df.show()

total = df.count()
# 2
Success_rate=(df[df["y"]=='yes'].count()/total)*100
Success_rate

# 3
Failure_rate=(df[df["y"]=='no'].count()/total)*100
Failure_rate

# 4
Age_metrics=df.select(max('age').alias('max_age'), min('age').alias('min_age'),mean('age').alias('mean_age')).show()
Age_metrics

# 5
Avg_Balance=df.select(mean('balance')).show()
Avg_Balance

Median_Balance=df.select(col('balance').cast('float').alias('bal')).approxQuantile('bal', [0.5], 0)
Median_Balance
# output => [448.0]

# 6
df.groupBy('age').count().sort('age').show()
# 7
df.groupBy('marital').count().show()
# 8
df.groupBy("age",'marital').count().sort('age').show()



