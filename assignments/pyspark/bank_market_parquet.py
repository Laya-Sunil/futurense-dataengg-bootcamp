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

df = spark.read.options(delimiter=';',header=True).csv("/home/laya/Codes/bank_market_modified.csv")
df.createOrReplaceTempView("bank")
df.show()

res = spark.sql("select case when age<13 then 'Kids' when age<=19 and age>12 then 'teenager' when age>19 and age<31 then 'Youngsters' when age<50 then 'Middle-Aged' else 'Senior' end as age_group, count(*)as count from bank where y='yes' group by age_group")
res.show()
res.write.format("parquet").save("/home/laya/Code/age_grp.parquet")
readParq = spark.read.format('parquet').load('/home/laya/Code/age_grp.parquet')
readParq.show()


res_1 = spark.sql("select case when age<13 then 'Kids' when age<=19 and age>12 then 'teenager' when age>19 and age<31 then 'Youngsters' when age<50 then 'Middle-Aged' else 'Senior' end as age_group, count(*)as count from bank where y='yes' group by age_group having count>2000")
res_1.show()
res_1.write.format("avro").save("/home/laya/Code/age_grp.avro")
readavro = spark.read.format('avro').load('/home/laya/Code/age_grp.avro')
readavro.show()

# run the code on cluster
# spark-submit bank_mark.py --master spark://laya-virtual-machine:7077 

# run the code on local
# spark-submit bank_mark.py 


