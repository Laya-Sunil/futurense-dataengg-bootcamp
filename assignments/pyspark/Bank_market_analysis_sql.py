
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create context
sc = SparkContext('local')
# create spark session
spark = SparkSession.builder.master("local[1]") \
                    .appName('Spark_bank_market_data_analysis') \
                    .getOrCreate()

df = spark.read.options(delimiter=';',header=True).csv("/home/laya/Codes/bank_market_modified.csv")
df.createOrReplaceTempView("bank")

# 2
spark.sql('select (sum(case when y="yes" then 1 else 0 end)/count(*))*100 as success_rate from bank').show()
# 3
spark.sql('select (sum(case when y="no" then 1 else 0 end)/count(*))*100 as failure_rate from bank').show()
# 4
spark.sql('select max(age) max_age, min(age) min_age, avg(age) avg_age from bank').show()
# 5
spark.sql('select avg(balance)mean_balance, percentile_approx(balance, 0.5) as median_balance from bank').show()
# 6
spark.sql('select age, count(*) from bank where y="yes" group by age order by age').show()
spark.sql("select case when age<13 then 'Kids' when age<=19 and age>12 then 'teenager' when age>19 and age<31 then 'Youngsters' when age<50 then 'Middle-Aged' else 'Senior' end as age_group, count(*)as count from bank where y='yes' group by age_group").show()


#res=spark.sql("select case when age<13 then 'Kids' when age<=19 and age>12 then 'teenager' when age>19 and age<31 then 'Youngsters' when age<50 then 'Middle-Aged' else 'Senior' end as age_group, count(*)as count from bank where y='yes' group by age_group")
#res.write.csv('/home/laya/Code')

# 7
spark.sql("select marital, count(*) as count from bank where y='yes' group by marital").show()

# 8
spark.sql("select age, marital, count(*) as count from bank where y='yes' group by age,marital").show()