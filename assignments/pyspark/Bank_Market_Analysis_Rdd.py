from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create context
sc = SparkContext('local')

# create rdd by loading bank market data
rdd=sc.textFile("C:\\Users\\miles\\Desktop\\Big Data\\pyspark\\bank_data_modified.csv")
rdd.collect()
total=rdd.count()
print(total)


rdd_1=rdd.map(lambda x:x.split(";"))

rdd_yes=rdd_1.filter(lambda x:x[-1]=='yes')
subs_count=rdd_yes.count()
subs_count

# marketing success rate. 
# (No. of people subscribed / total no. of entries)
success_rate=(subs_count/total)*100
success_rate

rdd_no=rdd_1.filter(lambda x:x[-1]=='no')
nonsubs_count=rdd_no.count()
nonsubs_count

# marketing failure rate
# (No. of people not subscribed / total no. of entries)
Failure_rate=(nonsubs_count/total)*100
Failure_rate\

heading=rdd_1.first()
rdd_2=rdd_1.filter(lambda b:b!=heading)
rdd_2.collect()

#4.Maximum, Mean, and Minimum age of the average targeted customer
rdd_3=rdd_2.map(lambda c:int(c[0]))
rdd_3.max()
rdd_3.min()
rdd_3.mean()

#5.	Check the quality of customers by checking the average balance,
#   median balance of customers
rdd_4=rdd_2.map(lambda d:int(d[5])).sortBy(lambda d:d)
rdd_4.collect()
rdd_4.mean()
value=int(total/2)
rdd_4.collect()[value]
# 6.Check if age matters in marketing subscription for deposit
rdd_5=rdd_2.map(lambda e:(e[0],1))
rdd_5.groupByKey().mapValues(list)
rdd_5.collect()
rdd_6=rdd_5.groupByKey().mapValues(list)
rdd_6.collect()
rdd_7=rdd_6.map(lambda g:(g[0],len(g[1])))
rdd_8=rdd_7.sortBy(lambda i:i[1],ascending=False)
rdd_8.collect()
# 7.Check if marital status mattered for subscription to deposit.
rdd_9=rdd_2.map(lambda j:(j[2],1))
rdd_10=rdd_9.groupByKey().mapValues(list)
rdd_11=rdd_10.map(lambda k:(k[0],len(k[1])))
rdd_11.collect()
rdd_marital_status=rdd_2.map(lambda j:(j[2],1)).groupByKey().mapValues(list).map(lambda k:(k[0],len(k[1])))
rdd_marital_status.collect()
# 8.Check if age and marital status together mattered 
# for subscription to deposit scheme
rdd_status=rdd_2.map(lambda l:(l[0],l[2],1)).groupBy(lambda m:((m[0],m[1]))).mapValues(list).map(lambda n:(n[0],len(n[1]))).sortBy(lambda o:o[1],ascending=False)
rdd_status.collect()






