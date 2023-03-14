
from pyspark import SparkContext

# create context
sc = SparkContext('local')
rdd=sc.textFile("/home/laya/Codes/weather_fine.txt")
type(rdd)

# display rdd contents
rdd.collect()

# split data by coma
rdd2=rdd.map(lambda a:a.split(" "))
# convert temperature to floats
rdd_max=rdd2.map(lambda x:float(x[5]))

# display max temperature
rdd_max.max()

# convert temperature to floats
rdd_min=rdd2.map(lambda b:float(b[6]))

# display min temperature
rdd_min.min()


rdd5=rdd2.map(lambda x:(x[1][5:7],float(x[5])))

rdd6=rdd5.groupByKey().mapValues(list)
rdd_month_max=rdd6.map(lambda x:(x[0],max(x[1])))
rdd_month_max.collect()

rdd8=rdd2.map(lambda x:(x[1][5:7],float(x[6])))
rdd9=rdd8.groupByKey().mapValues(list)

rdd_month_min=rdd9.map(lambda x:(x[0],min(x[1])))

rdd_month_min.collect()
