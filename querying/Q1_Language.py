import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
#from pyspark.sql import SQLContext
import time



####### QUERY STRUCTURE # 1 ########
# Sum of view counts by language over a date range


### QUERY PARAMETERS ###
query_name = 'mattquery12'
# keyword = ''


## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \
    # .setMaster("spark://10.90.61.249:7077")

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# 7 random dates
dates_to_query = [20151201, 20151202]
tables_to_query = ["t"+str(i) for i in dates_to_query]

time_start = time.time()
for i, date_table in enumerate(tables_to_query):
    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable(query_name)


    query_call = "SELECT language, SUM(view_count) as viewCount FROM "+query_name+" GROUP BY language ORDER BY viewCount desc"
    mydf_sql = sqlContext.sql(query_call)

    if i == 0:
        dfAgg = mydf_sql.select( mydf_sql.language, mydf_sql.viewCount.alias("total"))
    else:
        mydf_sql.registerTempTable(query_name)
        query_call2 = "SELECT Agg.language, Agg.total + Temp1.viewCount as total FROM aggTable as Agg JOIN "+query_name+" as Temp1 ON Agg.language = Temp1.language"
        dfAgg = sqlContext.sql(query_call2)

    dfAgg.registerTempTable("aggTable")
    sqlContext.cacheTable("aggTable")

dfAgg.orderBy( dfAgg.total.desc()).show()

print "Total query runtime: %d seconds" % (time.time() - time_start)

# do something with results...

# to go back to an RDD, just go like rddNew = mydf_sql.rdd
