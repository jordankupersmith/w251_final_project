import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
#from pyspark.sql import SQLContext
import time



####### QUERY STRUCTURE # 1 ########
# Sum of view counts by page filtered by keyword over a date range


### QUERY PARAMETERS ###
query_name = 'mattquery13'
# keyword = 'Throne'  ## Enter this below
dates_to_query = [20151201, 20151203]


## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \
    # .setMaster("spark://10.90.61.249:7077") #Uncomment this to run on the main cluster

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

tables_to_query = ["t"+str(i) for i in dates_to_query]

time_start = time.time()
for i, date_table in enumerate(tables_to_query):
    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable(query_name)


    query_call = "SELECT page_name, SUM(view_count) as viewCount FROM "+query_name+" GROUP BY page_name ORDER BY viewCount desc"
    mydf_sql = sqlContext.sql(query_call)

    # keyword_search = keyword + '%'
    if i == 0:
        dfAgg = mydf_sql.select( mydf_sql.page_name, mydf_sql.viewCount.alias("total")).filter(mydf_sql.page_name.like('Throne%'))
    else:
        mydf_sql.registerTempTable(query_name)
        query_call2 = "SELECT Agg.page_name, Agg.total + Temp1.viewCount as total FROM aggTable as Agg JOIN "+query_name+" as Temp1 ON Agg.page_name = Temp1.page_name"
        dfAgg = sqlContext.sql(query_call2)

    dfAgg.registerTempTable("aggTable")
    sqlContext.cacheTable("aggTable")

dfAgg.orderBy( dfAgg.total.desc()).show()

print "Total query runtime: %d seconds" % (time.time() - time_start)

# do something with results...
### OUTPUT TABLE/FILE NAME - MAX LENGTH IS 63 CHARS###
### (assumes there are at least 2 dates) ###
#Sum_of_Views_Per_Language_From
output_table_name = "Query2_" + page_name + "_From_" + str(dates_to_query[0]) \
                    + "_to_" + str(dates_to_query[-1])

dfAgg.toPandas().to_csv('/data/spark_queries/outputFiles/' + output_table_name + '.csv', header=True)
