import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
#from pyspark.sql import SQLContext
import time

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# 7 random dates
dates_to_query = [20151202, 20151225, 20160515, 20160518, 20160619, 20170526, 20170528]
tables_to_query = ["t"+str(i) for i in dates_to_query]

time_start = time.time()

for i, date_table in enumerate(tables_to_query):

    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable("DaveTemp1") # change to another name!
    
    mydf_sql = sqlContext.sql("SELECT language, SUM(view_count) as viewCount FROM DaveTemp1 GROUP BY language ORDER BY viewCount desc")
    
    if i == 0:
        dfAgg = mydf_sql.select( mydf_sql.language, mydf_sql.viewCount.alias("total"))
    else:
        mydf_sql.registerTempTable("DaveTemp1")
        dfAgg = sqlContext.sql("SELECT Agg.language, Agg.total + Temp1.viewCount as total FROM aggTable as Agg JOIN DaveTemp1 as Temp1 ON Agg.language = Temp1.language")
    
    dfAgg.registerTempTable("aggTable")

    # this is like a checkpoint because it forces df evaluation and saves the result in memory
    # my thinking is that this will be like insurance against memory explosion and executor loss
    #
    # NB:
    # this table is ~230 rows. Tiny. Each time we call cacheTable, the old one stays in memory but is
    # no longer accessible. Let's not call it a "leak" though because we're in Python, so it's by design...
    # We have a ton of RAM on these machines, so it's no big deal HERE, but following this same query
    # architecture and caching large tables (millions of rows) over weeks or months COULD cause a problem.
    # This is probably not going to affect us, but just keep it in mind.
    # to uncache a table, call sqlContext.unCacheTable("oldTableName") - keep in mind we need it cached tho,
    # so we'd have to fudge with the names and keep them unique, ie add an index
    sqlContext.cacheTable("aggTable")
    
dfAgg.orderBy( dfAgg.total.desc()).show()    

print "Total query runtime: %d seconds" % (time.time() - time_start)

# do something with results...

# to go back to an RDD, just go like rddNew = mydf_sql.rdd
