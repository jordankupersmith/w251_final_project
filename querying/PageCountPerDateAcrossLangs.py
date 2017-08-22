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
page_name = "Donald[_]Trump"
#page_name = "Prince_(musician)"
#page_name = "Main[_]Page"
dates_to_query = [20170528]#20151202, 20151225, 20160515, 20160518, 20160619, 20170526]
tables_to_query = ["t"+str(i) for i in dates_to_query]
temp_table = "DaveTemp1"

time_start = time.time()
results = []
for i, date_table in enumerate(tables_to_query):

    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable(temp_table)
    #query = "SELECT SUM(view_count) as viewCount FROM " + temp_table + " WHERE page_name LIKE '" + page_name + "'"

    query = "SELECT * FROM " + temp_table + " WHERE page_name LIKE '" + page_name + "'"
    print "query:\n", query
    mydf_sql = sqlContext.sql( query)

    # show results
    result = mydf_sql.collect()
    print "\n\n\n\n\nResults for %s: %s" % (date_table, result)
    print mydf_sql.show()

    # store in list
    results += [(date_table, result)]

# store list in a dataframe
print "Results:"
for r in results:
    print "%s: %s" % (r[0], r[1])

print "Total query runtime: %d seconds" % (time.time() - time_start)


# do something with results...

# to go back to an RDD, just go like rddNew = mydf_sql.rdd
