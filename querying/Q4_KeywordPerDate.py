import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
import time
import csv

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \
    .setMaster("spark://10.90.61.249:7077")

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# 7 random dates
page_name = "Donald_Trump"
#page_name = "Prince_(musician)"
#page_name = "Main[_]Page"
#dates_to_query = [20170528, 20161120]#20151202, 20151225, 20160515, 20160518, 20160619, 20170526]
dates_to_query = [20160101, 20160201, 20160301, 20160401, 20160501, 20160601]
tables_to_query = ["t"+str(i) for i in dates_to_query]
temp_table = "Q4Temp1"

time_start = time.time()
results = [("date", "count")]
for i, date_table in enumerate(tables_to_query):

    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable(temp_table)
    sqlContext.cacheTable(temp_table)
    query = "SELECT SUM(view_count) as viewCount FROM " + temp_table + " WHERE page_name LIKE '%" + page_name + "%'"
    #print "query:\n", query
    mydf_sql = sqlContext.sql( query)

    # show results
    result = mydf_sql.collect()
    print "\n\n\n\n\nResults for %s: %s" % (date_table, result[0].viewCount)
    #print mydf_sql.show()

    # store in list
    results += [(date_table, result[0].viewCount)]

# store list in a dataframe
print "Results:"
for r in results:
    print "%s: %s" % (r[0], r[1])

# do something with results...
### OUTPUT TABLE/FILE NAME - MAX LENGTH IS 63 CHARS###
### (assumes there are at least 2 dates) ###
#Sum_of_Views_Per_Language_From
output_table_name = "Query4_" + page_name + "_From_" + str(dates_to_query[0]) \
                    + "_to_" + str(dates_to_query[-1])

with open('/data/spark_queries/outputFiles/' + output_table_name + '.csv', "wb") as f:
    writer = csv.writer( f)
    writer.writerows( results)

print "Total query runtime: %d seconds" % (time.time() - time_start)


