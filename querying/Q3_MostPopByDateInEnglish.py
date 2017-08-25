import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
import time

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \
    .setMaster("spark://10.90.61.249:7077")

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# this is a single date query, only use one date!!
dates_to_query = [20170120]#, 20160831, 20170120, 20161120, 20151202, 20151225, 20160515, 20160518, 20160619, 20170526]
tables_to_query = ["t"+str(i) for i in dates_to_query]
temp_table = "Q3Temp1"

time_start = time.time()
results = []
language = 'en'
for i, date_table in enumerate(tables_to_query):

    mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                   load(keyspace="wikikeyspace", table=date_table)
    mydf.registerTempTable(temp_table)
    sqlContext.cacheTable(temp_table)
    #query = "SELECT SUM(view_count) as viewCount FROM " + temp_table + " WHERE page_name LIKE '" + page_name + "'"

    #query = "SELECT * FROM " + temp_table + " WHERE page_name LIKE '%" + page_name + "%'"
    query = "SELECT page_name, language, view_count FROM " + temp_table + \
            " WHERE language = '" + language + "' ORDER BY view_count desc"
    print "query:\n", query
    mydf_sql = sqlContext.sql( query)

    # show results
    #result = mydf_sql.collect()
    #print "\n\n\n\n\nResults for %s: %s" % (date_table, result)
    #print mydf_sql.show()

# store in list
#results += [(result.viewCount)]

# store list in a dataframe
#print "Results:"
#for r in result:
#    print "%s: %s" % (r[0], r[1], r[2])

# table name is date

print "Total query runtime: %d seconds" % (time.time() - time_start)


# do something with results...
### OUTPUT TABLE/FILE NAME - MAX LENGTH IS 63 CHARS###
### (assumes there are at least 2 dates) ###
#Sum_of_Views_Per_Language_From
output_table_name = "Query3_MostPopular_On_" + str(dates_to_query[0])

mydf_sql.limit(50).toPandas().to_csv('/data/spark_queries/outputFiles/' + output_table_name + '.csv', header=True, index=True, encoding='utf-8')


                                                                                                       
                       
