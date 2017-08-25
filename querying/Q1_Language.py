import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
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
    .setMaster("spark://10.90.61.249:7077")

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

# 7 dates
#dates_to_query = [20151225, 20151226, 20151227, 20151228, 20151229, 20151230, 20151231]
dates_to_query = [20160108, 20160109, 20160110, 20160111, 20160112, 20160113, 20160114]

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

### OUTPUT TABLE/FILE NAME - MAX LENGTH IS 63 CHARS###
### (assumes there are at least 2 dates) ###
#Sum_of_Views_Per_Language_From
output_table_name = "Query1_From_" + str(dates_to_query[0]) \
                    + "_to_" + str(dates_to_query[-1])

dfAgg.toPandas().to_csv('/data/spark_queries/outputFiles/' + output_table_name + '.csv', header=True)

#try:
    # connect to DB
    #con = psycopg2.connect("dbname='251FinalProject' user='janbodnar'")   
    #cur = con.cursor()
    
    # query name is (start_date - end_date) - that is table name
    # create table
    #cur.execute("DROP TABLE IF EXISTS " + output_table_name)
    #cur.execute("CREATE TABLE " + output_table_name + " (Language TEXT, Name TEXT, Price INT)")
    
    # create sql insert statement
    #sqlInsert = "INSERT INTO " + output_table_name + " (language, count) VALUES(%s, %s)"
    
    # then insert in table
    #for row in dfAgg:
        # cur.execute( sqlInsert, (row.language, row.total))
    
    #cur.commit()
##except psycopg2.DatabaseError, e:
##    
##    if con:
##        con.rollback()
##    
##    print 'Error %s' % e    
##    #sys.exit(1)
##    
##    
##finally:
##    
##    if con:
##        con.close()


print "Total query runtime: %d seconds" % (time.time() - time_start)

# do something with results...

# to go back to an RDD, just go like rddNew = mydf_sql.rdd
