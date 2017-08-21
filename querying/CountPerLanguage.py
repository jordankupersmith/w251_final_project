import pyspark_cassandra
from pyspark import SparkConf
from pyspark.sql import *
import pyspark.sql.functions as F
#from pyspark.sql import SQLContext

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

conf = SparkConf() \
    .setAppName(APP_NAME) \

sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
sqlContext = SQLContext(sc)

date = 't20151202'

#current_monthly_table = sc.cassandraTable("wikikeyspace", date_table)
#mydf = current_monthly_table.toDF()
mydf = sqlContext.read.format("org.apache.spark.sql.cassandra").\
               load(keyspace="wikikeyspace", table=date)
mydf.registerTempTable("DaveTemp1") # change to another name!

# this query leads to 230 rows
# you can pretty much query anything in the table you just registered
# haven't quite determined if we are really saving time registering,
# but if you don't register, you have to use the dataframe equivalent calls
# for SQL queries, like .agg, .select, .orderby, etc, and it is less intuitive
# this is likely the path of least resistance...
mydf_sql = sqlContext.sql("SELECT language, SUM(view_count) as count FROM DaveTemp1 GROUP BY language ORDER BY count desc")

# show has an optional param for # of rows to show, default is 20
mydf_sql.show()

# do something with results...

# to go back to an RDD, just go like rddNew = mydf_sql.rdd
