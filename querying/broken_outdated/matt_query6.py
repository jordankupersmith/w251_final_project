# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages \TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=/10.90.61.249 /root/queries/mattquery4.py

# Takes a while to run, but you can pull up the results in CASSANDRA_HOME/bin/cqlsh
# select wikikeyspace;
# select * from mattquery2 limit 50;
# Couldn't get Order By to work...

def query_first_then_join(sc):

    # Select Dates
    # dates_to_query = [20160808,20160809]
    # tables_to_query = ["t"+str(i) for i in dates_to_query]

    # rdd_list_count = 0



    current_monthly_table = sc \
    .cassandraTable("wikikeyspace", "t20151201") \
    .joinWithCassandraTable("wikikeyspace", "t20151202") \
    .select("page_name", "view_count") \
    .filter(lambda x: "Clinton" in x[0]) \
    .reduceByKey(lambda x,y: x+y) \
    .map(lambda (a, b): (b, a)) \
    .sortByKey(False) \
    .map(lambda (a, b): (b, a))
    # .persist()
    # .where("language=?", "en") \
    # combined_rdd = current_monthly_table#.persist()
    # rdd_list_count += 1
    # print(rdd_list_count)
    # continue
        # else:
        #
        # # Query Structure (Total all pages with the word Trump in it over the date range)
        #     current_monthly_table = sc \
        #     .cassandraTable("wikikeyspace", date_table) \
        #     .select("page_name", "view_count") \
        #     .filter(lambda x: "Clinton" in x["page_name"])
        # # .persist()
        #
        #     combined_rdd = combined_rdd.union(current_monthly_table) \
        #     .reduceByKey(lambda x,y: x["view_count"]+y["view_count"]) \
        #     .map(lambda (a, b): (b, a)) \
        #     .sortByKey(False) \
        #     .map(lambda (a, b): (b, a))
        #     # .persist()

        # .rdd2.union(rdd1).reduceByKey(lambda x,y : x+y)
        # .map(lambda r: (r["page_name"], "view_count") \ #mapping and reducing changes current_monthly_table from an RDD, preventing saving to a cassandra table?
        # .reduceByKey(lambda a, b: a + b)
        # .collect()

        # Save to Cassandra (nested dictionaries create the structure of the cassandra table)
    current_monthly_table.saveToCassandra("wikikeyspace", "mattquery6")


## Imports
from pyspark import SparkConf
# from pyspark.sql import HiveContext
# from pyspark.sql.types import *
import pyspark_cassandra

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

import subprocess

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery4( page_name text PRIMARY KEY, view_count int );\""
subprocess.call(cql_call, shell=True)


## Main functionality
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf() \
    .setAppName(APP_NAME) \
    # .setMaster("spark:///10.90.61.249:7077")
    # .set("spark.cassandra.connection.host", "10.90.61.249")

    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    # Execute Main functionality
    # main(sc)

    # Execute Query
    query_first_then_join(sc)
    # join_first_then_query(sc)
