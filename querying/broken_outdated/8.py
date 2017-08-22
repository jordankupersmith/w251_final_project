# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages \TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=127.0.0.1 /root/queries/8.py

def query(sc):

    # dates_to_query = [20160803, 20160804] # Cluster
    dates_to_query = [20151202, 20151203] #Standalone
    tables_to_query = ["t"+str(i) for i in dates_to_query]

    rdd_list_count = 0

    for date_table in tables_to_query:

        if rdd_list_count == 0:
            current_monthly_table = sc \
            .cassandraTable("wikikeyspace", date_table) \
            .select("language", "page_name", "view_count") \
            .limit(10000) \
            .filter(lambda x: "Clinton" in x["page_name"])

            current_monthly_table.saveToCassandra("wikikeyspace", "mattquery7temp", row_format=pyspark_cassandra.RowFormat.TUPLE)
            rdd_list_count += 1

        else:

        # Query Structure (Total all pages with the word Trump in it over the date range)
            current_monthly_table = sc \
            .cassandraTable("wikikeyspace", date_table) \
            .select("language", "page_name", "view_count") \
            .limit(10000) \
            .filter(lambda x: "Clinton" in x["page_name"])

            current_monthly_table \
            .saveToCassandra("wikikeyspace", "mattquery7temp")
            # .saveToCassandra("wikikeyspace", "mattquery7temp") #, row_format=pyspark_cassandra.RowFormat.TUPLE)

    def reduce_func(x,y):
        try:
            return int(x[1]) + int(y[1])
        except:
            return 0


    final_rdd = sc \
    .cassandraTable("wikikeyspace", "mattquery7temp") \
    .select("page_name", "view_count") \
    .map(lambda x: (x["page_name"], x["view_count"])) \
    .reduceByKey(lambda x,y: x + y) \
    .map(lambda x: (x[1], x[0])) \
    .sortByKey(False) \
    .map(lambda x: (x[1], x[0]))

    final_rdd.saveToCassandra("wikikeyspace", "mattquery7")

## Imports
from pyspark import SparkConf
# from pyspark.sql import HiveContext
# from pyspark.sql.types import *
import pyspark_cassandra

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

import subprocess

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE IF EXISTS wikikeyspace.mattquery7;\""
subprocess.call(cql_call, shell=True)

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE IF EXISTS wikikeyspace.mattquery7temp;\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery7temp( language text, page_name text PRIMARY KEY, view_count int );\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery7( page_name text PRIMARY KEY, view_count int);\""
subprocess.call(cql_call, shell=True)


## Main functionality
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf() \
    .setAppName(APP_NAME) \
    # .setMaster("spark://10.90.61.249:7077")
    # .set("spark.cassandra.connection.host", "10.90.61.249")

    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    # Execute Query
    query(sc)
