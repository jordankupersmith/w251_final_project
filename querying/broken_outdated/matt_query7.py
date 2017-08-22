# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages \TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=127.0.0.1/root/queries/mattquery7.py

# Takes a while to run, but you can pull up the results in CASSANDRA_HOME/bin/cqlsh
# select wikikeyspace;
# select * from mattquery2 limit 50;
# Couldn't get Order By to work...

def query(sc):

    # Select Dates
    # dates_to_query = [20160808,20160809]
    # tables_to_query = ["t"+str(i) for i in dates_to_query]

    # rdd_list_count = 0

    dates_to_query = [20151204, 20151205]
    tables_to_query = ["t"+str(i) for i in dates_to_query]

    rdd_list_count = 0

    for date_table in tables_to_query:

        if rdd_list_count == 0:
            current_monthly_table = sc \
            .cassandraTable("wikikeyspace", date_table) \
            .filter(lambda x: "Clinton" in x["page_name"])
            # .map(lambda r: (r["page_name"], r["view_count"]))
            # .where("language=?", "en") \
            current_monthly_table.saveToCassandra("wikikeyspace", "mattquery7temp")
            rdd_list_count += 1
            # .persist()
            # # .where("language=?", "en") \
            # combined_rdd = current_monthly_table#.persist()
            # rdd_list_count += 1
            # print(rdd_list_count)
            # continue
            # .select("language", "page_name", "view_count") \
        else:

        # Query Structure (Total all pages with the word Trump in it over the date range)
            current_monthly_table = sc \
            .cassandraTable("wikikeyspace", date_table) \
            .joinWithCassandraTable("wikikeyspace", "mattquery7temp") \
            .on("page_name") \
            .filter(lambda x: "Clinton" in x["page_name"])
            # .reduceByKey(lambda x,y: x+y) \
            # .map(lambda x: (x["view_count"],x["page_name"])) \
            # .sortByKey(False) \
            # .map(lambda x: (x["page_name"],x["view_count"]))
            # .map(lambda x: (x[1],x[0]))

            current_monthly_table.saveToCassandra("wikikeyspace", "mattquery7temp")

    final_rdd = sc \
    .cassandraTable("wikikeyspace", "mattquery7temp") \
    .select("page_name", "view_count") \
    .reduceByKey(lambda x,y: x+y) \
    .map(lambda x: (x["view_count"],x["page_name"])) \
    .sortByKey(False) \
    .map(lambda x: (x["page_name"],x["view_count"]))

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
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace.mattquery7;\""
subprocess.call(cql_call, shell=True)

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace.mattquery7temp;\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery7temp( language text, page_name text PRIMARY KEY, view_count int );\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery7( page_name text PRIMARY KEY, view_count int );\""
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
    query(sc)
    # join_first_then_query(sc)
