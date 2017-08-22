# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages \TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=127.0.0.1 /root/queries/8.py

query_name = 'mattquery10'
text_filter = "Thrones"


def query(sc):

    # dates_to_query = [20160803, 20160804] # Cluster
    dates_to_query = [20151201, 20151202, 20151203, 20151204, 20151205] #Standalone
    tables_to_query = ["t"+str(i) for i in dates_to_query]

    for date_table in tables_to_query:

        current_monthly_table = sc \
        .cassandraTable("wikikeyspace", date_table) \
        .select("language", "page_name", "view_count") \
        .filter(lambda x: text_filter in x["page_name"])

        current_monthly_table.saveToCassandra("wikikeyspace", query_name+"temp")



    temp2_rdd = sc \
    .cassandraTable("wikikeyspace", query_name+"temp") \
    .select("page_name", "view_count") \
    .map(lambda x: (x["page_name"], x["view_count"])) \
    .reduceByKey(lambda x,y: x + y)

    temp2_rdd.saveToCassandra("wikikeyspace", query_name+"temp2")

    final_rdd = sc \
    .cassandraTable("wikikeyspace", query_name+"temp2") \
    .select("view_count", "page_name") \
    .map(lambda x: (x["view_count"], x["page_name"])) \
    .sortByKey(False) \
    .saveToCassandra("wikikeyspace", query_name)

    try: # SUPER IMPORTANT
        current_monthly_table.unpersist()
        temp2_rdd.unpersist()
        final_rdd.unpersist()
    except:
        pass

## Imports
from pyspark import SparkConf
# from pyspark.sql import HiveContext
# from pyspark.sql.types import *
import pyspark_cassandra

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

import subprocess

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace."+query_name+";\""
subprocess.call(cql_call, shell=True)

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace."+query_name+"temp;\""
subprocess.call(cql_call, shell=True)

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace."+query_name+"temp2;\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace."+query_name+"temp( language text, page_name text PRIMARY KEY, view_count int );\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace."+query_name+"temp2( page_name text PRIMARY KEY, view_count int );\""
subprocess.call(cql_call, shell=True)

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace."+query_name+"( view_count int PRIMARY KEY, page_name text );\""
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

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace."+query_name+"temp;\""
subprocess.call(cql_call, shell=True)

# Drop Table in Cassandra if exists
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"DROP TABLE wikikeyspace."+query_name+"temp2;\""
subprocess.call(cql_call, shell=True)
