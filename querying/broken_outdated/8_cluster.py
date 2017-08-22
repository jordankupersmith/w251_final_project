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
    dates_to_query = [20160803, 20160804] # Cluster
    # dates_to_query = [20151202, 20151203] #Standalone
    tables_to_query = ["t"+str(i) for i in dates_to_query]

    rdd_list_count = 0




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
    .setMaster("spark://10.90.61.249:7077")
    # .set("spark.cassandra.connection.host", "10.90.61.249")

    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

    # Execute Main functionality
    # main(sc)

    # Execute Query
    query(sc)
    # join_first_then_query(sc)
