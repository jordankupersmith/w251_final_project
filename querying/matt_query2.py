# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=10.90.61.249 /data/spark_queries/matt_query2.py

###THis is a sample quiery in cql
### SELECT "page_name", "view_count" FROM "wikikeyspace"."t20150916" WHERE token("page_name") > ? AND token("page_name") <= ? AND language=?   ALLOW FILTERING: Predicates on non-primary-key columns (language) are not yet supported for non secondary index queries) [duplicate 17]


# Takes a while to run, but you can pull up the results in CASSANDRA_HOME/bin/cqlsh
# use wikikeyspace;
# select * from mattquery2 limit 50;
# Couldn't get Order By to work...

#  page_name                                                 | view_count
# -----------------------------------------------------------+------------
#                                     File:Trumpsumoscar.jpg |          1
#                          Jean_Barker,_Baroness_Trumpington |         96
#                      Feast_of_Trumpets_(Christian_holiday) |          2
#                                              Trumpet_plant |          2
#                                                 Trump_coup |          6
#                                                   Trumped! |          6
#                                                  Trump_Ice |         12
#                                               Trumpet_Vine |          1
#                                         Top_Trumps_Console |          1
#                   File:Trump_Toronto_entrance_at_night.JPG |          1
#                                      Category:Trump_family |          4
#                                                First_Trump |          2
#                                      A_Trumpet_in_the_Wadi |          2
#                                               Trump_family |         10
#                                     USS_Trumpeter_(DE-180) |          1
#                                              Four_No-Trump |          1
#                                          Talk:Ivanka_Trump |          1
#                                      Top_Trumps_Adventures |          3
#                                        HMS_Trumpeter_(D09) |         13
#                                          Trump_Park_Avenue |         19
#                                            Johnny_No-Trump |          5
#                                        Donald_Trump_(song) |         44
#                                              Trump_chicago |         19
#                                        Altenburg_Trumpeter |          5
#                                        Trumpet/Maintenance |          1
#           File:Trump_Tower_as_seen_from_the_Chicago_El.jpg |          1
#                                       Talk:Trump_promotion |          1
#  Hebrew_Roots/Holy_Days/Trumpets/Preparation_for_the_Feast |          1
#                                Top_Trumps_Adventures_(Wii) |          3
#                                            Katie_Trumpener |          4
#                                   Malaysian_Trumpet_Snails |          3
#                                               Trump_(card) |          1
#                                          Bokhara_Trumpeter |         17
#            Trump_International_Hotel_&_Tower_(New_Orleans) |          6
#                                       Philadelph_Van_Trump |          4
#           File:Donald_Trump_hair_from_above_and_behind.jpg |          4
#                                    List_of_Jazz_Trumpeters |          1
#                                              Trump_(cards) |          8
#                                 Philemon_Beecher_Van_Trump |          1
#                                                Trump_virus |          2
#                                      Pale-winged_Trumpeter |          1
#                                                  Jay_Trump |          1


def query_first_then_join(sc):

    # Select Dates
    dates_to_query = [20150916]
    #dates_to_query =[20150915, 20160122, 20170426]
    tables_to_query = ["t"+str(i) for i in dates_to_query]
    
    
    d = {}
    rdd_list = []
    
    #for  name in tables_to_query:
        #d[name] =
    
    for i, date_table in enumerate(tables_to_query):

        #globals()['string%s' % x]
        # Query Structure (Total all pages with the word Trump in it over the date range)
        
        #current_monthly_table = sc \
        ##this "globals" command creates a RDD name based on the order in the loop. The idea is to create an RDD for each date and then join them at the end after the loop
        #globals()['current_monthly_table%s' % i] = sc \
        current_monthly_table = sc \
        .cassandraTable("wikikeyspace", date_table) \
        .select("page_name", "view_count") \
        .filter(lambda r: "Obama" in r["page_name"]) 
        
        d[date_table]=current_monthly_table
        #.where("language=?", "en") \
        
        # .map(lambda r: (r["page_name"], "view_count") \ #mapping and reducing changes current_monthly_table from an RDD, preventing saving to a cassandra table?
        # .reduceByKey(lambda a, b: a + b)
        # .collect()

        # rdd_list.append(current_monthly_table)

        # Join (Regular RDD joins)
        # Joining Two Tables
        # combined_rdd = rdd_list[0]
        # for rdds in rdd_list[1:]:
        #     combined_rdd.join(rdds).mapValues(lambda x: x[0] + (x[1], ))

        # Map and Reduce by Key (page name)
        # current_monthly_table \
        # .map(lambda r: (r["page_name"], "view_count") \
        # .reduceByKey(lambda a, b: a + b)
        # .collect()

        # print(type(current_monthly_table))

        # Save to Cassandra (nested dictionaries create the structure of the cassandra table)
        ####this makes a list of an RDD so we can join them after the loop.
        #add_to_list='current_monthly_table%s' % i
        add_to_list=date_table
        rdd_list.append(add_to_list)
    
    for name, df in d.iteritems():
        to_cassandra=df
    print(rdd_list)
    #current_monthly_table0.count()   ###testing here only to make sure all RDDs were created.
    #current_monthly_table1.count() ###testing here only to make sure all RDDs were created.
    #current_monthly_table2.count() ###testing here only to make sure all RDDs were created.
    
    
###This sc.union command didn't seem to be working, but maybe it was only due to not enough free memory?    
    #to_cassandra = sc.union(rdd_list)
    to_cassandra.saveToCassandra("wikikeyspace", "mattquery2")


# def join_first_then_query(sc): #Careful with this! Could quickly exceed Spark available Memory
#
#     dates_to_query = [20150601, 20150602, 20150603]
#     tables_to_query = ["t"+str(i) for i in dates_to_query]
#
#
#     # Query Structure
#     monthly_table = sc \
#     .cassandraTable("wikikeyspace", "table") \
#     .select("col-a", "col-b") \
#     .where("key=?", "x") \
#     .filter(lambda r: r["col-b"].contains("foo")) \
#     .map(lambda r: (r["col-a"], 1)
#     .reduceByKey(lambda a, b: a + b)
#     .collect()
#
#
#     # Save to Cassandra (nested dictionaries create the structure of the cassandra table)
#     rdd = sc.parallelize([{
#     "key": k,
#     "stamp": datetime.now(),
#     "val": random() * 10,
#     "tags": ["a", "b", "c"],
#     "options": {
#     "foo": "bar",
#     "baz": "qux",
#     }
#     } for k in ["x", "y", "z"]])
#
#     rdd.saveToCassandra(
#     "keyspace",
#     "table",
#     ttl=timedelta(hours=1),
#     )
#
#
# def main(sc):
#     pass


## Imports
from pyspark import SparkConf
# from pyspark.sql import HiveContext
# from pyspark.sql.types import *
import pyspark_cassandra

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

import subprocess

# Create Table in Cassandra to Store Results
cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery2( page_name text PRIMARY KEY, view_count int );\""
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
    query_first_then_join(sc)
    # join_first_then_query(sc)
