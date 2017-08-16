# Spark Query

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######

# $SPARK_HOME/bin/spark-submit \
	# --packages TargetHolding/pyspark-cassandra:0.3.5 \
	# --conf spark.cassandra.connection.host=wiki1,wiki2,wiki3,wiki4 \
    # /spark_query.py


def query_first_then_join(sc):

    # Select Dates
    dates_to_query = [20150519, 20150604]
    tables_to_query = ["t"+str(i) for i in dates_to_query]

    rdd_list = []
    for date_table in tables_to_query:


    # Query Structure (Total all pages with the word Trump in it over the date range)
        current_monthly_table = sc \
    	.cassandraTable("wikikeyspace", date_table) \
    	.select("page_name", "view_count") \
    	.where("language=?", "en") \
    	.filter(lambda r: r["page_name"].contains("Trump")) \
    	# .map(lambda r: (r["page_name"], "view_count")
    	# .reduceByKey(lambda a, b: a + b)
    	.collect()

        rdd_list.append(current_monthly_table)

    # Join (Regular RDD joins)
    # Joining Two Tables
    combined_rdd = rdd_list[0]
    for rdds in rdd_list[1:]:
        combined_rdd.join(rdds).mapValues(lambda x: x[0] + (x[1], ))

    # Map and Reduce by Key (page name)
    combined_rdd \
    .map(lambda r: (r["page_name"], "view_count") \
    .reduceByKey(lambda a, b: a + b)


    # Save to Cassandra (nested dictionaries create the structure of the cassandra table)
    combined_rdd.saveToCassandra(
        "wikikeyspace",
        "mattquery1",
        ttl=timedelta(hours=1),
    )


def join_first_then_query(sc): #Careful with this! Could quickly exceed Spark available Memory

    dates_to_query = [20150601, 20150602, 20150603]
    tables_to_query = ["t"+str(i) for i in dates_to_query]


    # Query Structure
    monthly_table = sc \
	.cassandraTable("wikikeyspace", "table") \
	.select("col-a", "col-b") \
	.where("key=?", "x") \
	.filter(lambda r: r["col-b"].contains("foo")) \
	.map(lambda r: (r["col-a"], 1)
	.reduceByKey(lambda a, b: a + b)
	.collect()


    # Save to Cassandra (nested dictionaries create the structure of the cassandra table)
    rdd = sc.parallelize([{
        "key": k,
        "stamp": datetime.now(),
        "val": random() * 10,
        "tags": ["a", "b", "c"],
        "options": {
            "foo": "bar",
            "baz": "qux",
        }
    } for k in ["x", "y", "z"]])

    rdd.saveToCassandra(
        "keyspace",
        "table",
        ttl=timedelta(hours=1),
    )


def main(sc):
    pass


## Imports
from pyspark import SparkConf
# from pyspark.sql import HiveContext
# from pyspark.sql.types import *
import pyspark_cassandra

## Module Constants
APP_NAME = "pySpark & Cassandra Query"

## Main functionality
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf() \
        .setAppName(APP_NAME) \
    	.setMaster("spark://spark-master:7077") \
    	.set("spark.cassandra.connection.host", "cas-1")

    sc = CassandraSparkContext(conf=conf)

    # Execute Main functionality
    main(sc)

    # Execute Query
    query_first_then_join(sc)
    # join_first_then_query(sc)
