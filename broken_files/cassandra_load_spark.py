# Load into Cassandra via Spark

# Cassandra RDD Operations & Pyspark Connector HERE
# https://github.com/TargetHolding/pyspark-cassandra

#### This should be how you run this script ######
# $SPARK_HOME/bin/spark-submit --packages TargetHolding/pyspark-cassandra:0.3.5 --conf spark.cassandra.connection.host=10.90.61.249 /data/filePrep/processed/2015/cassandra_load_spark.py

## Imports
import os
import subprocess
from pyspark import SparkConf
import numpy as np
import pyspark_cassandra

## Module Constants
APP_NAME = "Cassandra Data Loader via Spark"

# Folder where raw files exists
folder = "/data/filePrep/processed/2015"

# Configure Spark
conf = SparkConf() \
.setAppName(APP_NAME) \
.setMaster("spark://10.90.61.249:7077")
# .set("spark.cassandra.connection.host", "10.90.61.249") # Not needed, done in script call
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)

# # Create Table in Cassandra to Store Results
# cql_call = "$CASSANDRA_HOME/bin/cqlsh -e \"CREATE TABLE wikikeyspace.mattquery2( page_name text PRIMARY KEY, view_count int );\""
# subprocess.call(cql_call, shell=True)


def get_file_names():
    file_list = []
    for filename in os.listdir(folder):
        file_list.append(filename)
    return file_list

def load_file_to_cassandra_table(sc, table_date, data):

    # rdd = sc.parallelize([{
	# "language": k[0],
	# "page_name": k[1],
	# "view_count": k[2]
    # } for k in data_in])

    import_counter = 0
    report_counter = 0
    dict_in = {}
    for k in data_in:
        if len(k) != 3:
            print("Skipping Line, does not contain 3 fields")
            continue
        try:
            dict_in = {
        	"language": str(k[0]),
        	"page_name": str(k[1]),
        	"view_count": int(k[2])
            }
            import_counter += 1
            if import_counter % 50000 == 0:
                report_counter += 1
                print("Lines Imported: %i" %report_counter*50000)
        except:
            print("Skipping Line, something went wrong")
            continue

    rdd = sc.parallelize([dict_in])

    # Create Table in Cassandra to Store Results
    cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE TABLE wikikeyspace.t"+str(table_date)+"( language text, page_name text PRIMARY KEY, view_count int );\""
    subprocess.call(cql_call, shell=True)

    print("Beginning Save to Cassandra")
    rdd.saveToCassandra("wikikeyspace", "t"+str(table_date))
    print("Finished Saving Table %s to Cassandra" %"t"+str(table_date))


# Get file names in current folder
file_names = get_file_names()

# Create Keyspace if not already done
cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE KEYSPACE wikikeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };\""
subprocess.call(cql_call, shell=True)

# Walk through files
for current in file_names:
    # Example File Name aggregateviews-20150517.gz
    print("Raw File: %s" %current)
    # Grab Date (assume already uncompressed)
    current_date = current[15:24]
    current_file = current

    # Check if the file is in fact compressed and if so, uncompress it & redefine names
    if current[-2:] == 'gz':
        # Unzip the raw file
        print("Unzipping")
        bash_call = "gunzip "+ str(current)
        subprocess.call(bash_call, shell=True)
        current_date = current[-11:-3]
        current_file = current[:-3]

    if current_file[:9] != 'aggregate':
        print("File Skipped because it is not the correct format")
        continue

    print("Current Date: %s" %current_date)
    print("Current File Name: %s" %current_file)


    # Grab unzipped file, skip if it doesn't exist
    # try:
    file_location = str(folder)+"/"+str(current_file)
    print("File Location: %s" %file_location)
    dt = np.dtype([('U', 'U', 'u')])
    data_in = np.fromfile(file_location, dtype=dt, sep=" ")
    print(data_in[:10])
    load_file_to_cassandra_table(sc, current_date, data_in)
    # except:
    #     print("File Skipped for some reason")
