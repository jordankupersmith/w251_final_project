# python Script for loading into Cassandra

# Ensure all files in the uncompressed folder are actually uncompressed
# Have this script run consistently on each machine (distributed compute) in
# order to ingest all the data in a reasonable amount of time

import os
import subprocess

# Grab file names from the folder (specify the folder)
def get_file_names():
    file_list = []
    for filename in os.listdir("/data/filePrep/processed/2016/uncompressed"):
        file_list.append(filename)
    return file_list

# Set folder where the uncompressed data is stored (used below)
uncompressed_folder = "/data/filePrep/processed/2016/uncompressed/"

# Get file names in current folder
file_names = get_file_names()

# Make sub directories (if not already created)
bash_call = "mkdir "+str(uncompressed_folder)+"success"
subprocess.call(bash_call, shell=True)
bash_call = "mkdir "+str(uncompressed_folder)+"failure"
subprocess.call(bash_call, shell=True)
bash_call = "mkdir "+str(uncompressed_folder)+"errors"
subprocess.call(bash_call, shell=True)


# Create Keyspace if not already done (should already be done)
# cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE KEYSPACE wikikeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };\""
# subprocess.call(cql_call, shell=True)

# Walk through files & Ingest to Cassandra using Cassandra-Loader
# (details for installing are in the cluster_setup_and_node_information.txt file)
for current in file_names:

    # Get Date
    print("Raw File: %s" %current)
    current_date = current[15:24]
    print("Current Date: %s" %current_date)

    # Create Table for this day
    print("Creating Table Name: %s" %("t"+str(current_date)))
    cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE TABLE wikikeyspace.t"+str(current_date)+"( language text, page_name text PRIMARY KEY, view_count int );\""
    subprocess.call(cql_call, shell=True)

    # Copy in rows from this day's uncompressed file
    bash_call = "/root/cassandra-loader/build/cassandra-loader \
    -f "+str(uncompressed_folder)+"aggregateviews-"+str(current_date)+" \
    -host 10.90.61.249 \
    -schema \"wikikeyspace.t"+str(current_date)+"(language, page_name, view_count)\" \
    -delim \" \" \
    -consistencyLevel ONE \
    -numRetries 1 \
    -maxErrors -1 \
    -maxInsertErrors -1 \
    -badDir "+str(uncompressed_folder)+"errors \
    -successDir "+str(uncompressed_folder)+"success \
    -failureDir "+str(uncompressed_folder)+"failure \
    -rateFile "+str(uncompressed_folder)+"rate_file.csv"

    subprocess.call(bash_call, shell=True)
