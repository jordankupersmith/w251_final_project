#!/usr/bin/env python
# Run on a cron job because it keeps hanging up after importing all rows. Need to restart the process every 30 mins or so...
# crontab -e
# */30 * * * * /data/filePrep/processed/2015/cassandra_load.py

# To check running processes
# ps ax | grep .py



# python Script for loading into Cassandra
import os
import subprocess

def get_file_names():
    file_list = []
    for filename in os.listdir("/data/filePrep/processed/2015"):
        file_list.append(filename)
    return file_list

# Get file names in current folder
file_names = get_file_names()

# Create Keyspace if not already done
cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE KEYSPACE wikikeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };\""
# os.system(cql_call)
# subprocess.call(["$CASSANDRA_HOME/bin/cqlsh -e","CREATE KEYSPACE wikikeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };"])
subprocess.call(cql_call, shell=True)

# Walk through files

for current in file_names:
    # Example File Name aggregateviews-20150517.gz
    print("Raw File: %s" %current)
    if current[-2:] == 'gz':
        # Unzip the raw file
        print("Unzipping")
        bash_call = "gunzip "+ str(current)
        subprocess.call(bash_call, shell=True)

        current_date = current[-11:-3]
        print("Current Date: %s" %current_date)

        #Delete .gz so it doesn't get unzipped again by a concurrently running process
        # bash_call = "rm -f /data/filePrep/processed/aggregateviews-"+str(current_date)+".gz"
        # subprocess.call(bash_call, shell=True)

        # TO CREATE A TABLE FOR EACH DATE filename
        print("Creating Table Name: %s" %("t"+str(current_date)))

        # Specify keyspace (not necessary if calling wikikeyspace.table in following commands)
        cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"USE wikikeyspace;\""
        subprocess.call(cql_call, shell=True)

        # Create Table
        cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE TABLE wikikeyspace.t"+str(current_date)+"( language text, page_name text PRIMARY KEY, view_count int );\""
        subprocess.call(cql_call, shell=True)

        # Create Index on views and language (Optional or Not Necessary?)
        cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE INDEX ON wikikeyspace.t"+str(current_date)+"(view_count);\""
        subprocess.call(cql_call, shell=True)
        cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"CREATE INDEX ON wikikeyspace.t"+str(current_date)+"(language);\""
        subprocess.call(cql_call, shell=True)

        # Copy in rows from current file
        cql_call = "/opt/apache-cassandra-2.2.10/bin/cqlsh -e \"COPY wikikeyspace.t"+str(current_date)+"(language, page_name, view_count) FROM '/data/filePrep/processed/2015/aggregateviews-"+str(current_date)+"' WITH DELIMITER = ' ';\""
        subprocess.call(cql_call, shell=True)

        # THIS SCRIPT IS HANGING HERE AND DOESN"T PROGRESS???

        # # Delete unzipped files from folder, then continue loop
        # bash_call = "rm -f /data/filePrep/processed/aggregateviews-"+str(current_date)
        # subprocess.call(bash_call, shell=True)


    else:
        print("Skip this File")
        continue
