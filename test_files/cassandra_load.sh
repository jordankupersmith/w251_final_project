#!/bin/bash

# Cassandra Loading Script

# Set up Keyspace
CREATE KEYSPACE wikikeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };

cqlsh -e "select * from ks.table limit 1;" > ~/output



# run one statement in cqlsh (Create Table)
cqlsh -e "CREATE TABLE 20150501( language text, page_name text PRIMARY KEY, view_count counter );"
cqlsh -e "COPY 20150501 (language, page_name, view_count) FROM aggregateviews-20150501  "

COPY log.chatlogs (ts, content, other) TO './chatlog.dat'
   ... WITH DELIMITER = '|' AND QUOTE = '''' AND ESCAPE = '''' AND NULL = '<null>';







$CASSANDRA_HOME/bin/cqlsh
Set the replication factor:<br>
>CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '4' };<br>

Create a test table:<br>
 >CREATE TABLE planet( catalog int PRIMARY KEY, name text, mass double, density float, albedo float ); <br>

Insert data into the table:<br>

 INSERT INTO planet (catalog, name, mass, density, albedo) VALUES ( 3, 'Earth', 5.9722E24, 5.513, 0.367);

Confirm that replication works by running the following on **each**  node: <br>

$CASSANDRA_HOME/bin/cqlsh -e "SELECT * FROM test.planet;"<br>
