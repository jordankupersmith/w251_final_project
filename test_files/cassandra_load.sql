-- Functional CQL calls




COPY 20150501 (language, page_name, view_count) FROM aggregateviews-20150501



-- Create Keyspace
CREATE KEYSPACE wikikeyspace1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '2' };

-- TO CREATE A TABLE FOR EACH DATE filename

-- Specify keyspace
USE wikikeyspace1;

-- Create Table
CREATE TABLE t20150501( language text, page_name text PRIMARY KEY, view_count int  );

-- Create index on all columns
 CREATE INDEX ON t20150501(view_count);
 CREATE INDEX ON t20150501(language);

-- Copy in rows from File
COPY t20150501 (language, page_name, view_count) FROM '/data/filePrep/processed/aggregateviews-20150501' WITH DELIMITER = ' ';

-- Create index on all columns
 CREATE INDEX ON t20150501(view_count);

-- Try to Query
SELECT * FROM t20150501 WHERE page_name CONTAINS 'Chris' LIMIT 10;



-- TO COMBINE ALL DATES INTO A SINGLE TABLE (DOES NOT WORK FOR QUERYING A NESTED INDEX)

-- Create Entry Type
CREATE TYPE wikikeyspace1.entry_type (language text, page_name text, view_count int );

-- Specify keyspace
USE wikikeyspace1;
-- Create Table
CREATE TABLE wikiviews1( recorded_date timestamp PRIMARY KEY, entry_list list<frozen <entry_type>> );

-- For each row in the original file, Insert Into List
INSERT INTO wikiviews1(recorded_date, entry_list) VALUES ('2015-05-01',
[('en', 'test_page_1', 3)]);
UPDATE wikiviews1 SET entry_list = entry_list +[('en', 'test_page_2', 4)] WHERE recorded_date = '2015-05-01';
-- INSERT INTO wikiviews1(recorded_date, entry_list) VALUES ('2015-05-01',
-- [('en', 'test_page_2', 4)]);
INSERT INTO wikiviews1(recorded_date, entry_list) VALUES ('2015-05-02',
[('en', 'test_page_2', 2)]);
UPDATE wikiviews1 SET entry_list = entry_list +[('en', 'test_page_4', 10)] WHERE recorded_date = '2015-05-02';
-- Try to Query Nested Data
SELECT * FROM wikiviews1 WHERE entry_list CONTAINS 'test_page_2';
-- Failed
SELECT * FROM wikiviews1;

-- To Remove things:
DROP TYPE entry_type;
DROP TABLE wikiviews1;


-- Example
-- cqlsh> USE ks;
-- cqlsh:ks> CREATE TABLE data_points (
--             id text PRIMARY KEY,
--             created_at timestamp,
--             previous_event_id varchar,
--             properties map<text,text>
--          );
-- cqlsh:ks> create index on data_points (properties);
-- cqlsh:ks> INSERT INTO data_points (id, properties) VALUES ('1', { 'fruit' : 'apple', 'band' : 'Beatles' });
-- cqlsh:ks> INSERT INTO data_points (id, properties) VALUES ('2', { 'fruit' : 'cherry', 'band' : 'Beatles' });
-- cqlsh:ks> SELECT * FROM data_points WHERE properties CONTAINS 'Beatles';
--
--  id | created_at | previous_event_id | properties
-- ----+------------+-------------------+----------------------------------------
--   2 |       null |              null | {'band': 'Beatles', 'fruit': 'cherry'}
--   1 |       null |              null |  {'band': 'Beatles', 'fruit': 'apple'}
