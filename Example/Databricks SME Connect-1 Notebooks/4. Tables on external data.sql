-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Registering Tables on External Data with Read Options
-- MAGIC
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC
-- MAGIC While there are many <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">additional configurations</a> you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Note that options are passed with keys as unquoted text and values in quotes. Spark supports many <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">data sources</a> with custom options, and additional systems may have unofficial support through external <a href="https://docs.databricks.com/libraries/index.html" target="_blank">libraries</a>. 
-- MAGIC
-- MAGIC

-- COMMAND ----------


use catalog hive_metastore;
drop schema if exists nyctaxidb cascade;
Create schema if not exists nyctaxidb;

show databases;


-- COMMAND ----------

use nyctaxidb;

Drop table if exists  yellowTripMay_ext1roopa;


CREATE TABLE yellowTripMay_ext1roopa
USING CSV OPTIONS (
  path = 'dbfs:/FileStore/tables/landing/yellowdata/yellow_tripdata_2019_05.csv',
  header = "true",
  mode = "FAILFAST"
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that no data has moved during table declaration. 
-- MAGIC
-- MAGIC Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location.
-- MAGIC
-- MAGIC Run the following cell to confirm that data is now being loaded correctly.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM yellowtripmay_ext1roopa;

-- COMMAND ----------

describe formatted yellowtripmay_ext1roopa;

-- COMMAND ----------

drop table if exists yellowTrip_csvroopa;

CREATE TABLE yellowTrip_csvroopa
(VendorID integer,tpep_pickup_datetime timestamp,tpep_dropoff_datetime timestamp 
,passenger_count integer, trip_distance double ,RatecodeID integer ,store_and_fwd_flag string 
,PULocationID integer ,DOLocationID integer ,payment_type integer ,fare_amount double ,extra 
double ,mta_tax double ,tip_amount double ,tolls_amount double,improvement_surcharge double 
,total_amount double,congestion_surcharge double)
USING CSV
OPTIONS (
 header = "true",
 delimiter = ","
)
LOCATION "dbfs:/FileStore/tables/landing/yellowdata/"


-- COMMAND ----------

-- MAGIC %md
-- MAGIC All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.
-- MAGIC
-- MAGIC **NOTE**: When working with CSVs as a data source, it's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.
-- MAGIC
-- MAGIC Running **`DESCRIBE EXTENDED`** on a table will show all of the metadata associated with the table definition.

-- COMMAND ----------

SELECT * FROM yellowTrip_csvroopa

-- COMMAND ----------

SELECT count(*) FROM yellowTrip_csvroopa

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limits of Tables with External Data Sources
-- MAGIC
-- MAGIC  Note that whenever we're defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and Lakehouse.
-- MAGIC
-- MAGIC For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Upload one more CSV file onto yellowdata folder location

-- COMMAND ----------

SELECT count(*) FROM yellowTrip_csvroopa

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### No of records is not updated

-- COMMAND ----------

refresh table yellowTrip_csvroopa

-- COMMAND ----------

SELECT count(*) FROM yellowTrip_csvroopa

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables/landing/yellowdata/

-- COMMAND ----------


