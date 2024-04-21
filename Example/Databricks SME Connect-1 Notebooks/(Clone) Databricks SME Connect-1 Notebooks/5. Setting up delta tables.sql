-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query.
-- MAGIC

-- COMMAND ----------


use catalog hive_metastore;
Create schema if not exists nyctaxidb;

show databases;


-- COMMAND ----------

CREATE OR REPLACE TABLE yellowtrips AS
SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata/yellow_tripdata_2019_05.csv`;

-- COMMAND ----------

DESCRIBE EXTENDED yellowtrips;

-- COMMAND ----------

select * from yellowtrips;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC  
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.
-- MAGIC
-- MAGIC CTAS statements also do not support specifying additional file options.
-- MAGIC
-- MAGIC

-- COMMAND ----------


CREATE OR REPLACE TABLE yellowtrips_unparsed AS
SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata/`;

SELECT * FROM yellowtrips_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC To correctly ingest this data to a Delta Lake table, we'll need to use a reference to the files that allows us to specify options.
-- MAGIC
-- MAGIC In the previous lesson, we showed doing this by registering an external table. Here, we'll slightly evolve this syntax to specify the options to a temporary view, and then use this temp view as the source for a CTAS statement to successfully register the Delta table.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW yellowtrips_tmp_vw 
(VendorID integer,tpep_pickup_datetime timestamp,tpep_dropoff_datetime timestamp 
,passenger_count integer, trip_distance double ,RatecodeID integer ,store_and_fwd_flag string 
,PULocationID integer ,DOLocationID integer ,payment_type integer ,fare_amount double ,extra 
double ,mta_tax double ,tip_amount double ,tolls_amount double,improvement_surcharge double 
,total_amount double,congestion_surcharge double)
USING CSV
OPTIONS (
  path = "dbfs:/FileStore/tables/landing/yellowdata/",
  header = "true",
  delimiter = ","
);


drop table if exists yellowtrips_delta;
CREATE TABLE yellowtrips_delta AS
  SELECT * FROM yellowtrips_tmp_vw;
  
SELECT * FROM yellowtrips_delta

-- COMMAND ----------

DESCRIBE EXTENDED yellowtrips_delta;

-- COMMAND ----------

SELECT count(*) FROM yellowtrips_delta
