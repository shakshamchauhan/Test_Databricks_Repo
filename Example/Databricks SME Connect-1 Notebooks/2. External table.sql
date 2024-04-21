-- Databricks notebook source

use catalog hive_metastore;

-- COMMAND ----------


drop schema if exists roopa_nyctaxidb cascade;
create schema if not exists roopa_nyctaxidb;

-- COMMAND ----------


describe schema extended roopa_nyctaxidb;

-- COMMAND ----------

use roopa_nyctaxidb;
drop table if exists external_taxizones;
create or replace table external_taxizones(LocationID int,Borough string,Zone string,service_zone string) location "dbfs:/FileStore/roopa/exttaxizones";
insert into external_taxizones values(1, "EWR","Newark Airport","EWR"),(2,"Queens","Jamaica Bay","Boro Zone");

-- COMMAND ----------

select * from external_taxizones;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/roopa/exttaxizones")
-- MAGIC display(files)

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe detail external_taxizones;

-- COMMAND ----------

describe extended external_taxizones;

-- COMMAND ----------

describe history external_taxizones;

-- COMMAND ----------

drop table external_taxizones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/FileStore/roopa/exttaxizones")
-- MAGIC display(files)
