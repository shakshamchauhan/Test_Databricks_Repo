-- Databricks notebook source

use catalog hive_metastore;

-- COMMAND ----------


drop schema if exists roopa_nyctaxidb cascade;
create schema if not exists roopa_nyctaxidb;

-- COMMAND ----------


describe schema extended roopa_nyctaxidb;

-- COMMAND ----------

use roopa_nyctaxidb;
create or replace table managed_taxizones(LocationID int,Borough string,Zone string,service_zone string);
insert into managed_taxizones values(1, "EWR","Newark Airport","EWR"),(2,"Queens","Jamaica Bay","Boro Zone");

-- COMMAND ----------

select * from managed_taxizones;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/user/hive/warehouse/roopa_nyctaxidb.db/managed_taxizones")
-- MAGIC display(files)

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe detail managed_taxizones;

-- COMMAND ----------

describe extended managed_taxizones;

-- COMMAND ----------

describe history managed_taxizones;

-- COMMAND ----------

drop table managed_taxizones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/user/hive/warehouse/roopa_nyctaxidb.db/managed_taxizones")
-- MAGIC display(files)
