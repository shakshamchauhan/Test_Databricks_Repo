-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Extracting Data Directly From Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query a Single File
-- MAGIC
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/data&#x60;</code></strong>
-- MAGIC
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path.

-- COMMAND ----------

use catalog hive_metastore;

-- COMMAND ----------

SELECT * FROM 
csv.`dbfs:/FileStore/tables/landing/yellowdata`

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables/landing/yellowdata

-- COMMAND ----------

CREATE OR REPLACE VIEW yellowdata_view
AS SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata`

-- COMMAND ----------

SELECT * FROM yellowdata_view

-- COMMAND ----------

show tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW yellowdata_temp_view
AS SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata`

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Detach the notebook and run the above statement again

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW yellowdata_global_temp_view
AS SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata`

-- COMMAND ----------

show tables 

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Detach the notebook and run the above statement again

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Apply CTEs for Reference within a Query 
-- MAGIC Common table expressions (CTEs) are perfect when you want a short-lived, human-readable reference to the results of a query.

-- COMMAND ----------

WITH cte_csv
AS (SELECT * FROM csv.`dbfs:/FileStore/tables/landing/yellowdata`)
SELECT * FROM cte_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CTEs only alias the results of a query while that query is being planned and executed.
-- MAGIC
-- MAGIC As such, **the following cell with throw an error when executed**.

-- COMMAND ----------

--SELECT COUNT(*) FROM cte_csv

-- COMMAND ----------

-- CTE with multiple column aliases
WITH t(x, y) AS (SELECT 1, 2)
  SELECT * FROM t WHERE x = 1 AND y = 2;




-- COMMAND ----------

WITH t AS (
    WITH t2 AS (SELECT 1)
    SELECT * FROM t2)
  SELECT * FROM t;

-- COMMAND ----------

-- CTE in subquery
SELECT max(c) FROM (
    WITH t(c,d) AS (SELECT 1,2)
    SELECT * FROM t);

-- COMMAND ----------

-- CTE in CREATE VIEW statement
CREATE VIEW v AS
    WITH t(a, b, c, d) AS (SELECT 1, 2, 3, 4)
    SELECT * FROM t;
