-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![](https://raw.githubusercontent.com/hubert-dudek/databricks-course/main/02-Schema_evolution/tinyint.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![Alt](https://raw.githubusercontent.com/hubert-dudek/databricks-course/main/02-Schema_evolution/schema.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 

-- COMMAND ----------

CREATE TABLE main.course.schema_volution
(
  id BYTE -- ByteType / tinyint: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127
)

-- COMMAND ----------

DESCRIBE TABLE main.course.schema_volution;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### integer overflow

-- COMMAND ----------

INSERT INTO main.course.schema_volution (id) VALUES (100000)

-- COMMAND ----------

ALTER TABLE main.course.schema_volution SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')

-- COMMAND ----------

-- ALTER TABLE main.course.schema_volution ALTER COLUMN id TYPE INT

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- COMMAND ----------

INSERT INTO main.course.schema_volution (id) VALUES (100000);

-- COMMAND ----------

DESCRIBE TABLE main.course.schema_volution;

-- COMMAND ----------

INSERT INTO main.course.schema_volution (id) VALUES (10000000000);

-- COMMAND ----------

DESCRIBE TABLE main.course.schema_volution;
