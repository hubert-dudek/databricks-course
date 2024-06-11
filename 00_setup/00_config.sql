-- Databricks notebook source
CREATE SCHEMA main.course;

-- COMMAND ----------

DROP TABLE main.course.transactions_source

-- COMMAND ----------

CREATE TABLE main.course.transactions_source (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    timestamp TIMESTAMP,
    transaction_date DATE GENERATED ALWAYS AS (CAST(timestamp AS DATE)),
    user_id INT,
    product_id INT,
    value_usd DECIMAL(10, 2),
    additional_column STRING
);

-- COMMAND ----------

DESCRIBE EXTENDED main.course.transactions_source

-- COMMAND ----------

--
INSERT INTO main.course.transactions_source BY NAME
SELECT 
    timestampadd(HOUR, - RAND() * 20000 * 3, current_timestamp()) AS timestamp,
    CAST(RAND() * 10000 AS INT) AS user_id,
    CAST(RAND() * 500 AS INT) AS product_id,
    ROUND(RAND() * 1000, 2) AS value_usd,
    substring('ABCDEFGHIJKLMNOPQRSTUVWXYZ', CAST((rand() * 25) + 1 AS INT), 10) AS additional_column
FROM
    (SELECT explode(sequence(1, 8000000)) AS id) AS n;

-- COMMAND ----------

OPTIMIZE main.course.transactions_source
