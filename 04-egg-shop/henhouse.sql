-- Databricks notebook source
USE egg_shop;

-- COMMAND ----------

-- DBTITLE 1,Database Schema for Storing Different Egg Types
-- Table: egg_types
CREATE TABLE egg_types (
    egg_type_id INT PRIMARY KEY,
    egg_type_name VARCHAR(50) NOT NULL COMMENT 'Name of the egg type',
    description VARCHAR(255) COMMENT 'Description of the egg type'
) COMMENT 'Stores information about different egg types.';

-- Sample Data
INSERT INTO egg_types (egg_type_id, egg_type_name, description) VALUES
(1, 'Chicken Egg', 'Standard chicken eggs.'),
(2, 'Duck Egg', 'Larger and richer than chicken eggs.'),
(3, 'Quail Egg', 'Small and delicate eggs.');


-- COMMAND ----------

-- DBTITLE 1,Creating Inventory Table and Inserting Sample Data
-- Table: inventory
CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    quantity INT NOT NULL COMMENT 'Current quantity of eggs in inventory',
    last_updated DATE NOT NULL COMMENT 'Date when the inventory was last updated',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Tracks the current inventory of eggs.';

-- Sample Data
INSERT INTO inventory (inventory_id, egg_type_id, quantity, last_updated) VALUES
(1, 1, 500, '2023-10-01'),
(2, 2, 300, '2023-10-01'),
(3, 3, 200, '2023-10-01');


-- COMMAND ----------

-- DBTITLE 1,SQL Table for Forecasted Egg Production Data
-- Table: forecast
CREATE TABLE forecast (
    forecast_id INT PRIMARY KEY,
    forecast_date DATE NOT NULL COMMENT 'Date for the forecasted production',
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    forecast_quantity INT NOT NULL COMMENT 'Forecasted quantity of eggs to be produced',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Contains forecasted egg production per day.';

-- Sample Data
INSERT INTO forecast (forecast_id, forecast_date, egg_type_id, forecast_quantity) VALUES
(1, '2023-10-02', 1, 550),
(2, '2023-10-02', 2, 320),
(3, '2023-10-02', 3, 210),
(4, '2023-10-03', 1, 560),
(5, '2023-10-03', 2, 330),
(6, '2023-10-03', 3, 220);


-- COMMAND ----------

-- DBTITLE 1,SQL Table Creation and Sample Data for Orders
-- Table: orders
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    order_date DATE NOT NULL COMMENT 'Date when the order was placed',
    delivery_date DATE NOT NULL COMMENT 'Date when the order is to be delivered',
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    quantity INT NOT NULL COMMENT 'Quantity of eggs ordered',
    customer_name VARCHAR(100) COMMENT 'Name of the customer',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Records customer orders for egg delivery.';

-- Sample Data
INSERT INTO orders (order_id, order_date, delivery_date, egg_type_id, quantity, customer_name) VALUES
(1, '2023-10-01', '2023-10-02', 1, 100, 'Alice Smith'),
(2, '2023-10-01', '2023-10-02', 2, 80, 'Bob Johnson'),
(3, '2023-10-02', '2023-10-03', 1, 150, 'Carol Williams');


-- COMMAND ----------

-- DBTITLE 1,SQL Function to Fetch Available Egg Quantity
-- Function: get_available_eggs
CREATE OR REPLACE FUNCTION get_available_eggs(egg_name STRING)
RETURNS INT
LANGUAGE SQL
COMMENT 'Returns the current available quantity of eggs for the specified egg type.'
AS
$$
/*
Parameters:
  egg_name (STRING): The name of the egg type (not case-sensitive).
    - The function uses ai_similarity to match the most similar egg type name.
Returns:
  INT: The available quantity of the specified egg type in the inventory.
*/
WITH matched_egg AS (
    SELECT
        et.egg_type_id
    FROM
        egg_types et
    ORDER BY
        ai_similarity(egg_name, et.egg_type_name) DESC
    LIMIT 1
)
SELECT
    COALESCE(i.quantity, 0) AS available_quantity
FROM
    matched_egg me
    LEFT JOIN inventory i ON me.egg_type_id = i.egg_type_id;
$$;


-- COMMAND ----------

-- DBTITLE 1,Function to Calculate Forecasted Egg Availability
-- Function: get_forecasted_availability
CREATE OR REPLACE FUNCTION get_forecasted_availability(egg_name STRING, forecast_date DATE)
RETURNS INT
LANGUAGE SQL
COMMENT 'Calculates the forecasted availability of eggs for a given egg type on a specific date.'
AS
$$
/*
Parameters:
  egg_name (STRING): The name of the egg type (not case-sensitive).
    - The function uses ai_similarity to match the most similar egg type name.
  forecast_date (DATE): The date for which to calculate the forecasted availability.
Returns:
  INT: The forecasted available quantity of the specified egg type on the given date.
*/
WITH matched_egg AS (
    SELECT
        et.egg_type_id
    FROM
        egg_types et
    ORDER BY
        ai_similarity(egg_name, et.egg_type_name) DESC
    LIMIT 1
),
current_inventory AS (
    SELECT
        i.quantity
    FROM
        inventory i
        JOIN matched_egg me ON i.egg_type_id = me.egg_type_id
),
forecasted_production AS (
    SELECT
        f.forecast_quantity
    FROM
        forecast f
        JOIN matched_egg me ON f.egg_type_id = me.egg_type_id
    WHERE
        f.forecast_date = forecast_date
),
scheduled_orders AS (
    SELECT
        COALESCE(SUM(o.quantity), 0) AS total_orders
        FROM
            orders o
        JOIN matched_egg me ON o.egg_type_id = me.egg_type_id
    WHERE
        o.delivery_date = forecast_date
)
SELECT
    (COALESCE(ci.quantity, 0) + COALESCE(fp.forecast_quantity, 0)) - so.total_orders AS forecasted_availability
FROM
    current_inventory ci
    CROSS JOIN forecasted_production fp
    CROSS JOIN scheduled_orders so;
$$;

