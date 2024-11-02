-- Databricks notebook source
USE main.egg_shop;

-- COMMAND ----------

-- DBTITLE 1,Database Schema for Storing Different Egg Types
-- Table: egg_types
CREATE OR REPLACE TABLE main.egg_shop.egg_types (
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
CREATE OR REPLACE TABLE inventory (
    inventory_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    quantity INT NOT NULL COMMENT 'Current quantity of eggs in inventory',
    last_updated DATE NOT NULL COMMENT 'Date when the inventory was last updated',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Tracks the current inventory of eggs.';

INSERT INTO inventory (egg_type_id, quantity, last_updated) VALUES
(1, 500, current_date()),
(2, 300, current_date()),
(3, 200, current_date());

SELECT * FROM inventory;

-- COMMAND ----------

-- DBTITLE 1,SQL Table for Forecasted Egg Production Data
-- Table: forecast
CREATE OR REPLACE TABLE forecast (
    forecast_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    forecast_date DATE NOT NULL COMMENT 'Date for the forecasted production',
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    forecast_quantity INT NOT NULL COMMENT 'Forecasted quantity of eggs to be produced',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Contains forecasted egg production per day.';

-- Sample Data
INSERT INTO forecast (forecast_date, egg_type_id, forecast_quantity) VALUES
(current_date() + 1, 1, 20),
(current_date() + 1, 2, 10),
(current_date() + 1, 3, 5),
(current_date() + 2, 1, 20),
(current_date() + 2, 2, 10),
(current_date() + 2, 3, 5),
(current_date() + 3, 1, 20),
(current_date() + 3, 2, 10),
(current_date() +3, 3, 5);

SELECT * FROM forecast;

-- COMMAND ----------

-- DBTITLE 1,SQL Table Creation and Sample Data for Orders
-- Table: orders
CREATE OR REPLACE TABLE orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_date DATE NOT NULL COMMENT 'Date when the order was placed',
    delivery_date DATE NOT NULL COMMENT 'Date when the order is to be delivered',
    egg_type_id INT NOT NULL COMMENT 'Foreign key referencing egg_types table',
    quantity INT NOT NULL COMMENT 'Quantity of eggs ordered',
    customer_name VARCHAR(100) COMMENT 'Name of the customer',
    FOREIGN KEY (egg_type_id) REFERENCES egg_types(egg_type_id)
) COMMENT 'Records customer orders for egg delivery.';

-- Sample Data
INSERT INTO orders (order_date, delivery_date, egg_type_id, quantity, customer_name) VALUES
(current_date(), current_date() + 1, 1, 100, 'Alice Smith'),
(current_date(), current_date() + 2, 2, 80, 'Bob Johnson'),
(current_date(), current_date() + 3, 1, 150, 'Carol Williams');

SELECT * FROM orders;

-- COMMAND ----------

-- DBTITLE 1,SQL Function to Fetch Available Egg Quantity
-- Function: get_available_eggs
CREATE
OR REPLACE FUNCTION get_available_eggs(
  egg_name_param STRING
  COMMENT "The name of the egg type (not case-sensitive)"
) RETURNS TABLE LANGUAGE SQL
COMMENT 'Returns the current available quantity of eggs for the specified egg type.
Parameters:
  egg_name_param (STRING): The name of the egg type (not case-sensitive).
    - The function uses ai_similarity to match the most similar egg type name.
Returns:
  INT: The available quantity of the specified egg type in the inventory.' 
RETURN (
  WITH matched_egg AS (
    SELECT
      et.egg_type_name AS egg_name,
      et.egg_type_id
    FROM
      egg_types et
    WHERE
      ai_similarity(egg_name_param, et.egg_type_name) > 0.85
  ),
  -- in case no eggs returned
  all_egs AS (
    SELECT
        et.egg_type_name,
        et.egg_type_id
    FROM
      egg_types et
    WHERE (SELECT count(*) FROM matched_egg) = 0
  ),
  eggs AS (
    SELECT * FROM matched_egg
    UNION
    SELECT * FROM all_egs
  )
  SELECT
    egg_name,
    COALESCE(i.quantity, 0) AS available_quantity
  FROM
    eggs me
    LEFT JOIN inventory i ON me.egg_type_id = i.egg_type_id
);

-- COMMAND ----------

SELECT * FROM get_available_eggs('')

-- COMMAND ----------

-- DBTITLE 1,Function to Calculate Forecasted Egg Availability
-- Function: get_forecasted_availability
CREATE
OR REPLACE FUNCTION get_forecasted_availability(
  egg_name STRING
  COMMENT " The name of the egg type (not case-sensitive)",
  forecast_date_param DATE
  COMMENT "The date for which to calculate the forecasted availability"
) RETURNS TABLE LANGUAGE SQL
COMMENT 'Calculates the forecasted availability of eggs for a given egg type on a specific date.
Parameters:
  egg_name (STRING): The name of the egg type (not case-sensitive).
    - The function uses ai_similarity to match the most similar egg type name.
  forecast_date (DATE): The date for which to calculate the forecasted availability.
Returns:
  INT: The forecasted available quantity of the specified egg type on the given date.' 
RETURN WITH matched_egg AS (
    SELECT
      et.egg_type_name AS egg_name,
      et.egg_type_id
    FROM
      egg_types et
    WHERE
      ai_similarity(egg_name, et.egg_type_name) > 0.85
  ), 
current_inventory AS (
  SELECT
    me.egg_type_id,
    i.quantity
  FROM
    inventory i
    JOIN matched_egg me ON i.egg_type_id = me.egg_type_id
),
forecasted_production AS (
  SELECT
    f.forecast_date,
    me.egg_type_id,
    f.forecast_quantity
  FROM
    forecast f
    JOIN matched_egg me ON f.egg_type_id = me.egg_type_id
  WHERE
    f.forecast_date = forecast_date_param
),
scheduled_orders AS (
  SELECT
    me.egg_type_id,
    COALESCE(SUM(o.quantity), 0) AS total_orders
  FROM
    orders o
    JOIN matched_egg me ON o.egg_type_id = me.egg_type_id
  WHERE
    o.delivery_date = forecast_date_param
  GROUP BY me.egg_type_id
)
SELECT
  fp.forecast_date,
  me.egg_name,
  (
    COALESCE(ci.quantity, 0) + COALESCE(fp.forecast_quantity, 0)
  ) - so.total_orders AS forecasted_availability
FROM
  current_inventory ci
JOIN forecasted_production fp ON ci.egg_type_id = fp.egg_type_id
JOIN scheduled_orders so ON fp.egg_type_id = so.egg_type_id
JOIN matched_egg me ON ci.egg_type_id = me.egg_type_id;

-- COMMAND ----------

SELECT * FROM get_forecasted_availability('chicken egg', '2024-11-02')

-- COMMAND ----------

-- Function: perform_order
CREATE OR REPLACE FUNCTION perform_order(
    egg_name STRING COMMENT ' egg_name (STRING): The name of the egg type (not case-sensitive).',
    delivery_date DATE COMMENT 'delivery_date (DATE): The date when the order is to be delivered.',
    order_quantity INT COMMENT  'order_quantity (INT): The quantity of eggs to order.',
    customer_name STRING COMMENT 'customer_name (STRING): The name of the customer placing the order.'
) RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Sends an order as JSON to an external URL using the requests library.
Parameters:
  egg_name (STRING): The name of the egg type (not case-sensitive).
  delivery_date (DATE): The date when the order is to be delivered.
  order_quantity (INT): The quantity of eggs to order.
  customer_name (STRING): The name of the customer placing the order.
Returns:
  STRING: A success message if the order is placed, or an error message if not.'
  AS $$
    import requests, datetime
    url = "https://eoflv2dix60jk99.m.pipedream.net"
    if isinstance(delivery_date, datetime.date):
        delivery_date = delivery_date.isoformat()
    payload = {
        "egg_name": egg_name,
        "delivery_date": delivery_date,
        "order_quantity": order_quantity,
        "customer_name": customer_name
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()
    return f"Order sent successfully: {response.text}"

  $$






-- COMMAND ----------

SELECT perform_order('Duck', '2023-10-03', 50, 'David Miller')
