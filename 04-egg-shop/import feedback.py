# Databricks notebook source
# MAGIC %pip install -U -qqqq databricks-agents
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks import agents

agents.enable_trace_reviews(
  model_name="main.egg_shop.egg_chat",
  request_ids=[
      "1bb31754-ea91-49d7-92bf-0a0ae21adbf4",
      "ed7a1bcd-9112-4220-a6a0-d733fddeeea6"
  ],
)
