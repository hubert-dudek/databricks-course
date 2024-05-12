# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user").split('@')[0]
stateless_checkpoint = f"/tmp/cp_sl_{username}"
stateful_checkpoint = f"/tmp/cp_sf_{username}"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Stateless streaming state

# COMMAND ----------

display(spark.read.format("state-metadata").load(stateless_checkpoint))
