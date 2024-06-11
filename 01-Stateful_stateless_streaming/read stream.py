# Databricks notebook source
username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .tags()
    .apply("user")
    .split("@")[0]
)
stateless_checkpoint = f"/tmp/cp_sl_{username}"
dbutils.fs.rm(stateless_checkpoint, True)
stateful_checkpoint_duplicates = f"/tmp/cp_sf_{username}_dp"
dbutils.fs.rm(stateful_checkpoint_duplicates, True)
stateful_checkpoint_scores = f"/tmp/cp_sf_{username}_sc"
dbutils.fs.rm(stateful_checkpoint_scores, True)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "my-events.servicebus.windows.net:9093")
    .option("subscribe", "my_events")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{dbutils.secrets.get("secrets", "event-hub-connection-string")}";')
    .load())

stream_df = df.selectExpr("CAST(value AS STRING) as json",
                          "json:event_id AS event_id",
                          "json:user AS user",
                          "json:score AS score",
                          "offset",
                          "timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Stateless streaming
# MAGIC
# MAGIC ![image](https://raw.githubusercontent.com/hubert-dudek/databricks-course/main/XX-Stateful_stateless_streaming/images/stateless.png)

# COMMAND ----------

query = (
    stream_df.writeStream.option("checkpointLocation", stateless_checkpoint)
    .format("memory")
    .queryName("stateless_stream")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we check what stream is generating
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   stateless_stream;

# COMMAND ----------

# we check state
display(spark.read.format("statestore").load(stateless_checkpoint))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Stateful streaming
# MAGIC Stateful stream remember the state. It remembers previous records in specified time called watermark. It remembers only needed data so for example if we check for duplicated id field, it saves only id field values.
# MAGIC ![image](https://raw.githubusercontent.com/hubert-dudek/databricks-course/main/XX-Stateful_stateless_streaming/images/stateful.png)

# COMMAND ----------

query = (
    stream_df.withWatermark("timestamp", "10 minutes")
    .dropDuplicates(["event_id"])
    .writeStream.option("checkpointLocation", stateful_checkpoint_duplicates)
    .format("memory")
    .queryName("stateful_stream")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we check what stream is generating
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   stateful_stream;

# COMMAND ----------

# we check state
display(spark.read.format("statestore").load(stateful_checkpoint_duplicates))

# COMMAND ----------

query = (
    stream_df.withWatermark("timestamp", "10 minutes")
    .groupBy(F.window("timestamp", "10 minutes"), "user")
    .agg(F.sum("score").alias("total_score"))
    .writeStream
    .option("checkpointLocation", stateful_checkpoint_scores)
    .outputMode("complete")
    .format("memory")
    .queryName("stateful_stream_scores")
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- check what stream is generating
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   stateful_stream_scores;

# COMMAND ----------

# check state
display(spark.read.format("statestore").load(stateful_checkpoint_scores))

# COMMAND ----------

## use rocks DB
spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set(
  "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
