# Databricks notebook source


# COMMAND ----------

# dbutils.notebook.run("1", 0)
# dbutils.notebook.run("2", 0)

# COMMAND ----------

# MAGIC %pip show simplejson

# COMMAND ----------

# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service import jobs
 
# w = WorkspaceClient()

# # w.jobs.submit(
# #     run_name=f"sdk-123",
# #     tasks=[
# #         jobs.SubmitTask(
# #             notebook_task=jobs.NotebookTask(notebook_path=f"/Users/{w.current_user.me().user_name}/course/03-run-all/2"),
# #             task_key=f"sdk-123",
# #         )
# #     ],
# # )

# COMMAND ----------


import os
from databricks.sdk.service import jobs

print(jobs.BaseRun.run_id)

# COMMAND ----------

all_configs = spark.sparkContext.getConf().getAll()

# Display all configurations
for config in all_configs:
    print(config)

# COMMAND ----------

print(spark.conf.get("spark.databricks.job.runId", None))

# COMMAND ----------


# nb_context = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
# nb_params = dbutils.notebook.entry_point.getCurrentBindings()

if os.environ["IS_SERVERLESS"] == 'TRUE':
   print(os.environ["DB_CLUSTER_ID"])
   print(os.environ["USER"])



# print(nb_context["tags"]["jobId"])


# print(nb_context["currentRunId"])

# print(nb_context["tags"]["jobRunId"])

# print(nb_context["extraContext"]["notebook_path"])

# print(nb_context["tags"]["user"])

# print(nb_context["tags"]["clusterId"])
