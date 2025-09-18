# Databricks notebook source
# dbutils.task.getVlaues("parameters","datasets")
# dbutils.task.getVlaues("parameters","layers")


# COMMAND ----------

catalog = 'labuser11612924_1758234044'

# COMMAND ----------



# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.source")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.source.customers")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.source.orders")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.source.products")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}._ops")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}._ops.autoloader")
