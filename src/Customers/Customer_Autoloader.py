# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

filename = dbutils.widgets.get('filename')

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

catalog = 'project'
schema = 'source'
# filename = 'customers'

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation", f"/Volumes/project/_ops/autoloader/{filename}/") \
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .option("cloudFiles.maxFilesPerTrigger", 10) \
  .load(f"/Volumes/project/source/src_{filename}/") \
  .withColumn('filename', col("_metadata.file_path")) \
  .withColumn('ingesttime', current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data

# COMMAND ----------

query = (
  df.writeStream
    .option("checkpointLocation", f"/Volumes/project/_ops/autoloader/{filename}")
    .trigger(availableNow=True)
    .outputMode("append")
    .table(f"{catalog}.bronze.{filename}")
)

query.awaitTermination()