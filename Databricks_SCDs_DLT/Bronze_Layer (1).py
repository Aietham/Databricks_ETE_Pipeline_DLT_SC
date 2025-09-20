# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Capabilities

# COMMAND ----------

dbutils.widgets.text('filename','')

# COMMAND ----------

filename = dbutils.widgets.get('filename')

# COMMAND ----------

catalog = 'labuser11612924_1758377596'
schema = 'source'

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/_ops/autoloader/_schema/{filename}") \
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .option("cloudFiles.maxFilesPerTrigger", 10) \
  .load(f"/Volumes/{catalog}/source/{filename}") \
  .withColumn('filename', input_file_name()) \
  .withColumn('ingesttime', current_timestamp())


# COMMAND ----------

f"/Volumes/{catalog}/_ops/autoloader/_schema/{filename}"

# COMMAND ----------

f'/Volumes/{catalog}/source/{filename}'


# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data

# COMMAND ----------

query = (
  df.writeStream
    .option("checkpointLocation", f"/Volumes/{catalog}/_ops/autoloader/_checkpoints/{filename}")
    .trigger(availableNow=True) 
    .table(f"{catalog}.bronze.{filename}")
)

query.awaitTermination()

# COMMAND ----------

# df.writeStream \
#   .format("parquet") \
#   .outputMode("append") \
#   .option("checkpointLocation", f"/Volumes/project/_ops/autoloader/_checkpoints/{filename}") \
#   .option("path", f"/Volumes/project/bronze/raw_{filename}/data") \
#   .trigger(once=True) \
#   .start()

# COMMAND ----------

df.display()

# COMMAND ----------

x
