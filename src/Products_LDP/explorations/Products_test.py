# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

last5_df = (
    spark.table("project.bronze.bronze_products")
         .orderBy(F.col("product_id").desc())  # or F.col("ingesttime").desc()
         .limit(5)
)

# 2️⃣ Make some sample changes
#    Example: bump price by 10%, append " (Updated)" to product_name
updates_df = (
    last5_df
    .withColumn("price", F.round(F.col("price") * 1.10, 2))  # +10% price
    .withColumn("product_name", F.concat(F.col("product_name"), F.lit(" (Updated)")))
    # give a NEW ingesttime so AUTO CDC treats these as newer versions
    .withColumn("ingesttime", F.current_timestamp())
)


# COMMAND ----------

df = updates_df.drop("_rescued_data", "filename","ingesttime")

# COMMAND ----------

df.display()

# COMMAND ----------

df.repartition(1).write.format('parquet').mode('append').save('/Volumes/project/source/src_products/')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project.silver.products_stage where product_id in ('P0490','P0489','P0488','P0487','P0486') order by product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project.silver.products_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from project.silver.products_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY project.silver.products_stage

# COMMAND ----------

