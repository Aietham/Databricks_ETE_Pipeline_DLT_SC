from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Autoloader to load the files Incrementally

@dp.table
def bronze_products():
    df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation","/Volumes/project/_ops/autoloader/products") \
    .load("/Volumes/project/source/src_products/") \
    .withColumn("filename",col("_metadata.file_path")) \
    .withColumn("ingesttime",current_timestamp())

    df.writeStream \
    .option("checkpointLocation", "/Volumes/project/_ops/autoloader/products") \
    .trigger(availableNow=True)

    return df

dp.create_streaming_table(
    name="project.silver.products_stage",
    schema="""
      product_id   STRING,
      product_name   STRING,
      category       STRING,
      brand          STRING,
      price          DOUBLE,
      ingesttime    TIMESTAMP,
      __START_AT     TIMESTAMP,
      __END_AT       TIMESTAMP
    """
)

dp.apply_changes(
    target="project.silver.products_stage",
    source="bronze_products",
    keys=["product_id"],
    sequence_by=col("ingesttime"),
    except_column_list=["_rescued_data","filename"],
    track_history_except_column_list=["ingesttime"],
    stored_as_scd_type="2"
)






