from pyspark.sql import SparkSession 
import pyspark
from delta import *
from pyspark.sql.functions import *

builder = pyspark.sql.SparkSession.builder.appName("AvgSignalsPerHour") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_table_delta = (spark
    .read.format("delta")
    .load("/home/xs459-tejshr/kafka/Xenon_data_engineering/Delta_table/")
)
# Explode the 'signals' MapType column 

exploded_df = df_table_delta.select( 

    hour("signal_ts").alias("hour"), 

    explode("signals").alias("signal_name", "signal_value") 

) 
exploded_df = exploded_df.withColumn("signal_value", exploded_df["signal_value"].cast("float")) 
avg_signals_per_hour = exploded_df.groupBy("hour", "signal_name").agg( 

    avg("signal_value").alias("avg_signal_value") 

) 
avg_signals_per_hour.show() 