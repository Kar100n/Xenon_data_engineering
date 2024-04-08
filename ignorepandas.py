from pyspark.sql import SparkSession 
import pyspark
from delta import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType 
from pyspark.sql.functions import explode, hour, avg, col 

builder = pyspark.sql.SparkSession.builder.appName("JOINBROADCAST") \
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
schema = StructType([ 

    StructField("sig_name", StringType(), True), 

    StructField("sig_mapping_name", StringType(), True) 

]) 

data = [ 

    {"sig_name": "LV_ActivePower", "sig_mapping_name": "active_power_average"}, 

    {"sig_name": "Wind_Speed", "sig_mapping_name": "wind_speed_average"}, 

    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "theo_power_curve_average"}, 

    {"sig_name": "Wind_Direction", "sig_mapping_name": "wind_direction_average"} 

] 

mapping_df = spark.createDataFrame(data, schema)
exploded_df = exploded_df.withColumn("signal_value", exploded_df["signal_value"].cast("float")) 
avg_signals_per_hour = exploded_df.groupBy("hour", "signal_name").agg( 

    avg("signal_value").alias("avg_signal_value") 

) 
Joined_avg_df = avg_signals_per_hour.join(broadcast(mapping_df), avg_signals_per_hour.signal_name == mapping_df.sig_name, "left_outer") 

 

# Select and potentially rename columns as needed 

Final_avg_df = Joined_avg_df.select( 

    "hour", 

    col("sig_mapping_name").alias("descriptive_signal_name"), 

    "avg_signal_value" 

) 
Final_avg_df.show()