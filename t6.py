from pyspark.sql import *
from pyspark.sql.functions import *
from delta import *
import pyspark
from pyspark.sql.types import StructType, StructField, StringType 
builder = pyspark.sql.SparkSession.builder.appName("SignalMapping") \
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

    {"sig_name": "LV ActivePower (kW)", "sig_mapping_name": "active_power_average"}, 

    {"sig_name": "Wind Speed (m/s)", "sig_mapping_name": "wind_speed_average"}, 

    {"sig_name": "Theoretical_Power_Curve (KWh)", "sig_mapping_name": "theo_power_curve_average"}, 

    {"sig_name": "Wind Direction (°)", "sig_mapping_name": "wind_direction_average"} 

] 

mapping_df = spark.createDataFrame(data, schema) 

from pyspark.sql.functions import broadcast 

 

# Perform the broadcast join 

joined_df = exploded_df.join(broadcast(mapping_df), exploded_df.signal_name == mapping_df.sig_name, "left_outer") 

 

# Optionally, select the columns of interest and/or rename as necessary 

result_df = joined_df.select( 

    "hour", 

    "signal_name", 

    "signal_value", 

    "sig_mapping_name"  # This holds the descriptive name for the signal 

) 

 

result_df.show() 