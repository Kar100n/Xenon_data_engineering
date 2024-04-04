from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col
from pyspark.sql.functions import when, col
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType 
import pyspark
from pyspark.sql import SparkSession 

from delta import *

builder = pyspark.sql.SparkSession.builder.appName("Broadcast") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_table_delta = (spark
    .read.format("delta")
    .load("/home/xs459-tejshr/kafka/Xenon_data_engineering/Delta_table/")
)
exploded_df = df_table_delta.select(explode("signals").alias("signal_name", "signal_value") 

) 

# Filter or identify ‘LV ActivePower (kW)’ rows and apply conditional logic 
LV_ActivePower_df = exploded_df.filter(col("signal_name") == "LV_ActivePower")
Generation_indicator_df = LV_ActivePower_df.withColumn("generation_indicator",

    when( 

        (col("signal_name") == "LV_ActivePower") & (col("signal_value").cast("float") < 200), "Low")
        .when( 

        (col("signal_name") == "LV_ActivePower") & (col("signal_value").cast("float") >= 200) & (col("signal_value").cast("float") < 600), "Medium"
    ).when( 

        (col("signal_name") == "LV_ActivePower") & (col("signal_value").cast("float") >= 600) & (col("signal_value").cast("float") < 1000), "High"

    ).when( 

        (col("signal_name") == "LV_ActivePower") & (col("signal_value").cast("float") >= 1000), "Exceptional"
    )
)

Schema = StructType([ 

    StructField("sig_name", StringType(), True), 

    StructField("sig_mapping_name", StringType(), True) 

]) 

 

# Define the data according to your JSON structure 

Data = [ 

    {"sig_name": "LV ActivePower (kW)", "sig_mapping_name": "active_power_average"}, 

    {"sig_name": "Wind Speed (m/s)", "sig_mapping_name": "wind_speed_average"}, 

    {"sig_name": "Theoretical_Power_Curve (KWh)", "sig_mapping_name": "theo_power_curve_average"}, 

    {"sig_name": "Wind Direction (°)", "sig_mapping_name": "wind_direction_average"} 

] 

 

# Create the DataFrame 
Spark = SparkSession.builder.appName("SignalMapping").getOrCreate()
Mapping_df = Spark.createDataFrame(Data, Schema) 

df_joined = Generation_indicator_df.join(broadcast(Mapping_df),
                                              Generation_indicator_df["signal_name"] == Mapping_df["sig_name"],
                                              #Generation_indicator_df["Wind Speed (m/s"] == Mapping_df["sig_name"],
                                              #Generation_indicator_df["Theoretical_Power_Curve (KWh)"] == Mapping_df["sig_name"],
                                             # Generation_indicator_df["Wind Direction (°)"] == Mapping_df["sig_name"],
                                              "left")

# Assuming you want to replace the signal names for all relevant columns, you would iterate over each mapping
# This example only shows the mechanism for one signal; you'd replicate this logic as needed

# For demonstration purposes, let's select a couple of columns to illustrate the transformation
df_final = df_joined.withColumnRenamed("sig_mapping_name", "new_signal_name")

df_final.select("signal_value", "new_signal_name").show()
