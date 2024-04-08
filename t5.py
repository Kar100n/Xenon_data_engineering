from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta import *

Spark = SparkSession.builder.appName("SignalMapping").getOrCreate()
 

# Define the schema for the new DataFrame 

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

Mapping_df = Spark.createDataFrame(Data, Schema) 

 

# Show the DataFrame to verify its content 

Mapping_df.show() 