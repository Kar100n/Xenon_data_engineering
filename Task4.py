from pyspark.sql.functions import *
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("generation_indicator_column") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_table_delta = (spark
    .read.format("delta")
    .load("/home/xs459-tejshr/kafka/Xenon_data_engineering/Delta_table/")
)
exploded_df = df_table_delta.select(explode("signals").alias("signal_name", "signal_value") 

) 

# Filtering ‘LV ActivePower (kW)’ rows and applying conditional logic using filter
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
Generation_indicator_df.show()