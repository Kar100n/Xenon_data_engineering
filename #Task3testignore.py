# Create the Spark Session
from sqlite3 import Timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, DecimalType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import  col
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date


spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

spark

streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Xenon-task") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
StructField('Date/Time', TimestampType(), True), \
StructField('LV ActivePower (kW)', DecimalType(38,10), True), \
StructField('Wind Speed (m/s)', DecimalType(38,10), True), \
StructField('Theoretical_Power_Curve (KWh)', DecimalType(38,10), True), \
StructField('Wind Direction (°)', DecimalType(38,10), True)])

json_df = streaming_df.selectExpr("cast(value as string) as value")

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

exploded_df = json_expanded_df \
    .select(to_date(col("Date/Time"), "yyyy-MM-dd").alias("signal_date"),"LV ActivePower (kW)","Wind Speed (m/s)","Theoretical_Power_Curve (KWh)","Wind Direction (°)",current_date().alias("create_date"),current_timestamp().alias("creare_ts"),to_timestamp(col("Date/Time"), "yyyy-MM-dd HH mm ss SSS").alias("signal_ts")) \
    .withColumn("signals" , create_map(lit("LV_ActivePower"),col("LV ActivePower (kW)"),
        lit("Wind_Speed"),col("Wind Speed (m/s)"),
        lit("Theoretical_Power_Curve"),col("Theoretical_Power_Curve (KWh)"),
        lit("Wind_Direction"),col("Wind Direction (°)"),
        )) \
    .drop("data")


writing_df = json_expanded_df.writeStream \
    .format("console") \
    .option("checkpointLocation","checkpoint_dir") \
    .outputMode("Update") \
    .start()
    
writing_df.awaitTermination()