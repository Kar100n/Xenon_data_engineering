# Create the Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


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
    .option("subscribe", "Xenontask") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

json_schema = StructType([
StructField('Date/Time', StringType(), True), \
StructField('LV ActivePower (kW)', StringType(), True), \
StructField('Wind Speed (m/s)', StringType(), True), \
StructField('Theoretical_Power_Curve (KWh)', StringType(), True), \
StructField('Wind Direction (°)', StringType(), True)])

json_df = streaming_df.selectExpr("cast(value as string) as value")

json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*") 

exploded_df = json_expanded_df \
    .select("Date/Time","LV ActivePower (kW)","Wind Speed (m/s)","Theoretical_Power_Curve (KWh)","Wind Direction (°)") \
    .drop("data")

writing_df = exploded_df.writeStream \
    .format("console") \
    .option("checkpointLocation","checkpoint_dir") \
    .outputMode("Update") \
    .start()   
writing_df.awaitTermination()