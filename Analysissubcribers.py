from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, MapType

# Defininig the schema that matches the structure sent by the producer
schema = StructType([
    StructField("signal_date", DateType()),
    StructField("signal_ts", TimestampType()),
    StructField("create_date", DateType()),
    StructField("create_ts", TimestampType()),
    StructField("signals", MapType(StringType(), StringType()))
])

# Initializing Spark session for Subscriber
spark = SparkSession.builder.appName("KafkaSubscriberDelta").getOrCreate()

# Reading from Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "Xenontask") \
    .load()

# Deserialize the JSON data and selecting fields 
df_deserialized = df_kafka.selectExpr("cast(value as string) as value")
from pyspark.sql.functions import from_json

json_expanded_df = df_deserialized.withColumn("value", from_json(df_deserialized["value"],schema)).select("value.*") 

from pyspark.sql.functions import *
exploded_df = json_expanded_df \
    .select("signal_date","signal_ts","create_date","create_ts","signals") \
    .drop("data")
'''
query_console = exploded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() 
query_console.awaitTermination() 
'''
# Write the stream to a Delta table
query = exploded_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/home/xs459-tejshr/kafka/Xenon_data_engineering/checkpoint") \
    .outputMode("append") \
    .start("/home/xs459-tejshr/kafka/Xenon_data_engineering/Delta_table")
query.awaitTermination()