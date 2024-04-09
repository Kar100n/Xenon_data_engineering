from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Initialize Spark session
spark = SparkSession.builder \
        .appName("KafkaProducer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .getOrCreate()
#csv
df = spark.read.option("header", "true").csv("/home/xs459-tejshr/Desktop/Kafka_xenontask/T1.csv")

# Specify the format "dd MM yyyy HH:mm" to match the input data
df_with_corrected_timestamp = df.withColumn("Date/Time", 
    from_unixtime(unix_timestamp(col("Date/Time"), "dd MM yyyy HH:mm"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

# Add additional fields as per requirements
df_processed = df_with_corrected_timestamp \
    .withColumn("signal_date", col("Date/Time").cast("date")) \
    .withColumn("signal_ts", col("Date/Time")) \
    .withColumn("create_date", current_date()) \
    .withColumn("create_ts", current_timestamp()) \
    .withColumn("signals" ,create_map(lit("LV_ActivePower"),col("LV ActivePower (kW)"),
        lit("Wind_Speed"),col("Wind Speed (m/s)"),
        lit("Theoretical_Power_Curve"),col("Theoretical_Power_Curve (KWh)"),
        lit("Wind_Direction"),col("Wind Direction (°)"),
        )
    )

# Serialize the DataFrame to JSON strings
df_json = df_processed.selectExpr("CAST(null AS STRING) as key", "to_json(struct(*)) AS value")

# Since we have already configured Kafka, publish the records
df_json.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Xenontask") \
    .save()