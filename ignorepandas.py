from pyspark.sql import SparkSession 
from pyspark.sql.functions import to_json, struct 
Spark = SparkSession.builder.appName("PublisherApp").getOrCreate()
import pandas as pd

 

# Define a custom date parser function for the 'Date/Time' column format 
from datetime import datetime
date_parser = lambda x: datetime.strptime(x, '%d %m %Y %H:%M') 
df1 = pd.read_csv('/home/xs459-tejshr/kafka/Xenon_data_engineering/T1.csv', parse_dates=['Date/Time'], date_parser=date_parser) 
#Df = Spark.read.csv("/home/xs459-tejshr/kafka/Xenon_data_engineering/T1.csv",inferSchema =True,header=True) 
Df = Spark.createDataFrame(df1)
Df.show     
Spark.stop()


from pyspark.sql import SparkSession 
Spark = SparkSession.builder \
    .appName("KafkaPublisher") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()
#Csv_path = "/home/xs459-tejshr/kafka/Xenon_data_engineering/T1.csv"
Df = Spark.createDataFrame(df1)
from pyspark.sql.functions import to_json, struct 
Df_json = Df.select(to_json(struct(*Df.columns)).alias("value")) 
Query = Df_json \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Xenon-task") \
    .save()
Query.awaitTermination() 
