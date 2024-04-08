from pyspark.sql import SparkSession 
from pyspark.sql.functions import to_json, struct 
Spark = SparkSession.builder.appName("ReadCsv").getOrCreate()
Df = Spark.read.csv("/home/xs459-tejshr/kafka/Xenon_data_engineering/T1.csv",inferSchema =True,header=True) 
Df.show() 
Spark.stop()

Spark = SparkSession.builder \
    .appName("KafkaPublisher") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()
Csv_path = "/home/xs459-tejshr/kafka/Xenon_data_engineering/T1.csv"
Df = Spark.read.option("header", "true").csv(Csv_path)
Df_json = Df.select(to_json(struct(*Df.columns)).alias("value")) 

#Publishing the Records to the Topic

Query = Df_json \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Xenontask") \
    .save()
Query.awaitTermination() 
