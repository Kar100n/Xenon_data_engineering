import pyspark
from delta import *
from pyspark.sql.functions import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df_table_delta = (spark
    .read.format("delta")
    .load("/home/xs459-tejshr/kafka/Xenon_data_engineering/Delta_table/")
)
df_with_date = df_table_delta.withColumn('signal_date', date_trunc('day', col('signal_ts'))) 
df_distinct_per_day = df_with_date.groupBy(col("signal_date")).agg(countDistinct("signal_ts").alias("Distinct"))
df_table_delta.show()
df_distinct_per_day.show() 