import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,io.delta:delta-core_2.12:2.0.0 pyspark-shell'
from pyspark.sql import SparkSession
import pyspark
from delta import *

spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

df = spark.read.format("delta").load("/home/xs391-sanjha/Task1.data.module(kafka&spark)/delta")
df.show()