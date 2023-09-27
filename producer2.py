import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 pyspark-shell'


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

#creating schema for producer

schema = StructType([
    StructField("Date/Time", StringType(), True),
    StructField("LVActivePower", DoubleType(), True),
    StructField("WindSpeed", DoubleType(), True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("WindDirection", DoubleType(), True),
])
#reading data from csv

df = spark.read.option("header", "true").schema(schema).csv("/home/xs391-sanjha/code/T2.csv")
df.show()

df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("topic","test1234").save()

