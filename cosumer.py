import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType, TimestampType
from pyspark.sql.functions import *

# Set the necessary packages and configurations
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,io.delta:delta-core_2.12:2.0.0 pyspark-shell'

spark = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# Define the schema for the incoming data

schema = StructType([
    StructField("create_date", DateType(), True),
    StructField("create_ts", TimestampType(), True),
    StructField("Date/Time",StringType(), True),
    StructField("LVActivePower", StringType(), True),
    StructField("WindSpeed", StringType(), True),
    StructField("Theoretical_Power_Curve", StringType(), True),
    StructField("WindDirection", StringType(), True),
])

# Read data from Kafka topic

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test1234") \
  .option("startingOffsets", "earliest") \
  .load()

# Convert the value column (which contains JSON) to structured DataFrame

df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json("value", schema).alias("data")) \
  .select("data.*")

# Add missing fields (create_date and create_ts)

df = df.withColumn("create_date", current_date()) \
       .withColumn("create_ts", current_timestamp())\
       .withColumn("signal_date", to_date(df["Date/Time"], 'dd MM yyyy HH:mm')) \
       .withColumn("signal_ts", to_timestamp(df["Date/Time"], 'dd MM yyyy HH:mm'))

# showing the updated data 

df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start().awaitTermination()

# Define a function to create the signals map

def create_signals_map(lv_active_power, wind_speed, theoretical_power_curve, wind_direction):
    return create_map(
        lit("LVActivePower"), lv_active_power,
        lit("WindSpeed"), wind_speed,
        lit("Theoretical_Power_Curve"), theoretical_power_curve,
        lit("WindDirection"), wind_direction
    )

# Apply the function to create the signals map

df = df.withColumn("signals", create_signals_map(
                        df["LVActivePower"],
                        df["WindSpeed"],
                        df["Theoretical_Power_Curve"],
                        df["WindDirection"]
                    ))

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint") \
    .option("mergeSchema", "true")\
    .start("delta") \
    .awaitTermination()
