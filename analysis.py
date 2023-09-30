import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,io.delta:delta-core_2.12:2.0.0 pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from delta import *

spark = pyspark.sql.SparkSession.builder.appName("deltaANA") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# TASKS
# Task 1: Read data from Delta Lake

df = spark.read.format("delta").load("/home/xs391-sanjha/code/delta")

# Task 2: Calculate number of distinct signal_ts datapoints per day

df_per_day = df.groupBy(df.signal_ts.cast("date")).agg({'signal_ts': 'count'})

df_per_day.show()

# Task 3: Calculate Average value of all the signals per hour

df_avg_per_hour = df.withColumn("Hour", df["Date/Time"].substr(12, 2).cast("int")) \
    .groupBy("Hour").agg({"LVActivePower": "avg", "WindSpeed": "avg", "Theoretical_Power_Curve": "avg", "WindDirection": "avg"})

df_avg_per_hour.show()

# Task 4: Add a column 'generation_indicator'

df = df.withColumn(
    'generation_indicator',
    when((col('LVActivePower') < 200), 'Low')
    .when((col('LVActivePower') >= 200) & (col('LVActivePower') < 600), 'Medium')
    .when((col('LVActivePower') >= 600) & (col('LVActivePower') < 1000), 'High')
    .when((col('LVActivePower') >= 1000), 'Exceptional')
    .otherwise('Unknown')
)
df.show(20,False)

# Task 5: Create a new dataframe with specified JSON

json_data = [
    {"sig_name": "LVActivePower", "sig_mapping_name": "active_power_average"},
    {"sig_name": "WindSpeed", "sig_mapping_name": "wind_speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "theo_power_curve_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "theo_power_curve_average"},
    {"sig_name": "WindDirection", "sig_mapping_name": "wind_direction_average"}
]
json_df = spark.createDataFrame(json_data)
json_df.show()

# Task 6: Change signal name in dataframe from step no 4 with mapping from step no 5 by performing broadcast join.
map_df = df.select(explode(df.signals).alias("sig_name", "value"))
map_df.show(10,False)

final_df = map_df.join(
    broadcast(json_df),
    map_df['sig_name'] == json_df['sig_name']
 ).drop(json_df['sig_name']).drop(map_df['sig_name'])
 
final_df.show(10, False)
