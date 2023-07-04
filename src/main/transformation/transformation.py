from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# Initialize a SparkSession
spark =  SparkSession.builder \
    .appName('my_app') \
    .master("local[*]")\
    .getOrCreate()
    
all_data_df = spark.read.format("csv")\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/main_cities_data.csv")
    
final_df = all_data_df.withColumn("datetime",F.from_unixtime(F.col("dt"))).select('lon', 'lat', 'aqi', 'co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'datetime')

final_df.show() 

