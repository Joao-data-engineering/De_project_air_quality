from pyspark.sql import SparkSession,Row
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, IntegerType,FloatType
from pyspark.sql.functions import udf, col, explode, from_json, concat, lit
from extraction_functions import *
import os 

# Initialize a SparkSession
spark =  SparkSession.builder \
    .appName('my_app') \
    .master("local[*]")\
    .getOrCreate()

# Load the CSV file
cities_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/pt.csv")
    

cities_df_filtered = cities_df.select("city","lat","lng")

api_key = os.environ.get('key')

start = "1606488670"
end = "1687247268"


cities_df_url = cities_df_filtered.withColumn("url",concat(lit("http://api.openweathermap.org/data/2.5/air_pollution/history?lat="),col("lat"),lit("&lon="),col("lng"),lit("&start="),lit(start),lit("&end="),lit(end),lit("&appid="),lit(api_key)))


request_df = cities_df_url.withColumn("verb",lit("get"))

udf_executeRestApi = udf(executeRestApi) 

request_df_json = request_df \
      .withColumn("result", udf_executeRestApi(col("verb"), col("url")))
    
      
# Parse the JSON string into DataFrame
# Define your schema to match your data
schema = StructType([
    StructField("coord", StructType([
        StructField("lon", FloatType()),
        StructField("lat", FloatType())
    ])),
    StructField("list", ArrayType(StructType([
        StructField("main", StructType([
            StructField("aqi", IntegerType())
        ])),
        StructField("components", StructType([
            StructField("co", DoubleType()),
            StructField("no", DoubleType()),
            StructField("no2", DoubleType()),
            StructField("o3", DoubleType()),
            StructField("so2", DoubleType()),
            StructField("pm2_5", DoubleType()),
            StructField("pm10", DoubleType()),
            StructField("nh3", DoubleType())
        ])),
        StructField("dt", IntegerType())
    ])))
])

df_json = request_df_json \
    .withColumn("result", from_json(col("result"), schema)) 
    
          
# Use explode on the "result.list" column
df_json_exploded = df_json.select("verb","url",explode(df_json.result.list).alias("list"), "result.coord.*")


# Now "list" is a struct column. Use selectExpr to further flatten the DataFrame:
df_json_flat = df_json_exploded.selectExpr("lon", "lat", "list.main.*", "list.components.*", "list.dt")






