from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherETL") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Define input and output paths
api_data_path = "s3://weather-etl-data-st0263/raw_data/api_data/"
mysql_data_path = "s3://weather-etl-data-st0263/raw_data/mysql_data/"
output_path = "s3://weather-etl-data-st0263/trusted/"

# Define schema for weather data
weather_schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("name", StringType(), True)
])

# Read all weather CSV files
weather_df = spark.read \
    .option("header", True) \
    .schema(weather_schema) \
    .csv(api_data_path)

# Group weather data by city and calculate daily statistics
daily_weather = weather_df \
    .withColumn("date", F.date_format("time", "yyyy-MM-dd")) \
    .groupBy("name", "date") \
    .agg(
        F.avg("temperature_2m").alias("avg_temperature"),
        F.min("temperature_2m").alias("min_temperature"),
        F.max("temperature_2m").alias("max_temperature")
    )

# Read MySQL data
cities_df = spark.read.option("header", True).csv(f"{mysql_data_path}/cities.csv")
consumption_df = spark.read.option("header", True).csv(f"{mysql_data_path}/city_consumption.csv")
population_df = spark.read.option("header", True).csv(f"{mysql_data_path}/population.csv")
regions_df = spark.read.option("header", True).csv(f"{mysql_data_path}/regions.csv")
city_region_df = spark.read.option("header", True).csv(f"{mysql_data_path}/city_region.csv")

# Join MySQL data including regions
city_info = cities_df \
    .join(consumption_df, cities_df.id == consumption_df.city_id) \
    .join(population_df, cities_df.id == population_df.city_id) \
    .join(city_region_df, cities_df.id == city_region_df.city_id) \
    .join(regions_df, city_region_df.region_id == regions_df.id) \
    .select(
        cities_df.name,
        cities_df.latitude,
        cities_df.longitude,
        consumption_df.water_m3,
        consumption_df.electricity_kwh,
        population_df.population,
        consumption_df.year,
        regions_df.region_name.alias("country")
    )

# Join weather data with city information
final_df = daily_weather \
    .join(city_info, daily_weather.name == city_info.name) \
    .select(
        daily_weather.name.alias("city"),
        daily_weather.date,
        "avg_temperature",
        "min_temperature",
        "max_temperature",
        "latitude",
        "longitude",
        "water_m3",
        "electricity_kwh",
        "population",
        "country",
        city_info.year
    )

# Write the final dataset to the trusted zone
final_df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .option("header", True) \
    .csv(output_path)

print("ETL process completed successfully. Data saved to trusted zone.")
