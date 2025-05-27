from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, min, max

spark = SparkSession.builder.appName("WeatherETL_Merged").getOrCreate()

api_path = "s3://weather-etl-data-st0263/raw_data/api_data/"
mysql_path = "s3://weather-etl-data-st0263/raw_data/mysql_data/cities.csv"
output_path = "s3://weather-etl-data-st0263/trusted_data/weather_summary/"

weather_df = spark.read.option("header", True).csv(api_path)
weather_df = weather_df.withColumn("temperature_2m", col("temperature_2m").cast("float"))
weather_df = weather_df.withColumn("date", to_date(col("time")))

cities_df = spark.read.option("header", True).csv(mysql_path)

merged_df = weather_df.join(cities_df, weather_df.name == cities_df.name, "inner")

summary_df = merged_df.groupBy("name", "date").agg(
    min("temperature_2m").alias("temp_min"),
    max("temperature_2m").alias("temp_max"),
    avg("temperature_2m").alias("temp_avg"),
    col("latitude"),
    col("longitude")
)

summary_df.write.mode("overwrite").option("header", True).csv(output_path)

print("ETL completado: combinación API + MySQL guardada en zona trusted.")
