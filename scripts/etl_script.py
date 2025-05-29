from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherETL") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

try:
    # Define input and output paths
    api_data_path = "s3://weather-etl-data-st0263/raw_data/api_data/"
    mysql_data_path = "s3://weather-etl-data-st0263/raw_data/mysql_data/"
    output_path = "s3://weather-etl-data-st0263/trusted/"

    print("\nIniciando proceso ETL...")

    # Define schema for weather data
    weather_schema = StructType([
        StructField("time", TimestampType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("name", StringType(), True)
    ])

    # Read all weather CSV files
    print("\nLeyendo datos meteorológicos...")
    weather_df = spark.read \
        .option("header", True) \
        .schema(weather_schema) \
        .csv(api_data_path)

    print(f"Registros meteorológicos leídos: {weather_df.count()}")

    # Group weather data by city and calculate daily statistics
    print("\nCalculando estadísticas diarias...")
    daily_weather = weather_df \
        .withColumn("date", F.date_format("time", "yyyy-MM-dd")) \
        .groupBy("name", "date") \
        .agg(
            F.avg("temperature_2m").alias("avg_temperature"),
            F.min("temperature_2m").alias("min_temperature"),
            F.max("temperature_2m").alias("max_temperature")
        )

    print(f"Registros de estadísticas diarias: {daily_weather.count()}")

    # Read MySQL data with error handling
    print("\nLeyendo datos de MySQL...")
    required_files = ["cities.csv", "city_consumption.csv", "population.csv", "regions.csv", "city_region.csv"]
    
    for file in required_files:
        file_path = f"{mysql_data_path}/{file}"
        try:
            # Check if file exists
            spark.read.option("header", True).csv(file_path).count()
        except Exception as e:
            raise Exception(f"Error al leer {file}: {str(e)}")

    cities_df = spark.read.option("header", True).csv(f"{mysql_data_path}/cities.csv")
    consumption_df = spark.read.option("header", True).csv(f"{mysql_data_path}/city_consumption.csv")
    population_df = spark.read.option("header", True).csv(f"{mysql_data_path}/population.csv")
    regions_df = spark.read.option("header", True).csv(f"{mysql_data_path}/regions.csv")
    city_region_df = spark.read.option("header", True).csv(f"{mysql_data_path}/city_region.csv")

    print("\nIntegrando datos...")
    # Join MySQL data including regions
    city_info = cities_df \
        .join(consumption_df, cities_df.id == consumption_df.city_id, "left") \
        .join(population_df, cities_df.id == population_df.city_id, "left") \
        .join(city_region_df, cities_df.id == city_region_df.city_id, "left") \
        .join(regions_df, city_region_df.region_id == regions_df.id, "left") \
        .select(
            cities_df.name,
            cities_df.latitude,
            cities_df.longitude,
            F.coalesce(consumption_df.water_m3, F.lit(0)).alias("water_m3"),
            F.coalesce(consumption_df.electricity_kwh, F.lit(0)).alias("electricity_kwh"),
            F.coalesce(population_df.population, F.lit(0)).alias("population"),
            consumption_df.year,
            F.coalesce(regions_df.region_name, F.lit("Unknown")).alias("country")
        )

    print(f"Registros de información de ciudades: {city_info.count()}")

    # Join weather data with city information
    print("\nGenerando dataset final...")
    final_df = daily_weather \
        .join(city_info, daily_weather.name == city_info.name, "inner") \
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

    final_count = final_df.count()
    print(f"\nRegistros finales a escribir: {final_count}")

    if final_count == 0:
        raise Exception("No hay datos para escribir en la zona trusted")

    # Write the final dataset to the trusted zone
    print("\nEscribiendo datos en la zona trusted...")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("year") \
        .option("header", True) \
        .csv(output_path)

    print("\nProceso ETL completado exitosamente.")
    print(f"Datos guardados en: {output_path}")

except Exception as e:
    print(f"\nError en el proceso ETL: {str(e)}")
    spark.stop()
    exit(1)

spark.stop()
