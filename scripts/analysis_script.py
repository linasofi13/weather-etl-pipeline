from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializar Spark
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# Leer datos ya integrados de la zona trusted
trusted_path = "s3://weather-etl-data-st0263/trusted/year=2025/"
df = spark.read.option("header", True).csv(trusted_path)

# Convertir columnas numéricas correctamente
df = df.withColumn("avg_temperature", F.col("avg_temperature").cast("double")) \
       .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double")) \
       .withColumn("population", F.col("population").cast("int"))

# Análisis 1: Temperatura promedio por ciudad
temp_avg = df.groupBy("city").agg(F.avg("avg_temperature").alias("mean_temp"))

# Análisis 2: Top ciudades con mayor consumo per cápita
consumo_per_capita = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
    .groupBy("city") \
    .agg(F.avg("kwh_per_person").alias("avg_kwh_per_person")) \
    .orderBy(F.desc("avg_kwh_per_person"))

# Guardar resultados en zona refined
temp_avg.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/temperature_per_city")
consumo_per_capita.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

print("Análisis completado y datos guardados en zona refined.")
