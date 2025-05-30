from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ML imports
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Inicializar Spark con configuración optimizada para AWS
spark = SparkSession.builder \
    .appName("WeatherAnalysisML") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=== INICIANDO ANÁLISIS AVANZADO CON SPARKML ===")

# Leer datos desde S3 zona trusted
trusted_path = "s3://weather-etl-data-st0263/trusted/year=2025/"
df = spark.read.option("header", True).csv(trusted_path)

print(f"Datos leídos desde S3: {df.count()} registros")

# Convertir columnas numéricas correctamente
df = df.withColumn("avg_temperature", F.col("avg_temperature").cast("double")) \
       .withColumn("min_temperature", F.col("min_temperature").cast("double")) \
       .withColumn("max_temperature", F.col("max_temperature").cast("double")) \
       .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double")) \
       .withColumn("water_m3", F.col("water_m3").cast("double")) \
       .withColumn("population", F.col("population").cast("int")) \
       .withColumn("latitude", F.col("latitude").cast("double")) \
       .withColumn("longitude", F.col("longitude").cast("double"))

# Crear variables derivadas
df = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
       .withColumn("water_per_person", F.col("water_m3") / F.col("population")) \
       .withColumn("temp_range", F.col("max_temperature") - F.col("min_temperature"))

# ————————————————————————————————————————
# Análisis descriptivo existente
# ————————————————————————————————————————

# 1. Temperatura promedio por ciudad
temp_avg = df.groupBy("city") \
             .agg(F.avg("avg_temperature").alias("mean_temp"))

# 2. Top ciudades con mayor consumo per cápita
consumo_per_capita = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
    .groupBy("city") \
    .agg(F.avg("kwh_per_person").alias("avg_kwh_per_person")) \
    .orderBy(F.desc("avg_kwh_per_person"))

# 3. Estadísticas generales por ciudad
city_stats = df.groupBy("city").agg(
    F.avg("avg_temperature").alias("avg_temp"),
    F.avg("kwh_per_person").alias("avg_kwh_per_capita"),
    F.avg("water_per_person").alias("avg_water_per_capita"),
    F.avg("temp_range").alias("avg_temp_range"),
    F.first("latitude").alias("latitude"),
    F.first("longitude").alias("longitude"),
    F.first("population").alias("population"),
    F.count("*").alias("records_count")
)

# Guardar resultados en zona refined S3
temp_avg.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/temperature_per_city")

consumo_per_capita.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

city_stats.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/city_comprehensive_stats")

print("Análisis descriptivo completado y datos guardados en zona refined S3.")

# ————————————————————————————————————————
# 1) Regresión lineal: predecir electricity_kwh
# ————————————————————————————————————————

print("\n=== MODELO DE REGRESIÓN LINEAL ===")

# Crear vector de características: temperatura, población y ubicación
assembler_reg = VectorAssembler(
    inputCols=["avg_temperature", "population", "latitude", "longitude"],
    outputCol="features"
)
df_reg = assembler_reg.transform(df).select("features", "electricity_kwh", "city", "avg_temperature")

# Dividir en train/test
train_reg, test_reg = df_reg.randomSplit([0.8, 0.2], seed=42)

# Entrenar modelo de regresión lineal
lr = LinearRegression(featuresCol="features", labelCol="electricity_kwh")
lr_model = lr.fit(train_reg)

# Hacer predicciones
predictions_reg = lr_model.transform(test_reg)
predictions_reg.select("prediction", "electricity_kwh", "city", "avg_temperature").show(10)

# Evaluar con RMSE
evaluator_reg = RegressionEvaluator(
    predictionCol="prediction",
    labelCol="electricity_kwh",
    metricName="rmse"
)
rmse = evaluator_reg.evaluate(predictions_reg)
r2 = evaluator_reg.setMetricName("r2").evaluate(predictions_reg)

print(f"Regresión lineal RMSE: {rmse:.2f}")
print(f"Regresión lineal R²: {r2:.3f}")

# Guardar predicciones en S3 con más información
predictions_reg.select("city", "avg_temperature", "electricity_kwh", "prediction") \
    .withColumn("error", F.abs(F.col("electricity_kwh") - F.col("prediction"))) \
    .coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/regression_predictions")


# ————————————————————————————————————————
# 2) Clustering K-Means: agrupar ciudades por patrones
# ————————————————————————————————————————

print("\n=== CLUSTERING K-MEANS DE CIUDADES ===")

# Usar estadísticas por ciudad para clustering más significativo
assembler_clust = VectorAssembler(
    inputCols=["avg_temp", "avg_kwh_per_capita", "avg_water_per_capita", "latitude", "longitude"],
    outputCol="features_clust"
)
city_features = assembler_clust.transform(city_stats)

# Entrenar K-Means con 4 clusters
kmeans = KMeans(featuresCol="features_clust", k=4, seed=1)
kmeans_model = kmeans.fit(city_features)

# Asignar cluster a cada ciudad
city_clustered = kmeans_model.transform(city_features)

# Mostrar información de clusters
print("Distribución de ciudades por cluster:")
city_clustered.groupBy("prediction").count().show()

# Mostrar características de cada cluster
print("Características promedio por cluster:")
cluster_summary = city_clustered.groupBy("prediction").agg(
    F.avg("avg_temp").alias("cluster_avg_temp"),
    F.avg("avg_kwh_per_capita").alias("cluster_avg_consumption"),
    F.count("*").alias("cities_in_cluster")
)
cluster_summary.show()

# Evaluar silhouette
evaluator_clust = ClusteringEvaluator(
    featuresCol="features_clust",
    metricName="silhouette"
)
silhouette = evaluator_clust.evaluate(city_clustered)
print(f"Clustering Silhouette score: {silhouette:.3f}")

# Guardar resultados detallados de clustering en S3
city_clustered.select("city", "avg_temp", "avg_kwh_per_capita", "population", 
                     "latitude", "longitude", "prediction") \
    .withColumnRenamed("prediction", "cluster") \
    .coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/city_clusters_detailed")

# Guardar resumen de clusters
cluster_summary.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/cluster_summary")

# ————————————————————————————————————————
# 3) Análisis de correlaciones
# ————————————————————————————————————————

print("\n=== ANÁLISIS DE CORRELACIONES ===")

# Calcular correlaciones importantes
correlations = {}
correlation_pairs = [
    ("avg_temperature", "electricity_kwh"),
    ("avg_temperature", "water_m3"),
    ("population", "electricity_kwh"),
    ("latitude", "avg_temperature"),
    ("kwh_per_person", "avg_temperature")
]

for col1, col2 in correlation_pairs:
    corr = df.stat.corr(col1, col2)
    correlations[f"{col1}_vs_{col2}"] = corr
    print(f"Correlación {col1} vs {col2}: {corr:.3f}")

# Crear DataFrame de correlaciones y guardarlo
correlation_data = spark.createDataFrame([
    (pair.replace("_vs_", " vs "), corr) 
    for pair, corr in correlations.items()
], ["variable_pair", "correlation"])

correlation_data.coalesce(1).write.mode("overwrite") \
    .option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/correlation_matrix")

print("\n=== RESUMEN FINAL ===")
print(f"✓ Modelos ML entrenados y evaluados")
print(f"✓ RMSE Regresión: {rmse:.2f}, R²: {r2:.3f}")
print(f"✓ Silhouette Clustering: {silhouette:.3f}")
print(f"✓ {len(correlations)} correlaciones calculadas")
print(f"✓ Resultados guardados en S3: s3://weather-etl-data-st0263/refined/")

print("\nArchivos generados:")
print("- temperature_per_city/ : Temperaturas promedio por ciudad")
print("- consumption_ranking/ : Ranking de consumo per cápita")
print("- city_comprehensive_stats/ : Estadísticas completas por ciudad")
print("- regression_predictions/ : Predicciones del modelo ML")
print("- city_clusters_detailed/ : Clustering detallado de ciudades")
print("- cluster_summary/ : Resumen de características por cluster")
print("- correlation_matrix/ : Matriz de correlaciones")

# Detener Spark
spark.stop()
