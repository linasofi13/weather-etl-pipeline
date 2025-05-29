from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# Imports adicionales para SparkML
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.stat import Correlation
from pyspark.ml import Pipeline
import numpy as np

# Inicializar Spark
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()

# Leer datos ya integrados de la zona trusted
trusted_path = "s3://weather-etl-data-st0263/trusted/year=2025/"
df = spark.read.option("header", True).csv(trusted_path)

# Convertir columnas numéricas correctamente
df = df.withColumn("avg_temperature", F.col("avg_temperature").cast("double")) \
       .withColumn("min_temperature", F.col("min_temperature").cast("double")) \
       .withColumn("max_temperature", F.col("max_temperature").cast("double")) \
       .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double")) \
       .withColumn("water_m3", F.col("water_m3").cast("double")) \
       .withColumn("population", F.col("population").cast("int")) \
       .withColumn("latitude", F.col("latitude").cast("double")) \
       .withColumn("longitude", F.col("longitude").cast("double"))

# Crear variables derivadas para mejor análisis
df = df.withColumn("temp_range", F.col("max_temperature") - F.col("min_temperature")) \
       .withColumn("kwh_per_capita", F.col("electricity_kwh") / F.col("population")) \
       .withColumn("water_per_capita", F.col("water_m3") / F.col("population")) \
       .withColumn("temp_squared", F.col("avg_temperature") * F.col("avg_temperature"))

# Análisis 1: Temperatura promedio por ciudad
temp_avg = df.groupBy("city").agg(F.avg("avg_temperature").alias("mean_temp"))

# Análisis 2: Top ciudades con mayor consumo per cápita
consumo_per_capita = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
    .groupBy("city") \
    .agg(F.avg("kwh_per_person").alias("avg_kwh_per_person")) \
    .orderBy(F.desc("avg_kwh_per_person"))

# === ANÁLISIS AVANZADO CON SPARKML ===

print("=== INICIANDO ANÁLISIS AVANZADO CON SPARKML ===")

# 1. MODELO DE REGRESIÓN: Predecir consumo eléctrico basado en temperatura y características de la ciudad
print("1. Modelo de Regresión Lineal: Predicción de consumo eléctrico")

# Preparar features para el modelo de regresión
feature_cols = ["avg_temperature", "temp_range", "temp_squared", "population", "latitude", "longitude"]
assembler_regression = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Preparar pipeline para regresión
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
lr = LinearRegression(featuresCol="scaled_features", labelCol="electricity_kwh")

pipeline_regression = Pipeline(stages=[assembler_regression, scaler, lr])

# Dividir datos en entrenamiento y prueba
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# Entrenar modelo
regression_model = pipeline_regression.fit(train_data)

# Hacer predicciones
predictions_lr = regression_model.transform(test_data)

# Evaluar modelo
evaluator = RegressionEvaluator(labelCol="electricity_kwh", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_lr)
print(f"RMSE del modelo de regresión lineal: {rmse:.2f}")

# 2. MODELO RANDOM FOREST para comparación
print("2. Modelo Random Forest: Predicción de consumo eléctrico")
rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="electricity_kwh", numTrees=10)
pipeline_rf = Pipeline(stages=[assembler_regression, scaler, rf])

rf_model = pipeline_rf.fit(train_data)
predictions_rf = rf_model.transform(test_data)

rmse_rf = evaluator.evaluate(predictions_rf)
print(f"RMSE del modelo Random Forest: {rmse_rf:.2f}")

# 3. CLUSTERING: Agrupar ciudades por patrones de consumo y clima
print("3. Análisis de Clustering K-Means: Segmentación de ciudades")

# Agregar datos por ciudad para clustering
city_features = df.groupBy("city").agg(
    F.avg("avg_temperature").alias("avg_temp"),
    F.avg("kwh_per_capita").alias("avg_kwh_per_capita"),
    F.avg("water_per_capita").alias("avg_water_per_capita"),
    F.avg("temp_range").alias("avg_temp_range"),
    F.first("latitude").alias("latitude"),
    F.first("longitude").alias("longitude")
)

# Preparar features para clustering
cluster_cols = ["avg_temp", "avg_kwh_per_capita", "avg_water_per_capita", "avg_temp_range", "latitude", "longitude"]
assembler_cluster = VectorAssembler(inputCols=cluster_cols, outputCol="features")
scaler_cluster = StandardScaler(inputCol="features", outputCol="scaled_features")

# Aplicar K-Means con 4 clusters
kmeans = KMeans(k=4, featuresCol="scaled_features", predictionCol="cluster")
pipeline_cluster = Pipeline(stages=[assembler_cluster, scaler_cluster, kmeans])

cluster_model = pipeline_cluster.fit(city_features)
city_clusters = cluster_model.transform(city_features)

print("Distribución de ciudades por cluster:")
city_clusters.groupBy("cluster").count().orderBy("cluster").show()

# 4. ANÁLISIS DE CORRELACIÓN
print("4. Análisis de Correlación entre variables")

# Preparar datos para matriz de correlación
correlation_cols = ["avg_temperature", "electricity_kwh", "water_m3", "population", "temp_range", "kwh_per_capita"]
assembler_corr = VectorAssembler(inputCols=correlation_cols, outputCol="features")
df_corr = assembler_corr.transform(df)

# Calcular matriz de correlación de Pearson
correlation_matrix = Correlation.corr(df_corr, "features", "pearson").collect()[0][0]
correlation_array = correlation_matrix.toArray()

print("Matriz de correlación (Pearson):")
print(f"Variables: {correlation_cols}")
for i, row in enumerate(correlation_array):
    print(f"{correlation_cols[i]}: {[f'{val:.3f}' for val in row]}")

# 5. ANÁLISIS DE OUTLIERS usando Statistical Summary
print("5. Detección de Outliers y Estadísticas Descriptivas")

# Estadísticas descriptivas por variable
stats_df = df.select("avg_temperature", "electricity_kwh", "kwh_per_capita", "temp_range")
stats_df.describe().show()

# Detectar outliers usando IQR para consumo per cápita
quartiles = df.select(
    F.expr("percentile_approx(kwh_per_capita, 0.25)").alias("Q1"),
    F.expr("percentile_approx(kwh_per_capita, 0.75)").alias("Q3")
).collect()[0]

Q1, Q3 = quartiles["Q1"], quartiles["Q3"]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

outliers = df.filter((F.col("kwh_per_capita") < lower_bound) | (F.col("kwh_per_capita") > upper_bound))
print(f"Outliers detectados en consumo per cápita: {outliers.count()}")

# === GUARDAR RESULTADOS ===

# Guardar resultados básicos (existentes)
temp_avg.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/temperature_per_city")
consumo_per_capita.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

# Guardar resultados de ML
print("Guardando resultados de análisis avanzado...")

# Predicciones del mejor modelo
best_predictions = predictions_rf if rmse_rf < rmse else predictions_lr
model_name = "random_forest" if rmse_rf < rmse else "linear_regression"
print(f"Mejor modelo: {model_name} con RMSE: {min(rmse_rf, rmse):.2f}")

best_predictions.select("city", "avg_temperature", "electricity_kwh", "prediction") \
    .write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/electricity_predictions")

# Resultados de clustering
city_clusters.write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/city_clusters")

# Estadísticas y outliers
outliers.select("city", "date", "kwh_per_capita", "avg_temperature") \
    .write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/consumption_outliers")

print("=== ANÁLISIS COMPLETADO ===")
print("Resultados guardados en zona refined:")
print("- temperature_per_city: Temperatura promedio por ciudad")
print("- consumption_ranking: Ranking de consumo per cápita")
print("- electricity_predictions: Predicciones de consumo eléctrico")
print("- city_clusters: Segmentación de ciudades por patrones")
print("- consumption_outliers: Outliers en consumo detectados")
print(f"- Mejor modelo predictivo: {model_name}")