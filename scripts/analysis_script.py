from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
import pandas as pd

# Inicializar Spark
spark = SparkSession.builder \
    .appName("WeatherAnalysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Leer datos ya integrados de la zona trusted
trusted_path = "s3://weather-etl-data-st0263/trusted/"

# Verificar si hay datos antes de proceder
try:
    # Leer todos los datos de la zona trusted
    df = spark.read.option("header", True).csv(trusted_path)
    
    if df.rdd.isEmpty():
        raise Exception("No se encontraron datos en la zona trusted")

    # Imprimir schema y conteo antes de la transformación
    print("\nSchema original:")
    df.printSchema()
    print(f"\nNúmero de registros: {df.count()}")

    # Convertir columnas numéricas correctamente con manejo de errores
    df = df.na.fill(0, ["avg_temperature", "min_temperature", "max_temperature", 
                       "latitude", "longitude", "water_m3", "electricity_kwh", "population"])
    
    df = df.withColumn("avg_temperature", F.col("avg_temperature").cast("double")) \
           .withColumn("min_temperature", F.col("min_temperature").cast("double")) \
           .withColumn("max_temperature", F.col("max_temperature").cast("double")) \
           .withColumn("latitude", F.col("latitude").cast("double")) \
           .withColumn("longitude", F.col("longitude").cast("double")) \
           .withColumn("water_m3", F.col("water_m3").cast("double")) \
           .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double")) \
           .withColumn("population", F.col("population").cast("int"))

    # Verificar si hay suficientes datos para el análisis
    if df.count() < 10:
        raise Exception("Insuficientes datos para realizar el análisis")

    # Análisis 1: Temperatura promedio por ciudad y país
    temp_avg = df.groupBy("city", "country").agg(
        F.avg("avg_temperature").alias("mean_temp"),
        F.stddev("avg_temperature").alias("temp_std")
    )

    # Análisis 2: Top ciudades con mayor consumo per cápita por país
    consumo_per_capita = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
        .groupBy("city", "country") \
        .agg(
            F.avg("kwh_per_person").alias("avg_kwh_per_person"),
            F.avg("water_m3").alias("avg_water_consumption")
        ) \
        .orderBy(F.desc("avg_kwh_per_person"))

    # Análisis 3: Correlación entre temperatura y consumo
    correlation_df = df.select(
        F.corr("avg_temperature", "electricity_kwh").alias("temp_electricity_corr"),
        F.corr("avg_temperature", "water_m3").alias("temp_water_corr"),
        F.corr("population", "electricity_kwh").alias("pop_electricity_corr"),
        F.corr("population", "water_m3").alias("pop_water_corr")
    )

    # Preparar datos para ML
    # Crear features para el modelo
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude", "population", "water_m3", "electricity_kwh"],
        outputCol="features"
    )

    # Dividir datos en entrenamiento y prueba
    data = assembler.transform(df.na.drop())
    splits = data.randomSplit([0.8, 0.2], seed=42)
    train_data = splits[0]
    test_data = splits[1]

    # Modelo 1: Regresión Lineal para predicción de temperatura
    lr = LinearRegression(
        featuresCol="features",
        labelCol="avg_temperature",
        maxIter=10,
        regParam=0.3,
        elasticNetParam=0.8
    )

    # Entrenar modelo de regresión lineal
    lr_model = lr.fit(train_data)

    # Evaluar modelo de regresión lineal
    lr_predictions = lr_model.transform(test_data)
    lr_evaluator = RegressionEvaluator(
        labelCol="avg_temperature",
        predictionCol="prediction",
        metricName="rmse"
    )
    lr_rmse = lr_evaluator.evaluate(lr_predictions)

    # Modelo 2: Random Forest para predicción de temperatura
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="avg_temperature",
        numTrees=10
    )

    # Entrenar modelo Random Forest
    rf_model = rf.fit(train_data)

    # Evaluar modelo Random Forest
    rf_predictions = rf_model.transform(test_data)
    rf_rmse = lr_evaluator.evaluate(rf_predictions)

    # Crear DataFrame con importancia de características para Random Forest
    feature_importance = pd.DataFrame(
        rf_model.featureImportances.toArray(),
        columns=['importance'],
        index=["latitude", "longitude", "population", "water_m3", "electricity_kwh"]
    )
    feature_importance_spark = spark.createDataFrame(feature_importance.reset_index())
    feature_importance_spark = feature_importance_spark.withColumnRenamed("index", "feature")

    # Predicciones para todas las ciudades
    all_predictions = rf_model.transform(data)
    city_predictions = all_predictions.select(
        "city",
        "country",
        "avg_temperature",
        "prediction",
        (F.col("avg_temperature") - F.col("prediction")).alias("prediction_error")
    )

    # Guardar resultados en zona refined
    temp_avg.write.mode("overwrite").option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/temperature_per_city")

    consumo_per_capita.write.mode("overwrite").option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

    correlation_df.write.mode("overwrite").option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/correlations")

    feature_importance_spark.write.mode("overwrite").option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/feature_importance")

    city_predictions.write.mode("overwrite").option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/temperature_predictions")

    # Imprimir métricas de evaluación
    print("\nResultados del Análisis:")
    print(f"RMSE Regresión Lineal: {lr_rmse}")
    print(f"RMSE Random Forest: {rf_rmse}")
    print("\nAnálisis completado y datos guardados en zona refined.")

except Exception as e:
    print(f"\nError en el procesamiento: {str(e)}")
    # Asegurar que el script termine con error
    spark.stop()
    exit(1)
