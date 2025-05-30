from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ML imports
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Inicializar Spark con configuraciones optimizadas para memoria limitada
spark = SparkSession.builder \
    .appName("WeatherAnalysisML") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.default.parallelism", "10") \
    .config("spark.sql.files.maxPartitionBytes", "128m") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") \
    .getOrCreate()

print("=== INICIANDO ANÁLISIS AVANZADO CON SPARKML ===")

try:
    # Leer datos desde S3 zona trusted con particionamiento
    trusted_path = "s3://weather-etl-data-st0263/trusted/year=2025/"
    df = spark.read.option("header", True) \
                   .option("inferSchema", "false") \
                   .csv(trusted_path)

    print(f"Datos leídos desde S3: {df.count()} registros")

    # Convertir columnas numéricas y aplicar casting de manera más eficiente
    numeric_columns = {
        "avg_temperature": "double",
        "min_temperature": "double",
        "max_temperature": "double",
        "electricity_kwh": "double",
        "water_m3": "double",
        "population": "int",
        "latitude": "double",
        "longitude": "double"
    }

    # Convertir tipos de columna de manera eficiente
    for col, dtype in numeric_columns.items():
        df = df.withColumn(col, F.col(col).cast(dtype))

    # Crear variables derivadas con manejo de nulos
    df = df.withColumn(
        "kwh_per_person",
        F.when(F.col("population") > 0, F.col("electricity_kwh") / F.col("population")).otherwise(0)
    ).withColumn(
        "water_per_person",
        F.when(F.col("population") > 0, F.col("water_m3") / F.col("population")).otherwise(0)
    ).withColumn(
        "temp_range",
        F.col("max_temperature") - F.col("min_temperature")
    )

    # Análisis descriptivo con optimización de memoria
    print("\nRealizando análisis descriptivo...")
    
    # 1. Temperatura promedio por ciudad (usando repartition para control de memoria)
    temp_avg = df.groupBy("city") \
                 .agg(F.avg("avg_temperature").alias("mean_temp")) \
                 .repartition(5)

    # 2. Consumo per cápita (optimizado)
    consumo_per_capita = df.groupBy("city") \
        .agg(
            F.avg("kwh_per_person").alias("avg_kwh_per_person"),
            F.avg("water_per_person").alias("avg_water_per_capita")
        ).repartition(5)

    # Preparación para ML con control de memoria
    print("\nPreparando datos para ML...")
    
    # Seleccionar solo las columnas necesarias para ML
    ml_cols = ["avg_temperature", "population", "latitude", "longitude", "electricity_kwh"]
    ml_df = df.select(*ml_cols).repartition(10)

    # Preparar features con manejo de memoria
    assembler = VectorAssembler(
        inputCols=["population", "latitude", "longitude"],
        outputCol="features"
    )
    
    # Preparar datos para ML con limpieza
    ml_data = assembler.transform(ml_df) \
        .select("features", "electricity_kwh") \
        .na.drop()

    # Dividir datos con tamaño controlado
    train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)

    print("\nEntrenando modelo de regresión...")
    # Modelo de regresión lineal con parámetros conservadores
    lr = LinearRegression(
        featuresCol="features",
        labelCol="electricity_kwh",
        maxIter=10,
        regParam=0.3,
        elasticNetParam=0.8,
        standardization=True
    )

    # Entrenar y evaluar
    lr_model = lr.fit(train_data)
    predictions = lr_model.transform(test_data)

    # Evaluar modelo
    evaluator = RegressionEvaluator(
        labelCol="electricity_kwh",
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)

    print(f"\nResultados del modelo:")
    print(f"RMSE: {rmse:.2f}")
    print(f"R²: {r2:.3f}")

    # Guardar resultados de manera eficiente
    print("\nGuardando resultados...")
    
    # Guardar con particionamiento y compresión
    temp_avg.write.mode("overwrite") \
        .option("compression", "snappy") \
        .option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/temperature_per_city")

    consumo_per_capita.write.mode("overwrite") \
        .option("compression", "snappy") \
        .option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

    # Guardar predicciones
    predictions.select("prediction", "electricity_kwh") \
        .withColumn("error", F.abs(F.col("prediction") - F.col("electricity_kwh"))) \
        .repartition(5) \
        .write.mode("overwrite") \
        .option("compression", "snappy") \
        .option("header", True) \
        .csv("s3://weather-etl-data-st0263/refined/model_predictions")

    print("\nAnálisis completado exitosamente")

except Exception as e:
    print(f"\nError en el procesamiento: {str(e)}")
    raise
finally:
    # Limpiar recursos
    spark.catalog.clearCache()
    spark.stop()
