from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import math

spark = SparkSession.builder.appName("WeatherAnalysisSimple").getOrCreate()

trusted_path = "s3://weather-etl-data-st0263/trusted/year=2025/"
df = spark.read.option("header", True).csv(trusted_path)

df = df.withColumn("avg_temperature", F.col("avg_temperature").cast("double")) \
       .withColumn("min_temperature", F.col("min_temperature").cast("double")) \
       .withColumn("max_temperature", F.col("max_temperature").cast("double")) \
       .withColumn("electricity_kwh", F.col("electricity_kwh").cast("double")) \
       .withColumn("water_m3", F.col("water_m3").cast("double")) \
       .withColumn("population", F.col("population").cast("int")) \
       .withColumn("latitude", F.col("latitude").cast("double")) \
       .withColumn("longitude", F.col("longitude").cast("double"))

df = df.withColumn("temp_range", F.col("max_temperature") - F.col("min_temperature")) \
       .withColumn("kwh_per_capita", F.col("electricity_kwh") / F.col("population")) \
       .withColumn("water_per_capita", F.col("water_m3") / F.col("population")) \
       .withColumn("temp_squared", F.col("avg_temperature") * F.col("avg_temperature"))

temp_avg = df.groupBy("city").agg(F.avg("avg_temperature").alias("mean_temp"))

consumo_per_capita = df.withColumn("kwh_per_person", F.col("electricity_kwh") / F.col("population")) \
    .groupBy("city") \
    .agg(F.avg("kwh_per_person").alias("avg_kwh_per_person")) \
    .orderBy(F.desc("avg_kwh_per_person"))

print("=== INICIANDO ANÁLISIS AVANZADO (VERSIÓN SIMPLE) ===")

def calculate_correlation(df, col1, col2):
    """Calcula correlación de Pearson entre dos columnas"""
    stats = df.select(
        F.count(col1).alias("n"),
        F.sum(col1).alias("sum_x"),
        F.sum(col2).alias("sum_y"),
        F.sum(F.col(col1) * F.col(col2)).alias("sum_xy"),
        F.sum(F.col(col1) * F.col(col1)).alias("sum_x2"),
        F.sum(F.col(col2) * F.col(col2)).alias("sum_y2")
    ).collect()[0]
    
    n = stats["n"]
    sum_x = stats["sum_x"]
    sum_y = stats["sum_y"]
    sum_xy = stats["sum_xy"]
    sum_x2 = stats["sum_x2"]
    sum_y2 = stats["sum_y2"]
    
    numerator = n * sum_xy - sum_x * sum_y
    denominator = math.sqrt((n * sum_x2 - sum_x**2) * (n * sum_y2 - sum_y**2))
    
    return numerator / denominator if denominator != 0 else 0

def simple_linear_regression(df, x_col, y_col):
    """Calcula regresión lineal simple: y = mx + b"""
    stats = df.select(
        F.count(x_col).alias("n"),
        F.sum(x_col).alias("sum_x"),
        F.sum(y_col).alias("sum_y"),
        F.sum(F.col(x_col) * F.col(y_col)).alias("sum_xy"),
        F.sum(F.col(x_col) * F.col(x_col)).alias("sum_x2")
    ).collect()[0]
    
    n = stats["n"]
    sum_x = stats["sum_x"]
    sum_y = stats["sum_y"]
    sum_xy = stats["sum_xy"]
    sum_x2 = stats["sum_x2"]
    
    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x**2)
    intercept = (sum_y - slope * sum_x) / n
    
    return slope, intercept

print("1. Análisis de Correlación entre Variables Clave")

correlation_pairs = [
    ("avg_temperature", "electricity_kwh"),
    ("avg_temperature", "water_m3"),
    ("kwh_per_capita", "avg_temperature"),
    ("population", "electricity_kwh"),
    ("temp_range", "electricity_kwh")
]

correlations = {}
for col1, col2 in correlation_pairs:
    corr = calculate_correlation(df, col1, col2)
    correlations[f"{col1}_vs_{col2}"] = corr
    print(f"Correlación {col1} vs {col2}: {corr:.3f}")

print("\n2. Modelo Predictivo: Temperatura -> Consumo Eléctrico")

slope, intercept = simple_linear_regression(df, "avg_temperature", "electricity_kwh")
print(f"Modelo: Consumo = {slope:.2f} * Temperatura + {intercept:.2f}")

predictions_df = df.withColumn("predicted_kwh", 
                               F.lit(slope) * F.col("avg_temperature") + F.lit(intercept))

rmse_stats = predictions_df.select(
    F.sqrt(F.avg(F.pow(F.col("electricity_kwh") - F.col("predicted_kwh"), 2))).alias("rmse")
).collect()[0]

rmse = rmse_stats["rmse"]
print(f"RMSE del modelo simple: {rmse:.2f}")

print("\n3. Segmentación de Ciudades por Patrones Climáticos y de Consumo")

city_features = df.groupBy("city").agg(
    F.avg("avg_temperature").alias("avg_temp"),
    F.avg("kwh_per_capita").alias("avg_kwh_per_capita"),
    F.avg("water_per_capita").alias("avg_water_per_capita"),
    F.avg("temp_range").alias("avg_temp_range"),
    F.first("latitude").alias("latitude"),
    F.first("longitude").alias("longitude")
)

temp_thresholds = city_features.select(
    F.expr("percentile_approx(avg_temp, 0.33)").alias("cold_threshold"),
    F.expr("percentile_approx(avg_temp, 0.67)").alias("warm_threshold")
).collect()[0]

consumption_thresholds = city_features.select(
    F.expr("percentile_approx(avg_kwh_per_capita, 0.33)").alias("low_consumption"),
    F.expr("percentile_approx(avg_kwh_per_capita, 0.67)").alias("high_consumption")
).collect()[0]

cold_temp = temp_thresholds["cold_threshold"]
warm_temp = temp_thresholds["warm_threshold"]
low_cons = consumption_thresholds["low_consumption"]
high_cons = consumption_thresholds["high_consumption"]

print(f"Umbrales de temperatura: Frío < {cold_temp:.1f}°C < Moderado < {warm_temp:.1f}°C < Cálido")
print(f"Umbrales de consumo: Bajo < {low_cons:.4f} < Medio < {high_cons:.4f} < Alto")

city_clusters = city_features.withColumn("temperature_category",
    F.when(F.col("avg_temp") < cold_temp, "Cold")
    .when(F.col("avg_temp") > warm_temp, "Warm")
    .otherwise("Moderate")
).withColumn("consumption_category",
    F.when(F.col("avg_kwh_per_capita") < low_cons, "Low")
    .when(F.col("avg_kwh_per_capita") > high_cons, "High")
    .otherwise("Medium")
).withColumn("cluster_type",
    F.concat(F.col("temperature_category"), F.lit("_"), F.col("consumption_category"))
)

print("Distribución de ciudades por cluster:")
cluster_distribution = city_clusters.groupBy("cluster_type").count().orderBy("cluster_type")
cluster_distribution.show()

print("\n4. Detección de Outliers y Estadísticas Descriptivas")

stats_df = df.select("avg_temperature", "electricity_kwh", "kwh_per_capita", "temp_range", "water_per_capita")
print("Estadísticas descriptivas:")
stats_df.describe().show()

outlier_variables = ["kwh_per_capita", "water_per_capita", "temp_range"]

for var in outlier_variables:
    quartiles = df.select(
        F.expr(f"percentile_approx({var}, 0.25)").alias("Q1"),
        F.expr(f"percentile_approx({var}, 0.75)").alias("Q3")
    ).collect()[0]
    
    Q1, Q3 = quartiles["Q1"], quartiles["Q3"]
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers_count = df.filter(
        (F.col(var) < lower_bound) | (F.col(var) > upper_bound)
    ).count()
    
    print(f"Outliers en {var}: {outliers_count} registros")

outliers = df.filter(
    (F.col("kwh_per_capita") < low_cons * 0.5) | 
    (F.col("kwh_per_capita") > high_cons * 1.5)
)

print("\n5. Análisis de Tendencias Temporales")

df_temporal = df.withColumn("date_parsed", F.to_date(F.col("date"), "yyyy-MM-dd")) \
               .withColumn("day_of_month", F.dayofmonth("date_parsed")) \
               .withColumn("week_of_year", F.weekofyear("date_parsed"))

daily_trends = df_temporal.groupBy("day_of_month").agg(
    F.avg("avg_temperature").alias("avg_daily_temp"),
    F.avg("electricity_kwh").alias("avg_daily_kwh")
).orderBy("day_of_month")

print("Tendencias de temperatura y consumo por día del mes:")
daily_trends.show(5)

print("Guardando resultados de análisis avanzado...")

temp_avg.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/temperature_per_city")
consumo_per_capita.write.mode("overwrite").option("header", True).csv("s3://weather-etl-data-st0263/refined/consumption_ranking")

predictions_df.select("city", "date", "avg_temperature", "electricity_kwh", "predicted_kwh") \
    .write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/electricity_predictions")

city_clusters.write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/city_clusters")

outliers.select("city", "date", "kwh_per_capita", "avg_temperature") \
    .write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/consumption_outliers")

daily_trends.write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/daily_trends")

correlation_data = spark.createDataFrame([
    (pair.replace("_vs_", " vs "), corr) 
    for pair, corr in correlations.items()
], ["variable_pair", "correlation"])

correlation_data.write.mode("overwrite").option("header", True) \
    .csv("s3://weather-etl-data-st0263/refined/correlation_matrix")

print("=== ANÁLISIS COMPLETADO ===")
print("Resultados guardados en zona refined:")
print("- temperature_per_city: Temperatura promedio por ciudad")
print("- consumption_ranking: Ranking de consumo per cápita")
print("- electricity_predictions: Predicciones con regresión lineal simple")
print("- city_clusters: Segmentación de ciudades por clima y consumo")
print("- consumption_outliers: Outliers en consumo detectados")
print("- daily_trends: Tendencias temporales por día")
print("- correlation_matrix: Matriz de correlaciones entre variables")
print(f"- Modelo predictivo: Regresión lineal con RMSE: {rmse:.2f}")
print(f"- Correlación más fuerte: {max(correlations.items(), key=lambda x: abs(x[1]))}")
