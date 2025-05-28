from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BasicETLTest").getOrCreate()

input_path = "s3://weather-etl-data-st0263/raw_data/api_data/"
output_path = "s3://weather-etl-data-st0263/test_output/"

df = spark.read.option("header", True).csv(input_path)
df.write.mode("overwrite").option("header", True).csv(output_path)

print("ETL básico completado con éxito.")
