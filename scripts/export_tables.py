import pandas as pd
import mysql.connector
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION", "us-east-1")
bucket_name = "weather-etl-data-st0263"

conn = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

tables = ["cities", "regions", "city_region", "population", "city_consumption"]
output_dir = "datasets/exported"
os.makedirs(output_dir, exist_ok=True)

for table in tables:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    file_path = os.path.join(output_dir, f"{table}.csv")
    df.to_csv(file_path, index=False)
    print(f"{table}.csv exportado a {file_path}")

    s3.upload_file(file_path, bucket_name, f"raw_data/mysql_data/{table}.csv")
    print(f"{table}.csv subido a S3 bucket {bucket_name}")

conn.close()
