import os
import json
import requests
from datetime import datetime
import boto3
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


def fetch_weather_data(latitude: float = 6.25, longitude: float = -75.56) -> dict:
    """Obtiene datos del clima desde la API de Open-Meteo."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}&hourly=temperature_2m"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def upload_to_s3(data: dict, bucket: str, key: str) -> None:
    """Sube los datos como archivo JSON a un bucket S3."""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN
    )

    json_body = json.dumps(data, indent=2, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=json_body, ContentType="application/json")


def main():
    print("📡 Consultando datos del clima desde Open-Meteo...")
    weather_data = fetch_weather_data()

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    s3_key = f"weather-data/medellin_{timestamp}.json"

    print(f"🚀 Subiendo archivo a s3://{BUCKET_NAME}/{s3_key} ...")
    upload_to_s3(weather_data, BUCKET_NAME, s3_key)
    print("✅ Datos subidos correctamente.")


if __name__ == "__main__":
    main()
