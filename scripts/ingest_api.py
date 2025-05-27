import os
import json
import requests
from datetime import datetime
import boto3

BUCKET_NAME = "project3-weather-test "
LATITUDE = 6.25
LONGITUDE = -75.56

def fetch_weather_data(latitude: float = LATITUDE, longitude: float = LONGITUDE) -> dict:
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
    s3 = boto3.client("s3")  # Usa las credenciales de 'aws configure' o el IAM role de EC2

    json_body = json.dumps(data, indent=2, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=json_body, ContentType="application/json")


def main():
    print("📡 Consultando datos del clima desde Open-Meteo...")
    weather_data = fetch_weather_data()

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    s3_key = f"raw_data/api_data/medellin_{timestamp}.json"  # Carpeta correcta

    print(f"🚀 Subiendo archivo a s3://{BUCKET_NAME}/{s3_key} ...")
    upload_to_s3(weather_data, BUCKET_NAME, s3_key)
    print("✅ Datos subidos correctamente.")


if __name__ == "__main__":
    main()
