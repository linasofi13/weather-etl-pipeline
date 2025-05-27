import os
import json
import requests
from datetime import datetime
import boto3

BUCKET_NAME = "weather-etl-data-st0263"
LATITUDE = 6.25
LONGITUDE = -75.56

def fetch_weather_data(latitude: float, longitude: float) -> dict:
    """Consulta clima desde Open-Meteo."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={latitude}&longitude={longitude}&hourly=temperature_2m"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_city_name(latitude: float, longitude: float) -> str:
    """Consulta nombre de ciudad desde la Geocoding API de Open-Meteo."""
    url = f"https://geocoding-api.open-meteo.com/v1/reverse?latitude={latitude}&longitude={longitude}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    if data.get("results"):
        return data["results"][0]["name"].replace(" ", "_").lower()
    return f"{latitude}_{longitude}"

def upload_to_s3(data: dict, bucket: str, key: str) -> None:
    """Sube el JSON al bucket S3."""
    s3 = boto3.client("s3")
    json_body = json.dumps(data, indent=2, ensure_ascii=False)
    s3.put_object(Bucket=bucket, Key=key, Body=json_body, ContentType="application/json")

def main():
    print("📡 Consultando clima y ciudad...")
    city_name = get_city_name(LATITUDE, LONGITUDE)
    weather_data = fetch_weather_data(LATITUDE, LONGITUDE)

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    s3_key = f"raw_data/api_data/{city_name}_{timestamp}.json"

    print(f"Subiendo archivo a s3://{BUCKET_NAME}/{s3_key} ...")
    upload_to_s3(weather_data, BUCKET_NAME, s3_key)
    print("Datos subidos correctamente.")

if __name__ == "__main__":
    main()
