import os
import csv
import json
import requests
import boto3
import pandas as pd
from datetime import datetime

BUCKET_NAME = "weather-etl-data-st0263"

# Lista de ciudades
CITIES = [
    {"id": 1, "name": "New York", "latitude": 40.7128, "longitude": -74.006},
    {"id": 2, "name": "London", "latitude": 51.5074, "longitude": -0.1278},
    {"id": 3, "name": "Tokyo", "latitude": 35.6895, "longitude": 139.692},
    {"id": 4, "name": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"id": 5, "name": "Sydney", "latitude": -33.8688, "longitude": 151.209},
    {"id": 6, "name": "Berlin", "latitude": 52.52, "longitude": 13.405},
    {"id": 7, "name": "Sao Paulo", "latitude": -23.5505, "longitude": -46.6333},
    {"id": 8, "name": "Toronto", "latitude": 43.651, "longitude": -79.347},
    {"id": 9, "name": "Cape Town", "latitude": -33.9249, "longitude": 18.4241},
    {"id": 10, "name": "Mumbai", "latitude": 19.076, "longitude": 72.8777},
    {"id": 11, "name": "Medellin", "latitude": 6.25, "longitude": -75.56},
    {"id": 12, "name": "Mexico City", "latitude": 19.4326, "longitude": -99.1332},
    {"id": 13, "name": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
    {"id": 14, "name": "Moscow", "latitude": 55.7558, "longitude": 37.6176},
    {"id": 15, "name": "Beijing", "latitude": 39.9042, "longitude": 116.407},
    {"id": 16, "name": "Rome", "latitude": 41.9028, "longitude": 12.4964},
    {"id": 17, "name": "Buenos Aires", "latitude": -34.6037, "longitude": -58.3816},
    {"id": 18, "name": "Santiago", "latitude": -33.4489, "longitude": -70.6693},
    {"id": 19, "name": "Seoul", "latitude": 37.5665, "longitude": 126.978},
    {"id": 20, "name": "Cairo", "latitude": 30.0444, "longitude": 31.2357},
    {"id": 21, "name": "Lima", "latitude": -12.0464, "longitude": -77.0428},
    {"id": 22, "name": "Oslo", "latitude": 59.9139, "longitude": 10.7522},
    {"id": 23, "name": "Amsterdam", "latitude": 52.3676, "longitude": 4.9041},
    {"id": 24, "name": "Stockholm", "latitude": 59.3293, "longitude": 18.0686},
    {"id": 25, "name": "Auckland", "latitude": -36.8485, "longitude": 174.763},
    {"id": 26, "name": "Bangkok", "latitude": 13.7563, "longitude": 100.502},
    {"id": 27, "name": "Jakarta", "latitude": -6.2088, "longitude": 106.846},
    {"id": 28, "name": "Hanoi", "latitude": 21.0285, "longitude": 105.854},
    {"id": 29, "name": "Istanbul", "latitude": 41.0082, "longitude": 28.9784},
    {"id": 30, "name": "Kyiv", "latitude": 50.4501, "longitude": 30.5234},
    {"id": 31, "name": "Warsaw", "latitude": 52.2297, "longitude": 21.0122},
]

def fetch_weather(city):
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={city['latitude']}&longitude={city['longitude']}"
        f"&hourly=temperature_2m&timezone=UTC"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    rows = [{"time": t, "temperature_2m": temp, "name": city["name"]} for t, temp in zip(times, temps)]
    return rows

def save_and_upload_csv(rows, city_name):
    df = pd.DataFrame(rows)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"{city_name}_{timestamp}.csv"
    local_path = f"/tmp/{filename}"

    df.to_csv(local_path, index=False)

    s3_key = f"raw_data/api_data/{filename}"
    s3 = boto3.client("s3")
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"{filename} subido a s3://{BUCKET_NAME}/{s3_key}")

def main():
    for city in CITIES:
        print(f"Procesando ciudad: {city['name']}")
        try:
            rows = fetch_weather(city)
            save_and_upload_csv(rows, city["name"].replace(" ", "_").lower())
        except Exception as e:
            print(f"Error procesando {city['name']}: {e}")

if __name__ == "__main__":
    main()
