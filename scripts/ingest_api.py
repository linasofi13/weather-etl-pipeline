import os
import csv
import json
import requests
import boto3
import pandas as pd
from datetime import datetime

BUCKET_NAME = "weather-etl-data-st0263"

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
    {"id": 32, "name": "Dubai", "latitude": 25.2048, "longitude": 55.2708},
    {"id": 33, "name": "Singapore", "latitude": 1.3521, "longitude": 103.8198},
    {"id": 34, "name": "Vienna", "latitude": 48.2082, "longitude": 16.3738},
    {"id": 35, "name": "Prague", "latitude": 50.0755, "longitude": 14.4378},
    {"id": 36, "name": "Athens", "latitude": 37.9838, "longitude": 23.7275},
    {"id": 37, "name": "Dublin", "latitude": 53.3498, "longitude": -6.2603},
    {"id": 38, "name": "Helsinki", "latitude": 60.1699, "longitude": 24.9384},
    {"id": 39, "name": "Brussels", "latitude": 50.8503, "longitude": 4.3517},
    {"id": 40, "name": "Lisbon", "latitude": 38.7223, "longitude": -9.1393},
    {"id": 41, "name": "Budapest", "latitude": 47.4979, "longitude": 19.0402},
    {"id": 42, "name": "Copenhagen", "latitude": 55.6761, "longitude": 12.5683},
    {"id": 43, "name": "Bogota", "latitude": 4.7110, "longitude": -74.0721},
    {"id": 44, "name": "Caracas", "latitude": 10.4806, "longitude": -66.9036},
    {"id": 45, "name": "Quito", "latitude": -0.1807, "longitude": -78.4678},
    {"id": 46, "name": "La Paz", "latitude": -16.4897, "longitude": -68.1193},
    {"id": 47, "name": "Montevideo", "latitude": -34.9011, "longitude": -56.1645},
    {"id": 48, "name": "Asuncion", "latitude": -25.2867, "longitude": -57.3333},
    {"id": 49, "name": "San Jose", "latitude": 9.9281, "longitude": -84.0907},
    {"id": 50, "name": "Panama City", "latitude": 8.9824, "longitude": -79.5199},
    {"id": 51, "name": "San Salvador", "latitude": 13.6929, "longitude": -89.2182},
    {"id": 52, "name": "Guatemala City", "latitude": 14.6349, "longitude": -90.5069},
    {"id": 53, "name": "Tegucigalpa", "latitude": 14.0723, "longitude": -87.1921},
    {"id": 54, "name": "Managua", "latitude": 12.1149, "longitude": -86.2362},
    {"id": 55, "name": "Port-au-Prince", "latitude": 18.5944, "longitude": -72.3074},
    {"id": 56, "name": "Santo Domingo", "latitude": 18.4861, "longitude": -69.9312},
    {"id": 57, "name": "San Juan", "latitude": 18.4655, "longitude": -66.1057},
    {"id": 58, "name": "Havana", "latitude": 23.1136, "longitude": -82.3666},
    {"id": 59, "name": "Kingston", "latitude": 18.0179, "longitude": -76.8099},
    {"id": 60, "name": "Nassau", "latitude": 25.0343, "longitude": -77.3963},
    {"id": 61, "name": "Vancouver", "latitude": 49.2827, "longitude": -123.1207},
    {"id": 62, "name": "Montreal", "latitude": 45.5017, "longitude": -73.5673},
    {"id": 63, "name": "Calgary", "latitude": 51.0447, "longitude": -114.0719},
    {"id": 64, "name": "Edmonton", "latitude": 53.5461, "longitude": -113.4938},
    {"id": 65, "name": "Ottawa", "latitude": 45.4215, "longitude": -75.6972},
    {"id": 66, "name": "Quebec City", "latitude": 46.8139, "longitude": -71.2080},
    {"id": 67, "name": "Winnipeg", "latitude": 49.8951, "longitude": -97.1384},
    {"id": 68, "name": "Halifax", "latitude": 44.6488, "longitude": -63.5752},
    {"id": 69, "name": "St Johns", "latitude": 47.5615, "longitude": -52.7126},
    {"id": 70, "name": "Victoria", "latitude": 48.4284, "longitude": -123.3656},
    {"id": 71, "name": "Regina", "latitude": 50.4452, "longitude": -104.6189},
    {"id": 72, "name": "Saskatoon", "latitude": 52.1332, "longitude": -106.6700},
    {"id": 73, "name": "Yellowknife", "latitude": 62.4540, "longitude": -114.3718},
    {"id": 74, "name": "Whitehorse", "latitude": 60.7212, "longitude": -135.0568},
    {"id": 75, "name": "Iqaluit", "latitude": 63.7467, "longitude": -68.5170},
    {"id": 76, "name": "Charlottetown", "latitude": 46.2382, "longitude": -63.1311},
    {"id": 77, "name": "Fredericton", "latitude": 45.9636, "longitude": -66.6431},
    {"id": 78, "name": "Saint John", "latitude": 45.2733, "longitude": -66.0633},
    {"id": 79, "name": "Moncton", "latitude": 46.0878, "longitude": -64.7782},
    {"id": 80, "name": "Thunder Bay", "latitude": 48.3809, "longitude": -89.2477},
    {"id": 81, "name": "Sudbury", "latitude": 46.4917, "longitude": -80.9930},
    {"id": 82, "name": "Windsor", "latitude": 42.3149, "longitude": -83.0364},
    {"id": 83, "name": "London ON", "latitude": 42.9849, "longitude": -81.2453},
    {"id": 84, "name": "Kingston ON", "latitude": 44.2312, "longitude": -76.4860},
    {"id": 85, "name": "Kelowna", "latitude": 49.8880, "longitude": -119.4960},
    {"id": 86, "name": "Victoria BC", "latitude": 48.4284, "longitude": -123.3656},
    {"id": 87, "name": "Nanaimo", "latitude": 49.1659, "longitude": -123.9401},
    {"id": 88, "name": "Kamloops", "latitude": 50.6745, "longitude": -120.3273},
    {"id": 89, "name": "Prince George", "latitude": 53.9171, "longitude": -122.7497},
    {"id": 90, "name": "Fort McMurray", "latitude": 56.7264, "longitude": -111.3803},
    {"id": 91, "name": "Red Deer", "latitude": 52.2690, "longitude": -113.8116},
    {"id": 92, "name": "Lethbridge", "latitude": 49.6956, "longitude": -112.8451},
    {"id": 93, "name": "Medicine Hat", "latitude": 50.0419, "longitude": -110.6768},
    {"id": 94, "name": "Grande Prairie", "latitude": 55.1699, "longitude": -118.7986},
    {"id": 95, "name": "Brandon", "latitude": 49.8437, "longitude": -99.9516},
    {"id": 96, "name": "Thompson", "latitude": 55.7435, "longitude": -97.8558},
    {"id": 97, "name": "Prince Albert", "latitude": 53.2034, "longitude": -105.7530},
    {"id": 98, "name": "Moose Jaw", "latitude": 50.3917, "longitude": -105.5347},
    {"id": 99, "name": "North Battleford", "latitude": 52.7894, "longitude": -108.2967},
    {"id": 100, "name": "Estevan", "latitude": 49.1392, "longitude": -102.9916}
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
