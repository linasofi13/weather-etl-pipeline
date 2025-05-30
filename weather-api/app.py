from chalice import Chalice, Response
from pyathena import connect
import boto3
import pandas as pd
import os
from dotenv import load_dotenv
import json
from datetime import datetime

load_dotenv()

app = Chalice(app_name='weather-api')
s3 = boto3.client('s3')
athena_connection = connect(
    s3_staging_dir=f's3://weather-etl-data-st0263/query_results/',
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

BUCKET = 'weather-etl-data-st0263'

def execute_athena_query(query):
    cursor = athena_connection.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    return [dict(zip(columns, row)) for row in results]

@app.route('/')
def index():
    return {
        'available_endpoints': {
            '/cities': 'List all cities with their latest weather data',
            '/city/{city_name}': 'Get detailed weather data for a specific city',
            '/temperature/rankings': 'Get temperature rankings across all cities',
            '/consumption/rankings': 'Get consumption rankings across all cities',
            '/predictions/electricity': 'Get electricity consumption predictions',
            '/correlations': 'Get correlation analysis between weather and consumption',
            '/clusters': 'Get city clusters based on weather and consumption patterns'
        }
    }

@app.route('/cities')
def list_cities():
    query = """
    SELECT DISTINCT city, country, latitude, longitude
    FROM trusted
    WHERE year = 2025
    """
    try:
        results = execute_athena_query(query)
        return {'cities': results}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/city/{city_name}')
def get_city_data(city_name):
    query = f"""
    SELECT city, date, avg_temperature, min_temperature, max_temperature,
           water_m3, electricity_kwh, population, country
    FROM trusted
    WHERE city = '{city_name}'
    AND year = 2025
    ORDER BY date DESC
    LIMIT 30
    """
    try:
        results = execute_athena_query(query)
        if not results:
            return Response(
                body={'error': 'City not found'},
                status_code=404
            )
        return {'city_data': results}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/temperature/rankings')
def get_temperature_rankings():
    query = """
    SELECT city, avg(avg_temperature) as avg_temp
    FROM trusted
    WHERE year = 2025
    GROUP BY city
    ORDER BY avg_temp DESC
    LIMIT 10
    """
    try:
        results = execute_athena_query(query)
        return {'temperature_rankings': results}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/consumption/rankings')
def get_consumption_rankings():
    query = """
    SELECT city, 
           avg(electricity_kwh/population) as avg_consumption_per_capita
    FROM trusted
    WHERE year = 2025 AND population > 0
    GROUP BY city
    ORDER BY avg_consumption_per_capita DESC
    LIMIT 10
    """
    try:
        results = execute_athena_query(query)
        return {'consumption_rankings': results}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/predictions/electricity')
def get_electricity_predictions():
    try:
        # Read predictions from refined zone
        predictions_df = pd.read_csv(
            f's3://{BUCKET}/refined/electricity_predictions/part-00000-*.csv'
        )
        predictions = predictions_df.to_dict('records')
        return {'predictions': predictions}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/correlations')
def get_correlations():
    query = """
    SELECT 
        corr(avg_temperature, electricity_kwh) as temp_electricity_corr,
        corr(avg_temperature, water_m3) as temp_water_corr,
        corr(electricity_kwh, water_m3) as electricity_water_corr
    FROM trusted
    WHERE year = 2025
    """
    try:
        results = execute_athena_query(query)
        return {'correlations': results[0]}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        )

@app.route('/clusters')
def get_clusters():
    try:
        # Read cluster results from refined zone
        clusters_df = pd.read_csv(
            f's3://{BUCKET}/refined/city_clusters/part-00000-*.csv'
        )
        clusters = clusters_df.to_dict('records')
        return {'clusters': clusters}
    except Exception as e:
        return Response(
            body={'error': str(e)},
            status_code=500
        ) 