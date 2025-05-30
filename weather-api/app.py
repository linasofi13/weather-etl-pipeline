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
s3 = boto3.client('s3', region_name=os.getenv('REGION', 'us-east-1'))
athena_connection = connect(
    s3_staging_dir=f's3://weather-etl-data-st0263/query_results/',
    region_name=os.getenv('REGION', 'us-east-1')
)

BUCKET = os.getenv('BUCKET_NAME', 'weather-etl-data-st0263')

def execute_athena_query(query):
    cursor = athena_connection.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    return [dict(zip(columns, row)) for row in results]

def read_csv_from_s3(path):
    """Helper function to read CSV files from S3"""
    try:
        # List objects to get the latest part file
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=path)
        latest_file = None
        
        for obj in objects.get('Contents', []):
            if obj['Key'].endswith('.csv') and 'part-00000-' in obj['Key']:
                latest_file = obj['Key']
                break
        
        if not latest_file:
            raise Exception(f"No CSV file found in {path}")
            
        # Get the object
        obj = s3.get_object(Bucket=BUCKET, Key=latest_file)
        
        # Read CSV content
        return pd.read_csv(obj['Body'])
    except Exception as e:
        raise Exception(f"Error reading from S3: {str(e)}")

@app.route('/')
def index():
    return {
        'available_endpoints': {
            '/cities': 'List all cities with their latest weather data',
            '/city/{city_name}': 'Get detailed weather data for a specific city',
            '/temperature/rankings': 'Get temperature rankings by city (mean_temp)',
            '/consumption/rankings': 'Get consumption rankings by city (avg_kwh_per)',
            '/predictions/electricity': 'Get electricity consumption predictions by city',
            '/correlations': 'Get correlation analysis between weather and consumption',
            '/clusters': 'Get city clusters based on weather and consumption patterns'
        }
    }

@app.route('/cities')
def list_cities():
    query = """
    SELECT DISTINCT city, country, latitude, longitude
    FROM weather_refined.trusted
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
    FROM weather_refined.trusted
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
    """
    Get temperature rankings from temperature_per_city data
    Returns: city and mean_temp for each city
    """
    try:
        df = read_csv_from_s3('refined/temperature_per_city/')
        
        # Ensure we have the expected columns
        if 'city' not in df.columns or 'mean_temp' not in df.columns:
            raise Exception("Invalid data format in temperature rankings file")
        
        # Sort by temperature descending
        df = df.sort_values('mean_temp', ascending=False)
        
        # Convert DataFrame to records
        rankings = df.to_dict('records')
        
        return {
            'status': 'success',
            'temperature_rankings': rankings
        }
    except Exception as e:
        return Response(
            body={
                'status': 'error',
                'message': str(e)
            },
            status_code=500
        )

@app.route('/consumption/rankings')
def get_consumption_rankings():
    """
    Get consumption rankings from consumption_ranking data
    Returns: city and avg_kwh_per_person for each city
    """
    try:
        df = read_csv_from_s3('refined/consumption_ranking/')
        
        # Ensure we have the expected columns
        if 'city' not in df.columns:
            raise Exception("Invalid data format in consumption rankings file")
        
        # Rename columns if necessary based on actual CSV structure
        if 'avg_kwh_per_person' not in df.columns and 'avg_kwh_pe' in df.columns:
            df = df.rename(columns={'avg_kwh_pe': 'avg_kwh_per_person'})
        
        # Sort by consumption descending
        df = df.sort_values('avg_kwh_per_person', ascending=False)
        
        # Convert DataFrame to records
        rankings = df.to_dict('records')
        
        return {
            'status': 'success',
            'consumption_rankings': rankings
        }
    except Exception as e:
        return Response(
            body={
                'status': 'error',
                'message': str(e)
            },
            status_code=500
        )

@app.route('/predictions/electricity')
def get_electricity_predictions():
    """
    Get electricity consumption predictions
    Returns: city, avg_temperature, electricity_kwh, and prediction for each city
    """
    try:
        df = read_csv_from_s3('refined/electricity_predictions/')
        
        # Ensure we have the expected columns
        expected_columns = ['city', 'avg_temperature', 'electricity_kwh', 'prediction']
        if not all(col in df.columns for col in expected_columns):
            raise Exception("Invalid data format in predictions file")
        
        # Convert DataFrame to records
        predictions = df.to_dict('records')
        
        return {
            'status': 'success',
            'predictions': predictions
        }
    except Exception as e:
        return Response(
            body={
                'status': 'error',
                'message': str(e)
            },
            status_code=500
        )

@app.route('/correlations')
def get_correlations():
    query = """
    SELECT 
        corr(avg_temperature, electricity_kwh) as temp_electricity_corr,
        corr(avg_temperature, water_m3) as temp_water_corr,
        corr(electricity_kwh, water_m3) as electricity_water_corr
    FROM weather_refined.trusted
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