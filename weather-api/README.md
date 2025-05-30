# Weather Data API

Esta API proporciona acceso a los datos meteorológicos, análisis y predicciones del pipeline ETL de datos meteorológicos.

## Requisitos

- Python 3.8+
- AWS CLI configurado con credenciales
- Permisos de AWS para:
  - API Gateway
  - Lambda
  - S3
  - Athena
  - IAM

## Instalación

1. Crear un entorno virtual:
```bash
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
```

2. Instalar dependencias:
```bash
pip install -r requirements.txt
```

3. Configurar variables de entorno en un archivo `.env`:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
```

## Despliegue

1. Crear el rol IAM necesario:
```bash
aws iam create-role --role-name weather-api-role --assume-role-policy-document file://trust-policy.json
```

2. Adjuntar las políticas necesarias:
```bash
aws iam attach-role-policy --role-name weather-api-role --policy-arn arn:aws:iam::aws:policy/AWSLambdaExecute
aws iam attach-role-policy --role-name weather-api-role --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
aws iam attach-role-policy --role-name weather-api-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

3. Desplegar la API:
```bash
chalice deploy
```

## Endpoints Disponibles

### GET /
Lista todos los endpoints disponibles.

### GET /cities
Lista todas las ciudades con sus datos básicos.

### GET /city/{city_name}
Obtiene datos detallados de una ciudad específica.

### GET /temperature/rankings
Obtiene el ranking de temperaturas de las ciudades.

### GET /consumption/rankings
Obtiene el ranking de consumo eléctrico per cápita.

### GET /predictions/electricity
Obtiene predicciones de consumo eléctrico.

### GET /correlations
Obtiene análisis de correlación entre variables.

### GET /clusters
Obtiene los clusters de ciudades basados en patrones de consumo y clima.

## Ejemplos de Uso

```python
import requests

# Base URL de tu API (reemplazar con la URL real después del despliegue)
BASE_URL = "https://your-api-id.execute-api.region.amazonaws.com/api"

# Listar todas las ciudades
response = requests.get(f"{BASE_URL}/cities")
cities = response.json()

# Obtener datos de una ciudad específica
city_name = "New York"
response = requests.get(f"{BASE_URL}/city/{city_name}")
city_data = response.json()

# Obtener rankings de temperatura
response = requests.get(f"{BASE_URL}/temperature/rankings")
rankings = response.json()
```

## Notas de Seguridad

- La API utiliza autenticación IAM
- Los datos son accedidos a través de Athena y S3
- Las consultas están optimizadas y tienen límites para prevenir sobrecarga

## Troubleshooting

1. Si hay problemas con los permisos:
   - Verificar que el rol IAM tenga todas las políticas necesarias
   - Verificar que las credenciales de AWS estén configuradas correctamente

2. Si hay problemas con las consultas:
   - Verificar que las tablas existan en Athena
   - Verificar que los datos estén presentes en S3
   - Revisar los logs de CloudWatch

3. Si hay problemas con el despliegue:
   - Verificar que chalice esté instalado y actualizado
   - Verificar que el archivo config.json esté correctamente configurado 