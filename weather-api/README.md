# API de Datos Meteorológicos

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

## Documentación de la API

### Descripción General
La API es un servicio REST construido con AWS Chalice que proporciona acceso a datos meteorológicos, métricas de consumo y análisis predictivo. Se integra con Amazon Athena para consultar datos procesados y S3 para acceder a conjuntos de datos refinados.

### Tecnologías Utilizadas
- **AWS Chalice**: Framework serverless para construir APIs REST
- **PyAthena**: Interfaz DB-API de Python para Amazon Athena
- **Pandas**: Manipulación y análisis de datos
- **Boto3**: SDK de AWS para Python
- **Python-dotenv**: Gestión de variables de entorno

### Dependencias
```
chalice==1.29.0
boto3==1.26.6         
pandas==2.2.2
pyathena==2.25.2
python-dotenv==1.0.1
```

### Endpoints de la API

#### 1. Listar Endpoints Disponibles
- **Endpoint**: `/`
- **Método**: GET
- **Descripción**: Devuelve una lista de todos los endpoints disponibles y sus descripciones
- **Ejemplo de Respuesta**:
```json
{
    "available_endpoints": {
        "/cities": "Lista de ciudades con sus coordenadas",
        "/city/{city_name}": "Datos meteorológicos detallados de una ciudad específica",
        "/temperature/rankings": "Rankings de temperatura por ciudad (temperatura media)",
        "/consumption/rankings": "Rankings de consumo por ciudad (kwh per cápita)",
        "/predictions/electricity": "Predicciones de consumo eléctrico por ciudad",
        "/correlations": "Análisis de correlación entre clima y consumo"
    }
}
```

#### 2. Listar Ciudades
- **Endpoint**: `/cities`
- **Método**: GET
- **Descripción**: Devuelve una lista de todas las ciudades con sus coordenadas geográficas
- **Ejemplo de Respuesta**:
```json
{
    "cities": [
        {
            "city": "Medellín",
            "country": "Colombia",
            "latitude": 6.2442,
            "longitude": -75.5812
        }
    ]
}
```

#### 3. Datos Meteorológicos por Ciudad
- **Endpoint**: `/city/{city_name}`
- **Método**: GET
- **Descripción**: Devuelve datos detallados meteorológicos y de consumo de una ciudad específica
- **Parámetros**: 
  - `city_name`: Nombre de la ciudad (codificado en URL)
- **Ejemplo de Solicitud**: `/city/Medellín`
- **Ejemplo de Respuesta**:
```json
{
    "city_data": [
        {
            "city": "Medellín",
            "date": "2025-03-15",
            "avg_temperature": 22.5,
            "min_temperature": 18.2,
            "max_temperature": 26.8,
            "water_m3": 150000,
            "electricity_kwh": 280000,
            "population": 2500000,
            "country": "Colombia"
        }
    ]
}
```

#### 4. Rankings de Temperatura
- **Endpoint**: `/temperature/rankings`
- **Método**: GET
- **Descripción**: Devuelve ciudades ordenadas por su temperatura media
- **Ejemplo de Respuesta**:
```json
{
    "status": "success",
    "temperature_rankings": [
        {
            "city": "Dubai",
            "mean_temp": 35.2
        }
    ]
}
```

#### 5. Rankings de Consumo
- **Endpoint**: `/consumption/rankings`
- **Método**: GET
- **Descripción**: Devuelve ciudades ordenadas por consumo eléctrico promedio por persona
- **Ejemplo de Respuesta**:
```json
{
    "status": "success",
    "consumption_rankings": [
        {
            "city": "New York",
            "avg_kwh_per_person": 4500.75
        }
    ]
}
```

#### 6. Predicciones de Electricidad
- **Endpoint**: `/predictions/electricity`
- **Método**: GET
- **Descripción**: Devuelve predicciones de consumo eléctrico basadas en datos meteorológicos
- **Ejemplo de Respuesta**:
```json
{
    "status": "success",
    "predictions": [
        {
            "city": "London",
            "avg_temperature": 15.5,
            "electricity_kwh": 320000,
            "prediction": 335000
        }
    ]
}
```

#### 7. Correlaciones Clima-Consumo
- **Endpoint**: `/correlations`
- **Método**: GET
- **Descripción**: Devuelve análisis de correlación entre métricas meteorológicas y consumo
- **Ejemplo de Respuesta**:
```json
{
    "correlations": {
        "temp_electricity_corr": 0.85,
        "temp_water_corr": 0.72,
        "electricity_water_corr": 0.65
    }
}
```

### Manejo de Errores
Todos los endpoints devuelven códigos de estado HTTP apropiados:
- 200: Solicitud exitosa
- 404: Recurso no encontrado
- 500: Error interno del servidor

Las respuestas de error incluyen un mensaje descriptivo:
```json
{
    "error": "Ciudad no encontrada"
}
```

### Fuentes de Datos
La API se integra con:
- Amazon Athena para consultar datos meteorológicos y de consumo procesados
- Buckets S3 para acceder a conjuntos de datos refinados y predicciones
- Capa de datos confiable para registros históricos meteorológicos

### Variables de Entorno
Variables de entorno requeridas:
- `REGION`: Región de AWS (por defecto: 'us-east-1')
- `BUCKET_NAME`: Nombre del bucket S3 (por defecto: 'weather-etl-data-st0263')

### Despliegue
La API se despliega como una aplicación serverless usando AWS Chalice. Para desarrollo local:

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar credenciales de AWS:
```bash
aws configure
```

3. Ejecutar localmente:
```bash
chalice local
```

4. Desplegar en AWS:
```bash
chalice deploy
```
