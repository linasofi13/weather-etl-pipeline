## ST0263 Tópicos Especiales en Telemática

### Estudiantes:
- Alejandro Ríos Muñoz - ariosm@eafit.edu.co
- Lina Sofía Ballesteros Merchán - lsballestm@eafit.edu.co
- Jhonnatan Stiven Ocampo Díaz - jsocampod@eafit.edu.co

### Profesor:
- Edwin Nelson Montoya Munera, emontoya@eafit.edu.co

## Weather ETL Pipeline
### 1. Descripción de la actividad
Este proyecto implementa un pipeline de datos para recolectar, procesar y analizar datos meteorológicos de varias ciudades alrededor del mundo. El sistema integra datos de una API meteorológica con información demográfica y de consumo almacenada en MySQL, realizando transformaciones y análisis predictivo usando Spark.

### 1.1. Aspectos desarrollados
- ✅ Recolección automática de datos meteorológicosd de ciudades del mundo mediante API
- ✅ Integración con base de datos MySQL para datos demográficos y de consumo
- ✅ Pipeline ETL completo usando Apache Spark
- ✅ Implementación de modelos de Machine Learning (Regresión Lineal y Random Forest)
- ✅ Almacenamiento en capas (raw, trusted, refined) en S3
- ✅ Automatización del despliegue en EMR
- ✅ Análisis de correlaciones y predicciones de temperatura
- ✅ Resultados expuestos para ser consultados a través de Athena y API Gateway.

### 1.2. Aspectos NO desarrollados
- Todo los objetivos fueron desarrollados.

## 2. Información de diseño

### 2.1. Arquitectura

```mermaid
graph TD
    subgraph "Fuentes de Datos"
        A[API Meteorológica] 
        B[(MySQL RDS)]
    end

    subgraph "Ingesta de Datos"
        C[ingest_api.py]
        D[export_tables.py]
    end

    subgraph "Almacenamiento S3"
        E[raw/api_data/]
        F[raw/mysql_data/]
        G[trusted/]
        H[refined/]
    end

    subgraph "Procesamiento EMR"
        I[ETL Script]
        J[Analysis Script]
        K[ML Models]
    end

    A -->|Datos Meteorológicos| C
    B -->|Datos Demográficos y Consumo| D
    C -->|Raw Data| E
    D -->|Raw Data| F
    E --> I
    F --> I
    I -->|Datos Procesados| G
    G --> J
    J --> K
    K -->|Predicciones y Análisis| H

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style E fill:#dfd,stroke:#333,stroke-width:2px
    style F fill:#dfd,stroke:#333,stroke-width:2px
    style G fill:#dfd,stroke:#333,stroke-width:2px
    style H fill:#dfd,stroke:#333,stroke-width:2px
    style K fill:#ffd,stroke:#333,stroke-width:2px

```

### 2.2. Arquitectura
El proyecto sigue una arquitectura de procesamiento por lotes con tres capas principales:
1. **Capa de Ingesta**: Scripts Python para recolección de datos
2. **Capa de Procesamiento**: Jobs Spark para transformación y análisis
3. **Capa de Almacenamiento**: Amazon S3 con estructura de medallón (raw, trusted, refined)

### 2.3. Patrones y Mejores Prácticas
- Arquitectura de medallón (Raw → Trusted → Refined)
- Código modular 
- Control de versiones para datos y código
- Manejo de errores y logging
- Configuración externalizada
- Automatización del despliegue a través de Cron en Bash.

## 3. Ambiente de Desarrollo

### 3.1. Tecnologías Utilizadas
- Python 3.8+
- Apache Spark 3.3.0
- PySpark 3.3.0
- Pandas 1.5.0
- Boto3 1.26.0
- MySQL Connector 8.0.31

### 3.2. Configuración del Ambiente
1. Clonar el repositorio:
```bash
git clone https://github.com/usuario/weather-etl-pipeline.git
cd weather-etl-pipeline
```
2. Configurar variables de ambiente:
```bash
DB_HOST=localhost
DB_USER=admin
DB_PASSWORD=password
DB_NAME=database
```

3. Configurar credenciales de acceso de AWS Academy:
```bash
aws configure
```

4. Ejecutar el script de despliegue:
```bash
./run.sh
```

Si lo desea, puede programar el despliegue a través de Cron en Bash.

Edita el archivo crontab para programar el despliegue a través de Cron en Bash.

```bash
crontab -e
```

Añade esta línea al final para que se ejecute a la hora en punto, todos los días, cada hora:

```bash
0 * * * * /home/ubuntu/weather-etl-pipeline/run.sh
```

### 3.3. Estructura del Proyecto
```
weather-etl-pipeline/
├── scripts/
│   ├── ingest_api.py       # Ingesta de datos meteorológicos
│   ├── export_tables.py    # Exportación de datos MySQL
│   ├── etl_script.py       # Procesamiento ETL en Spark
│   ├── analysis_script.py  # Análisis y ML
│   └── emr_creation.py     # Creación cluster EMR
├── sql/
│   └── create_tables.sql   # Esquema de base de datos
├── datasets/
│   ├── raw_data/
│   │   ├── api_data/
│   │   └── mysql_data/
│   ├── trusted/
│   └── refined/
└── README.md
```

## 4. Ambiente de Producción

### 4.1. Infraestructura
- Amazon EMR 6.15.0
- Amazon S3
- Amazon RDS (MySQL)

### 4.2 Amazon Athena
Amazon Athena se utilizó como mecanismo de consulta interactiva para validar los resultados del procesamiento analítico realizado en Spark y almacenado en S3. Su integración permite consultar, desde SQL, los datos ya transformados y enriquecidos tanto desde la API como desde la consola de AWS, sin necesidad de servidores ni bases de datos adicionales.

#### Estructura en S3
Para que Athena pueda acceder a los datos, se organizó el almacenamiento en S3 en diferentes carpetas correspondientes a las zonas del pipeline:

- **raw_data/**: Contiene los archivos originales provenientes de la API y la base de datos relacional.
- **trusted/**: Almacena los datos procesados y unificados en Spark, incluyendo atributos de clima, consumo y población por ciudad y día.
- **refined/**: Contiene resultados específicos del análisis, como rankings de temperatura, consumo eléctrico per cápita y predicciones generadas por modelos.
- **query_results/**: Carpeta designada para guardar los resultados de cada consulta ejecutada en Athena.

#### Configuración de Athena
Se configuró Athena con las siguientes especificaciones:

- Ubicación predeterminada de resultados: carpeta `query_results/` dentro del bucket
- Base de datos lógica: `weather_refined`
- Tablas externas que apuntan directamente a los archivos CSV almacenados en S3
- Tabla principal `trusted` particionada por año
- Ejecución del comando `MSCK REPAIR TABLE` para registrar correctamente las particiones dinámicas

#### Validaciones con Athena


- Consultar datos climáticos y de consumo por ciudad y fecha
- Obtener rankings de ciudades según consumo energético per cápita
- Analizar correlaciones estadísticas entre variables como temperatura, agua y electricidad
- Explorar las predicciones de consumo generadas por modelos entrenados

Estos datos también son accedidos automáticamente desde la API desplegada en AWS Chalice, lo que conecta los resultados analíticos con una interfaz REST pública.

### 4.3 Configuración
1. Variables de ambiente necesarias:
```bash
export DB_HOST=localhost
export DB_USER=admin
export DB_PASSWORD=password
export DB_NAME=database
```

Además, deberá configurar las variables de entorno de autenticación de AWS Academy con `aws configure`.

2. Estructura de buckets S3:
```
s3://weather-etl-data-st0263/
├── logs/
├── query_results/
├── raw_data/
│   ├── api_data/
│   └── mysql_data/
├── trusted/
├── scripts/
└── refined/
```

### 4.4 Ejecución
1. Ingesta de datos:
```bash
python scripts/ingest_api.py
python scripts/export_tables.py
```

2. Procesamiento en EMR:
```bash
python scripts/emr_creation.py
```

### 4.5 Resultados
Los datos procesados se almacenan en las siguientes ubicaciones:
- Datos crudos: `s3://weather-etl-data-st0263/raw_data/`
- Datos procesados: `s3://weather-etl-data-st0263/trusted/`
- Análisis y predicciones: `s3://weather-etl-data-st0263/refined/`

### 4.6 API REST
Se ha implementado una API REST utilizando AWS Chalice que expone los datos procesados y análisis realizados. La API proporciona endpoints para:
- Consultar datos meteorológicos por ciudad
- Rankings de temperatura y consumo
- Predicciones de consumo eléctrico
- Análisis de correlaciones entre variables

La API está construida con AWS Chalice y se integra con Amazon Athena para consultar los datos procesados en S3. Para más detalles sobre la implementación, endpoints disponibles y ejemplos de uso, consulte la [documentación detallada de la API](weather-api/README.md).

## 5. Información Adicional
- El proyecto incluye manejo de errores y reintentos para la ingesta de API
- Los modelos ML se reentrenan diariamente con nuevos datos
- Las predicciones tienen un RMSE promedio de X°C

## Referencias
- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Apache Spark ML Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [AWS EMR Documentation](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html)
- [Modern Data Engineering with Apache Spark](https://www.databricks.com/learn/training/modern-data-engineering-with-apache-spark)
