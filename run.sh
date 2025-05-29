#! /bin/bash
source venv/bin/activate
# Ingest API data
python3 scripts/ingest_api.py

# Ingest MySQL data
python3 scripts/ingest_mysql.py

# Copy ETL script
aws s3 cp etl_script.py s3://weather-etl-data-st0263/scripts/

# Copy analysis script
aws s3 cp analysis_script.py s3://weather-etl-data-st0263/scripts/