#! /bin/bash
source venv/bin/activate
# Ingest API data
python3 scripts/ingest_api.py

# Ingest MySQL data
python3 scripts/export_tables.py

# Copy ETL script
aws s3 cp scripts/etl_script.py s3://weather-etl-data-st0263/scripts/

# Copy analysis script
aws s3 cp scripts/analysis_script.py s3://weather-etl-data-st0263/scripts/

# Create EMR cluster
python3 scripts/create_emr.py