# weather-etl-pipeline
Weather-Data-Pipeline is a project focused on automating the processing of weather data. Developed for the seventh-semester course "Special Topics in Telematics" (ST0263) at EAFIT University, taught by professor Edwin Montoya.

## Execution

1. Ingest data from API to S3 bucket

```bash
python scripts/ingest_api.py
```

2. Export data from MySQL to S3 bucket. Remember to add the .env file with the credentials of the MySQL database in AWS RDS.

```bash
python scripts/export_tables.py
```

**Note:** If you want to create the tables in MySQL, you can use the following command:

```bash
mysql -h weather-database.cr9l2ltlcjjs.us-east-1.rds.amazonaws.com -u admin -p <password> weather_project < sql/create_tables.sql
```

3. Run the EMR creation script.

```bash
python scripts/emr_creation.py
```

This will run automatically the ETL script and the data will be stored in the S3 bucket.