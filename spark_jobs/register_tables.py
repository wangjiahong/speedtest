import os
from pyspark.sql import SparkSession

# Configurations
ICEBERG_CATALOG = os.getenv('ICEBERG_CATALOG', 'iceberg_catalog')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'speedtest')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'iceberg')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'iceberg')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'iceberg')

# Start Spark session
spark = SparkSession.builder \
    .appName('RegisterIcebergTables') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG, 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.catalog-impl', 'org.apache.iceberg.jdbc.JdbcCatalog') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.uri', f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}') \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.jdbc.user', POSTGRES_USER) \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.jdbc.password', POSTGRES_PASSWORD) \
    .config('spark.sql.catalog.' + ICEBERG_CATALOG + '.warehouse', f's3a://{MINIO_BUCKET}/warehouse') \
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT) \
    .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

# Create schemas and register tables
schemas = ['bronze', 'silver', 'gold']
tables = {
    'bronze': ['speedtest_raw'],
    'silver': ['speedtest_clean'],
    'gold': ['speedtest_agg']
}

for schema in schemas:
    print(f"Creating schema: {schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ICEBERG_CATALOG}.{schema}")
    
    for table in tables[schema]:
        table_path = f"s3a://{MINIO_BUCKET}/warehouse/{schema}/{table}"
        print(f"Registering table: {schema}.{table} from {table_path}")
        
        # Try to register the existing table
        try:
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{schema}.{table} (
                timestamp BIGINT,
                user_id STRING,
                upload_speed DOUBLE,
                download_speed DOUBLE,
                latency DOUBLE,
                location STRING,
                date DATE
            ) PARTITIONED BY (date)
            LOCATION '{table_path}'
            """)
            print(f"Successfully registered {schema}.{table}")
        except Exception as e:
            print(f"Error registering {schema}.{table}: {e}")

spark.stop() 