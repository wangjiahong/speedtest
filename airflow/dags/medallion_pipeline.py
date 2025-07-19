from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def spark_submit_cmd(script):
    return f"docker exec speedtest_c-spark-1 spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 /opt/spark-apps/{script}"

def spark_submit_cmd_no_kafka(script):
    return f"docker exec speedtest_c-spark-1 spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.2 /opt/spark-apps/{script}"

def_env = {
    'KAFKA_BROKER': 'kafka:9092',
    'SCHEMA_REGISTRY_URL': 'http://schema-registry:8081',
    'KAFKA_TOPIC': 'speedtest_raw',
    'ICEBERG_CATALOG': 'iceberg_catalog',
    'BRONZE_DB': 'bronze',
    'BRONZE_TABLE': 'speedtest_raw',
    'SILVER_DB': 'silver',
    'SILVER_TABLE': 'speedtest_clean',
    'GOLD_DB': 'gold',
    'GOLD_TABLE': 'speedtest_agg',
    'MINIO_ENDPOINT': 'http://minio:9000',
    'MINIO_ACCESS_KEY': 'minioadmin',
    'MINIO_SECRET_KEY': 'minioadmin',
    'MINIO_BUCKET': 'speedtest',
    'POSTGRES_HOST': 'postgres',
    'POSTGRES_PORT': '5432',
    'POSTGRES_DB': 'iceberg',
    'POSTGRES_USER': 'iceberg',
    'POSTGRES_PASSWORD': 'iceberg',
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Medallion architecture ETL pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

ingest_bronze = BashOperator(
    task_id='ingest_bronze',
    bash_command=spark_submit_cmd('ingest_bronze.py'),
    env=def_env,
    dag=dag,
)

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=spark_submit_cmd_no_kafka('bronze_to_silver.py'),
    env=def_env,
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=spark_submit_cmd_no_kafka('silver_to_gold.py'),
    env=def_env,
    dag=dag,
)

iceberg_maintenance = BashOperator(
    task_id='iceberg_maintenance',
    bash_command=spark_submit_cmd_no_kafka('iceberg_maintenance.py'),
    env=def_env,
    dag=dag,
)

# Set task dependencies
ingest_bronze >> bronze_to_silver >> silver_to_gold >> iceberg_maintenance 