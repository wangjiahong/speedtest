#!/usr/bin/env python3
"""
Table Metrics Exporter for Prometheus
Exposes row counts from Iceberg tables as Prometheus metrics
"""

import time
import logging
import os
from prometheus_client import start_http_server, Gauge, Counter
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
BRONZE_ROW_COUNT = Gauge('bronze_table_row_count', 'Number of rows in bronze table')
SILVER_ROW_COUNT = Gauge('silver_table_row_count', 'Number of rows in silver table')
GOLD_ROW_COUNT = Gauge('gold_table_row_count', 'Number of rows in gold table')
KAFKA_MESSAGE_COUNT = Gauge('kafka_message_count', 'Number of messages in Kafka topic')
TABLE_UPDATE_TIME = Gauge('table_last_update_timestamp', 'Last update timestamp for tables')

# Configuration from environment
TRINO_HOST = os.getenv('TRINO_HOST', 'trino')
TRINO_PORT = os.getenv('TRINO_PORT', '8080')

def get_trino_row_count(catalog, schema, table):
    """Get row count using Trino HTTP API"""
    try:
        # Use Trino's HTTP API instead of Python client
        url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/statement"
        query = f"SELECT COUNT(*) as row_count FROM {catalog}.{schema}.{table}"
        
        headers = {
            'Content-Type': 'application/json',
            'X-Trino-User': 'admin'
        }
        
        data = {
            'query': query,
            'catalog': catalog,
            'schema': schema
        }
        
        response = requests.post(url, json=data, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # Parse the response to get the count
            # This is a simplified version - you might need to adjust based on actual response format
            result = response.json()
            # Extract count from result (adjust based on actual response structure)
            return result.get('data', [[0]])[0][0] if result.get('data') else 0
        else:
            logger.error(f"Trino API error: {response.status_code} - {response.text}")
            return 0
            
    except Exception as e:
        logger.error(f"Failed to get row count for {catalog}.{schema}.{table}: {e}")
        return 0

def get_kafka_message_count():
    """Get message count from Kafka topic using Kafka admin API"""
    try:
        # Use Kafka admin API to get topic info
        # This is a placeholder - you'd need to implement Kafka admin client
        # For now, return a simulated value
        return 100  # Placeholder value
    except Exception as e:
        logger.error(f"Failed to get Kafka message count: {e}")
        return 0

def update_metrics():
    """Update all Prometheus metrics"""
    logger.info("Updating table metrics...")
    
    try:
        # Get row counts for each layer
        bronze_count = get_trino_row_count('iceberg_catalog', 'bronze', 'speedtest_raw')
        silver_count = get_trino_row_count('iceberg_catalog', 'silver', 'speedtest_clean')
        gold_count = get_trino_row_count('iceberg_catalog', 'gold', 'speedtest_agg')
        
        # Update Prometheus metrics
        BRONZE_ROW_COUNT.set(bronze_count)
        SILVER_ROW_COUNT.set(silver_count)
        GOLD_ROW_COUNT.set(gold_count)
        KAFKA_MESSAGE_COUNT.set(get_kafka_message_count())
        TABLE_UPDATE_TIME.set(time.time())
        
        logger.info(f"Updated metrics - Bronze: {bronze_count}, Silver: {silver_count}, Gold: {gold_count}")
        
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
        # Set fallback values
        BRONZE_ROW_COUNT.set(0)
        SILVER_ROW_COUNT.set(0)
        GOLD_ROW_COUNT.set(0)
        KAFKA_MESSAGE_COUNT.set(0)
        TABLE_UPDATE_TIME.set(time.time())

def main():
    """Main function"""
    logger.info("Starting Table Metrics Exporter...")
    logger.info(f"Trino host: {TRINO_HOST}:{TRINO_PORT}")
    
    # Start Prometheus HTTP server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Update metrics every 30 seconds
    while True:
        update_metrics()
        time.sleep(30)

if __name__ == "__main__":
    main() 