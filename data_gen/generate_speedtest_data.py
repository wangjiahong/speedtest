import os
import time
import random
import uuid
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'speedtest_raw')
RECORDS_TO_GENERATE = int(os.getenv('RECORDS_TO_GENERATE', 1000000))  # Adjust for ~1GB
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000))
MESSAGES_PER_SECOND = int(os.getenv('MESSAGES_PER_SECOND', 10))

producer_config = {
    'bootstrap.servers': KAFKA_BROKER
}

producer = Producer(producer_config)

locations = [
    'New York', 'London', 'Tokyo', 'Berlin', 'Sydney', 'San Francisco',
    'Paris', 'Singapore', 'Toronto', 'Sao Paulo'
]

def generate_record():
    return {
        'timestamp': int(time.time() * 1000),
        'user_id': str(uuid.uuid4()),
        'upload_speed': round(random.uniform(1, 1000), 2),
        'download_speed': round(random.uniform(1, 1000), 2),
        'latency': round(random.uniform(1, 100), 2),
        'location': random.choice(locations)
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def main():
    print(f"Producing {RECORDS_TO_GENERATE} JSON records to topic '{TOPIC}' at {MESSAGES_PER_SECOND} msg/sec...")
    
    # Show sample message format
    sample_record = generate_record()
    sample_json = json.dumps(sample_record, indent=2)
    print(f"====> Sample message format:")
    print(sample_json)
    print("====> Starting data generation...")
    
    sent = 0
    while sent < RECORDS_TO_GENERATE:
        batch_size = min(BATCH_SIZE, RECORDS_TO_GENERATE - sent, MESSAGES_PER_SECOND)
        batch = [generate_record() for _ in range(batch_size)]
        for i, record in enumerate(batch):
            try:
                # Convert to JSON string
                json_value = json.dumps(record)
                
                # Print first few messages to see the format
                if sent + i < 5:  # Print first 5 messages
                    print(f"====> Sending message {sent + i + 1}: {json_value}")
                
                producer.produce(
                    topic=TOPIC, 
                    value=json_value.encode('utf-8'),
                    callback=delivery_report
                )
            except Exception as e:
                print(f"Error producing record: {e}")
        producer.flush()
        sent += len(batch)
        print(f"Sent {sent}/{RECORDS_TO_GENERATE} JSON records")
        time.sleep(0.1)  # Throttle to MESSAGES_PER_SECOND
    print("JSON data generation complete.")

if __name__ == "__main__":
    main() 