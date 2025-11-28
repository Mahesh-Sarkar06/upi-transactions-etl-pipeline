import json
import argparse
import os
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError


def saveToLocal(records, output_dir, prefix='batch'):
    """Write buffered records to a local JSON file."""
    if not records:
        return

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(output_dir, f"{prefix}_{timestamp}.json")

    with open(file_path, 'w') as fs:
        json.dump(records, fs, indent=2)

    print(f"✅ Total {len(records)} messages written to {file_path}")


def consumerMain(output_dir):
    parser = argparse.ArgumentParser(description='UPI Kafka Consumer to local file')
    parser.add_argument('--kafka-bootstrap', required=True, help='Kafka Server URL')
    parser.add_argument('--kafka-topic', required=True, help='Kafka topic')
    parser.add_argument('--count', type=int, default=100, help='Number of messages to read per batch')
    parser.add_argument('--group-id', default='UPI-Databricks', help='Kafka Group ID')
    args = parser.parse_args()

    consumer_conf = {
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id": args.group_id,
        "auto.offset.reset": 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.kafka_topic])

    print(f"🚀 Reading messages from Kafka topic: {args.kafka_topic}")
    print(f"Batch size: {args.count}\n")

    read = 0
    buffer = []
    last_flush_time = time.time()
    flush_interval = 10  # seconds

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # Check if it's time to flush old data
                if buffer and (time.time() - last_flush_time >= flush_interval):
                    saveToLocal(buffer, output_dir)
                    buffer.clear()
                    last_flush_time = time.time()
                continue

            if msg.error():
                print(f"⚠️ Kafka Error: {msg.error()}")
                continue

            txn = json.loads(msg.value().decode('utf-8'))
            buffer.append(txn)
            read += 1

            if len(buffer) >= args.count:
                saveToLocal(buffer, output_dir)
                buffer.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("🛑 Interrupted by User. Saving remaining messages...")

    finally:
        if buffer:
            saveToLocal(buffer, output_dir)
        consumer.close()
        print("✅ Consumer closed successfully.")
        print(f"📊 Total messages consumed: {read}")


if __name__ == '__main__':
    OUTPUT_DIR = '/Users/maheshsarkar/Desktop/Hackathon'
    consumerMain(OUTPUT_DIR)

# docker-compose up -d