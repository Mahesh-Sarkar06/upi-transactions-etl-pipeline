import argparse
import json
from datetime import datetime, timezone
import boto3
from confluent_kafka import Consumer
from botocore.exceptions import NoCredentialsError, ClientError

def consumerMain():
    parser = argparse.ArgumentParser(description='UPI Kafka Consumer to S3')
    parser.add_argument('--kafka-bootstrap', required=True, help='Kafka server URL')
    parser.add_argument('--kafka-topic', required=True, help='Kafka topic name')
    parser.add_argument('--count', type=int, default=100, help='number of messages to read')
    parser.add_argument('--group-id', default='upi-consumer-group', help='Kafka Consumer group ID')
    parser.add_argument('--s3-bucket', default='{AWS-BUCKET-NAME}', help='Target S3 bucket name without s3')
    parser.add_argument('--s3-folder', default='{RAW-DATA-FOLDER}', help='S3 folder path')
    parser.add_argument('--region', default='ap-south-1', help='AWS S3 bucket region')
    args = parser.parse_args()


    consumer_conf = {
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id": args.group_id,
        "auto.offset.reset": 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([args.kafka_topic])

    print(f'Reading {args.count} messages from Kafka topic {args.kafka_topic} and writing to {args.s3_bucket} \n')

    buffer = []
    read = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            txn = json.loads(msg.value())
            buffer.append(txn)
            read += 1

            if len(buffer) >= args.count:
                writeToS3(buffer, args.s3_bucket)
                buffer.clear()

    except KeyboardInterrupt:
        print("Interrupted by user. Uploading failed!!!")

    finally:
        # Flushed remaining messages
        if buffer:
            writeToS3(buffer, args.s3_bucket)
        consumer.close()
        print("Consumer closed successfully!!!")
        print(f"Finished consuming {read} messages.")


def writeToS3(records, bucket_name, prefix='kafka_raw_json_data'):
    s3_client = boto3.client('s3')

    now = datetime.now(timezone.utc)
    s3_path = f"{prefix}/transactions_{now.strftime('%Y%m%d_%H%M%S')}.json"

    batch_data = json.dumps(records, indent=2)

    try:
        s3_client.put_object(
            Bucket = bucket_name,
            Key = s3_path,
            Body = batch_data.encode('utf-8'),
            ContentType = 'application/json'
        )

        print(f"Uploaded {len(records)} transactions to {bucket_name}.")

    except NoCredentialsError:
        print("AWS credentials not found. Configure them using `aws configure`")
    except ClientError as err:
        print(f"Failed to upload to S3: {err}")


if __name__ == '__main__':
    consumerMain()