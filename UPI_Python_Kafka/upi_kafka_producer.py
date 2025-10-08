import argparse
import time
import json
from confluent_kafka import Producer
from user_data import UPITransactionGenerator

def deliveryReport(err, msg):
    if err is not None:
        print(f'Delivery is failed: {err}')
    else:
        print(f'Message delivered to: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def producerMain():
    parser = argparse.ArgumentParser(description='UPI Kafka Producer')
    parser.add_argument('--count', type=int, default=500, help='number of transactions')
    parser.add_argument('--users', type=int, default=100, help='number of users')
    parser.add_argument('--failed-pct', type=float, default=0.02, help='fraction of failed transaction')
    # parser.add_argument('--kafka-bootstrap', required=True, help='Kafka bootstrap server')
    parser.add_argument('--kafka-topic', required=True, help='Kafka topic name')
    parser.add_argument('--rate', type=float, default=0.0, help='events/sec, 0.0 -> as fast as possible')
    parser.add_argument('--seed', type=int, default=None, help='random seed')
    args = parser.parse_args()

    producer_conf = {
        "bootstrap.servers": "localhost:9092"
    }
    producer = Producer(producer_conf)
    topic = "UPI-transactions"

    gen = UPITransactionGenerator(n_users=args.users, failed_pct=args.failed_pct, seed=args.seed)
    sent = 0

    for txn in gen.generate_transaction(count=args.count):
        producer.produce(
            topic,
            key = txn['user_details']['user_id'],
            value = json.dumps(txn).encode('utf-8'),
            callback = deliveryReport
        )
        sent += 1
        
        if args.rate > 0:
            time.sleep(args.rate)

        producer.poll(0)

    producer.flush()
    print(f'\n Sent {sent} transactions to Kafka topic: {topic}')


if __name__ == '__main__':
    producerMain()