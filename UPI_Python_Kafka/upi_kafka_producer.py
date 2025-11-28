import argparse
import time
import json
import random
from confluent_kafka import Producer
from user_data import UPITransactionGenerator
import psycopg2 as pg
from psycopg2 import Error


def getPGUsers():
    conn = None
    try:
        conn = pg.connect(
            host='localhost',
            port='5432',
            user='postgres',
            password='pgSQL',
            database='upi_user_data'
        )

        curs = conn.cursor()
        curs.execute("""
            SELECT user_id, user_name, bank_name, bank_ifsc, user_kyc, id_proof
            FROM users;
        """)
        rows = curs.fetchall()
        curs.close()

        users = [
            {
                "user_id": row[0],
                "user_name": row[1],
                "bank_name": row[2],
                "bank_ifsc": row[3],
                "user_kyc": row[4],
                "id_proof": row[5]
            }
            for row in rows
        ]

        return users
    except (Exception, Error) as err:
        print(f"Failed to connect with pg DB: {err}")
        return []
    finally:
        if conn:
            conn.close()


def deliveryReport(err, msg):
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}')


def producerMain():
    parser = argparse.ArgumentParser(description='UPI Kafka Producer')
    parser.add_argument('--kafka-bootstrap', required=True, help='Kafka server URL')
    parser.add_argument('--kafka-topic', required=True, help='Kafka topic where messages are queued')
    parser.add_argument('--count', required=True, type=int, default=1000)
    parser.add_argument('--users', required=True, type=int, default=100)
    parser.add_argument('--failed-pct', required=True, type=float, help='Total error records')
    parser.add_argument('--rate', type=float, default=0.0)
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()

    producer_conf = {"bootstrap.servers": args.kafka_bootstrap}
    producer = Producer(producer_conf)
    topic = args.kafka_topic

    users = getPGUsers()
    if not users:
        print('⚠️ No records found in PostgreSQL database!')
        return

    sent = 0
    txn_gen = UPITransactionGenerator(n_users=args.users, failed_pct=args.failed_pct, seed=args.seed)

    for txn in txn_gen.generate_transaction(count=args.count):
        user = random.choice(users)

        record = {
            "user_details": {
                "user_id": user['user_id'],
                "user_name": user['user_name'],
                "user_bank_name": user['bank_name'],
                "user_bank_ifsc": user['bank_ifsc'],
                "demographic_details": {
                    "kyc": user['user_kyc'],
                    "id": user['id_proof']
                }
            },
            "transaction_details": {
                "transaction_id": txn['transaction_id'],
                "receiver_details": {
                    "receiver_bank_name": txn['bank_name'],
                    "receiver_bank_ifsc": txn['ifsc_code'],
                    "amount": txn['amount'],
                    "category": txn['category']
                }
            },
            "payment_gateway": txn['payment_gateway'],
            "state": txn['state'],
            "status": txn['status'],
            "timestamp": txn['timestamp']
        }

        producer.produce(
            topic,
            key=str(user['user_id']),
            value=json.dumps(record),
            callback=deliveryReport
        )

        sent += 1
        producer.poll(0)

        if args.rate > 0:
            time.sleep(1 / args.rate)

    producer.flush()
    print(f'\n✅ Sent {sent} transactions to Kafka topic: {topic}')


if __name__ == '__main__':
    producerMain()