from confluent_kafka import Consumer, KafkaException
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'upi-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['UPI-transactions'])

print("🟢 Listening for messages...\nPress Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        record = json.loads(msg.value().decode('utf-8'))
        #print(json.dumps(record, indent=4))
        print(record)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()