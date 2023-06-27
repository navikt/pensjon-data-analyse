from kafka import KafkaConsumer
import json

def create_kafka_consumer(topic: str, kafka_brokers: []) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=kafka_brokers,
        api_version=(0, 10),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
