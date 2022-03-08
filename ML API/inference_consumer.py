from kafka import KafkaConsumer, KafkaProducer
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

TOPIC_NAME = "INFERENCE"

KAFKA_SERVER = "localhost:9092"

EMAIL_TOPIC = "EMAIL"

consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)

producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    api_version = (0, 11, 15)
)

def inferencProcessFunction(data):
	PredictSentiment(data)
	email_data = {...}
	producer.send(EMAIL_TOPIC, email_data)
	producer.flush()

for inf in consumer:
	
	inf_data = inf.value
  
        inferencProcessFunction(inf_data)