from kafka import KafkaConsumer, KafkaProducer
import os
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
TOPIC_NAME = "EMAIL"
consumer = KafkaConsumer(
    TOPIC_NAME,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
)
def sendEmail(data):
	print("E-mail has been sent")
	
for email in consumer:
	
	email_data = email.value
	
	sendEmail(email_data)