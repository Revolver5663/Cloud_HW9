# consumer.py
from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'order-events',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='order-group'
)

mongo = MongoClient("mongodb://mongodb:27017/")
db = mongo['store']
collection = db['product_popularity']

def update_popularity(event):
    product_id = event['product_id']
    collection.update_one(
        {"product_id": product_id},
        {"$inc": {"popularity": 1}},
        upsert=True
    )
    print(f"Updated popularity for {product_id}")

if __name__ == "__main__":
    for msg in consumer:
        update_popularity(msg.value)
