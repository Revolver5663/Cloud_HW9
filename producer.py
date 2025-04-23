# producer.py
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_order_event(order_id, product_id):
    event = {
        "order_id": order_id,
        "product_id": product_id,
        "timestamp": time.time()
    }
    producer.send('order-events', event)
    print(f"Sent: {event}")

# Simulate sending an event
if __name__ == "__main__":
    send_order_event(order_id=123, product_id="XYZ456")
