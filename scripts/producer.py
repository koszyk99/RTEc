import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Connection configuration
# 'bootstrap.servers' tells the script where to look for Kafka
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

# Callback - a function that will confirm whether the data has reached Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery error: {err}")
    else:
        print(f"Order shipped to topic: {msg.topic()} [Partition: {msg.partition()}]")

# Helper listsfor generating random orders
products = ['Laptop', 'Smartphone', 'Monitor', 'Keyboard', 'Mouse', 'Webcam']
categories = ['Electronics', 'Accessories', 'Office']

print("Starting the order generator... (Press Ctrl+C to stop)")

try:
    while True:
        # Create data
        order = {
            "order_id": random.randint(1000, 99999),
            "user_id": random.randint(1, 500),
            "product": random.choice(products),
            "category": random.choice(categories),
            "price": round(random.uniform(20.0, 4500.0), 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Serialization: Dictionary -> JSON -> Bytes (UTF-8)
        payload = json.dumps(order).encode('utf-8')

        # Sending data to the 'order' topic
        # produce() is asynchronous - it does not block the program
        producer.produce(
            topic='orders',
            value=payload,
            callback=delivery_report
        )

        # flush() forces a massage from the local buffer to be sent to Kafka
        producer.flush()

        time.sleep(1) # We wait 1 second before the next order

except KeyboardInterrput:
    print("\nClosing the generator...")
