import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

regions = ["US", "EU", "IN", "CA", "AU"]
devices = ["mobile", "desktop", "tablet"]

def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "item_id": f"ITEM-{random.randint(100,999)}",
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 300), 2),
        "currency": "USD",
        "order_timestamp": datetime.utcnow().isoformat(),
        "region": random.choice(regions),
        "device_type": random.choice(devices)
    }

while True:
    event = generate_order()
    p.produce("orders", json.dumps(event).encode("utf-8"))
    p.flush()
    print("Sent order:", event["order_id"])
    time.sleep(1)