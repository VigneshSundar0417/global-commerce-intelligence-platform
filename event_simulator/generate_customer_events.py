import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

event_types = ["page_view", "add_to_cart", "search", "wishlist", "checkout"]
pages = ["home", "product", "search", "cart", "checkout"]
regions = ["US", "EU", "IN", "CA", "AU"]
devices = ["mobile", "desktop", "tablet"]

def generate_customer_event():
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "page": random.choice(pages),
        "device_type": random.choice(devices),
        "region": random.choice(regions),
        "event_timestamp": datetime.utcnow().isoformat()
    }

while True:
    event = generate_customer_event()
    p.produce("customer_events", json.dumps(event).encode("utf-8"))
    p.flush()
    print("Sent customer event:", event["event_id"])
    time.sleep(0.8)