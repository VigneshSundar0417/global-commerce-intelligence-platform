import json
import time
import random
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

carriers = ["UPS", "FedEx", "DHL", "USPS"]

def generate_shipment():
    return {
        "shipment_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "carrier": random.choice(carriers),
        "shipping_cost": round(random.uniform(5, 50), 2),
        "estimated_delivery_date": (datetime.utcnow() + timedelta(days=random.randint(2,7))).isoformat(),
        "shipment_timestamp": datetime.utcnow().isoformat()
    }

while True:
    event = generate_shipment()
    p.produce("shipments", json.dumps(event).encode("utf-8"))
    p.flush()
    print("Sent shipment:", event["shipment_id"])
    time.sleep(2)