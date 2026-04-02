import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

statuses = ["SUCCESS", "FAILED"]
methods = ["card", "paypal", "upi", "bank_transfer"]
regions = ["US", "EU", "IN", "CA"]

def generate_payment():
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "amount": round(random.uniform(10, 300), 2),
        "currency": "USD",
        "payment_status": random.choice(statuses),
        "payment_method": random.choice(methods),
        "payment_timestamp": datetime.utcnow().isoformat(),
        "region": random.choice(regions)
    }

while True:
    event = generate_payment()
    p.produce("payments", json.dumps(event).encode("utf-8"))
    p.flush()
    print("Sent payment:", event["payment_id"])
    time.sleep(1.5)