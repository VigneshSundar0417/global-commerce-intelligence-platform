import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})

change_types = ["INCREASE", "DECREASE"]

def generate_inventory():
    qty_change = random.randint(1, 20)
    new_qty = random.randint(50, 500)

    return {
        "item_id": f"ITEM-{random.randint(100,999)}",
        "warehouse_id": f"WH-{random.randint(1,5)}",
        "change_type": random.choice(change_types),
        "quantity_change": qty_change,
        "new_quantity": new_qty,
        "update_timestamp": datetime.utcnow().isoformat()
    }

while True:
    event = generate_inventory()
    p.produce("inventory", json.dumps(event).encode("utf-8"))
    p.flush()
    print("Sent inventory update:", event["item_id"])
    time.sleep(2)