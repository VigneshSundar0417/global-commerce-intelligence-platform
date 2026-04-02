from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

topics = [
    "orders",
    "payments",
    "inventory",
    "shipments",
    "customer_events"
]

new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]

fs = admin.create_topics(new_topics)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic created: {topic}")
    except Exception as e:
        print(f"Failed to create {topic}: {e}")