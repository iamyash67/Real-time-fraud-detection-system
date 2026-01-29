from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    user_id = random.randint(1000, 9999)
    amount = round(random.uniform(10.0, 5000.0), 2)
    is_foreign = random.choice([True, False])
    location = random.choice(["IN", "US", "DE", "SG", "CN"])
    timestamp = datetime.utcnow().isoformat()

    return {
        "user_id": user_id,
        "amount": amount,
        "location": location,
        "timestamp": timestamp,
        "is_foreign": is_foreign
    }

if __name__ == "__main__":
    while True:
        transaction = generate_transaction()
        producer.send("transactions", transaction)
        print("Sent:", transaction)
        time.sleep(1)
