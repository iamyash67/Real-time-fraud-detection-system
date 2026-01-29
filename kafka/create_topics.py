from kafka.admin import KafkaAdminClient, NewTopic

# Connect to Kafka (port 29092 exposed from Docker)
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='fraud-topic-manager'
)


# Define the topics
topics = [
    NewTopic(name="transactions", num_partitions=1, replication_factor=1),
    NewTopic(name="fraud_alerts", num_partitions=1, replication_factor=1)
]

# Create topics if not already existing
try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully!")
except Exception as e:
    print("Topics may already exist or failed to create:")
    print(e)
