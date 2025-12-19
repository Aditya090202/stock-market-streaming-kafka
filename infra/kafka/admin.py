from confluent_kafka.admin import AdminClient, NewTopic

# Create admin client
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Define topics with 3 partitions each
new_topics = [
    NewTopic('market.trades', num_partitions=3, replication_factor=1),
    NewTopic('market.quotes', num_partitions=3, replication_factor=1),
    NewTopic('market.bars', num_partitions=3, replication_factor=1)
]

# Create topics and handle results
print("Creating topics...")
futures = admin.create_topics(new_topics)

# Wait for each topic creation to complete
for topic, future in futures.items():
    try:
        future.result()  # Wait for the result
        print(f"✅ Topic '{topic}' created successfully with 3 partitions")
    except Exception as e:
        print(f"❌ Failed to create topic '{topic}': {e}")

print("\nDone!")