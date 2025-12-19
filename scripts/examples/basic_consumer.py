from confluent_kafka import Consumer

# Have the user define which group to join
group_id = input("Enter a group id: ")

# Have the user enter topics (comma-separated)
topics_input = input("Enter topics to subscribe to (comma-separated, e.g., market.quotes,market.trades): ")

# Parse the input into a list of topics
topics = [topic.strip() for topic in topics_input.split(',') if topic.strip()]

if not topics:
    print("No topics provided! Exiting.")
    exit(1)

print(f"\n📌 Subscribing to topics: {topics}")
print(f"📌 Consumer group: {group_id}\n")

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(topics)

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))