from confluent_kafka import Consumer

#have the user define which group to join when this file is run
group_id = input("Enter a group id: ")


consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'{group_id}',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['market.quotes'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))