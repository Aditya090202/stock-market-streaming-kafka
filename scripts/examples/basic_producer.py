from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

#debugging function used from the docs
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush()."""
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))



while True:
    # start listening for input 
    user_input = input("> ")
    # this is creating a message and sending it to the kafka cluster
    producer.produce('market.quotes', key='NVDA', value='{"s": "NVDA", "p": "175.17"}')
    producer.produce('market.trades', key='MSFT', value=f'{user_input}')
    print("Sending message to topic.....")


# #this sends the messages into the kafka cluster
# producer.flush()