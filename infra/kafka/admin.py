from confluent_kafka.admin import AdminClient, NewTopic

# this creates a admin client using confluent's python client for kafka
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

#lets create some new topics to test out kafka
new_topics = [NewTopic('market.trades', num_partitions=3, replication_factor=1),
              NewTopic('market.quotes', num_partitions=3, replication_factor=1),
              NewTopic('market.bars', num_partitions=3, replication_factor=1)]
admin.create_topics(new_topics)