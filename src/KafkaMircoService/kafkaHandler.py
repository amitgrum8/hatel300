import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


class KafkaHandler:

    def __init__(self, servers="localhost:9092"):
        self.consumer = None
        self.servers = servers
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_message(self, topic, message):
        if self.producer is None:
            raise Exception("Producer not initialized. Call start_producer first.")
        self.producer.send(topic, message)
        self.producer.flush()

    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        admin_client = KafkaAdminClient(bootstrap_servers=self.servers, client_id='test')
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    def start_consumer(self, topic, group_id, auto_offset_reset='earliest'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.servers,
            auto_offset_reset=auto_offset_reset,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    def consume_messages(self):
        if self.consumer:
            for message in self.consumer:
                yield message
