from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json


def create_producer():
    return KafkaProducer(
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def send_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

    topic_list = []
    topic_list.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    topic_list


if __name__ == "__main__":
    create_topic("yan_is_king")
    producer = create_producer()
    topic = 'yan_is_king'
    message = {'key': 'value'}  # Your message here
    send_message(producer, topic, message)