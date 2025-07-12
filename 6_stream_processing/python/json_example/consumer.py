from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from ride import Ride
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


class JsonConsumer:
    def __init__(self, props: Dict):
        # Create a streaming data consumer that connects to Kafka cluster
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        # Tell Kafka which data streams (topics) we want to read from
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # Check for new streaming messages every second
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                # Each topic can have multiple partitions - process all messages
                for message_key, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    # Settings for how our streaming consumer should behave
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',  # Read all historical messages when starting fresh
        'enable_auto_commit': True,
        'key_deserializer': lambda key: int(key.decode('utf-8')),  # Convert message keys from bytes to numbers
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),  # Convert JSON messages to Python Ride objects
        'group_id': 'consumer.group.id.json-example.1',
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])