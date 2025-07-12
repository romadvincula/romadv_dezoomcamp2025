import os
from typing import Dict, List

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import dict_to_ride_record_key
from ride_record import dict_to_ride_record
from settings import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, \
    RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, KAFKA_TOPIC


class RideAvroConsumer:
    def __init__(self, props: Dict):

        # Schema Registry and Serializer-Deserializer Configurations
        # Set up Avro schema management for reading structured streaming data
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        # Create deserializers that convert Avro binary format back to Python objects
        self.avro_key_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                      schema_str=key_schema_str,
                                                      from_dict=dict_to_ride_record_key)
        self.avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                        schema_str=value_schema_str,
                                                        from_dict=dict_to_ride_record)

        # Initialize Kafka consumer for reading streaming messages
        consumer_props = {'bootstrap.servers': props['bootstrap.servers'],
                          'group.id': 'datatalkclubs.taxirides.avro.consumer.2',
                          'auto.offset.reset': "earliest"}  # Read all historical messages when starting fresh
        self.consumer = Consumer(consumer_props)

    @staticmethod
    def load_schema(schema_path: str):
        """Load Avro schema definition for data structure validation"""
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    def consume_from_kafka(self, topics: List[str]):
        """Read and process streaming messages from Kafka topics using Avro deserialization"""
        # Tell Kafka which data streams (topics) we want to read from
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # Check for new streaming messages every second
                msg = self.consumer.poll(1.0)  # SIGINT can't be handled when polling, limit timeout to 1 second.
                if msg is None:
                    continue
                # Convert Avro binary data back to Python objects for processing
                key = self.avro_key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                record = self.avro_value_deserializer(msg.value(),
                                                      SerializationContext(msg.topic(), MessageField.VALUE))
                if record is not None:
                    print("{}, {}".format(key, record))
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
    # Configuration for Avro-based streaming consumer
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,  # Schema registry for data structure management
        'schema.key': RIDE_KEY_SCHEMA_PATH,  # Avro schema for message keys
        'schema.value': RIDE_VALUE_SCHEMA_PATH,  # Avro schema for message values
    }
    avro_consumer = RideAvroConsumer(props=config)
    avro_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])  # Start consuming ride data stream