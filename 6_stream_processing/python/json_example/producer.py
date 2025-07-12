import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        # Create a streaming data producer that sends messages to Kafka
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str):
        # Load data from CSV file and convert to streaming-ready format
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip column names
            for row in reader:
                records.append(Ride(arr=row))  # Convert each row to a Ride object
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        # Send each ride record as a streaming message to Kafka topic
        for ride in messages:
            try:
                # Send message with Pickup location ID as key for partitioning
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    # Settings for how our streaming producer should send data
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),  # Convert keys to bytes for network transmission
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')  # Convert Python objects to JSON bytes
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)  # Load ride data from file
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)  # Stream all rides to Kafka